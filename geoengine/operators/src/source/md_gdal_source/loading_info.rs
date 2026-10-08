use std::ops::Range;

use geoengine_datatypes::primitives::{
    CacheHint, CacheTtlSeconds, RegularTimeDimension, TimeDimension, TimeGranularity, TimeInstance,
    TimeInterval, TimeStep,
};
use geoengine_datatypes::raster::GeoTransform;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};

use super::error::MdGdalSourceError;
use crate::engine::{RasterResultDescriptor, TimeDescriptor};
use crate::source::gdal_worker_process::{GdalDatasetGeoTransform, GdalDatasetParameters};

/// The transform a tile's stored grid is *presented* as.
///
/// A stored MD array can be south-up (ascending `lat`) and/or store longitudes as 0..360,
/// while a Geo Engine raster is always north-up and presents -180..180. The read advise
/// compensates the y direction via `flip_y`; the longitude shift is this function.
///
/// It lives here rather than next to the probe because dataset registration needs it to
/// check a declared transform against the dataset's, which has nothing to do with probing.
pub fn presented_geo_transform(
    raw: GdalDatasetGeoTransform,
    height: usize,
    wrap: bool,
) -> GeoTransform {
    let abs_y = raw.y_pixel_size.abs();
    let origin_x = if wrap {
        raw.origin_coordinate.x - 180.0
    } else {
        raw.origin_coordinate.x
    };
    // the north edge is the stored origin for a descending y axis, the south edge otherwise
    let origin_y = if raw.y_pixel_size < 0.0 {
        raw.origin_coordinate.y
    } else {
        raw.origin_coordinate.y + height as f64 * abs_y
    };

    GeoTransform::new((origin_x, origin_y).into(), raw.x_pixel_size, -abs_y)
}

/// How the z (leading) dimension of an MD array maps onto the 2D raster output.
///
/// A GDAL multidim array has no raster bands at all: every array is `(z, y, x)` and `z` is
/// just a dimension. What a *Geo Engine* raster does have is bands, so this enum decides what
/// each z slice becomes. `Variable` turns z into the time axis, `Band` turns it into bands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub enum ZRole {
    /// Each z-slice is an output band of a single time step.
    ///
    /// The leading dimension has no CF time units, so there is no time axis to speak of and
    /// every slice is a band. Example: `reflectance(band, lat, lon)`.
    Band,
    /// Each selected *variable* is an output band, and within a variable the z-slices are
    /// time steps. All variables must share one time axis, which is what lets a single
    /// `time_steps` vector serve every band.
    ///
    /// For a single-variable dataset this degenerates to one band and one axis
    /// (previously `ZRole::Time`). Every selected variable reads **every** file: the probe
    /// opens each file once and probes all of them in it, so a `(file, variable)` pair
    /// becomes one row and the same file appears once per variable.
    Variable,
}

/// The dataset-level metadata of an `MdGdalSource`, mirroring `GdalMultiBand`: everything
/// per-file lives in `dataset_md_tiles`, so this only carries the result descriptor and the
/// two properties that are constant across all files.
#[derive(Serialize, Deserialize, Debug, Clone, FromSql, ToSql, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GdalMdMetaData {
    pub result_descriptor: RasterResultDescriptor,
    pub z_role: ZRole,
    pub wrap: bool,
    /// Upper bound on how many consecutive z slices one worker request may return.
    ///
    /// This lives on the dataset rather than on the workflow's operator parameters because a
    /// batch is sized against the data's slice size, which the operator cannot know.
    ///
    /// `i64` rather than `usize` because this is a `bigint` column and postgres has no
    /// `usize` codec; callers get the bounds check for free from `try_from`.
    pub max_z_batch_size: Option<i64>,
    /// Dataset-level TTL fallback used when no tile-level TTL is provided, mirroring
    /// `GdalMultiBand::cache_ttl`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_ttl: Option<CacheTtlSeconds>,
}

impl GdalMdMetaData {
    #[must_use]
    pub fn new(
        result_descriptor: RasterResultDescriptor,
        z_role: ZRole,
        wrap: bool,
        max_z_batch_size: Option<i64>,
        cache_ttl: Option<CacheTtlSeconds>,
    ) -> Self {
        Self {
            result_descriptor,
            z_role,
            wrap,
            max_z_batch_size,
            cache_ttl,
        }
    }
}

/// The time axis of one MD array file: the explicit per-slice intervals plus a
/// `TimeDescriptor` describing them.
///
/// Both are always stored. The descriptor is what the API advertises and it collapses a
/// uniform axis to `origin + step`, but the intervals are never omitted, so reading a file's
/// axis never has to expand a descriptor and there is exactly one representation to go wrong.
/// `descriptor` is derived from `steps` by [`MdFileTimes::from_intervals`] and is checked for
/// consistency on the way in.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MdFileTimes {
    /// how this axis is advertised: `Regular` with `origin + step` when the steps are
    /// uniformly spaced, `Irregular` otherwise
    pub descriptor: TimeDescriptor,
    /// one interval per z slice, in slice order
    pub steps: Vec<TimeInterval>,
}

impl MdFileTimes {
    /// Builds the compact form from explicit per-slice intervals. An axis with a constant
    /// spacing collapses to `Regular`, everything else stays `Irregular`.
    ///
    /// The round trip is verified against the regular axis itself, so a step that cannot
    /// reproduce the input intervals exactly falls back to `Irregular` instead of silently
    /// shifting slices.
    #[must_use]
    pub fn from_intervals(steps: Vec<TimeInterval>) -> Self {
        Self {
            descriptor: time_descriptor_for(&steps),
            steps,
        }
    }

    /// One interval per z slice, in slice order.
    #[must_use]
    pub fn intervals(&self) -> Vec<TimeInterval> {
        self.steps.clone()
    }

    /// The overall extent of this file's time axis.
    #[must_use]
    pub fn bounds(&self) -> TimeInterval {
        TimeInterval::new_unchecked(
            self.steps
                .first()
                .map_or(TimeInstance::MIN, TimeInterval::start),
            self.steps
                .last()
                .map_or(TimeInstance::MAX, TimeInterval::end),
        )
    }

    /// Start of this file's time axis, i.e. the first z slice's start.
    #[must_use]
    pub fn start(&self) -> TimeInstance {
        self.bounds().start()
    }

    /// The interval of this file's first z slice.
    ///
    /// `ZRole::Band` datasets have no real time axis: their intervals are synthetic
    /// `[k, k+1)` unit steps and tiles are stamped with `time_steps[global_z]`, not with
    /// this. It stays available as the file's own starting point.
    #[must_use]
    pub fn first_interval(&self) -> Option<TimeInterval> {
        self.steps.first().copied()
    }

    /// Reads a stored row back, rejecting a descriptor that disagrees with the intervals.
    ///
    /// Both columns come from outside the process, so this is the trust boundary: a stale or
    /// hand-edited row would otherwise be read with `steps` and advertised with `descriptor`.
    pub fn from_columns(
        descriptor: TimeDescriptor,
        steps: Vec<TimeInterval>,
    ) -> Result<Self, MdGdalSourceError> {
        let times = Self { descriptor, steps };
        if !times.is_consistent() {
            return Err(MdGdalSourceError::InconsistentTimeAxis);
        }
        if !times.is_ordered() {
            return Err(MdGdalSourceError::UnorderedTimeAxis);
        }
        Ok(times)
    }

    /// Whether `descriptor` actually describes `steps`.
    ///
    /// Both are stored, so a hand-written or stale row can disagree; `time_steps` is what the
    /// read path uses, `descriptor` is what the API advertises. Callers that accept a row
    /// from outside should check this rather than trust either one.
    #[must_use]
    pub fn is_consistent(&self) -> bool {
        time_descriptor_for(&self.steps) == self.descriptor
    }

    /// Whether the steps run in non-decreasing start order, i.e. slice order is time order.
    ///
    /// [`Self::bounds`] takes the extent from the first and last step, so an axis that is
    /// out of order would describe the file as covering a window it does not and would
    /// stamp slices with the wrong time. Gaps are fine, disorder is not.
    #[must_use]
    pub fn is_ordered(&self) -> bool {
        self.steps
            .windows(2)
            .all(|pair| pair[0].start() <= pair[1].start())
    }
}

/// The time descriptor for an axis of explicit per-slice intervals: `Regular` if a uniform
/// step reproduces them exactly, `Irregular` otherwise. Bounds always span the whole axis,
/// from the first slice's start to the last slice's end.
fn time_descriptor_for(intervals: &[TimeInterval]) -> TimeDescriptor {
    if let Some(descriptor) = regular_descriptor(intervals) {
        return descriptor;
    }

    let bounds = TimeInterval::new(
        intervals
            .first()
            .map_or(TimeInstance::MIN, TimeInterval::start),
        intervals
            .last()
            .map_or(TimeInstance::MAX, TimeInterval::end),
    )
    .ok();

    TimeDescriptor::new_irregular(bounds)
}

/// The regular descriptor that reproduces `intervals` exactly, if there is one.
fn regular_descriptor(intervals: &[TimeInterval]) -> Option<TimeDescriptor> {
    let (first, last) = (intervals.first()?, intervals.last()?);
    let bounds = TimeInterval::new(first.start(), last.end()).ok()?;
    let step = time_step_of_ms(first.duration_ms())?;
    let regular = RegularTimeDimension::new(first.start(), step);

    let reproduces = regular
        .contained_intervals(bounds)
        .ok()?
        .is_some_and(|it| it.eq(intervals.iter().copied()));
    if !reproduces {
        return None;
    }

    Some(TimeDescriptor::new(
        Some(bounds),
        TimeDimension::Regular(regular),
    ))
}

/// `delta_ms` as a `TimeStep`, preferring the coarsest unit that keeps the step count
/// inside `u32` (`TimeStep` counts steps in `u32`, so 1 ms units overflow after ~49 days).
fn time_step_of_ms(delta_ms: u64) -> Option<TimeStep> {
    const UNITS: &[(u64, TimeGranularity)] = &[
        (86_400_000, TimeGranularity::Days),
        (3_600_000, TimeGranularity::Hours),
        (60_000, TimeGranularity::Minutes),
        (1_000, TimeGranularity::Seconds),
        (1, TimeGranularity::Millis),
    ];

    UNITS
        .iter()
        .find(|(millis, _)| {
            delta_ms.is_multiple_of(*millis) && u32::try_from(delta_ms / millis).is_ok()
        })
        .and_then(|(millis, granularity)| {
            TimeStep::new(*granularity, (delta_ms / millis) as u32).ok()
        })
}

/// One MD array file together with its slice range within the concatenated z axis.
/// `z_start`/`z_end` are *global* indices into `MdLoadingInfo::time_steps`; `output_band` is
/// the Geo Engine output band this file's variable maps to (0 for single-variable datasets).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MdDatasetFile {
    pub params: GdalDatasetParameters,
    /// name of the array within `group` the worker reads
    pub array_name: String,
    /// "/"-separated path to the MD group below the root group; `None` = root group
    #[serde(default)]
    pub group: Option<String>,
    pub z_start: usize,
    pub z_end: usize,
    /// file-local index of the first interval that falls inside the clipped time axis;
    /// `local_z = (gz - z_start) + local_offset` maps a global z to the file's z dimension
    #[serde(default)]
    pub local_offset: usize,
    /// the file's own time axis, one interval per z slice
    pub times: MdFileTimes,
    #[serde(default)]
    pub output_band: u32,
    /// Fixed index into each dimension between z and (y, x), so one row is one slice of a
    /// 4D array - `[depth]` for `(time, depth, y, x)`.
    ///
    /// Empty for 3D, and per row rather than per dataset so that bands of one dataset can
    /// each select a different slice: a `(time, depth, y, x)` file registered with
    /// `variables_as_bands` becomes one dataset whose band `b` is depth `b`.
    #[serde(default)]
    pub leading_prefix: Vec<i64>,
}

/// A contiguous chunk of z-slices from a single file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MdZBatch {
    pub file_idx: usize,
    pub local_z: Range<usize>,
    pub global_z: Range<usize>,
}

/// The loading information of a `MdGdalSource`: a set of MD arrays that are
/// concatenated along the z axis (in time order).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MdLoadingInfo {
    /// one interval per z slice (in concatenated/global z order)
    time_steps: Vec<TimeInterval>,
    /// the files in z order; their z ranges are disjoint and cover `[0, time_steps.len())`
    files: Vec<MdDatasetFile>,
    /// Fallback TTL used when the dataset does not provide its own.
    #[serde(default)]
    cache_ttl: Option<CacheTtlSeconds>,
    z_role: ZRole,
    /// re-apply the stored 0..360° coverage onto -180..180° (one-way wrap-around)
    wrap: bool,
    /// upper bound on how many consecutive z slices one worker request may return; a
    /// dataset-level knob, since a batch is sized against the data's slice size
    max_z_batch_size: Option<usize>,
}

impl MdLoadingInfo {
    #[must_use]
    pub fn new(
        time_steps: Vec<TimeInterval>,
        files: Vec<MdDatasetFile>,
        cache_ttl: Option<CacheTtlSeconds>,
        z_role: ZRole,
        wrap: bool,
        max_z_batch_size: Option<usize>,
    ) -> Self {
        debug_assert!(!time_steps.is_empty(), "time_steps must not be empty");
        debug_assert!(
            time_steps.windows(2).all(|w| w[0] <= w[1]),
            "time_steps must be sorted"
        );
        let mut bands: Vec<u32> = files.iter().map(|f| f.output_band).collect();
        bands.sort_unstable();
        bands.dedup();
        for band in bands {
            let band_files: Vec<&MdDatasetFile> =
                files.iter().filter(|f| f.output_band == band).collect();
            // ordered and non-overlapping, but *not* contiguous: a z index that no file
            // covers is a gap-filling step that `z_batches` reports as missing, and the
            // operator turns into an empty tile
            debug_assert!(
                band_files.windows(2).all(|w| w[0].z_end <= w[1].z_start),
                "files of a band must be ordered and must not overlap along the z axis"
            );
            debug_assert!(
                band_files.iter().all(|f| {
                    let slices = f.z_end - f.z_start;
                    f.times.intervals().len() >= f.local_offset + slices
                }),
                "a file must have at least one time interval per z slice"
            );
        }

        debug_assert!(
            max_z_batch_size.is_none_or(|size| size > 0),
            "a z batch must hold at least one slice"
        );
        debug_assert!(
            files
                .iter()
                .all(|f| f.params.width > 0 && f.params.height > 0),
            "a tile must have a non-empty footprint"
        );

        Self {
            time_steps,
            files,
            cache_ttl,
            z_role,
            wrap,
            max_z_batch_size,
        }
    }

    #[must_use]
    pub fn time_steps(&self) -> &[TimeInterval] {
        &self.time_steps
    }

    /// The dataset's whole time axis as a `TimeDescriptor`: bounds span the concatenated
    /// timeline, and the dimension is `Regular` when the slices are uniformly spaced.
    ///
    /// This is what the dataset's result descriptor advertises, and it is also what
    /// decides whether a query time range can be filled from a regular grid. Note the
    /// timeline may contain gap-filling steps for files that are not present, which keeps
    /// the axis irregular — correct, since the steps really are irregular then.
    #[must_use]
    pub fn time_descriptor(&self) -> TimeDescriptor {
        time_descriptor_for(&self.time_steps)
    }

    #[must_use]
    pub fn files(&self) -> &[MdDatasetFile] {
        &self.files
    }

    #[must_use]
    pub fn z_role(&self) -> ZRole {
        self.z_role
    }

    #[must_use]
    pub fn wrap(&self) -> bool {
        self.wrap
    }

    /// The cache TTL for a tile of this dataset, falling back to the context default.
    #[must_use]
    pub fn cache_hint(&self, default_ttl: CacheTtlSeconds) -> CacheHint {
        self.cache_ttl.unwrap_or(default_ttl).into()
    }

    /// How many consecutive z slices one worker request may return.
    #[must_use]
    pub fn max_z_batch_size(&self) -> usize {
        self.max_z_batch_size
            .unwrap_or(super::DEFAULT_MAX_Z_BATCH_SIZE)
    }

    #[must_use]
    /// Global z indices of the time steps that intersect `time`.
    pub fn z_indices_in_time(&self, time: TimeInterval) -> Vec<usize> {
        self.time_steps
            .iter()
            .enumerate()
            .filter(|(_, t)| t.intersects(&time))
            .map(|(i, _)| i)
            .collect()
    }

    /// Splits the given ascending global z indices into per-file contiguous batches
    /// (each at most `batch_size` slices) and returns the z indices that no file covers.
    /// With `output_band`, only files of that Geo Engine output band are considered;
    /// `None` considers all files (single-variable datasets).
    #[must_use]
    pub fn z_batches(
        &self,
        global_z: &[usize],
        output_band: Option<u32>,
        batch_size: usize,
    ) -> (Vec<MdZBatch>, Vec<usize>) {
        let mut batches: Vec<MdZBatch> = Vec::new();
        let mut missing = Vec::new();

        // candidate files of the requested band, sorted by z_start (probe keeps them sorted);
        // binary search per z-index gives O(z log files) instead of O(z * files).
        // `z_start == z_end` marks a file with no slice in the query window; such rows carry
        // a placeholder `z_start = 0` that would break the ascending order the search needs.
        let candidates: Vec<usize> = self
            .files
            .iter()
            .enumerate()
            .filter(|(_, f)| output_band.is_none_or(|b| f.output_band == b) && f.z_start < f.z_end)
            .map(|(i, _)| i)
            .collect();

        for &gz in global_z {
            let Some(&file_idx) = (candidates.partition_point(|&i| self.files[i].z_start <= gz))
                .checked_sub(1)
                .map(|i| &candidates[..][i])
            else {
                missing.push(gz);
                continue;
            };

            if gz >= self.files[file_idx].z_end {
                missing.push(gz);
                continue;
            }

            let local = gz - self.files[file_idx].z_start + self.files[file_idx].local_offset;

            match batches.last_mut() {
                Some(b)
                    if b.file_idx == file_idx
                        && b.local_z.end == local
                        && b.local_z.len() < batch_size =>
                {
                    b.local_z.end += 1;
                    b.global_z.end += 1;
                }
                _ => {
                    let file = &self.files[file_idx];
                    let g0 = file.z_start + (local - file.local_offset);
                    batches.push(MdZBatch {
                        file_idx,
                        local_z: local..local + 1,
                        global_z: g0..g0 + 1,
                    })
                }
            }
        }

        (batches, missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::gdal_worker_process::GdalDatasetParameters;
    use crate::source::gdal_worker_process::{FileNotFoundHandling, GdalDatasetGeoTransform};

    fn interval(start: i64, end: i64) -> TimeInterval {
        TimeInterval::new_unchecked(
            TimeInstance::from_millis_unchecked(start),
            TimeInstance::from_millis_unchecked(end),
        )
    }

    #[test]
    fn regular_axis_round_trips_through_the_descriptor() {
        let intervals = vec![
            interval(0, 86_400_000),
            interval(86_400_000, 172_800_000),
            interval(172_800_000, 259_200_000),
        ];
        let times = MdFileTimes::from_intervals(intervals.clone());

        assert_eq!(
            times.descriptor.dimension,
            TimeDimension::Regular(RegularTimeDimension::new(
                TimeInstance::from_millis_unchecked(0),
                TimeStep::days(1).unwrap()
            ))
        );
        assert_eq!(times.steps, intervals);
        assert!(times.is_consistent());
        assert_eq!(times.intervals(), intervals);
        assert_eq!(times.first_interval(), Some(interval(0, 86_400_000)));
    }

    #[test]
    fn a_row_whose_descriptor_disagrees_with_its_steps_is_rejected() {
        // the two columns come from the DB, so they can be stale or hand-edited; reading such
        // a row with `steps` while advertising `descriptor` would shift every slice
        let regular = MdFileTimes::from_intervals(vec![
            interval(0, 86_400_000),
            interval(86_400_000, 172_800_000),
        ]);

        assert!(MdFileTimes::from_columns(regular.descriptor, regular.steps.clone()).is_ok());

        let wrong =
            TimeDescriptor::new(Some(interval(0, 2 * 86_400_000)), TimeDimension::Irregular);
        assert!(matches!(
            MdFileTimes::from_columns(wrong, regular.steps),
            Err(MdGdalSourceError::InconsistentTimeAxis)
        ));
    }

    #[test]
    fn irregular_axis_keeps_its_explicit_intervals() {
        let intervals = vec![interval(0, 7), interval(9, 20), interval(100, 130)];
        let times = MdFileTimes::from_intervals(intervals.clone());

        assert_eq!(times.descriptor.dimension, TimeDimension::Irregular);
        assert_eq!(times.intervals(), intervals);
        assert_eq!(times.bounds(), interval(0, 130));
    }

    #[test]
    fn unequal_spacing_stays_irregular() {
        // the first gap is 10 ms, so a regular axis built from it would tile the whole
        // [0, 100) bounds with 10 slices instead of the 3 given here
        let intervals = vec![interval(0, 10), interval(10, 20), interval(20, 100)];
        let times = MdFileTimes::from_intervals(intervals.clone());

        assert_eq!(times.descriptor.dimension, TimeDimension::Irregular);
        assert_eq!(times.intervals(), intervals);
    }

    fn file(band: u32, z_start: usize, z_end: usize, slices: usize) -> MdDatasetFile {
        let intervals = (0..slices)
            .map(|i| interval((z_start + i) as i64, (z_start + i) as i64 + 1))
            .collect::<Vec<_>>();
        MdDatasetFile {
            params: GdalDatasetParameters {
                file_path: "/test.nc".into(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: (0., 0.).into(),
                    x_pixel_size: 1.0,
                    y_pixel_size: -1.0,
                },
                width: 8,
                height: 8,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: None,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: false,
                retry: None,
            },
            array_name: "temperature".to_owned(),
            group: None,
            z_start,
            z_end,
            local_offset: 0,
            times: MdFileTimes::from_intervals(intervals),
            output_band: band,
            leading_prefix: Vec::new(),
        }
    }

    #[test]
    fn z_batches_filters_by_band() {
        let files = vec![file(0, 0, 4, 4), file(0, 4, 8, 4), file(1, 0, 8, 8)];
        let loading_info = MdLoadingInfo::new(
            (0..8).map(|i| interval(i, i + 1)).collect(),
            files,
            None,
            ZRole::Variable,
            false,
            None,
        );
        let all_z = (0..8).collect::<Vec<_>>();

        let (batches, missing) = loading_info.z_batches(&all_z, Some(0), 16);
        assert!(missing.is_empty());
        assert_eq!(
            batches,
            vec![
                MdZBatch {
                    file_idx: 0,
                    local_z: 0..4,
                    global_z: 0..4,
                },
                MdZBatch {
                    file_idx: 1,
                    local_z: 0..4,
                    global_z: 4..8,
                },
            ]
        );

        let (batches, missing) = loading_info.z_batches(&all_z, Some(1), 16);
        assert!(missing.is_empty());
        assert_eq!(
            batches,
            vec![MdZBatch {
                file_idx: 2,
                local_z: 0..8,
                global_z: 0..8,
            }]
        );
    }

    #[test]
    fn z_batches_reports_missing_per_band() {
        // band 0 only covers z 0..4, so z 4..8 is a gap
        let files = vec![file(0, 0, 4, 4), file(1, 0, 8, 8)];
        let loading_info = MdLoadingInfo::new(
            (0..8).map(|i| interval(i, i + 1)).collect(),
            files,
            None,
            ZRole::Variable,
            false,
            None,
        );

        let (batches, missing) = loading_info.z_batches(&(0..8).collect::<Vec<_>>(), Some(0), 2);

        assert_eq!(
            batches,
            vec![
                MdZBatch {
                    file_idx: 0,
                    local_z: 0..2,
                    global_z: 0..2,
                },
                MdZBatch {
                    file_idx: 0,
                    local_z: 2..4,
                    global_z: 2..4,
                },
            ]
        );
        assert_eq!(missing, vec![4, 5, 6, 7]);
    }
}
