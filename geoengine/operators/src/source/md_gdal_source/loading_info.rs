use geoengine_datatypes::primitives::{
    CacheHint, CacheTtlSeconds, RegularTimeDimension, TimeDimension, TimeGranularity, TimeInstance,
    TimeInterval, TimeStep,
};
use serde::{Deserialize, Serialize};

use super::error::MdGdalSourceError;
use super::reader::MdTileRequest;
use crate::engine::{RasterResultDescriptor, TimeDescriptor};
use crate::source::gdal_worker_process::{GdalDatasetParameters, MdAxisMapping};

/// The dataset-level metadata of an `MdGdalSource`, mirroring `GdalMultiBand`: everything
/// per-file lives in `dataset_md_tiles`, so this only carries the result descriptor and the
/// properties that are constant across all files.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GdalMdMetaData {
    pub result_descriptor: RasterResultDescriptor,
    /// The variables of the dataset; the index of a variable is its Geo Engine output band.
    pub variables: Vec<MdVariable>,
    pub wrap: bool,
    /// Dataset-level TTL fallback used when no tile-level TTL is provided, mirroring
    /// `GdalMultiBand::cache_ttl`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_ttl: Option<CacheTtlSeconds>,
}

impl GdalMdMetaData {
    #[must_use]
    pub fn new(
        result_descriptor: RasterResultDescriptor,
        variables: Vec<MdVariable>,
        wrap: bool,
        cache_ttl: Option<CacheTtlSeconds>,
    ) -> Self {
        Self {
            result_descriptor,
            variables,
            wrap,
            cache_ttl,
        }
    }
}

/// One variable of an MD dataset: a named array and the role of each of its dimensions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MdVariable {
    /// unique within the dataset; files refer to a variable by this name
    pub name: String,
    /// one entry per source dimension, in array order
    pub axis_mapping: Vec<MdAxisMapping>,
}

impl MdVariable {
    /// The time axis of this variable: its `Time` dimension, `None` when it has none - a fixed
    /// index on the band or level dimension and no time of its own.
    #[must_use]
    pub fn z_axis(&self) -> Option<MdAxisMapping> {
        self.axis_mapping.iter().copied().find(|axis| axis.is_z())
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
    pub fn intervals(&self) -> &[TimeInterval] {
        &self.steps
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
    /// index into `GdalMdMetaData::variables` of the variable this file holds
    #[serde(default)]
    pub output_band: u32,
}

/// The loading information of a `MdGdalSource`: a set of MD arrays that are
/// concatenated along the z axis (in time order).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MdLoadingInfo {
    /// one interval per z slice (in concatenated/global z order)
    time_steps: Vec<TimeInterval>,
    /// the files in ascending `z_start` order; files of one variable may share z slices when
    /// they are spatial pieces of the same time step
    files: Vec<MdDatasetFile>,
    /// the dataset's variables, indexed by the `output_band` of their files
    variables: Vec<MdVariable>,
    /// Fallback TTL used when the dataset does not provide its own.
    #[serde(default)]
    cache_ttl: Option<CacheTtlSeconds>,
    /// show the stored 0..360° coverage as −180..180° (see [`MdPresentation`])
    ///
    /// [`MdPresentation`]: crate::source::MdPresentation
    wrap: bool,
}

impl MdLoadingInfo {
    pub fn new(
        time_steps: Vec<TimeInterval>,
        files: Vec<MdDatasetFile>,
        variables: Vec<MdVariable>,
        cache_ttl: Option<CacheTtlSeconds>,
        wrap: bool,
    ) -> Result<Self, MdGdalSourceError> {
        for (i, variable) in variables.iter().enumerate() {
            if variables[..i]
                .iter()
                .any(|other| other.name == variable.name)
            {
                return Err(MdGdalSourceError::DuplicateVariableName {
                    name: variable.name.clone(),
                });
            }
        }
        if let Some(file) = files
            .iter()
            .find(|f| f.output_band as usize >= variables.len())
        {
            return Err(MdGdalSourceError::UnknownVariable {
                index: file.output_band,
                count: variables.len(),
            });
        }
        // shortcut: every variable's time axis is the one shared `time_steps`, so a variable
        // with a `Time` dimension cannot be mixed with one that has none, upgrade when each
        // variable can carry its own axis
        let time_axes: std::collections::HashSet<Option<MdAxisMapping>> =
            variables.iter().map(MdVariable::z_axis).collect();
        if time_axes.len() > 1 {
            return Err(MdGdalSourceError::MixedZAxisKinds);
        }

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
            // ordered by z_start, possibly overlapping (spatial mosaic), and *not* contiguous:
            // a z index that no file covers is a gap-filling step that `z_slices` reports as
            // missing, and the operator turns into an empty tile
            debug_assert!(
                band_files.windows(2).all(|w| w[0].z_start <= w[1].z_start),
                "files of a band must be ordered by z_start"
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
            files
                .iter()
                .all(|f| f.params.width > 0 && f.params.height > 0),
            "a tile must have a non-empty footprint"
        );

        Ok(Self {
            time_steps,
            files,
            variables,
            cache_ttl,
            wrap,
        })
    }

    #[must_use]
    pub fn time_steps(&self) -> &[TimeInterval] {
        &self.time_steps
    }

    #[must_use]
    pub fn variables(&self) -> &[MdVariable] {
        &self.variables
    }

    /// The variable a file holds; `new` checked that every file's index is in range.
    #[must_use]
    pub fn variable_of(&self, file: &MdDatasetFile) -> &MdVariable {
        &self.variables[file.output_band as usize]
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
    pub fn wrap(&self) -> bool {
        self.wrap
    }

    /// The cache TTL for a tile of this dataset, falling back to the context default.
    #[must_use]
    pub fn cache_hint(&self, default_ttl: CacheTtlSeconds) -> CacheHint {
        self.cache_ttl.unwrap_or(default_ttl).into()
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

    /// Maps each ascending global z index to every file of `output_band` that holds it, with
    /// the file-local z index in each, and returns the z indices that no file covers.
    /// Several files hold one z index when they are spatial pieces of the same time step;
    /// the reader blits them into one tile.
    #[must_use]
    pub fn z_slices(
        &self,
        global_z: &[usize],
        output_band: u32,
    ) -> (Vec<(usize, Vec<MdTileRequest>)>, Vec<usize>) {
        let mut slices = Vec::new();
        let mut missing = Vec::new();

        // candidate files of the requested band, sorted by z_start.
        // `z_start == z_end` marks a file with no slice in the query window; such rows carry
        // a placeholder `z_start = 0` that would break the ascending order the search needs.
        let candidates: Vec<usize> = self
            .files
            .iter()
            .enumerate()
            .filter(|(_, f)| f.output_band == output_band && f.z_start < f.z_end)
            .map(|(i, _)| i)
            .collect();
        // a file can only hold `gz` if it starts less than its own length before `gz`
        let longest = candidates
            .iter()
            .map(|&i| self.files[i].z_end - self.files[i].z_start)
            .max()
            .unwrap_or(0);

        for &gz in global_z {
            let first = candidates.partition_point(|&i| self.files[i].z_start + longest <= gz);
            let last = candidates.partition_point(|&i| self.files[i].z_start <= gz);
            let covering: Vec<MdTileRequest> = candidates[first..last]
                .iter()
                .filter(|&&i| gz < self.files[i].z_end)
                .map(|&file_idx| {
                    let file = &self.files[file_idx];
                    MdTileRequest {
                        file_idx,
                        local_z: gz - file.z_start + file.local_offset,
                    }
                })
                .collect();

            if covering.is_empty() {
                missing.push(gz);
            } else {
                slices.push((gz, covering));
            }
        }

        (slices, missing)
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
        assert_eq!(times.steps.first().copied(), Some(interval(0, 86_400_000)));
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
    fn unequal_spacing_stays_irregular() {
        // the first gap is 10 ms, so a regular axis built from it would tile the whole
        // [0, 100) bounds with 10 slices instead of the 3 given here
        let intervals = vec![interval(0, 10), interval(10, 20), interval(20, 100)];
        let times = MdFileTimes::from_intervals(intervals.clone());

        assert_eq!(times.descriptor.dimension, TimeDimension::Irregular);
        assert_eq!(times.intervals(), intervals);

        // gaps of any size keep their explicit intervals and span the bounds
        let gapped = vec![interval(0, 7), interval(9, 20), interval(100, 130)];
        let times = MdFileTimes::from_intervals(gapped.clone());

        assert_eq!(times.descriptor.dimension, TimeDimension::Irregular);
        assert_eq!(times.intervals(), gapped);
        assert_eq!(times.bounds(), interval(0, 130));
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
        }
    }

    fn time_variable(name: &str) -> MdVariable {
        MdVariable {
            name: name.to_owned(),
            axis_mapping: vec![MdAxisMapping::Time, MdAxisMapping::Y, MdAxisMapping::X],
        }
    }

    fn loading_info(files: Vec<MdDatasetFile>, variables: Vec<MdVariable>) -> MdLoadingInfo {
        MdLoadingInfo::new(
            (0..8).map(|i| interval(i, i + 1)).collect(),
            files,
            variables,
            None,
            false,
        )
        .unwrap()
    }

    #[test]
    fn z_slices_filters_by_band() {
        let files = vec![file(0, 0, 4, 4), file(0, 4, 8, 4), file(1, 0, 8, 8)];
        let loading_info = loading_info(
            files,
            vec![time_variable("temperature"), time_variable("precipitation")],
        );
        let all_z = (0..8).collect::<Vec<_>>();

        let (slices, missing) = loading_info.z_slices(&all_z, 0);
        assert!(missing.is_empty());
        assert_eq!(slices.len(), 8);
        assert_eq!(
            slices[0],
            (
                0,
                vec![MdTileRequest {
                    file_idx: 0,
                    local_z: 0
                }]
            )
        );
        assert_eq!(
            slices[4],
            (
                4,
                vec![MdTileRequest {
                    file_idx: 1,
                    local_z: 0
                }]
            )
        );

        let (slices, missing) = loading_info.z_slices(&all_z, 1);
        assert!(missing.is_empty());
        assert_eq!(
            slices[7],
            (
                7,
                vec![MdTileRequest {
                    file_idx: 2,
                    local_z: 7
                }]
            )
        );
    }

    #[test]
    fn z_slices_returns_every_file_holding_a_slice() {
        // two spatial pieces of the same time steps (a mosaic), then a file for the next ones
        let files = vec![file(0, 0, 4, 4), file(0, 0, 4, 4), file(0, 4, 8, 4)];
        let loading_info = loading_info(files, vec![time_variable("temperature")]);

        let (slices, missing) = loading_info.z_slices(&(0..8).collect::<Vec<_>>(), 0);

        assert!(missing.is_empty());
        assert_eq!(
            slices[1],
            (
                1,
                vec![
                    MdTileRequest {
                        file_idx: 0,
                        local_z: 1
                    },
                    MdTileRequest {
                        file_idx: 1,
                        local_z: 1
                    },
                ]
            )
        );
        assert_eq!(slices[5].1.len(), 1);
    }

    #[test]
    fn z_slices_reports_missing_per_band() {
        // band 0 only covers z 0..4, so z 4..8 is a gap
        let files = vec![file(0, 0, 4, 4), file(1, 0, 8, 8)];
        let loading_info = loading_info(
            files,
            vec![time_variable("temperature"), time_variable("precipitation")],
        );

        let (slices, missing) = loading_info.z_slices(&(0..8).collect::<Vec<_>>(), 0);

        assert_eq!(slices.len(), 4);
        assert_eq!(missing, vec![4, 5, 6, 7]);
    }

    /// A variable on a fixed (non-time) z dimension, e.g. one index of a band axis.
    fn fixed_variable(name: &str) -> MdVariable {
        MdVariable {
            name: name.to_owned(),
            axis_mapping: vec![
                MdAxisMapping::Fixed { index: 0 },
                MdAxisMapping::Y,
                MdAxisMapping::X,
            ],
        }
    }

    #[test]
    fn variable_names_must_be_unique() {
        let result = MdLoadingInfo::new(
            vec![interval(0, 1)],
            vec![file(0, 0, 1, 1)],
            vec![time_variable("temperature"), time_variable("temperature")],
            None,
            false,
        );

        assert!(matches!(
            result,
            Err(MdGdalSourceError::DuplicateVariableName { name }) if name == "temperature"
        ));
    }

    #[test]
    fn files_must_refer_to_an_existing_variable() {
        let result = MdLoadingInfo::new(
            vec![interval(0, 1)],
            vec![file(1, 0, 1, 1)],
            vec![time_variable("temperature")],
            None,
            false,
        );

        assert!(matches!(
            result,
            Err(MdGdalSourceError::UnknownVariable { index: 1, count: 1 })
        ));
    }

    #[test]
    fn time_and_fixed_variables_cannot_mix() {
        let result = MdLoadingInfo::new(
            vec![interval(0, 1)],
            vec![file(0, 0, 1, 1), file(1, 0, 1, 1)],
            vec![time_variable("temperature"), fixed_variable("reflectance")],
            None,
            false,
        );

        assert!(matches!(result, Err(MdGdalSourceError::MixedZAxisKinds)));
    }

    #[test]
    fn variables_may_all_have_no_time_axis() {
        let loading_info = loading_info(
            vec![file(0, 0, 1, 1), file(1, 0, 1, 1)],
            vec![fixed_variable("reflectance"), fixed_variable("brightness")],
        );

        assert_eq!(
            loading_info.variable_of(&loading_info.files()[1]).name,
            "brightness"
        );
    }
}
