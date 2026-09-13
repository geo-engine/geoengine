use std::ops::Range;

use geoengine_datatypes::primitives::{CacheHint, TimeInterval};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};

use crate::source::gdal_worker_process::GdalDatasetParameters;

/// How the z dimension of an MD array maps to the 2D raster output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub enum ZRole {
    /// Each z-slice is a time step; the array has a single 2D "band".
    Time,
    /// Each z-slice is a raster band of a single time step.
    Band,
    /// Each *selected variable* is a raster band; within a variable the z-slices are
    /// time steps and the time axis is shared by all variables.
    Variable,
}

/// One MD array file together with its slice range within the concatenated z axis.
/// `z_start`/`z_end` are *global* indices into `MdLoadingInfo::time_steps`; `band` is
/// the Geo Engine band this file's variable maps to (0 for single-variable datasets).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MdDatasetFile {
    pub params: GdalDatasetParameters,
    pub array_name: String,
    pub z_start: usize,
    pub z_end: usize,
    pub time: TimeInterval,
    #[serde(default)]
    pub band: u32,
}

/// A contiguous chunk of z-slices from a single file.
#[derive(Debug, Clone)]
pub struct MdZBatch {
    pub file_idx: usize,
    pub local_z: Range<usize>,
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
    cache_hint: CacheHint,
    z_role: ZRole,
    /// re-apply the stored 0..360° coverage onto -180..180° (one-way wrap-around)
    wrap: bool,
}

impl MdLoadingInfo {
    #[must_use]
    pub fn new(
        time_steps: Vec<TimeInterval>,
        files: Vec<MdDatasetFile>,
        cache_hint: CacheHint,
        z_role: ZRole,
        wrap: bool,
    ) -> Self {
        debug_assert!(!time_steps.is_empty(), "time_steps must not be empty");
        debug_assert!(
            time_steps.windows(2).all(|w| w[0] <= w[1]),
            "time_steps must be sorted"
        );
        let mut bands: Vec<u32> = files.iter().map(|f| f.band).collect();
        bands.sort_unstable();
        bands.dedup();
        for band in bands {
            let band_files: Vec<&MdDatasetFile> = files.iter().filter(|f| f.band == band).collect();
            debug_assert!(
                band_files.windows(2).all(|w| w[0].z_end == w[1].z_start),
                "files of a band must be contiguous along the z axis"
            );
        }

        Self {
            time_steps,
            files,
            cache_hint,
            z_role,
            wrap,
        }
    }

    #[must_use]
    pub fn time_steps(&self) -> &[TimeInterval] {
        &self.time_steps
    }

    #[must_use]
    pub fn files(&self) -> &[MdDatasetFile] {
        &self.files
    }

    #[must_use]
    pub fn files_mut(&mut self) -> &mut Vec<MdDatasetFile> {
        &mut self.files
    }

    #[must_use]
    pub fn cache_hint(&self) -> CacheHint {
        self.cache_hint
    }

    #[must_use]
    pub fn z_role(&self) -> ZRole {
        self.z_role
    }

    #[must_use]
    pub fn wrap(&self) -> bool {
        self.wrap
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
    /// With `band`, only files of that Geo Engine band are considered; `None` considers
    /// all files (single-variable datasets).
    #[must_use]
    pub fn z_batches(
        &self,
        global_z: &[usize],
        band: Option<u32>,
        batch_size: usize,
    ) -> (Vec<MdZBatch>, Vec<usize>) {
        let mut batches: Vec<MdZBatch> = Vec::new();
        let mut missing = Vec::new();

        for &gz in global_z {
            let Some(file_idx) = self
                .files
                .iter()
                .position(|f| band.is_none_or(|b| f.band == b) && f.z_start <= gz && gz < f.z_end)
            else {
                missing.push(gz);
                continue;
            };

            let local = gz - self.files[file_idx].z_start;

            match batches.last_mut() {
                Some(b)
                    if b.file_idx == file_idx
                        && b.local_z.end == local
                        && b.local_z.len() < batch_size =>
                {
                    b.local_z.end += 1;
                }
                _ => batches.push(MdZBatch {
                    file_idx,
                    local_z: local..local + 1,
                }),
            }
        }

        (batches, missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::gdal_worker_process::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
    };

    fn file(z_start: usize, z_end: usize, band: u32) -> MdDatasetFile {
        MdDatasetFile {
            params: GdalDatasetParameters {
                file_path: "/test.nc".into(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: (0., 0.).into(),
                    x_pixel_size: 1.,
                    y_pixel_size: -1.,
                },
                width: 1,
                height: 1,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: None,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: false,
                retry: None,
            },
            array_name: "a".to_owned(),
            z_start,
            z_end,
            time: TimeInterval::new_unchecked(0, 1),
            band,
        }
    }

    fn times(n: usize) -> Vec<TimeInterval> {
        (0..n)
            .map(|i| TimeInterval::new_unchecked(i as i64, i as i64 + 1))
            .collect()
    }

    #[test]
    fn z_batches_filters_by_band() {
        // band 0: files [0..4), [4..8); band 1: a single file [0..8)
        let li = MdLoadingInfo::new(
            times(8),
            vec![file(0, 4, 0), file(4, 8, 0), file(0, 8, 1)],
            CacheHint::default(),
            ZRole::Variable,
            false,
        );

        // band 1 -> one batch in the third file, local indices 2..6
        let (batches, missing) = li.z_batches(&[2, 3, 4, 5], Some(1), 8);
        assert!(missing.is_empty());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].file_idx, 2);
        assert_eq!(batches[0].local_z, 2..6);

        // band 0 -> the z range crosses its two files
        let (batches, missing) = li.z_batches(&[2, 3, 4, 5], Some(0), 8);
        assert!(missing.is_empty());
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].file_idx, 0);
        assert_eq!(batches[0].local_z, 2..4);
        assert_eq!(batches[1].file_idx, 1);
        assert_eq!(batches[1].local_z, 0..2);
    }

    #[test]
    fn z_batches_reports_missing_per_band() {
        let li = MdLoadingInfo::new(
            times(8),
            vec![file(0, 4, 0)],
            CacheHint::default(),
            ZRole::Variable,
            false,
        );

        // band 1 has no files -> all requested slices are missing
        let (batches, missing) = li.z_batches(&[0, 1, 5], Some(1), 8);
        assert!(batches.is_empty());
        assert_eq!(missing, vec![0, 1, 5]);

        // band 0 covers only [0..4)
        let (batches, missing) = li.z_batches(&[0, 1, 5], Some(0), 8);
        assert_eq!(batches.len(), 1);
        assert_eq!(missing, vec![5]);
    }
}
