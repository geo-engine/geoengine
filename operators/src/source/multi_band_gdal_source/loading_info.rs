use super::GdalDatasetParameters;
use crate::engine::RasterResultDescriptor;
use geoengine_datatypes::{
    primitives::{CacheHint, SpatialPartition2D, SpatialPartitioned, TimeInterval},
    raster::TileInformation,
};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, FromSql, ToSql, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GdalMultiBand {
    pub result_descriptor: RasterResultDescriptor,
}

#[derive(Debug, Clone)]
pub struct MultiBandGdalLoadingInfo {
    files: Vec<TileFile>,
    time_steps: Vec<TimeInterval>,
    cache_hint: CacheHint,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TileFile {
    pub time: TimeInterval,
    pub spatial_partition: SpatialPartition2D,
    pub band: u32,
    pub z_index: u32,
    pub params: GdalDatasetParameters,
}

impl MultiBandGdalLoadingInfo {
    pub fn new(time_steps: Vec<TimeInterval>, files: Vec<TileFile>, cache_hint: CacheHint) -> Self {
        debug_assert!(!time_steps.is_empty(), "time_steps must not be empty");

        debug_assert!(
            time_steps.windows(2).all(|w| w[0] <= w[1]),
            "time_steps must be sorted"
        );

        #[cfg(debug_assertions)]
        {
            let mut groups: std::collections::HashMap<(TimeInterval, u32), Vec<&TileFile>> =
                std::collections::HashMap::new();

            for file in &files {
                groups.entry((file.time, file.band)).or_default().push(file);
            }

            for ((time, band), group) in &groups {
                debug_assert!(
                    group.windows(2).all(|w| w[0].z_index <= w[1].z_index),
                    "Files for time {time:?} and band {band} are not sorted by z_index",
                );
            }
        }

        Self {
            files,
            time_steps,
            cache_hint,
        }
    }

    /// Return a gap-free list of time steps for the current loading info and query time.
    pub fn time_steps(&self) -> &[TimeInterval] {
        &self.time_steps
    }

    /// Return all files necessary to load a single tile, sorted by z-index. Might be empty if no files are needed.
    pub fn tile_files(
        &self,
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    ) -> Vec<GdalDatasetParameters> {
        let tile_partition = tile.spatial_partition();

        let mut files = vec![];
        for file in &self.files {
            if time.intersects(&file.time)
                && file.spatial_partition.intersects(&tile_partition)
                && file.band == band
            {
                debug_assert!(file.time == time, "file's time must match query time");

                files.push(file.params.clone());
            }
        }

        files
    }

    pub fn cache_hint(&self) -> CacheHint {
        self.cache_hint
    }
}
