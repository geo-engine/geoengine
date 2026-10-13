use super::GdalDatasetParameters;
use crate::engine::RasterResultDescriptor;
use geoengine_datatypes::{
    primitives::{
        CacheHint, CacheTtlSeconds, SpatialPartition2D, SpatialPartitioned, TimeInterval,
    },
    raster::TileInformation,
};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, FromSql, ToSql, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GdalMultiBand {
    pub result_descriptor: RasterResultDescriptor,
    /// Dataset-level TTL fallback used when no tile-level TTL is provided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_ttl: Option<CacheTtlSeconds>,
}

#[derive(Debug, Clone)]
pub struct MultiBandGdalLoadingInfo {
    files: Vec<TileFile>,
    time_steps: Vec<TimeInterval>,
    /// Fallback TTL used when a tile does not provide its own TTL.
    cache_ttl: Option<CacheTtlSeconds>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TileFile {
    pub time: TimeInterval,
    pub spatial_partition: SpatialPartition2D,
    pub band: u32,
    pub z_index: i64,
    pub params: GdalDatasetParameters,
}

impl MultiBandGdalLoadingInfo {
    pub fn new(
        time_steps: Vec<TimeInterval>,
        files: Vec<TileFile>,
        cache_ttl: Option<CacheTtlSeconds>,
    ) -> Self {
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
            cache_ttl,
        }
    }

    /// Return a gap-free list of time steps for the current loading info and query time.
    pub fn time_steps(&self) -> &[TimeInterval] {
        &self.time_steps
    }

    /// Return all files necessary to load a single tile, sorted by z-index.
    /// Might be empty if no files are needed.
    /// Spatial coverage alone does not establish occlusion: higher-z files may
    /// contain no-data. The reader checks actual pixel validity before skipping files.
    pub fn tile_files(
        &self,
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    ) -> Vec<GdalDatasetParameters> {
        let tile_partition = tile.spatial_partition();

        self.files
            .iter()
            .filter(|file| {
                time.intersects(&file.time)
                    && file.spatial_partition.intersects(&tile_partition)
                    && file.band == band
            })
            .map(|file| file.params.clone())
            .collect()
    }

    pub fn cache_hint(&self, default_ttl: CacheTtlSeconds) -> CacheHint {
        self.cache_ttl.unwrap_or(default_ttl).into()
    }
}
