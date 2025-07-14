use super::{GdalDatasetParameters, GdalSourceTimePlaceholder};
use crate::{
    engine::{MetaData, RasterResultDescriptor},
    error::Error,
    util::Result,
};
use async_trait::async_trait;
use geoengine_datatypes::{
    primitives::{
        CacheHint, CacheTtlSeconds, QueryRectangle, RasterQueryRectangle, SpatialPartition2D,
        SpatialPartitioned, TimeInstance, TimeInterval, TimeStep, TimeStepIter,
    },
    raster::TileInformation,
};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};

#[derive(Debug, Clone)]
pub struct MultiBandGdalLoadingInfo {
    // TODO: store as hashmap or tree, or add an index
    files: Vec<TileFile>,
    time_steps: Vec<TimeInterval>,
    cache_hint: CacheHint,
}

#[derive(Debug, Clone)]
pub struct TileFile {
    pub time: TimeInterval,
    pub spatial_partition: SpatialPartition2D,
    pub band: u32,
    pub z_index: u32,
    pub params: GdalDatasetParameters,
}

impl MultiBandGdalLoadingInfo {
    pub fn new(time_steps: Vec<TimeInterval>, files: Vec<TileFile>, cache_hint: CacheHint) -> Self {
        // TODO: ensure order of files, sorted by time, band and z_index

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
    pub async fn tile_files(
        &self,
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    ) -> Result<Vec<GdalDatasetParameters>> {
        let tile_partition = tile.spatial_partition();

        let mut files = vec![];
        for file in &self.files {
            if time.intersects(&file.time)
                && file.spatial_partition.intersects(&tile_partition)
                && file.band == band
            {
                files.push(file.params.clone());
            }
        }

        Ok(files)
    }

    pub fn cache_hint(&self) -> CacheHint {
        self.cache_hint
    }
}

#[cfg(test)]
mod tests {}
