use super::{GdalDatasetParameters, GdalSourceTimePlaceholder};
use crate::{
    engine::{MetaData, RasterResultDescriptor},
    error::Error,
    util::Result,
};
use async_trait::async_trait;
use geoengine_datatypes::{
    primitives::{
        CacheHint, CacheTtlSeconds, QueryRectangle, RasterQueryRectangle, TimeInstance,
        TimeInterval, TimeStep, TimeStepIter,
    },
    raster::TileInformation,
};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// maybe two implementations:
// - internal datasets: e.g. encapsulate a connection to postgres
// - external providers: e.g. just store all files/tiles in a map and filter during TilesAccess::tiles
#[derive(Debug, Clone)]
pub enum MultiBandGdalLoadingInfo {
    // TODO: just a connection to the postgres db?
}

impl MultiBandGdalLoadingInfo {
    pub fn time_steps(&self) -> Vec<TimeInterval> {
        todo!()
    }

    // TODO: query should be a a single tile! only one output grid cell and only one band
    // TODO: return Option instead of empty Vec?
    /// Return all files necessary to load a single tile, sorted by z-index. Might be empty if no files are needed.
    pub async fn tile_files(
        &self,
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    ) -> Result<Vec<GdalDatasetParameters>> {
        todo!()
    }

    pub fn num_retries(&self) -> u32 {
        todo!()
    }

    pub fn cache_hint(&self) -> CacheHint {
        todo!()
    }

    pub fn uses_vsi_curl(&self) -> bool {
        todo!()
    }
}

#[cfg(test)]
mod tests {}
