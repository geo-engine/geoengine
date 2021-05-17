use std::path::Path;

use geoengine_datatypes::primitives::TimeInterval;
use geojson::FeatureCollection;
use serde::{Deserialize, Serialize};

use crate::{
    engine::{MetaData, QueryRectangle, RasterResultDescriptor},
    source::gdal_source::{GdalLoadingInfoPart, GdalLoadingInfoPartIterator},
    util::Result,
};

use super::{GdalDatasetParameters, GdalLoadingInfo};

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataSentinelTiles {
    pub stac_tile_features: FeatureCollection,
    pub params: GdalDatasetParameters,
    pub result_descriptor: RasterResultDescriptor,
}

impl MetaData<GdalLoadingInfo, RasterResultDescriptor> for GdalMetaDataSentinelTiles {
    fn loading_info(&self, _query: QueryRectangle) -> Result<GdalLoadingInfo> {
        Ok(GdalLoadingInfo {
            info: GdalLoadingInfoPartIterator::Static {
                parts: vec![GdalLoadingInfoPart {
                    time: self.time.unwrap_or_else(TimeInterval::default),
                    params: self.params.clone(),
                }]
                .into_iter(),
            },
        })
    }

    fn result_descriptor(&self) -> Result<RasterResultDescriptor> {
        Ok(self.result_descriptor.clone())
    }

    fn box_clone(&self) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>> {
        Box::new(self.clone())
    }
}
