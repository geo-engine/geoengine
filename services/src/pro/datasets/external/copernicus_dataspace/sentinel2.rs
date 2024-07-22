use geoengine_datatypes::primitives::RasterQueryRectangle;
use geoengine_operators::{
    engine::{MetaData, RasterResultDescriptor},
    source::GdalLoadingInfo,
};

use async_trait::async_trait;

use super::ids::{Sentinel2Band, Sentinel2Product, UtmZone};

#[derive(Debug, Clone)]
pub struct Sentinel2Metadata {
    pub stac_url: String,
    pub s3_url: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub product: Sentinel2Product,
    pub zone: UtmZone,
    pub band: Sentinel2Band,
}

#[async_trait]
impl MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle> for Sentinel2Metadata {
    async fn loading_info(
        &self,
        _query: RasterQueryRectangle,
    ) -> geoengine_operators::util::Result<GdalLoadingInfo> {
        // TODO: query the STAC API to gather all files

        // TODO: map band to subdatasets in file
        todo!()
    }

    async fn result_descriptor(&self) -> geoengine_operators::util::Result<RasterResultDescriptor> {
        // TODO: hard code and return the specifics of the bands
        todo!()
    }

    fn box_clone(
        &self,
    ) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> {
        Box::new(self.clone())
    }
}
