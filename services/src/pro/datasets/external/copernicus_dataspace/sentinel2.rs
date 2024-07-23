use crate::pro::datasets::external::copernicus_dataspace::stac::{
    load_stac_items, resolve_datetime_duplicates,
};
use geoengine_datatypes::primitives::RasterQueryRectangle;
use geoengine_operators::{
    engine::{MetaData, RasterResultDescriptor},
    source::GdalLoadingInfo,
};

use async_trait::async_trait;
use snafu::{ResultExt, Snafu};
use url::Url;

use super::{
    ids::{Sentinel2Band, Sentinel2Product, UtmZone},
    stac::CopernicusStacError,
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum CopernicusSentinel2Error {
    CannotRetrieveStacItems { source: CopernicusStacError },
    CannotParseStacUrl { source: url::ParseError },
    CannotResolveDateTimeDuplicates { source: CopernicusStacError },
}

#[derive(Debug, Clone)]
pub struct Sentinel2Metadata {
    pub stac_url: String, // TODO: use url crate
    pub s3_url: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub product: Sentinel2Product,
    pub zone: UtmZone,
    pub band: Sentinel2Band,
}

impl Sentinel2Metadata {
    async fn crate_loading_info(
        &self,
        query: RasterQueryRectangle,
    ) -> Result<GdalLoadingInfo, CopernicusSentinel2Error> {
        let mut stac_items = load_stac_items(
            Url::parse(&self.stac_url).context(CannotParseStacUrl)?,
            "SENTINEL-2",
            query,
            self.product.product_type(),
        )
        .await
        .context(CannotRetrieveStacItems)?;

        resolve_datetime_duplicates(&mut stac_items).context(CannotResolveDateTimeDuplicates)?;

        // TODO: map band to subdatasets in file
        todo!()
    }
}

#[async_trait]
impl MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle> for Sentinel2Metadata {
    async fn loading_info(
        &self,
        query: RasterQueryRectangle,
    ) -> geoengine_operators::util::Result<GdalLoadingInfo> {
        self.crate_loading_info(query).await.map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })
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
