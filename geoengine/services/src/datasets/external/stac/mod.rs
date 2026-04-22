use crate::contexts::GeoEngineDb;
use crate::datasets::listing::ProvenanceOutput;
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{Layer, LayerCollection, LayerCollectionListOptions};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider, ProviderCapabilities};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataId, DataProviderId, LayerId};
use geoengine_datatypes::primitives::{
    RasterQueryRectangle, SpatialResolution, TimeDimension, VectorQueryRectangle,
};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacDataProviderDefinition")]
#[serde(rename_all = "camelCase")]
pub struct StacDataProviderDefinition {
    pub name: String,
    pub id: DataProviderId,
    pub description: String,
    pub priority: Option<i16>,
    pub api_url: String,
    pub collection_name: String,
    pub s3_credentials: Option<StacProviderS3Credentials>,
    pub time_dimension: TimeDimension, // TODO: should this tbe on dataset level?
    pub datasets: Vec<StacProviderDataset>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacProviderS3Credentials")]
pub struct StacProviderS3Credentials {
    pub access_key: String,
    pub secret_key: String,
}

/// A geo engine dataset derived from a STAC collectin.
/// As all bands and tiles of a geo engine data set must have the same data type, resolution and projection,
/// a stac collection will be split into multiple geo engine datasets if it contains bands with different data types, resolutions or projections.
/// In order to make them browsable they are defined as part of the stac provider definition.
///
/// TODO: different approach would be to just provide data type, resolution and projection + bands and compute all combinations as possible datasets,
/// but not all combinations actually exist and would lead to empty collection.
///
/// TODO: could also be gathered from collection api and probeb from items
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacProviderDataset")]
pub struct StacProviderDataset {
    pub name: String, // TODO: derive from collection name + data type + resolution + projection?
    pub description: String,
    pub data_type: RasterDataType,
    pub resolution: SpatialResolution,
    pub projection: SpatialReference,
    pub bands: Vec<StacProviderDatasetBand>, // bands in order!
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacProviderDatasetBand")]
pub struct StacProviderDatasetBand {
    pub name: String, // the identifier in the STAC response
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for StacDataProviderDefinition {
    async fn initialize(self: Box<Self>, _db: D) -> crate::error::Result<Box<dyn DataProvider>> {
        Ok(Box::new(StacDataProvider::new(
            self.id,
            self.name,
            self.description,
        )))
    }

    fn type_name(&self) -> &'static str {
        "Stac"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        self.id
    }

    fn priority(&self) -> i16 {
        self.priority.unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub struct StacDataProvider {
    id: DataProviderId,
    name: String,
    description: String,
}

impl StacDataProvider {
    pub fn new(id: DataProviderId, name: String, description: String) -> Self {
        Self {
            id,
            name,
            description,
        }
    }
}

#[async_trait]
impl DataProvider for StacDataProvider {
    async fn provenance(&self, _id: &DataId) -> crate::error::Result<ProvenanceOutput> {
        todo!("stac provenance blueprint")
    }
}

#[async_trait]
impl LayerCollectionProvider for StacDataProvider {
    fn capabilities(&self) -> ProviderCapabilities {
        todo!("stac capabilities blueprint")
    }

    fn name(&self) -> &str {
        let _ = self.id;
        let _ = &self.name;
        todo!("stac provider name blueprint")
    }

    fn description(&self) -> &str {
        let _ = &self.description;
        todo!("stac provider description blueprint")
    }

    async fn load_layer_collection(
        &self,
        _collection: &LayerCollectionId,
        _options: LayerCollectionListOptions,
    ) -> crate::error::Result<LayerCollection> {
        todo!("stac layer collection loading blueprint")
    }

    async fn get_root_layer_collection_id(&self) -> crate::error::Result<LayerCollectionId> {
        todo!("stac root layer collection blueprint")
    }

    async fn load_layer(&self, _id: &LayerId) -> crate::error::Result<Layer> {
        todo!("stac layer loading blueprint")
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for StacDataProvider
{
    async fn meta_data(
        &self,
        _id: &DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        todo!("stac raster metadata blueprint")
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for StacDataProvider
{
    async fn meta_data(
        &self,
        _id: &DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        todo!("stac vector metadata blueprint")
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for StacDataProvider
{
    async fn meta_data(
        &self,
        _id: &DataId,
    ) -> Result<
        Box<
            dyn MetaData<
                    MockDatasetDataSourceLoadingInfo,
                    VectorResultDescriptor,
                    VectorQueryRectangle,
                >,
        >,
        geoengine_operators::error::Error,
    > {
        todo!("stac mock vector metadata blueprint")
    }
}
