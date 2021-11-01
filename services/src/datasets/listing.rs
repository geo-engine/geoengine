use crate::contexts::Session;
use crate::datasets::storage::Dataset;
use crate::error;
use crate::error::Result;
use crate::projects::Symbology;
use crate::util::config::{get_config_element, DatasetService};
use crate::util::user_input::{UserInput, Validated};
use async_trait::async_trait;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_operators::engine::{
    MetaDataProvider, RasterQueryRectangle, RasterResultDescriptor, TypedResultDescriptor,
    VectorQueryRectangle, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
use serde::{Deserialize, Serialize};
use snafu::ensure;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DatasetListing {
    pub id: DatasetId,
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    pub source_operator: String,
    pub result_descriptor: TypedResultDescriptor,
    pub symbology: Option<Symbology>,
    // TODO: meta data like bounds, resolution
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatasetListOptions {
    // TODO: permissions
    pub filter: Option<String>,
    pub order: OrderBy,
    pub offset: u32,
    pub limit: u32,
}

impl UserInput for DatasetListOptions {
    fn validate(&self) -> Result<()> {
        let limit = get_config_element::<DatasetService>()?.list_limit;
        ensure!(
            self.limit <= limit,
            error::InvalidListLimit {
                limit: limit as usize
            }
        );

        if let Some(filter) = &self.filter {
            ensure!(
                filter.len() >= 3 && filter.len() <= 256,
                error::InvalidStringLength {
                    parameter: "filter".to_string(),
                    min: 3_usize,
                    max: 256_usize
                }
            );
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub enum OrderBy {
    NameAsc,
    NameDesc,
}

/// Listing of stored datasets
#[async_trait]
pub trait DatasetProvider<S: Session>: Send
    + Sync
    + MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    + MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
{
    // TODO: filter, paging
    async fn list(
        &self,
        session: &S,
        options: Validated<DatasetListOptions>,
    ) -> Result<Vec<DatasetListing>>;

    async fn load(&self, session: &S, dataset: &DatasetId) -> Result<Dataset>;

    async fn provenance(&self, session: &S, dataset: &DatasetId) -> Result<ProvenanceOutput>;
}

/// A provider of datasets that are not hosted by Geo Engine itself but some external party
// TODO: Authorization: the provider needs to accept credentials for the external data source.
//       The credentials should be generic s.t. they are independent of the Session type and
//       extensible to new provider types. E.g. a key-value map of strings where the provider
//       checks that the necessary information is present and how they are incorporated in
//       the requests.
#[async_trait]
pub trait ExternalDatasetProvider: Send
    + Sync
    + MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    + MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
{
    // TODO: authorization, filter, paging
    async fn list(&self, options: Validated<DatasetListOptions>) -> Result<Vec<DatasetListing>>;

    // TODO: authorization
    // TODOis this method useful?
    async fn load(&self, dataset: &DatasetId) -> Result<Dataset>;

    async fn provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput>;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ProvenanceOutput {
    pub dataset: DatasetId,
    pub provenance: Option<Provenance>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct Provenance {
    pub citation: String,
    pub license: String,
    pub uri: String,
}
