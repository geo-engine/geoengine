use crate::api::model::datatypes::{DataId, DatasetId};
use crate::api::model::operators::TypedResultDescriptor;
use crate::contexts::Session;
use crate::datasets::storage::Dataset;
use crate::error;
use crate::error::Result;
use crate::projects::Symbology;
use crate::util::config::{get_config_element, DatasetService};
use crate::util::user_input::{UserInput, Validated};
use async_trait::async_trait;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_operators::engine::{
    MetaData, RasterResultDescriptor, ResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
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

#[derive(Debug, Serialize, Deserialize, Clone, IntoParams)]
pub struct DatasetListOptions {
    // TODO: permissions
    #[param(example = "Germany")]
    pub filter: Option<String>,
    #[param(example = "NameAsc")]
    pub order: OrderBy,
    #[param(example = 0)]
    pub offset: u32,
    #[param(example = 2)]
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSchema)]
pub enum OrderBy {
    NameAsc,
    NameDesc,
}

/// This is like the `MetaDataProvider` trait but also accepts a session
#[async_trait]
pub trait SessionMetaDataProvider<S, L, R, Q>
where
    S: Session,
    R: ResultDescriptor,
{
    async fn session_meta_data(
        &self,
        session: &S,
        id: &DataId,
    ) -> Result<Box<dyn MetaData<L, R, Q>>>;
}

/// Listing of stored datasets
#[async_trait]
pub trait DatasetProvider<S: Session>:
    Send
    + Sync
    + SessionMetaDataProvider<
        S,
        MockDatasetDataSourceLoadingInfo,
        VectorResultDescriptor,
        VectorQueryRectangle,
    > + SessionMetaDataProvider<S, OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    + SessionMetaDataProvider<S, GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, ToSchema)]
pub struct ProvenanceOutput {
    pub data: DataId,
    pub provenance: Option<Vec<Provenance>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, ToSchema)]
pub struct Provenance {
    pub citation: String,
    pub license: String,
    pub uri: String,
}
