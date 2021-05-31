use crate::datasets::storage::Dataset;
use crate::error;
use crate::error::Result;
use crate::users::user::UserId;
use crate::util::config::{get_config_element, DatasetService};
use crate::util::user_input::{UserInput, Validated};
use async_trait::async_trait;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_operators::engine::{
    MetaDataProvider, RasterResultDescriptor, TypedResultDescriptor, VectorResultDescriptor,
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
pub trait DatasetProvider:
    Send
    + Sync
    + MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>
    + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor>
    + MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor>
{
    // TODO: filter, paging
    async fn list(
        &self,
        user: UserId,
        options: Validated<DatasetListOptions>,
    ) -> Result<Vec<DatasetListing>>;

    // TODO: is this method useful?
    async fn load(&self, user: UserId, dataset: &DatasetId) -> Result<Dataset>;
}
