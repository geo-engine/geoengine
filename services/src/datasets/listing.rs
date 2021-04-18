use crate::datasets::storage::{AddDatasetProvider, AddMockDatasetProvider, Dataset};
use crate::error;
use crate::error::Result;
use crate::users::user::UserId;
use crate::util::config::{get_config_element, DatasetService};
use crate::util::user_input::{UserInput, Validated};
use async_trait::async_trait;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterResultDescriptor, ResultDescriptor, TypedResultDescriptor,
    VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
use serde::{Deserialize, Serialize};
use snafu::ensure;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
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

    async fn load(&self, user: UserId, dataset: &DatasetId) -> Result<Dataset>;
}

pub enum TypedDatasetProvider {
    Mock(MockDatasetProvider), // wcs, ...
}

impl TypedDatasetProvider {
    pub fn new(input: AddDatasetProvider) -> Self {
        match input {
            AddDatasetProvider::AddMockDatasetProvider(add) => {
                Self::Mock(MockDatasetProvider::new(add))
            }
        }
    }

    pub fn into_box(self) -> Box<dyn DatasetProvider> {
        match self {
            TypedDatasetProvider::Mock(provider) => Box::new(provider),
        }
    }
}

pub struct MockDatasetProvider {
    pub datasets: Vec<Dataset>,
}

impl MockDatasetProvider {
    fn new(input: AddMockDatasetProvider) -> Self {
        Self {
            datasets: input.datasets,
        }
    }
}

impl<L, R> MetaDataProvider<L, R> for MockDatasetProvider
where
    R: ResultDescriptor,
{
    fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> std::result::Result<Box<dyn MetaData<L, R>>, geoengine_operators::error::Error> {
        todo!()
    }
}

#[async_trait]
impl DatasetProvider for MockDatasetProvider {
    async fn list(
        &self,
        _user: UserId,
        _options: Validated<DatasetListOptions>,
    ) -> Result<Vec<DatasetListing>> {
        todo!()
    }

    async fn load(&self, _user: UserId, _dataset: &DatasetId) -> Result<Dataset> {
        todo!()
    }
}
