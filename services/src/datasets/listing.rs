use crate::datasets::storage::{
    AddDataSetProvider, AddMockDataSetProvider, DataSet, DataSetResultDescriptor,
};
use crate::error;
use crate::error::Result;
use crate::users::user::UserId;
use crate::util::config::{get_config_element, DataSetService};
use crate::util::user_input::{UserInput, Validated};
use async_trait::async_trait;
use geoengine_datatypes::dataset::DataSetId;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, ResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDataSetDataSourceLoadingInfo;
use geoengine_operators::source::OgrSourceDataset;
use serde::{Deserialize, Serialize};
use snafu::ensure;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct DataSetListing {
    pub id: DataSetId,
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    pub source_operator: String,
    pub result_descriptor: DataSetResultDescriptor,
    // TODO: meta data like bounds, resolution
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataSetListOptions {
    // TODO: permissions
    pub filter: Option<String>,
    pub order: OrderBy,
    pub offset: u32,
    pub limit: u32,
}

impl UserInput for DataSetListOptions {
    fn validate(&self) -> Result<()> {
        let limit = get_config_element::<DataSetService>()?.list_limit;
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

/// Listing of stored data sets
#[async_trait]
pub trait DataSetProvider:
    Send
    + Sync
    + MetaDataProvider<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>
    + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor>
{
    // TODO: filter, paging
    async fn list(
        &self,
        user: UserId,
        options: Validated<DataSetListOptions>,
    ) -> Result<Vec<DataSetListing>>;
}

pub enum TypedDataSetProvider {
    Mock(MockDataSetProvider), // wcs, ...
}

impl TypedDataSetProvider {
    pub fn new(input: AddDataSetProvider) -> Self {
        match input {
            AddDataSetProvider::AddMockDataSetProvider(add) => {
                Self::Mock(MockDataSetProvider::new(add))
            }
        }
    }

    pub fn into_box(self) -> Box<dyn DataSetProvider> {
        match self {
            TypedDataSetProvider::Mock(provider) => Box::new(provider),
        }
    }
}

pub struct MockDataSetProvider {
    pub data_sets: Vec<DataSet>,
}

impl MockDataSetProvider {
    fn new(input: AddMockDataSetProvider) -> Self {
        Self {
            data_sets: input.data_sets,
        }
    }
}

impl<L, R> MetaDataProvider<L, R> for MockDataSetProvider
where
    R: ResultDescriptor,
{
    fn meta_data(
        &self,
        _data_set: &DataSetId,
    ) -> std::result::Result<Box<dyn MetaData<L, R>>, geoengine_operators::error::Error> {
        todo!()
    }
}

#[async_trait]
impl DataSetProvider for MockDataSetProvider {
    async fn list(
        &self,
        _user: UserId,
        _options: Validated<DataSetListOptions>,
    ) -> Result<Vec<DataSetListing>> {
        todo!()
    }
}
