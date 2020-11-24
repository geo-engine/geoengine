use crate::error::Result;
use crate::users::user::UserId;
use async_trait::async_trait;
use geoengine_datatypes::dataset::DataSetId;
use geoengine_operators::engine::LoadingInfo;

pub struct DataSetListing {
    pub id: DataSetId,
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    // TODO: meta data like bounds, crs, resolution
}

#[async_trait]
pub trait DataSetProvider: Send + Sync {
    // TODO: filter, paging
    async fn list(&self, user: UserId) -> Result<Vec<DataSetListing>>;

    async fn loading_info(&self, user: UserId, data_set: DataSetId) -> Result<LoadingInfo>;
}

#[derive(Clone)]
pub struct MockDataSetProvider {}

#[async_trait]
impl DataSetProvider for MockDataSetProvider {
    async fn list(&self, _user: UserId) -> Result<Vec<DataSetListing>> {
        todo!()
    }

    async fn loading_info(&self, _user: UserId, _data_set: DataSetId) -> Result<LoadingInfo> {
        todo!()
    }
}
