use crate::datasets::listing::DataSetProvider;
use crate::datasets::storage::{DataSet, DataSetDB, DataSetPermission, DataSetProviderListing};
use crate::error::Result;
use crate::users::user::UserId;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataSetId, DataSetProviderId};

pub struct PostgresDataSetDB {}

#[async_trait]
impl DataSetDB for PostgresDataSetDB {
    async fn add(&mut self, _user: UserId, _data_set: DataSet) -> Result<()> {
        todo!()
    }

    async fn add_data_set_permission(
        &mut self,
        _data_set: DataSetId,
        _user: UserId,
        _permission: DataSetPermission,
    ) -> Result<()> {
        todo!()
    }

    async fn list_data_set_providers(&self, _user: UserId) -> Result<Vec<DataSetProviderListing>> {
        todo!()
    }

    async fn data_set_provider(
        &self,
        _user: UserId,
        _provider: DataSetProviderId,
    ) -> Result<&dyn DataSetProvider> {
        todo!()
    }
}
