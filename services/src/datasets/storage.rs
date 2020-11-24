use crate::datasets::listing::DataSetProvider;
use crate::error::Result;
use crate::projects::project::LayerInfo;
use crate::users::user::UserId;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataSetId, DataSetProviderId};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};

pub struct DataSet {
    pub id: DataSetId,
    pub name: String,
    pub data_type: LayerInfo, // TODO: custom type?
}

pub struct CreateDataSet {
    pub name: String,
}

pub struct DataSetProviderListing {
    pub id: DataSetProviderId,
    pub name: String,
    pub description: String,
    // more meta data (number of data sets, ...)
}

#[async_trait]
pub trait DataSetDB: Send + Sync {
    async fn add(&mut self, user: UserId, data_set: DataSet) -> Result<()>;
    // TODO: delete?
    async fn add_data_set_permission(
        &mut self,
        data_set: DataSetId,
        user: UserId,
        permission: DataSetPermission,
    ) -> Result<()>;

    // TODO: update permissions

    // TODO: update data set

    async fn list_data_set_providers(&self, user: UserId) -> Result<Vec<DataSetProviderListing>>;

    async fn data_set_provider(
        &self,
        user: UserId,
        provider: DataSetProviderId,
    ) -> Result<&dyn DataSetProvider>;
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSql, FromSql)]
pub enum DataSetPermission {
    Read,
    Write,
    Owner,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserDataSetPermission {
    pub user: UserId,
    pub data_set: DataSetId,
    pub permission: DataSetPermission,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSql, FromSql)]
pub enum DataSetProviderPermission {
    Read,
    Write,
    Owner,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserDataSetProviderPermission {
    pub user: UserId,
    pub external_provider: DataSetProviderId,
    pub permission: DataSetProviderPermission,
}
