use crate::pro::users::UserId;
use geoengine_datatypes::dataset::{DatasetProviderId, InternalDatasetId};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub enum DatasetPermission {
    Read,
    Write,
    Owner,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserDatasetPermission {
    pub user: UserId,
    pub dataset: InternalDatasetId,
    pub permission: DatasetPermission,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub enum DatasetProviderPermission {
    Read,
    Write,
    Owner,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserDatasetProviderPermission {
    pub user: UserId,
    pub external_provider: DatasetProviderId,
    pub permission: DatasetProviderPermission,
}
