use crate::pro::users::UserId;
use geoengine_datatypes::{
    dataset::{DatasetId, DatasetProviderId},
    identifier,
};
use serde::{Deserialize, Serialize};

identifier!(RoleId);

impl From<UserId> for RoleId {
    fn from(user_id: UserId) -> Self {
        RoleId(user_id.0)
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct Role {
    pub id: RoleId,
    pub name: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub enum Permission {
    Read,
    Write,
    Owner,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct DatasetPermission {
    pub role: RoleId,
    pub dataset: DatasetId,
    pub permission: Permission,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct DatasetProviderPermission {
    pub role: RoleId,
    pub external_provider: DatasetProviderId,
    pub permission: Permission,
}
