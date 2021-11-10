use std::str::FromStr;

use crate::error::Result;
use crate::pro::users::{UserId, UserSession};
use async_trait::async_trait;
use geoengine_datatypes::{
    dataset::{DatasetId, DatasetProviderId},
    identifier,
};
#[cfg(feature = "postgres")]
use postgres_types::{FromSql, ToSql};
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

impl Role {
    pub fn system_role_id() -> RoleId {
        RoleId::from_str("d5328854-6190-4af9-ad69-4e74b0961ac9").expect("valid")
    }

    pub fn user_role_id() -> RoleId {
        RoleId::from_str("4e8081b6-8aa6-4275-af0c-2fa2da557d28").expect("valid")
    }

    pub fn anonymous_role_id() -> RoleId {
        RoleId::from_str("fd8e87bf-515c-4f36-8da6-1a53702ff102").expect("valid")
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
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

#[async_trait]
pub trait UpdateDatasetPermissions {
    async fn add_dataset_permission(
        &mut self,
        session: &UserSession,
        permission: DatasetPermission,
    ) -> Result<()>;
}
