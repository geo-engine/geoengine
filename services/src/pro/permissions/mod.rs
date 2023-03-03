use std::str::FromStr;

use async_trait::async_trait;
#[cfg(feature = "postgres")]
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};

use geoengine_datatypes::util::Identifier;
use utoipa::ToSchema;

use crate::api::model::datatypes::{DatasetId, LayerId};
use crate::error::Result;
use crate::identifier;
use crate::layers::listing::LayerCollectionId;
use crate::projects::ProjectId;

use super::users::UserId;

pub mod in_memory_permissiondb;
pub mod postgres_permissiondb;

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
    // TODO: rename to admin role?
    pub fn system_role_id() -> RoleId {
        RoleId::from_str("d5328854-6190-4af9-ad69-4e74b0961ac9").expect("valid")
    }

    // TODO: rename to register role?
    pub fn user_role_id() -> RoleId {
        RoleId::from_str("4e8081b6-8aa6-4275-af0c-2fa2da557d28").expect("valid")
    }

    pub fn anonymous_role_id() -> RoleId {
        RoleId::from_str("fd8e87bf-515c-4f36-8da6-1a53702ff102").expect("valid")
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSchema)]
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
pub enum Permission {
    Read,
    Owner,
}

impl Permission {
    /// Return true if this permission includes the given permission.
    pub fn allows(&self, permission: &Permission) -> bool {
        if self == permission {
            true
        } else {
            match self {
                Permission::Owner => true,
                _ => false,
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
#[serde(tag = "type")]
pub enum ResourceId {
    Layer(LayerId),                     // TODO: UUID?
    LayerCollection(LayerCollectionId), // TODO: UUID?
    Project(ProjectId),
    DatasetId(DatasetId),
}

impl From<LayerId> for ResourceId {
    fn from(layer_id: LayerId) -> Self {
        ResourceId::Layer(layer_id)
    }
}

impl From<LayerCollectionId> for ResourceId {
    fn from(layer_collection_id: LayerCollectionId) -> Self {
        ResourceId::LayerCollection(layer_collection_id)
    }
}

impl From<ProjectId> for ResourceId {
    fn from(project_id: ProjectId) -> Self {
        ResourceId::Project(project_id)
    }
}

impl From<DatasetId> for ResourceId {
    fn from(dataset_id: DatasetId) -> Self {
        ResourceId::DatasetId(dataset_id)
    }
}

// TODO: accept references of things that are Into<ResourceId> as well
#[async_trait]
pub trait PermissionDb {
    /// Check `permission` for `resource`.
    async fn has_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
    ) -> Result<bool>;

    /// Give `permission` to `role` for `resource`.
    /// Requires `Owner` permission for `resource`.
    async fn add_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
    ) -> Result<()>;

    /// Remove `permission` from `role` for `resource`.
    /// Requires `Owner` permission for `resource`.
    async fn remove_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
    ) -> Result<()>;

    /// Remove all `permission` for `resource`.
    /// Requires `Owner` permission for `resource`.
    async fn remove_permissions<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
    ) -> Result<()>;

    // TODO: own RoleDB trait?

    // /// Create a new role
    // /// Requires `Admin` Role
    // async fn create_role(&self, name: &str) -> Result<RoleId>;

    // /// List all roles
    // /// Requires `Admin` Role
    // // TODO: pagination
    // async fn list_roles(&self) -> Result<Vec<Role>>;

    // TODO: delete role

    // /// Assign role to user
    // /// Requires `Admin` Role
    // /// TODO: make roles their own resource with Owners?
    // async fn assign_role(&self, user: &UserId, role: &RoleId) -> Result<()>;

    // /// Revoke role from user
    // /// Requires `Admin` Role
    // /// TODO: make roles their own resource with Owners?
    // async fn revoke_role(&self, user: &UserId, role: &RoleId) -> Result<()>;
}
