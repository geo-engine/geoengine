use super::users::UserId;
use crate::error::{self, Error, Result};
use crate::identifier;
use crate::layers::listing::LayerCollectionId;
use crate::projects::ProjectId;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DatasetId, LayerId};
use geoengine_datatypes::pro::MlModelId;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::str::FromStr;
use utoipa::ToSchema;
use uuid::Uuid;

pub mod postgres_permissiondb;

identifier!(RoleId);

impl From<UserId> for RoleId {
    fn from(user_id: UserId) -> Self {
        RoleId(user_id.0)
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSchema)]
pub struct Role {
    pub id: RoleId,
    pub name: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSchema)]
pub struct RoleDescription {
    pub role: Role,
    pub individual: bool,
}

impl Role {
    #[allow(clippy::missing_panics_doc)]
    pub fn admin_role_id() -> RoleId {
        RoleId::from_str("d5328854-6190-4af9-ad69-4e74b0961ac9").expect("valid")
    }

    #[allow(clippy::missing_panics_doc)]
    pub fn registered_user_role_id() -> RoleId {
        RoleId::from_str("4e8081b6-8aa6-4275-af0c-2fa2da557d28").expect("valid")
    }

    #[allow(clippy::missing_panics_doc)]
    pub fn anonymous_role_id() -> RoleId {
        RoleId::from_str("fd8e87bf-515c-4f36-8da6-1a53702ff102").expect("valid")
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSchema, ToSql, FromSql)]
pub enum Permission {
    Read,
    Owner,
}

impl Permission {
    /// Return true if this permission includes the given permission.
    pub fn allows(&self, permission: &Permission) -> bool {
        self == permission || (self == &Permission::Owner)
    }

    /// Return the implied permissions for the given permission.
    pub fn implied_permissions(&self) -> Vec<Permission> {
        match self {
            Permission::Read => vec![Permission::Read],
            Permission::Owner => vec![Permission::Owner, Permission::Read],
        }
    }

    /// Return the required permissions for the given permission.
    /// One of the returned permissions must be granted to the user.
    pub fn required_permissions(&self) -> Vec<Permission> {
        match self {
            Permission::Read => vec![Permission::Owner, Permission::Read],
            Permission::Owner => vec![Permission::Owner],
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, ToSchema)]
#[serde(tag = "type", content = "id")]
pub enum ResourceId {
    Layer(LayerId),                     // TODO: UUID?
    LayerCollection(LayerCollectionId), // TODO: UUID?
    Project(ProjectId),
    DatasetId(DatasetId),
    ModelId(MlModelId),
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

impl TryFrom<(String, String)> for ResourceId {
    type Error = Error;

    fn try_from(value: (String, String)) -> Result<Self> {
        Ok(match value.0.as_str() {
            "layer" => ResourceId::Layer(LayerId(value.1)),
            "layer_collection" => ResourceId::LayerCollection(LayerCollectionId(value.1)),
            "project" => {
                ResourceId::Project(ProjectId(Uuid::from_str(&value.1).context(error::Uuid)?))
            }
            "dataset" => {
                ResourceId::DatasetId(DatasetId(Uuid::from_str(&value.1).context(error::Uuid)?))
            }
            "model" => {
                ResourceId::ModelId(MlModelId(Uuid::from_str(&value.1).context(error::Uuid)?))
            }
            _ => {
                return Err(Error::InvalidResourceId {
                    resource_type: value.0,
                    resource_id: value.1,
                })
            }
        })
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
pub struct PermissionListing {
    pub resource_id: ResourceId,
    pub role: Role,
    pub permission: Permission,
}

/// Management and checking of permissions.
// TODO: accept references of things that are Into<ResourceId> as well
#[async_trait]
pub trait PermissionDb {
    /// Create a new resource. Gives the current user the owner permission.
    async fn create_resource<R: Into<ResourceId> + Send + Sync>(&self, resource: R) -> Result<()>;

    /// Check `permission` for `resource`.
    async fn has_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
    ) -> Result<bool>;

    /// Ensure `permission` for `resource` exists. Throws error if not allowed.
    #[must_use]
    async fn ensure_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
    ) -> Result<()>;

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

    /// list all `permission` for `resource`.
    /// Requires `Owner` permission for `resource`.
    async fn list_permissions<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<PermissionListing>>;
}
