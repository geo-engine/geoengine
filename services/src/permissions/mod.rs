use crate::error::Result;
use crate::identifier;
use crate::layers::listing::LayerCollectionId;
use crate::machine_learning::MlModelId;
use crate::projects::ProjectId;
use crate::users::UserId;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DatasetId, LayerId};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::str::FromStr;
use utoipa::ToSchema;

mod postgres_permissiondb;

pub use postgres_permissiondb::TxPermissionDb;

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

impl std::fmt::Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Permission::Read => write!(f, "Read"),
            Permission::Owner => write!(f, "Owner"),
        }
    }
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

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
#[serde(tag = "type", content = "id")]
pub enum ResourceId {
    Layer(LayerId),                     // TODO: UUID?
    LayerCollection(LayerCollectionId), // TODO: UUID?
    Project(ProjectId),
    DatasetId(DatasetId),
    MlModel(MlModelId),
}

impl std::fmt::Display for ResourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceId::Layer(layer_id) => write!(f, "layer:{}", layer_id.0),
            ResourceId::LayerCollection(layer_collection_id) => {
                write!(f, "layerCollection:{}", layer_collection_id.0)
            }
            ResourceId::Project(project_id) => write!(f, "project:{}", project_id.0),
            ResourceId::DatasetId(dataset_id) => write!(f, "dataset:{}", dataset_id.0),
            ResourceId::MlModel(ml_model_id) => write!(f, "mlModel:{}", ml_model_id.0),
        }
    }
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PermissionListing {
    pub resource_id: ResourceId,
    pub role: Role,
    pub permission: Permission,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(PermissionDbError)))]
pub enum PermissionDbError {
    #[snafu(display("Permission {permission} for resource {resource_id} denied."))]
    PermissionDenied {
        resource_id: ResourceId,
        permission: Permission,
    },
    #[snafu(display("Must be admin to perform this action."))]
    MustBeAdmin,
    #[snafu(display(
        "Permission {permission} for resource {resource_id} and roles {} not found.", role_ids.iter().map(std::string::ToString::to_string).collect::<Vec<String>>().join(", ")
    ))]
    PermissionNotFound {
        resource_id: ResourceId,
        permission: Permission,
        role_ids: Vec<RoleId>,
    },
    #[snafu(display("Cannot revoke own permission"))]
    CannotRevokeOwnPermission,
    #[snafu(display("Cannot grant Owner permission, because there can only be one owner."))]
    CannotGrantOwnerPermission,
    #[snafu(display("Resource Id {resource_id} is not a valid Uuid."))]
    ResourceIdIsNotAValidUuid { resource_id: String },
    #[snafu(display("An unexpected database error occurred."))]
    Postgres { source: tokio_postgres::Error },
    #[snafu(display("An unexpected database error occurred."))]
    Bb8 {
        source: bb8_postgres::bb8::RunError<tokio_postgres::Error>,
    },
}

/// Management and checking of permissions.
// TODO: accept references of things that are Into<ResourceId> as well
#[async_trait]
pub trait PermissionDb {
    /// Create a new resource. Gives the current user the owner permission.
    async fn create_resource<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
    ) -> Result<(), PermissionDbError>;

    /// Check `permission` for `resource`.
    async fn has_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
    ) -> Result<bool, PermissionDbError>;

    /// Ensure `permission` for `resource` exists. Throws error if not allowed.
    #[must_use]
    async fn ensure_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
    ) -> Result<(), PermissionDbError>;

    /// Ensure user is admin
    #[must_use]
    async fn ensure_admin<R: Into<ResourceId> + Send + Sync>(
        &self,
    ) -> Result<(), PermissionDbError>;

    /// Give `permission` to `role` for `resource`.
    /// Requires `Owner` permission for `resource`.
    async fn add_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
    ) -> Result<(), PermissionDbError>;

    /// Remove `permission` from `role` for `resource`.
    /// Requires `Owner` permission for `resource`.
    async fn remove_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
    ) -> Result<(), PermissionDbError>;

    /// Remove all `permission` for `resource`.
    /// Requires `Owner` permission for `resource`.
    async fn remove_permissions<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
    ) -> Result<(), PermissionDbError>;

    /// list all `permission` for `resource`.
    /// Requires `Owner` permission for `resource`.
    async fn list_permissions<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<PermissionListing>, PermissionDbError>;
}
