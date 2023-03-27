use crate::error::{self, Result};
use crate::layers::add_from_directory::UNSORTED_COLLECTION_ID;
use crate::layers::storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID;
use crate::pro::contexts::ProInMemoryDb;

use crate::layers::listing::LayerCollectionId;

use super::{Permission, PermissionDb, ResourceId, Role, RoleId};

use async_trait::async_trait;
use snafu::ensure;

pub struct InMemoryPermissionDbBackend {
    permissions: Vec<InMemoryPermission>,
}

impl Default for InMemoryPermissionDbBackend {
    fn default() -> Self {
        // create the default permissions
        let mut permissions = vec![];
        for collection_id in [
            INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
            UNSORTED_COLLECTION_ID,
            INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
        ] {
            permissions.push(InMemoryPermission {
                role: Role::admin_role_id(),
                resource: ResourceId::LayerCollection(LayerCollectionId(collection_id.to_string())),
                permission: Permission::Owner,
            });
            permissions.push(InMemoryPermission {
                role: Role::registered_user_role_id(),
                resource: ResourceId::LayerCollection(LayerCollectionId(collection_id.to_string())),
                permission: Permission::Read,
            });
            permissions.push(InMemoryPermission {
                role: Role::anonymous_role_id(),
                resource: ResourceId::LayerCollection(LayerCollectionId(collection_id.to_string())),
                permission: Permission::Read,
            });
        }

        Self { permissions }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct InMemoryPermission {
    role: RoleId,
    resource: ResourceId,
    permission: Permission,
}

#[async_trait]
impl PermissionDb for ProInMemoryDb {
    async fn create_resource<R: Into<ResourceId> + Send + Sync>(&self, resource: R) -> Result<()> {
        let mut backend = self.backend.permission_db.write().await;

        let resource: ResourceId = resource.into();

        ensure!(
            !backend.permissions.iter().any(|p| p.resource == resource),
            error::PermissionDenied,
        );

        backend.permissions.push(InMemoryPermission {
            role: self.session.user.id.into(),
            resource,
            permission: Permission::Owner,
        });

        Ok(())
    }

    async fn has_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
    ) -> Result<bool> {
        let backend = self.backend.permission_db.read().await;

        let resource: ResourceId = resource.into();

        Ok(backend.permissions.iter().any(|p| {
            self.session.roles.iter().any(|r| r == &p.role)
                && p.resource == resource
                && p.permission.allows(&permission)
        }))
    }

    async fn add_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
    ) -> Result<()> {
        let resource: ResourceId = resource.into();

        ensure!(
            self.has_permission(resource.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        let mut backend = self.backend.permission_db.write().await;

        backend.permissions.push(InMemoryPermission {
            role,
            resource: resource.clone(),
            permission,
        });

        Ok(())
    }

    async fn remove_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
    ) -> Result<()> {
        let resource: ResourceId = resource.into();

        ensure!(
            self.has_permission(resource.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        let mut backend = self.backend.permission_db.write().await;

        let ip = InMemoryPermission {
            role,
            resource,
            permission,
        };

        backend.permissions.retain(|p| p != &ip);

        Ok(())
    }

    async fn remove_permissions<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
    ) -> Result<()> {
        let resource: ResourceId = resource.into();

        ensure!(
            self.has_permission(resource.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        let mut backend = self.backend.permission_db.write().await;

        backend.permissions.retain(|p| p.resource != resource);

        Ok(())
    }
}
