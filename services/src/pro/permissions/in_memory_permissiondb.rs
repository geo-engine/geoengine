use crate::contexts::{Db, InMemoryDb};
use crate::error::{self, Error, Result};
use crate::pro::contexts::ProInMemoryDb;
use crate::pro::users::UserSession;
use crate::{api::model::datatypes::LayerId, layers::listing::LayerCollectionId};

use super::{Permission, PermissionDb, ResourceId, RoleId};

use async_trait::async_trait;
use snafu::ensure;

#[derive(Default)]
pub struct InMemoryPermissionDbBackend {
    permissions: Vec<InMemoryPermission>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct InMemoryPermission {
    role: RoleId,
    resource: ResourceId,
    permission: Permission,
}

#[async_trait]
impl PermissionDb for ProInMemoryDb {
    async fn has_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
    ) -> Result<bool> {
        let backend = self.backend.permission_db.read().await;

        let resource: ResourceId = resource.into();

        Ok(backend
            .permissions
            .iter()
            .any(|p| p.resource == resource && p.permission.allows(&permission)))
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
        todo!()
    }
}
