use async_trait::async_trait;
use snafu::ensure;
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};
use uuid::Uuid;

use crate::error::{self, Error, Result};
use crate::pro::contexts::PostgresDb;

use super::{Permission, PermissionDb, ResourceId, RoleId};

// TODO: a postgres specific permission db implementation that allows re-using connections and transaction

trait ResourceTypeName {
    fn resource_type_name(&self) -> &'static str;

    fn uuid(&self) -> Result<Uuid>;
}

impl ResourceTypeName for ResourceId {
    fn resource_type_name(&self) -> &'static str {
        match self {
            ResourceId::Layer(_) => "layer_id",
            ResourceId::LayerCollection(_) => "layer_collection_id",
            ResourceId::Project(_) => "project_id",
            ResourceId::DatasetId(_) => "dataset_id",
        }
    }

    fn uuid(&self) -> Result<Uuid> {
        match self {
            ResourceId::Layer(id) => Uuid::parse_str(&id.0).map_err(|_| Error::InvalidUuid),
            ResourceId::LayerCollection(id) => {
                Uuid::parse_str(&id.0).map_err(|_| Error::InvalidUuid)
            }
            ResourceId::Project(id) => Ok(id.0),
            ResourceId::DatasetId(id) => Ok(id.0),
        }
    }
}

#[async_trait]
impl<Tls> PermissionDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn create_resource<R: Into<ResourceId> + Send + Sync>(&self, resource: R) -> Result<()> {
        let resource: ResourceId = resource.into();

        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(&format!(
                "
            INSERT INTO permissions (role_id, permission, {resource_type})
            VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;",
                resource_type = resource.resource_type_name()
            ))
            .await?;

        conn.execute(
            &stmt,
            &[
                &RoleId::from(self.session.user.id),
                &Permission::Owner,
                &resource.uuid()?,
            ],
        )
        .await?;

        Ok(())
    }

    async fn has_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
    ) -> Result<bool> {
        let resource: ResourceId = resource.into();

        let conn = self.conn_pool.get().await?;

        // TODO: perform join to get all roles of a user instead of using the roles from the session object?
        let stmt = conn
            .prepare(&format!(
                "
            SELECT COUNT(*) FROM permissions WHERE role_id = ANY($1) AND permission = ANY($2) AND {resource_type} = $3;",
                resource_type = resource.resource_type_name()
            ))
            .await?;

        let row = conn
            .query_one(
                &stmt,
                &[
                    &self.session.roles,
                    &permission.required_permissions(),
                    &resource.uuid()?,
                ],
            )
            .await?;

        Ok(row.get::<usize, i64>(0) > 0)
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

        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(&format!(
                "
            INSERT INTO permissions (role_id, permission, {resource_type})
            VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;",
                resource_type = resource.resource_type_name()
            ))
            .await?;

        conn.execute(&stmt, &[&role, &permission, &resource.uuid()?])
            .await?;

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

        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(&format!(
                "
            DELETE FROM permissions WHERE role_id = $1 AND permission = $2 AND {resource_type} = $3;",
                resource_type = resource.resource_type_name()
            ))
            .await?;

        conn.execute(&stmt, &[&role, &permission, &resource.uuid()?])
            .await?;

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

        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(&format!(
                "
            DELETE FROM permissions WHERE {resource_type} = $3;",
                resource_type = resource.resource_type_name()
            ))
            .await?;

        conn.execute(&stmt, &[&resource.uuid()?]).await?;

        Ok(())
    }
}
