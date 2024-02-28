use super::{Permission, PermissionDb, PermissionListing, ResourceId, RoleId};
use crate::error::{self, Error, Result};
use crate::pro::contexts::ProPostgresDb;
use crate::pro::permissions::Role;
use async_trait::async_trait;
use snafu::ensure;
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};
use uuid::Uuid;

// TODO: a postgres specific permission db trait that allows re-using connections and transactions

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
            ResourceId::ModelId(_) => "model_id",
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
            ResourceId::ModelId(id) => Ok(id.0),
        }
    }
}

/// internal functionality for transactional permission db
///
/// In contrast to the `PermissionDb` this is not to be used by services but only by the `ProPostgresDb` internally.
/// This is because services do not know about database transactions.
#[async_trait]
pub trait TxPermissionDb {
    /// Create a new resource. Gives the current user the owner permission.
    async fn create_resource_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<()>;

    /// Check `permission` for `resource`.
    async fn has_permission_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<bool>;

    /// Ensure `permission` for `resource` exists. Throws error if not allowed.
    #[must_use]
    async fn ensure_permission_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<()>;

    /// Give `permission` to `role` for `resource`.
    /// Requires `Owner` permission for `resource`.
    async fn add_permission_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<()>;

    /// Remove `permission` from `role` for `resource`.
    /// Requires `Owner` permission for `resource`.
    async fn remove_permission_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<()>;

    /// Remove all `permission` for `resource`.
    /// Requires `Owner` permission for `resource`.
    async fn remove_permissions_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<()>;

    async fn list_permissions_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        offset: u32,
        limit: u32,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<Vec<PermissionListing>>;
}

#[async_trait]
impl<Tls> TxPermissionDb for ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn create_resource_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<()> {
        let resource: ResourceId = resource.into();

        let stmt = tx
            .prepare(&format!(
                "
            INSERT INTO permissions (role_id, permission, {resource_type})
            VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;",
                resource_type = resource.resource_type_name()
            ))
            .await?;

        tx.execute(
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

    async fn has_permission_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<bool> {
        let resource: ResourceId = resource.into();

        // TODO: perform join to get all roles of a user instead of using the roles from the session object?
        let stmt = tx
            .prepare(&format!(
                "
            SELECT COUNT(*) FROM permissions WHERE role_id = ANY($1) AND permission = ANY($2) AND {resource_type} = $3;",
                resource_type = resource.resource_type_name()
            ))
            .await?;

        let row = tx
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

    #[must_use]
    async fn ensure_permission_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<()> {
        let has_permission = self.has_permission_in_tx(resource, permission, tx).await?;

        ensure!(has_permission, error::PermissionDenied);

        Ok(())
    }

    async fn add_permission_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<()> {
        let resource: ResourceId = resource.into();

        ensure!(
            self.has_permission_in_tx(resource.clone(), Permission::Owner, tx)
                .await?,
            error::PermissionDenied
        );

        let stmt = tx
            .prepare(&format!(
                "
            INSERT INTO permissions (role_id, permission, {resource_type})
            VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;",
                resource_type = resource.resource_type_name()
            ))
            .await?;

        tx.execute(&stmt, &[&role, &permission, &resource.uuid()?])
            .await?;

        Ok(())
    }

    async fn remove_permission_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<()> {
        let resource: ResourceId = resource.into();

        ensure!(
            self.has_permission_in_tx(resource.clone(), Permission::Owner, tx)
                .await?,
            error::PermissionDenied
        );

        ensure!(
            role != RoleId::from(self.session.user.id),
            error::CannotRevokeOwnPermission,
        );

        let stmt = tx
            .prepare(&format!(
                "
            DELETE FROM permissions WHERE role_id = $1 AND permission = $2 AND {resource_type} = $3;",
                resource_type = resource.resource_type_name()
            ))
            .await?;

        tx.execute(&stmt, &[&role, &permission, &resource.uuid()?])
            .await?;

        Ok(())
    }

    async fn remove_permissions_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<()> {
        let resource: ResourceId = resource.into();

        ensure!(
            self.has_permission_in_tx(resource.clone(), Permission::Owner, tx)
                .await?,
            error::PermissionDenied
        );

        let stmt = tx
            .prepare(&format!(
                "
            DELETE FROM permissions WHERE {resource_type} = $3;",
                resource_type = resource.resource_type_name()
            ))
            .await?;

        tx.execute(&stmt, &[&resource.uuid()?]).await?;

        Ok(())
    }

    async fn list_permissions_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        offset: u32,
        limit: u32,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<Vec<PermissionListing>> {
        let resource: ResourceId = resource.into();

        ensure!(
            self.has_permission_in_tx(resource.clone(), Permission::Owner, tx)
                .await?,
            error::PermissionDenied
        );

        let stmt = tx
            .prepare(&format!(
                "
            SELECT 
                r.id, r.name, p.permission 
            FROM 
                permissions p JOIN roles r ON (p.role_id = r.id) 
            WHERE 
                {resource_type} = $1
            ORDER BY r.name ASC
            OFFSET $2
            LIMIT $3;",
                resource_type = resource.resource_type_name()
            ))
            .await?;

        let rows = tx
            .query(
                &stmt,
                &[&resource.uuid()?, &(i64::from(offset)), &(i64::from(limit))],
            )
            .await?;

        let permissions = rows
            .into_iter()
            .map(|row| PermissionListing {
                resource_id: resource.clone(),
                role: Role {
                    id: row.get(0),
                    name: row.get(1),
                },
                permission: row.get(2),
            })
            .collect();

        Ok(permissions)
    }
}

#[async_trait]
impl<Tls> PermissionDb for ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn create_resource<R: Into<ResourceId> + Send + Sync>(&self, resource: R) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        self.create_resource_in_tx(resource, &tx).await?;

        tx.commit().await?;

        Ok(())
    }

    async fn has_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
    ) -> Result<bool> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        let result = self.has_permission_in_tx(resource, permission, &tx).await?;

        tx.commit().await?;

        Ok(result)
    }

    #[must_use]
    async fn ensure_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
    ) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        self.ensure_permission_in_tx(resource, permission, &tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn add_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
    ) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        self.add_permission_in_tx(role, resource, permission, &tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn remove_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
    ) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        self.remove_permission_in_tx(role, resource, permission, &tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn remove_permissions<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
    ) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        self.remove_permissions_in_tx(resource, &tx).await?;

        tx.commit().await?;

        Ok(())
    }

    async fn list_permissions<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<PermissionListing>> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        let permissions = self
            .list_permissions_in_tx(resource, offset, limit, &tx)
            .await?;

        tx.commit().await?;

        Ok(permissions)
    }
}
