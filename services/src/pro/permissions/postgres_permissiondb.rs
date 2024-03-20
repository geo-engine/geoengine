use super::{
    Bb8PermissionDbError, Permission, PermissionDb, PermissionDbError, PermissionListing,
    PostgresPermissionDbError, ResourceId, RoleId,
};
use crate::error::Result;
use crate::pro::contexts::ProPostgresDb;
use crate::pro::permissions::{
    CannotGrantOwnerPermissionPermissionDbError, CannotRevokeOwnPermissionPermissionDbError,
    MustBeAdminPermissionDbError, PermissionDeniedPermissionDbError, Role,
};
use async_trait::async_trait;
use snafu::{ensure, ResultExt};
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};
use uuid::Uuid;

// TODO: a postgres specific permission db trait that allows re-using connections and transactions

trait ResourceTypeName {
    fn resource_type_name(&self) -> &'static str;

    fn uuid(&self) -> Result<Uuid, PermissionDbError>;
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

    fn uuid(&self) -> Result<Uuid, PermissionDbError> {
        match self {
            ResourceId::Layer(id) => {
                Uuid::parse_str(&id.0).map_err(|_| PermissionDbError::ResourceIdIsNotAValidUuid {
                    resource_id: id.0.clone(),
                })
            }
            ResourceId::LayerCollection(id) => {
                Uuid::parse_str(&id.0).map_err(|_| PermissionDbError::ResourceIdIsNotAValidUuid {
                    resource_id: id.0.clone(),
                })
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
    ) -> Result<(), PermissionDbError>;

    /// Check `permission` for `resource`.
    async fn has_permission_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<bool, PermissionDbError>;

    /// Ensure `permission` for `resource` exists. Throws error if not allowed.
    #[must_use]
    async fn ensure_permission_in_tx(
        &self,
        resource: ResourceId,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<(), PermissionDbError>;

    /// Ensure user is admin
    async fn ensure_admin_in_tx(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<(), PermissionDbError>;

    /// Give `permission` to `role` for `resource`.
    /// Requires `Owner` permission for `resource`.
    async fn add_permission_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<(), PermissionDbError>;

    /// Remove `permission` from `role` for `resource`.
    /// Requires `Owner` permission for `resource`.
    async fn remove_permission_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<(), PermissionDbError>;

    /// Remove all `permission` for `resource`.
    /// Requires `Owner` permission for `resource`.
    async fn remove_permissions_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<(), PermissionDbError>;

    async fn list_permissions_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        offset: u32,
        limit: u32,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<Vec<PermissionListing>, PermissionDbError>;
}

#[async_trait]
impl<Tls> TxPermissionDb for ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn create_resource_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<(), PermissionDbError> {
        let resource: ResourceId = resource.into();

        let stmt = tx
            .prepare(&format!(
                "
            INSERT INTO permissions (role_id, permission, {resource_type})
            VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;",
                resource_type = resource.resource_type_name()
            ))
            .await
            .context(PostgresPermissionDbError)?;

        tx.execute(
            &stmt,
            &[
                &RoleId::from(self.session.user.id),
                &Permission::Owner,
                &resource.uuid()?,
            ],
        )
        .await
        .context(PostgresPermissionDbError)?;

        Ok(())
    }

    async fn has_permission_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<bool, PermissionDbError> {
        let resource: ResourceId = resource.into();

        // TODO: perform join to get all roles of a user instead of using the roles from the session object?
        let stmt = tx
            .prepare(&format!(
                "
            SELECT COUNT(*) FROM permissions WHERE role_id = ANY($1) AND permission = ANY($2) AND {resource_type} = $3;",
                resource_type = resource.resource_type_name()
            ))
            .await.context(PostgresPermissionDbError)?;

        let row = tx
            .query_opt(
                &stmt,
                &[
                    &self.session.roles,
                    &permission.required_permissions(),
                    &resource.uuid()?,
                ],
            )
            .await
            .context(PostgresPermissionDbError)?
            .ok_or(PermissionDbError::PermissionNotFound {
                permission,
                resource_id: resource,
                role_ids: self.session.roles.clone(),
            })?;

        Ok(row.get::<usize, i64>(0) > 0)
    }

    #[must_use]
    async fn ensure_permission_in_tx(
        &self,
        resource: ResourceId,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<(), PermissionDbError> {
        let has_permission = self
            .has_permission_in_tx(resource.clone(), permission.clone(), tx)
            .await?;

        ensure!(
            has_permission,
            PermissionDeniedPermissionDbError {
                permission,
                resource_id: resource,
            }
        );

        Ok(())
    }

    async fn ensure_admin_in_tx(
        &self,
        _tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<(), PermissionDbError> {
        ensure!(self.session.is_admin(), MustBeAdminPermissionDbError);

        Ok(())
    }

    async fn add_permission_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<(), PermissionDbError> {
        let resource: ResourceId = resource.into();

        self.ensure_permission_in_tx(resource.clone(), Permission::Owner, tx)
            .await?;

        let stmt = tx
            .prepare(&format!(
                "
            INSERT INTO permissions (role_id, permission, {resource_type})
            VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;",
                resource_type = resource.resource_type_name()
            ))
            .await
            .context(PostgresPermissionDbError)?;

        tx.execute(&stmt, &[&role, &permission, &resource.uuid()?])
            .await
            .context(PostgresPermissionDbError)?;

        Ok(())
    }

    async fn remove_permission_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<(), PermissionDbError> {
        let resource: ResourceId = resource.into();

        self.ensure_permission_in_tx(resource.clone(), Permission::Owner, tx)
            .await?;

        ensure!(
            role != RoleId::from(self.session.user.id),
            CannotRevokeOwnPermissionPermissionDbError,
        );

        let stmt = tx
            .prepare(&format!(
                "
            DELETE FROM permissions WHERE role_id = $1 AND permission = $2 AND {resource_type} = $3;",
                resource_type = resource.resource_type_name()
            ))
            .await.context(PostgresPermissionDbError)?;

        tx.execute(&stmt, &[&role, &permission, &resource.uuid()?])
            .await
            .context(PostgresPermissionDbError)?;

        Ok(())
    }

    async fn remove_permissions_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<(), PermissionDbError> {
        let resource: ResourceId = resource.into();

        self.ensure_permission_in_tx(resource.clone(), Permission::Owner, tx)
            .await?;

        let stmt = tx
            .prepare(&format!(
                "
            DELETE FROM permissions WHERE {resource_type} = $3;",
                resource_type = resource.resource_type_name()
            ))
            .await
            .context(PostgresPermissionDbError)?;

        tx.execute(&stmt, &[&resource.uuid()?])
            .await
            .context(PostgresPermissionDbError)?;

        Ok(())
    }

    async fn list_permissions_in_tx<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        offset: u32,
        limit: u32,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<Vec<PermissionListing>, PermissionDbError> {
        let resource: ResourceId = resource.into();

        self.ensure_permission_in_tx(resource.clone(), Permission::Owner, tx)
            .await?;

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
            .await
            .context(PostgresPermissionDbError)?;

        let rows = tx
            .query(
                &stmt,
                &[&resource.uuid()?, &(i64::from(offset)), &(i64::from(limit))],
            )
            .await
            .context(PostgresPermissionDbError)?;

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
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn create_resource<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
    ) -> Result<(), PermissionDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8PermissionDbError)?;

        let tx = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresPermissionDbError)?;

        self.create_resource_in_tx(resource, &tx).await?;

        tx.commit().await.context(PostgresPermissionDbError)?;

        Ok(())
    }

    async fn has_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
    ) -> Result<bool, PermissionDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8PermissionDbError)?;
        let tx = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresPermissionDbError)?;

        let result = self.has_permission_in_tx(resource, permission, &tx).await?;

        tx.commit().await.context(PostgresPermissionDbError)?;

        Ok(result)
    }

    #[must_use]
    async fn ensure_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        permission: Permission,
    ) -> Result<(), PermissionDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8PermissionDbError)?;
        let tx = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresPermissionDbError)?;

        self.ensure_permission_in_tx(resource.into(), permission, &tx)
            .await?;

        tx.commit().await.context(PostgresPermissionDbError)?;

        Ok(())
    }

    async fn ensure_admin<R: Into<ResourceId> + Send + Sync>(
        &self,
    ) -> Result<(), PermissionDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8PermissionDbError)?;
        let tx = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresPermissionDbError)?;

        self.ensure_admin_in_tx(&tx).await?;

        tx.commit().await.context(PostgresPermissionDbError)?;

        Ok(())
    }

    async fn add_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
    ) -> Result<(), PermissionDbError> {
        ensure!(
            permission != Permission::Owner,
            CannotGrantOwnerPermissionPermissionDbError
        );

        let mut conn = self.conn_pool.get().await.context(Bb8PermissionDbError)?;
        let tx = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresPermissionDbError)?;

        self.add_permission_in_tx(role, resource, permission, &tx)
            .await?;

        tx.commit().await.context(PostgresPermissionDbError)?;

        Ok(())
    }

    async fn remove_permission<R: Into<ResourceId> + Send + Sync>(
        &self,
        role: RoleId,
        resource: R,
        permission: Permission,
    ) -> Result<(), PermissionDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8PermissionDbError)?;
        let tx = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresPermissionDbError)?;

        self.remove_permission_in_tx(role, resource, permission, &tx)
            .await?;

        tx.commit().await.context(PostgresPermissionDbError)?;

        Ok(())
    }

    async fn remove_permissions<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
    ) -> Result<(), PermissionDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8PermissionDbError)?;
        let tx = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresPermissionDbError)?;

        self.remove_permissions_in_tx(resource, &tx).await?;

        tx.commit().await.context(PostgresPermissionDbError)?;

        Ok(())
    }

    async fn list_permissions<R: Into<ResourceId> + Send + Sync>(
        &self,
        resource: R,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<PermissionListing>, PermissionDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8PermissionDbError)?;
        let tx = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresPermissionDbError)?;

        let permissions = self
            .list_permissions_in_tx(resource, offset, limit, &tx)
            .await?;

        tx.commit().await.context(PostgresPermissionDbError)?;

        Ok(permissions)
    }
}
