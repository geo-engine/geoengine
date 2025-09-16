use super::database_migration::{DatabaseVersion, Migration};
use crate::contexts::migrations::Migration0020ProviderPermissions;
use crate::error::Result;
use crate::permissions::{Permission, Role};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration sets default permissions for existing providers which don't
/// have any permissions set yet. This fixes migration 0020, which added the ability to set
/// permissions for providers but did not set any default permissions for already existing
/// ones. Any providers that have been created since migration 0020, and thus already
/// have permissions set, are unaffected by this migration.
pub struct Migration0021DefaultPermissionsForExistingProviders;

#[async_trait]
impl Migration for Migration0021DefaultPermissionsForExistingProviders {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0020ProviderPermissions.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0021_default_permissions_for_existing_providers".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.execute(
            "
            CREATE TEMP TABLE skip AS
            SELECT provider_id AS skip_id
            FROM permissions
            WHERE provider_id IS NOT NULL
        ",
            &[],
        )
        .await?;

        let stmt = tx
            .prepare(
                "
            INSERT INTO permissions (role_id, permission, provider_id)
            SELECT
                $1 AS role_id,
                $2 AS permission,
                p.id AS provider_id
            FROM layer_providers AS p
            WHERE NOT EXISTS (
                SELECT s.skip_id FROM skip AS s
                WHERE p.id = s.skip_id
            )
        ",
            )
            .await?;

        tx.execute(&stmt, &[&Role::admin_role_id().0, &Permission::Owner])
            .await?;

        tx.execute(
            &stmt,
            &[&Role::registered_user_role_id().0, &Permission::Read],
        )
        .await?;

        tx.execute(&stmt, &[&Role::anonymous_role_id().0, &Permission::Read])
            .await?;

        tx.execute("DROP TABLE skip", &[]).await?;

        Ok(())
    }
}
