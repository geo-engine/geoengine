use super::database_migration::{DatabaseVersion, Migration};
use crate::contexts::migrations::Migration0020ProviderPermissions;
use crate::error::Result;
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
        tx.batch_execute(include_str!(
            "migration_0021_default_permissions_for_existing_providers.sql"
        ))
        .await?;

        Ok(())
    }
}
