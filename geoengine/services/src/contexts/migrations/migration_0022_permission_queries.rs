use super::database_migration::{DatabaseVersion, Migration};
use crate::contexts::migrations::migration_0021_default_permissions_for_existing_providers::Migration0021DefaultPermissionsForExistingProviders;
use crate::error::Result;
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the provider permissions
pub struct Migration0022PermissionQueries;

#[async_trait]
impl Migration for Migration0022PermissionQueries {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0021DefaultPermissionsForExistingProviders.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0022_permission_queries".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0022_permission_queries.sql"))
            .await?;

        Ok(())
    }
}
