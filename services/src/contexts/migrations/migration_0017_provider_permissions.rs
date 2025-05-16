use super::database_migration::{DatabaseVersion, Migration};
use crate::contexts::migrations::Migration0016MergeProviders;
use crate::error::Result;
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the provider permissions
pub struct Migration0017ProviderPermissions;

#[async_trait]
impl Migration for Migration0017ProviderPermissions {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0016MergeProviders.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0017_provider_permissions".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0017_provider_permissions.sql"))
            .await?;

        Ok(())
    }
}
