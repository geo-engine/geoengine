use super::database_migration::{DatabaseVersion, Migration};
use crate::contexts::migrations::Migration0018WildliveConnector;
use crate::error::Result;
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the provider permissions
pub struct Migration0019ProviderPermissions;

#[async_trait]
impl Migration for Migration0019ProviderPermissions {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0018WildliveConnector.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0019_provider_permissions".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0019_provider_permissions.sql"))
            .await?;

        Ok(())
    }
}
