use super::database_migration::{DatabaseVersion, Migration};
use crate::contexts::migrations::migration_0019_ml_model_no_data::Migration0019MlModelNoData;
use crate::error::Result;
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the provider permissions
pub struct Migration0020ProviderPermissions;

#[async_trait]
impl Migration for Migration0020ProviderPermissions {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0019MlModelNoData.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0020_provider_permissions".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0020_provider_permissions.sql"))
            .await?;

        Ok(())
    }
}
