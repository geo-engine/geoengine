use super::database_migration::{DatabaseVersion, Migration};
use crate::{contexts::migrations::Migration0041NonZeroMaxOverviewLevel, error::Result};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// Stores optional structured layer loading costs without application-specific limits.
pub struct Migration0042LayerLoadingCost;

#[async_trait]
impl Migration for Migration0042LayerLoadingCost {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0041NonZeroMaxOverviewLevel.version())
    }
    fn version(&self) -> DatabaseVersion {
        "0042_layer_loading_cost".into()
    }
    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0042_layer_loading_cost.sql"))
            .await?;
        Ok(())
    }
}
