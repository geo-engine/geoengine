use super::database_migration::{DatabaseVersion, Migration};
use crate::{contexts::migrations::Migration0037RenameWorkflowsToProcessingGraphs, error::Result};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// Adds the key of the STAC asset to the bands of a STAC provider, so that searches can request
/// exactly the assets they need. Some servers, like CDSE, do not support `assets.*` in `fields`.
pub struct Migration0038StacAssetKey;

#[async_trait]
impl Migration for Migration0038StacAssetKey {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0037RenameWorkflowsToProcessingGraphs.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0038_stac_asset_key".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0038_stac_asset_key.sql"))
            .await?;
        Ok(())
    }
}
