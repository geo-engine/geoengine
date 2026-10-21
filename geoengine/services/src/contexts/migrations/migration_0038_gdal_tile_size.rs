use super::database_migration::{DatabaseVersion, Migration};
use crate::{contexts::migrations::migration_0037_rename_workflows_to_processing_graphs::Migration0037RenameWorkflowsToProcessingGraphs, error::Result};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds an optional tile size override to GDAL dataset parameters.
/// Existing datasets get `NULL`, so they keep using the configured default tile size.
pub struct Migration0038GdalTileSize;

#[async_trait]
impl Migration for Migration0038GdalTileSize {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0037RenameWorkflowsToProcessingGraphs.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0038_gdal_tile_size".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0038_gdal_tile_size.sql"))
            .await?;
        Ok(())
    }
}
