use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0037_rename_workflows_to_processing_graphs::Migration0037RenameWorkflowsToProcessingGraphs,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration moves the per-file data of an `MdGdalSource` out of the dataset's
/// meta data and into a `dataset_md_tiles` table, mirroring how `MultiBandGdalSource`
/// stores its tiles. The meta data itself becomes a typed composite carrying only the
/// result descriptor, the variables and the dataset-level property `wrap`.
pub struct Migration0038MdDatasetTiles;

#[async_trait]
impl Migration for Migration0038MdDatasetTiles {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0037RenameWorkflowsToProcessingGraphs.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0038_md_dataset_tiles".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0038_md_dataset_tiles.sql"))
            .await?;

        Ok(())
    }
}
