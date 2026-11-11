use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0037_rename_workflows_to_processing_graphs::Migration0037RenameWorkflowsToProcessingGraphs,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// Adds the limit of parallel STAC searches to the provider definition, since it depends on the
/// STAC server. Existing providers keep the previous limit through the `NULL` default.
pub struct Migration0038StacMaxConcurrentCellSearches;

#[async_trait]
impl Migration for Migration0038StacMaxConcurrentCellSearches {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0037RenameWorkflowsToProcessingGraphs.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0038_stac_max_concurrent_cell_searches".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!(
            "migration_0038_stac_max_concurrent_cell_searches.sql"
        ))
        .await?;
        Ok(())
    }
}
