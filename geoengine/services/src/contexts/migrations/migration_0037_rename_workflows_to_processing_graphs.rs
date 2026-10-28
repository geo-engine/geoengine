use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0036_remove_aruna_provider::Migration0036RemoveArunaProvider,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// Renames the `workflows` table and all `workflow_id` columns to `processing_graphs` and
/// `processing_graph_id`, matching the naming of the API.
/// The stored data and the processing graph ids stay the same.
pub struct Migration0037RenameWorkflowsToProcessingGraphs;

#[async_trait]
impl Migration for Migration0037RenameWorkflowsToProcessingGraphs {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0036RemoveArunaProvider.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0037_rename_workflows_to_processing_graphs".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!(
            "migration_0037_rename_workflows_to_processing_graphs.sql"
        ))
        .await?;
        Ok(())
    }
}
