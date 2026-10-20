use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0036_remove_aruna_provider::Migration0036RemoveArunaProvider,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration moves the per-file data of an `MdGdalSource` out of the dataset's
/// meta data and into a `dataset_md_tiles` table, mirroring how `MultiBandGdalSource`
/// stores its tiles. The meta data itself becomes a typed composite carrying only the
/// result descriptor, the variables and the dataset-level property `wrap`.
pub struct Migration0037MdDatasetTiles;

#[async_trait]
impl Migration for Migration0037MdDatasetTiles {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0036RemoveArunaProvider.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0037_md_dataset_tiles".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0037_md_dataset_tiles.sql"))
            .await?;

        Ok(())
    }
}
