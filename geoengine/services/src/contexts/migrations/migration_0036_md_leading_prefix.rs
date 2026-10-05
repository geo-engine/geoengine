use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0035_md_dataset_tiles::Migration0035MdDatasetTiles,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// Adds the leading prefix to an MD dataset's per-file rows, which is what makes a 4D array
/// `(time, depth, y, x)` readable as a set of depth bands in one dataset.
pub struct Migration0036MdLeadingPrefix;

#[async_trait]
impl Migration for Migration0036MdLeadingPrefix {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0035MdDatasetTiles.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0036_md_leading_prefix".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0036_md_leading_prefix.sql"))
            .await?;

        Ok(())
    }
}
