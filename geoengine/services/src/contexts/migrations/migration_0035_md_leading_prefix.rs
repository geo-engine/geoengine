use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0034_md_dataset_tiles::Migration0034MdDatasetTiles,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// Adds the leading prefix to an MD dataset's meta data, which is what makes a 4D array
/// `(time, depth, y, x)` readable as one depth.
pub struct Migration0035MdLeadingPrefix;

#[async_trait]
impl Migration for Migration0035MdLeadingPrefix {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0034MdDatasetTiles.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0035_md_leading_prefix".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0035_md_leading_prefix.sql"))
            .await?;

        Ok(())
    }
}
