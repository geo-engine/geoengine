use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0038_stac_max_concurrent_cell_searches::Migration0038StacMaxConcurrentCellSearches,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the overview levels a source dataset declares to its spatial grid
/// descriptor, so that sources read overviews only where they exist and never let GDAL
/// resample. Existing datasets get `NULL`: unknown levels, read as before.
pub struct Migration0039MaxOverviewLevel;

#[async_trait]
impl Migration for Migration0039MaxOverviewLevel {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0038StacMaxConcurrentCellSearches.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0039_max_overview_level".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0039_max_overview_level.sql"))
            .await?;

        Ok(())
    }
}
