use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0039_max_overview_level::Migration0039MaxOverviewLevel,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds an optional tile size override to GDAL dataset parameters.
/// Existing datasets get `NULL`, so they keep using the configured default tile size.
pub struct Migration0040GdalTileSize;

#[async_trait]
impl Migration for Migration0040GdalTileSize {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0039MaxOverviewLevel.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0040_gdal_tile_size".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0040_gdal_tile_size.sql"))
            .await?;
        Ok(())
    }
}
