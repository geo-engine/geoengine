use super::database_migration::{DatabaseVersion, Migration};
use crate::{contexts::migrations::migration_0035_stac_grid::Migration0035StacGrid, error::Result};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds an optional tile size override to GDAL dataset parameters.
/// Existing datasets get `NULL`, so they keep using the configured default tile size.
pub struct Migration0036GdalTileSize;

#[async_trait]
impl Migration for Migration0036GdalTileSize {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0035StacGrid.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0036_gdal_tile_size".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0036_gdal_tile_size.sql"))
            .await?;
        Ok(())
    }
}
