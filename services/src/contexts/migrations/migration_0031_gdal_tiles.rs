use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0030_raster_result_desc::Migration0030RasterResultDesc,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds support for GDAL tiles in the database schema.
pub struct Migration0031GdalTiles;

#[async_trait]
impl Migration for Migration0031GdalTiles {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0030RasterResultDesc.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0031_gdal_tiles".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0031_gdal_tiles.sql"))
            .await?;

        Ok(())
    }
}
