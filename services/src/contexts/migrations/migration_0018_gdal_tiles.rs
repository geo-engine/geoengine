use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0017_raster_result_desc::Migration0017RasterResultDesc,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds support for GDAL tiles in the database schema.
pub struct Migration0018GdalTiles;

#[async_trait]
impl Migration for Migration0018GdalTiles {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0017RasterResultDesc.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0018_gdal_tiles".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0018_gdal_tiles.sql"))
            .await?;

        Ok(())
    }
}
