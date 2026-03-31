use super::database_migration::{DatabaseVersion, Migration};
use crate::{contexts::migrations::Migration0025TimeDescriptor, error::Result};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds support for GDAL tiles in the database schema.
pub struct Migration0026GdalTiles;

#[async_trait]
impl Migration for Migration0026GdalTiles {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0025TimeDescriptor.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0026_gdal_tiles".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0026_gdal_tiles.sql"))
            .await?;

        Ok(())
    }
}
