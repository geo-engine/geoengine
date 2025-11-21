use super::database_migration::{DatabaseVersion, Migration};
use crate::{contexts::migrations::Migration0031TimeDescriptor, error::Result};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds support for GDAL tiles in the database schema.
pub struct Migration0032GdalTiles;

#[async_trait]
impl Migration for Migration0032GdalTiles {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0031TimeDescriptor.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0032_gdal_tiles".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0032_gdal_tiles.sql"))
            .await?;

        Ok(())
    }
}
