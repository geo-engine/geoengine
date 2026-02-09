use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0032_gdal_tiles::Migration0032GdalTiles, error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration changes the z index of tiles to i64
pub struct Migration0033TileZIndex;

#[async_trait]
impl Migration for Migration0033TileZIndex {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0032GdalTiles.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0033_tile_z_index".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0033_tile_z_index.sql"))
            .await?;

        Ok(())
    }
}
