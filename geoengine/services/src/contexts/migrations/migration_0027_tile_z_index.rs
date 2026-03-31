use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0026_gdal_tiles::Migration0026GdalTiles, error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration changes the z index of tiles to i64
pub struct Migration0027TileZIndex;

#[async_trait]
impl Migration for Migration0027TileZIndex {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0026GdalTiles.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0027_tile_z_index".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0027_tile_z_index.sql"))
            .await?;

        Ok(())
    }
}
