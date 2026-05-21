use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0027_tile_z_index::Migration0027TileZIndex, error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the STAC provider definition to the provider union type.
pub struct Migration0028StacProvider;

#[async_trait]
impl Migration for Migration0028StacProvider {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0027TileZIndex.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0028_stac_provider".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0028_stac_provider.sql"))
            .await?;

        Ok(())
    }
}
