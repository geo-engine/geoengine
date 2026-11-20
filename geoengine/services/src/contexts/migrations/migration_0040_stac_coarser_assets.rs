use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0039_max_overview_level::Migration0039MaxOverviewLevel,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds coarser assets to the bands of STAC provider datasets, so that the
/// source reads smaller files for overview levels. Existing bands get `NULL`: no coarser
/// assets, read as before.
pub struct Migration0040StacCoarserAssets;

#[async_trait]
impl Migration for Migration0040StacCoarserAssets {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0039MaxOverviewLevel.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0040_stac_coarser_assets".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0040_stac_coarser_assets.sql"))
            .await?;

        Ok(())
    }
}
