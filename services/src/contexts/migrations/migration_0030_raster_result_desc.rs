use super::database_migration::{DatabaseVersion, Migration};
use crate::{contexts::migrations::Migration0020ProviderPermissions, error::Result};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration reworks the raster result descritptor and some other small changes from the rewrite branch
pub struct Migration0030RasterResultDesc;

#[async_trait]
impl Migration for Migration0030RasterResultDesc {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0020ProviderPermissions.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0019_raster_result_desc".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0030_remove_stack_zone_band.sql"))
            .await?;

        tx.batch_execute(include_str!("migration_0030_raster_result_desc.sql"))
            .await?;
        Ok(())
    }
}
