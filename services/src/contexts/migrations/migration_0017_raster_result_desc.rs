use super::{
    database_migration::{DatabaseVersion, Migration},
    Migration0016MergeProviders,
};
use crate::error::Result;
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration reworks the raster result descritptor and some other small changes from the rewrite branch
pub struct Migration0017RasterResultDesc;

#[async_trait]
impl Migration for Migration0017RasterResultDesc {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0016MergeProviders.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0017_raster_result_desc".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0017_remove_stack_zone_band.sql"))
            .await?;

        tx.batch_execute(include_str!("migration_0017_raster_result_desc.sql"))
            .await?;
        Ok(())
    }
}
