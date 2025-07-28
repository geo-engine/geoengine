use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0019_ml_model_no_data::Migration0019MlModelNoData,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration reworks the raster result descritptor and some other small changes from the rewrite branch
pub struct Migration0020RasterResultDesc;

#[async_trait]
impl Migration for Migration0020RasterResultDesc {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0019MlModelNoData.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0019_raster_result_desc".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0020_remove_stack_zone_band.sql"))
            .await?;

        tx.batch_execute(include_str!("migration_0020_raster_result_desc.sql"))
            .await?;
        Ok(())
    }
}
