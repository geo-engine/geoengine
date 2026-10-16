use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0034_plots_single_raster_source::Migration0034PlotsSingleRasterSource,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration moves the per-file data of an `MdGdalSource` out of the dataset's
/// meta data and into a `dataset_md_tiles` table, mirroring how `MultiBandGdalSource`
/// stores its tiles. The meta data itself becomes a typed composite carrying only the
/// result descriptor, the variables and the dataset-level property `wrap`.
pub struct Migration0035MdDatasetTiles;

#[async_trait]
impl Migration for Migration0035MdDatasetTiles {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0034PlotsSingleRasterSource.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0035_md_dataset_tiles".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0035_md_dataset_tiles.sql"))
            .await?;

        Ok(())
    }
}
