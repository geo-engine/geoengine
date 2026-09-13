use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0033_gdal_multiband_cache_ttl::Migration0033GdalMultibandCacheTtl,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration moves the per-file data of an `MdGdalSource` out of the dataset's
/// meta data and into a `dataset_md_tiles` table, mirroring how `MultiBandGdalSource`
/// stores its tiles. The meta data itself becomes a typed composite carrying only the
/// result descriptor and the two dataset-level properties (`z_role`, `wrap`).
pub struct Migration0034MdDatasetTiles;

#[async_trait]
impl Migration for Migration0034MdDatasetTiles {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0033GdalMultibandCacheTtl.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0034_md_dataset_tiles".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0034_md_dataset_tiles.sql"))
            .await?;

        Ok(())
    }
}