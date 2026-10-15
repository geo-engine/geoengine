use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0033_gdal_multiband_cache_ttl::Migration0033GdalMultibandCacheTtl,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

pub struct Migration0034StacGrid;

#[async_trait]
impl Migration for Migration0034StacGrid {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0033GdalMultibandCacheTtl.version())
    }
    fn version(&self) -> DatabaseVersion {
        "0034_stac_grid".into()
    }
    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0034_stac_grid.sql"))
            .await?;
        Ok(())
    }
}
