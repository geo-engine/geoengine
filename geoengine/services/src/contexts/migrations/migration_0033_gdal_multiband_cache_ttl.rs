use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0032_stac_provider_cache_ttl::Migration0032StacProviderCacheTtl,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds an optional dataset-level cache TTL to multi-band GDAL definitions.
pub struct Migration0033GdalMultibandCacheTtl;

#[async_trait]
impl Migration for Migration0033GdalMultibandCacheTtl {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0032StacProviderCacheTtl.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0033_gdal_multiband_cache_ttl".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0033_gdal_multiband_cache_ttl.sql"))
            .await?;
        Ok(())
    }
}
