use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0031_stac_provider_authentication::Migration0031StacProviderAuthentication,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the optional provider-level cache TTL to STAC definitions.
pub struct Migration0032StacProviderCacheTtl;

#[async_trait]
impl Migration for Migration0032StacProviderCacheTtl {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0031StacProviderAuthentication.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0032_stac_provider_cache_ttl".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0032_stac_provider_cache_ttl.sql"))
            .await?;
        Ok(())
    }
}
