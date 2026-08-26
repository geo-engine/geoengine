use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0030_stac_provider_band_name::Migration0030StacProviderBandName,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds optional OAuth password-grant authentication to STAC providers.
pub struct Migration0031StacProviderAuthentication;

#[async_trait]
impl Migration for Migration0031StacProviderAuthentication {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0030StacProviderBandName.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0031_stac_provider_authentication".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!(
            "migration_0031_stac_provider_authentication.sql"
        ))
        .await?;

        Ok(())
    }
}
