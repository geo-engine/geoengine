use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0029_stac_provider_band_name::Migration0029StacProviderBandName,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds optional OAuth password-grant authentication to STAC providers.
pub struct Migration0030StacProviderAuthentication;

#[async_trait]
impl Migration for Migration0030StacProviderAuthentication {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0029StacProviderBandName.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0030_stac_provider_authentication".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!(
            "migration_0030_stac_provider_authentication.sql"
        ))
        .await?;

        Ok(())
    }
}
