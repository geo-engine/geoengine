use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds a configurable time buffer to the s2 stac provider
pub struct Migration0010S2StacTimeBuffers;

#[async_trait]
impl Migration for Migration0010S2StacTimeBuffers {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0009_oidc_tokens".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0010_s2_stac_time_buffers".into()
    }

    async fn migrate(&self, _tx: &Transaction<'_>) -> Result<()> {
        // provider only exists in Pro, nothing to do here

        Ok(())
    }
}
