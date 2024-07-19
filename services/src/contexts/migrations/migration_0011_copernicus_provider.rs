use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration the Copernicus provider
pub struct Migration0011CopernicusProvider;

#[async_trait]
impl Migration for Migration0011CopernicusProvider {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0010_s2_stac_time_buffers".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0011_copernicus_provider".into()
    }

    async fn migrate(&self, _tx: &Transaction<'_>) -> Result<()> {
        // provider only exists in pro
        Ok(())
    }
}
