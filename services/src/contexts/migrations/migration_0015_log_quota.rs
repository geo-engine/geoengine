use super::{
    database_migration::{DatabaseVersion, Migration},
    Migration0014MultibandColorizer,
};
use crate::error::Result;
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the multiband raster colorizer
pub struct Migration0015LogQuota;

#[async_trait]
impl Migration for Migration0015LogQuota {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0014MultibandColorizer.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0015_log_quota".into()
    }

    async fn migrate(&self, _tx: &Transaction<'_>) -> Result<()> {
        // Nothing to do here, only in Pro
        Ok(())
    }
}
