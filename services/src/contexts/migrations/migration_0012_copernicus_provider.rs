use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration the Copernicus provider
pub struct Migration0012CopernicusProvider;

#[async_trait]
impl Migration for Migration0012CopernicusProvider {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0011_remove_xgb".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0012_copernicus_provider".into()
    }

    async fn migrate(&self, _tx: &Transaction<'_>) -> Result<()> {
        // provider only exists in pro
        Ok(())
    }
}
