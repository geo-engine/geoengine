use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration removes XGB related entries
pub struct Migration0011RemoveXgb;

#[async_trait]
impl Migration for Migration0011RemoveXgb {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0010_s2_stac_time_buffers".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0011_remove_xgb".into()
    }

    async fn migrate(&self, _tx: &Transaction<'_>) -> Result<()> {
        // XGB only exist(ed) in Pro, nothing to do here

        Ok(())
    }
}
