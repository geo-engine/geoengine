use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds new delete options for uploaded user datasets
pub struct Migration0012FairUploadDeletion;

#[async_trait]
impl Migration for Migration0012FairUploadDeletion {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0011_remove_xgb".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0012_fair_upload_deletion".into()
    }

    async fn migrate(&self, _tx: &Transaction<'_>) -> Result<()> {
        Ok(())
    }
}
