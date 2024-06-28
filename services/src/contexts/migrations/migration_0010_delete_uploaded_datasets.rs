use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds new delete options for uploaded user datasets
pub struct Migration0010DeleteUploadedDatasets;

#[async_trait]
impl Migration for Migration0010DeleteUploadedDatasets {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0009_oidc_tokens".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0010_delete_uploaded_datasets".into()
    }

    async fn migrate(&self, _tx: &Transaction<'_>) -> Result<()> {
        Ok(())
    }
}
