use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds a check that there is only one owner per resource
pub struct Migration0007OwnerRole;

#[async_trait]
impl Migration for Migration0007OwnerRole {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0006_ebv_provider".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0007_owner_role".into()
    }

    async fn migrate(&self, _tx: &Transaction<'_>) -> Result<()> {
        // permissions only exist in Pro, nothing to do here

        Ok(())
    }
}
