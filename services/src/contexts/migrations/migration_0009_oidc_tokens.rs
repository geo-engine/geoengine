use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds Open ID Connect token storage
pub struct Migration0009OidcTokens;

#[async_trait]
impl Migration for Migration0009OidcTokens {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0008_band_names".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0009_oidc_tokens".into()
    }

    async fn migrate(&self, _tx: &Transaction<'_>) -> Result<()> {
        Ok(())
    }
}
