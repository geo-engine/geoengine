use super::database_migration::{DatabaseVersion, Migration};
use crate::{contexts::migrations::Migration0028StacProvider, error::Result};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the STAC provider definition to the provider union type.
pub struct Migration0029WildliveOptionalFields;

#[async_trait]
impl Migration for Migration0029WildliveOptionalFields {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0028StacProvider.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0029_wildlive_optional_fields".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0029_wildlive_optional_fields.sql"))
            .await?;

        Ok(())
    }
}
