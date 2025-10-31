use super::database_migration::{DatabaseVersion, Migration};
use crate::{contexts::migrations::Migration0030RasterResultDesc, error::Result};
use async_trait::async_trait;
use tokio_postgres::Transaction;

pub struct Migration0031TimeDescriptor;

#[async_trait]
impl Migration for Migration0031TimeDescriptor {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0030RasterResultDesc.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0031_time_descriptor".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0031_time_descriptor.sql"))
            .await?;

        Ok(())
    }
}
