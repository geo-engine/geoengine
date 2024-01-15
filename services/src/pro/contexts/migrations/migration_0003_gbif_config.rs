use async_trait::async_trait;
use tokio_postgres::Transaction;

use super::database_migration::{ProMigration, ProMigrationImpl};
use crate::contexts::Migration0003GbifConfig;
use crate::error::Result;

#[async_trait]
impl ProMigration for ProMigrationImpl<Migration0003GbifConfig> {
    async fn pro_migrate(&self, _tx: &Transaction<'_>) -> Result<()> {
        // nothing to do

        Ok(())
    }
}
