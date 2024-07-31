use async_trait::async_trait;
use tokio_postgres::Transaction;

use super::database_migration::{ProMigration, ProMigrationImpl};
use crate::{contexts::Migration0011RemoveXgb, error::Result};

#[async_trait]
impl ProMigration for ProMigrationImpl<Migration0011RemoveXgb> {
    async fn pro_migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute("DROP TABLE IF EXISTS ml_models;").await?;
        Ok(())
    }
}
