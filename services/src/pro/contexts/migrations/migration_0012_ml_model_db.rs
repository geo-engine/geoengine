use async_trait::async_trait;
use tokio_postgres::Transaction;

use super::database_migration::{ProMigration, ProMigrationImpl};
use crate::{contexts::Migration0012MlModelDb, error::Result};

#[async_trait]
impl ProMigration for ProMigrationImpl<Migration0012MlModelDb> {
    async fn pro_migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0012_ml_model_db.sql"))
            .await?;

        Ok(())
    }
}
