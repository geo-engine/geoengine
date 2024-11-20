use async_trait::async_trait;
use tokio_postgres::Transaction;

use super::database_migration::{ProMigration, ProMigrationImpl};
use crate::{contexts::Migration0015LogQuota, error::Result};

#[async_trait]
impl ProMigration for ProMigrationImpl<Migration0015LogQuota> {
    async fn pro_migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(
            r"
                CREATE TABLE quota_log (
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    user_id UUID NOT NULL,
                    workflow_id UUID NOT NULL,
                    computation_id UUID NOT NULL,
                    operator_path TEXT NOT NULL
                );

                CREATE INDEX ON quota_log (user_id, timestamp, computation_id);
            ",
        )
        .await?;

        Ok(())
    }
}
