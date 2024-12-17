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
                    timestamp timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    user_id uuid NOT NULL,
                    workflow_id uuid NOT NULL,
                    computation_id uuid NOT NULL,
                    operator_name text NOT NULL,
                    operator_path text NOT NULL,
                    data text
                );

                CREATE INDEX ON quota_log (user_id, timestamp, computation_id);
            ",
        )
        .await?;

        Ok(())
    }
}
