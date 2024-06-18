use async_trait::async_trait;
use tokio_postgres::Transaction;

use super::database_migration::{ProMigration, ProMigrationImpl};
use crate::{contexts::Migration0010S2StacTimeBuffers, error::Result};

#[async_trait]
impl ProMigration for ProMigrationImpl<Migration0010S2StacTimeBuffers> {
    async fn pro_migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        // add priority column to layer_providers table and move description column to the definition types
        tx.batch_execute(
            r#"
            CREATE TYPE "StacQueryBuffer" AS (start_seconds bigint, end_seconds bigint);

            ALTER TYPE "SentinelS2L2ACogsProviderDefinition"
            ADD ATTRIBUTE query_buffer "StacQueryBuffer";            
        "#,
        )
        .await?;

        tx.batch_execute(
            r"
            UPDATE pro_layer_providers
            SET
                definition.sentinel_s2_l2_a_cogs_provider_definition.query_buffer.start_seconds = -60,
                definition.sentinel_s2_l2_a_cogs_provider_definition.query_buffer.end_seconds = 60
            WHERE NOT ((definition).sentinel_s2_l2_a_cogs_provider_definition IS NULL);
            ",
        ).await?;
        Ok(())
    }
}
