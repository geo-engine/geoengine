use async_trait::async_trait;
use tokio_postgres::Transaction;

use super::database_migration::{ProMigration, ProMigrationImpl};
use crate::{contexts::Migration0004DatasetListingProviderPrio, error::Result};

#[async_trait]
impl ProMigration for ProMigrationImpl<Migration0004DatasetListingProviderPrio> {
    async fn pro_migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        // add priority column to layer_providers table and move description column to the definition types
        tx.batch_execute(
            r#"
            ALTER TABLE pro_layer_providers
            ADD COLUMN priority smallint NOT NULL DEFAULT 0;

            ALTER TYPE "SentinelS2L2ACogsProviderDefinition"
            ADD ATTRIBUTE description text;
            ALTER TYPE "SentinelS2L2ACogsProviderDefinition"
            ADD ATTRIBUTE priority smallint;
        "#,
        )
        .await?;

        tx.batch_execute(
            r"
            UPDATE pro_layer_providers
            SET
                definition.sentinel_s2_l2_a_cogs_provider_definition.name = 'Sentinel-2 L2A COGs',
                definition.sentinel_s2_l2_a_cogs_provider_definition.description = 'Sentinel-2 L2A COGs hosted at AWS by Element 84',
                priority = 0
            WHERE NOT ((definition).sentinel_s2_l2_a_cogs_provider_definition IS NULL);
            ",
        ).await?;
        Ok(())
    }
}
