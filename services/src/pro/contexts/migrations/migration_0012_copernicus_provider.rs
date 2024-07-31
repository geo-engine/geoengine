use async_trait::async_trait;
use tokio_postgres::Transaction;

use super::database_migration::{ProMigration, ProMigrationImpl};
use crate::{contexts::Migration0012CopernicusProvider, error::Result};

#[async_trait]
impl ProMigration for ProMigrationImpl<Migration0012CopernicusProvider> {
    async fn pro_migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(
            r#"
                CREATE TYPE "CopernicusDataspaceDataProviderDefinition" AS (
                    "name" text,
                    id uuid,
                    stac_url text,
                    s3_url text,
                    s3_access_key text,
                    s3_secret_key text,
                    description text,
                    priority smallint
                );
                
                ALTER TYPE "ProDataProviderDefinition"
                ADD ATTRIBUTE "copernicus_dataspace_provider_definition" "CopernicusDataspaceDataProviderDefinition";
            "#,
        )
        .await?;

        Ok(())
    }
}
