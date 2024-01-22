use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds the dataset layer listing provider
pub struct Migration0002DatasetListingProvider;

#[async_trait]
impl Migration for Migration0002DatasetListingProvider {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0001_raster_stacks".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0002_dataset_listing_provider".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(
            r#"   
            CREATE TYPE "DatasetLayerListingCollection" AS (
                "name" text,
                description text,
                tags text []
            );

            CREATE TYPE "DatasetLayerListingProviderDefinition" AS (
                id uuid,
                "name" text,
                description text,
                collections "DatasetLayerListingCollection" []
            );

            ALTER TYPE "DataProviderDefinition" 
            ADD ATTRIBUTE dataset_layer_listing_provider_definition "DatasetLayerListingProviderDefinition";
        "#,
        )
        .await?;

        Ok(())
    }
}
