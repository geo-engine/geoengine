use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds tensor shape to `MlModel` input and output
pub struct Migration0019MlModelNoData;

#[async_trait]
impl Migration for Migration0019MlModelNoData {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0018_widlive_connector".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0019_ml_model_no_data".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(
            r#"
                CREATE TYPE "MlModelInputNoDataHandlingVariant" AS ENUM (
                    'EncodedNoData',
                    'SkipIfNoData'
                );

                CREATE TYPE "MlModelInputNoDataHandling" AS (
                    variant "MlModelInputNoDataHandlingVariant",
                    no_data_value real
                );

                CREATE TYPE "MlModelOutputNoDataHandlingVariant" AS ENUM (
                    'EncodedNoData',
                    'NanIsNoData'
                );

                CREATE TYPE "MlModelOutputNoDataHandling" AS (
                    variant "MlModelOutputNoDataHandlingVariant",
                    no_data_value real
                );

                ALTER TYPE "MlModelMetadata" ADD ATTRIBUTE input_no_data_handling "MlModelInputNoDataHandling";
                ALTER TYPE "MlModelMetadata" ADD ATTRIBUTE output_no_data_handling "MlModelOutputNoDataHandling";
                ALTER TABLE ml_models ADD file_name text;

                WITH qqqq AS (
                    SELECT 
                        id, 
                        metadata,
                        file_name
                    FROM ml_models
                    WHERE (metadata).file_name IS NOT NULL
                )
                UPDATE ml_models
                SET 
                    metadata.input_no_data_handling = ('SkipIfNoData', null),
                    metadata.output_no_data_handling = ('NanIsNoData', null),
                    file_name = qqqq.file_name
                FROM qqqq
                WHERE ml_models.id = qqqq.id;

                ALTER TYPE "MlModelMetadata" DROP ATTRIBUTE file_name;        

            "#,
        )
        .await?;
        Ok(())
    }
}
