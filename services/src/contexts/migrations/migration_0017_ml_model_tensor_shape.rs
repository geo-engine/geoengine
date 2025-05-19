use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds tensor shape to `MlModel` input and output
pub struct Migration0017MlModelTensorShape;

#[async_trait]
impl Migration for Migration0017MlModelTensorShape {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0016_merge_providers".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0017_ml_model_tensor_shape".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(
            r#"
                CREATE TYPE "MlTensorShape3D" AS (
                    x OID,
                    y OID,
                    bands OID
                );

                ALTER TYPE "MlModelMetadata" ADD ATTRIBUTE input_shape "MlTensorShape3D";
                ALTER TYPE "MlModelMetadata" ADD ATTRIBUTE output_shape "MlTensorShape3D";
            
                WITH qqqq AS (
                    SELECT 
                        id, 
                        metadata
                    FROM ml_models
                    WHERE (metadata).num_input_bands IS NOT NULL
                )
                UPDATE ml_models
                SET 
                    metadata.input_shape = (1, 1, (qqqq.metadata).num_input_bands)::"MlTensorShape3D",
                    metadata.output_shape = (1, 1, 1)::"MlTensorShape3D"
                FROM qqqq
                WHERE ml_models.id = qqqq.id;
                
                ALTER TYPE "MlModelMetadata" DROP ATTRIBUTE num_input_bands;
            "#,
        )
        .await?;
        Ok(())
    }
}
