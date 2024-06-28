use async_trait::async_trait;
use tokio_postgres::Transaction;

use super::database_migration::{ProMigration, ProMigrationImpl};
use crate::{contexts::Migration0010DeleteUploadedDatasets, error::Result};

#[async_trait]
impl ProMigration for ProMigrationImpl<Migration0010DeleteUploadedDatasets> {
    async fn pro_migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(
            r#"
                CREATE TYPE "InternalUploadedDatasetStatus" AS ENUM (
                    'Available',
                    'Expires',
                    'Expired',
                    'UpdateExpired',
                    'Deleted',
                    'DeletedWithError'
                );

                CREATE TABLE uploaded_user_datasets (
                    user_id uuid,
                    upload_id uuid,
                    dataset_id uuid,
                    status "InternalUploadedDatasetStatus" NOT NULL,
                    created timestamp with time zone NOT NULL,
                    expiration timestamp with time zone,
                    deleted timestamp with time zone,
                    delete_data boolean NOT NULL,
                    delete_record boolean NOT NULL,
                    PRIMARY KEY (user_id, dataset_id, upload_id)
                );
            "#,
        )
        .await?;

        Ok(())
    }
}
