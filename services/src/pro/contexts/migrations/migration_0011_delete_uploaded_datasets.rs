use async_trait::async_trait;
use tokio_postgres::Transaction;

use super::database_migration::{ProMigration, ProMigrationImpl};
use crate::{contexts::Migration0011DeleteUploadedDatasets, error::Result};

#[async_trait]
impl ProMigration for ProMigrationImpl<Migration0011DeleteUploadedDatasets> {
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

                CREATE TYPE "DatasetDeletionType" AS ENUM (
                    'DeleteRecordAndData',
                    'DeleteData'
                );

                CREATE TABLE uploaded_user_datasets (
                    user_id uuid,
                    upload_id uuid,
                    dataset_id uuid,
                    status "InternalUploadedDatasetStatus" NOT NULL,
                    created timestamp with time zone NOT NULL,
                    expiration timestamp with time zone,
                    deleted timestamp with time zone,
                    deletion_type "DatasetDeletionType",
                    PRIMARY KEY (user_id, dataset_id, upload_id)
                );

                CREATE VIEW updatable_uploaded_user_datasets AS
                SELECT
                    u.dataset_id,
                    u.user_id,
                    u.status,
                    u.deletion_type
                FROM
                    uploaded_user_datasets AS u INNER JOIN
                    user_permitted_datasets AS p ON (u.user_id = p.user_id)
                WHERE
                    u.expiration <= CURRENT_TIMESTAMP
                    AND (u.status = 'Expires' OR u.status = 'UpdateExpired');
            "#,
        )
        .await?;

        Ok(())
    }
}
