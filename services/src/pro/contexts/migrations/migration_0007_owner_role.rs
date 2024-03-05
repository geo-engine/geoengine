use async_trait::async_trait;
use tokio_postgres::Transaction;

use super::database_migration::{ProMigration, ProMigrationImpl};
use crate::{contexts::Migration0007OwnerRole, error::Result};

#[async_trait]
impl ProMigration for ProMigrationImpl<Migration0007OwnerRole> {
    async fn pro_migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(
            "
                CREATE UNIQUE INDEX permissions_datasets_owner_unique ON permissions (dataset_id) WHERE permission = 'Owner';
                CREATE UNIQUE INDEX permissions_layer_owner_unique ON permissions (layer_id) WHERE permission = 'Owner';
                CREATE UNIQUE INDEX permissions_layer_collections_owner_unique ON permissions (layer_collection_id) WHERE permission = 'Owner';
                CREATE UNIQUE INDEX permissions_projects_owner_unique ON permissions (project_id) WHERE permission = 'Owner';
            ",
        )
        .await?;

        Ok(())
    }
}
