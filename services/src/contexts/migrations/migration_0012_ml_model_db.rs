use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds the ml model db
pub struct Migration0012MlModelDb;

#[async_trait]
impl Migration for Migration0012MlModelDb {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0010_s2_stac_time_buffers".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0012_ml_model_db".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0012_ml_model_db.sql"))
            .await?;

        Ok(())
    }
}
