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
        tx.batch_execute(include_str!("migration_0019_ml_model_no_data.sql"))
            .await?;
        Ok(())
    }
}
