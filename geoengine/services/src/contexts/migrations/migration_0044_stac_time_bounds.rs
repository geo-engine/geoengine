use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0043_stac_datetime_frequency_aggregation::Migration0043StacDatetimeFrequencyAggregation,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// Adds to the STAC provider definition the time of its datasets, for collections whose temporal
/// extent is missing or wrong. Existing providers keep using the temporal extent of the collection
/// through the `NULL` default.
pub struct Migration0044StacTimeBounds;

#[async_trait]
impl Migration for Migration0044StacTimeBounds {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0043StacDatetimeFrequencyAggregation.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0044_stac_time_bounds".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0044_stac_time_bounds.sql"))
            .await?;
        Ok(())
    }
}
