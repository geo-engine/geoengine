use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0042_stac_netcdf_assets::Migration0042StacNetcdfAssets,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// Adds to the STAC provider definition whether its server answers `datetime_frequency`
/// aggregations in the bucket size it is asked for. An answer does not tell the size of its
/// buckets, so this is a fact about the server that the provider has to state. Existing providers
/// do not use aggregations through the `NULL` default.
pub struct Migration0043StacDatetimeFrequencyAggregation;

#[async_trait]
impl Migration for Migration0043StacDatetimeFrequencyAggregation {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0042StacNetcdfAssets.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0043_stac_datetime_frequency_aggregation".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!(
            "migration_0043_stac_datetime_frequency_aggregation.sql"
        ))
        .await?;
        Ok(())
    }
}
