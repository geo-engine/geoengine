use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0041_non_zero_max_overview_level::Migration0041NonZeroMaxOverviewLevel,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// Adds two STAC provider settings for assets without STAC grid metadata, such as `NetCDF` files:
/// whether an item's time range decides its time steps, and how many asset file headers a search
/// reads in parallel. Existing providers get `NULL` and keep their behavior.
pub struct Migration0042StacNetcdfAssets;

#[async_trait]
impl Migration for Migration0042StacNetcdfAssets {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0041NonZeroMaxOverviewLevel.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0042_stac_netcdf_assets".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0042_stac_netcdf_assets.sql"))
            .await?;
        Ok(())
    }
}
