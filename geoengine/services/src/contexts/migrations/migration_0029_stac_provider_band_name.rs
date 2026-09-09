use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0028_stac_provider::Migration0028StacProvider, error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration bundles the band addressing fields of `StacProviderDatasetBand`
/// into a nested `StacAssetBand` type and adds a `band_descriptor` attribute of
/// type `RasterBandDescriptor` for the band in the resulting geo engine dataset
/// layer. It also takes over the `page_limit` attribute of
/// `StacDataProviderDefinition` from the released migration 0028.
pub struct Migration0029StacProviderBandName;

#[async_trait]
impl Migration for Migration0029StacProviderBandName {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0028StacProvider.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0029_stac_provider_band_name".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0029_stac_provider_band_name.sql"))
            .await?;

        let dropped_providers = tx
            .execute(
                "
                DELETE FROM layer_providers
                WHERE (definition).stac_data_provider_definition IS NOT NULL
                ",
                &[],
            )
            .await?;

        if dropped_providers > 0 {
            tracing::warn!(
                "Dropped {dropped_providers} existing STAC provider(s) during migration 0029; \
                 they could not be reliably migrated to the new schema"
            );
        }

        Ok(())
    }
}
