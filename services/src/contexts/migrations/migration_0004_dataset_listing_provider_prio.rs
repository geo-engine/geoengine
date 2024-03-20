use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds the dataset layer listing provider prios and moves the description column to the definition types
pub struct Migration0004DatasetListingProviderPrio;

#[async_trait]
impl Migration for Migration0004DatasetListingProviderPrio {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0003_gbif_config".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0004_dataset_listing_provider_prio".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        // add priority column to layer_providers table and move description column to the definition types
        tx.batch_execute(
            r#"
            ALTER TABLE layer_providers
            ADD COLUMN priority smallint NOT NULL DEFAULT 0;

            ALTER TYPE "DatasetLayerListingProviderDefinition"
            ADD ATTRIBUTE priority smallint;

            ALTER TYPE "ArunaDataProviderDefinition"
            ADD ATTRIBUTE description text;
            ALTER TYPE "ArunaDataProviderDefinition"
            ADD ATTRIBUTE priority smallint;

            ALTER TYPE "GbifDataProviderDefinition"
            ADD ATTRIBUTE description text;
            ALTER TYPE "GbifDataProviderDefinition"
            ADD ATTRIBUTE priority smallint;

            ALTER TYPE "GfbioAbcdDataProviderDefinition"
            ADD ATTRIBUTE description text;
            ALTER TYPE "GfbioAbcdDataProviderDefinition"
            ADD ATTRIBUTE priority smallint;

            ALTER TYPE "GfbioCollectionsDataProviderDefinition"
            ADD ATTRIBUTE description text;
            ALTER TYPE "GfbioCollectionsDataProviderDefinition"
            ADD ATTRIBUTE priority smallint;

            ALTER TYPE "EbvPortalDataProviderDefinition"
            ADD ATTRIBUTE description text;
            ALTER TYPE "EbvPortalDataProviderDefinition"
            ADD ATTRIBUTE priority smallint;

            ALTER TYPE "NetCdfCfDataProviderDefinition"
            ADD ATTRIBUTE description text;
            ALTER TYPE "NetCdfCfDataProviderDefinition"
            ADD ATTRIBUTE priority smallint;

            ALTER TYPE "PangaeaDataProviderDefinition"
            ADD ATTRIBUTE description text;
            ALTER TYPE "PangaeaDataProviderDefinition"
            ADD ATTRIBUTE priority smallint;

            ALTER TYPE "EdrDataProviderDefinition"
            ADD ATTRIBUTE description text;
            ALTER TYPE "EdrDataProviderDefinition"
            ADD ATTRIBUTE priority smallint;
        "#,
        )
        .await?;

        // as ALTER TYPE ADD ATTRIBUTE does not support default values we manually have to set the default value of the new field in the layer_providers table
        tx.batch_execute(
            r"

            UPDATE layer_providers
            SET 
                definition.dataset_layer_listing_provider_definition.description = 'Catalog of your personal data and workflows',
                definition.dataset_layer_listing_provider_definition.name = 'Personal Data Catalog',
                name = 'Personal Data Catalog',
                priority = 100            
            WHERE NOT ((definition).dataset_layer_listing_provider_definition IS NULL);

            UPDATE layer_providers
            SET
                definition.aruna_data_provider_definition.description = 'Access to NFDI4Bio data stored in Aruna',
                priority = -10
            WHERE NOT ((definition).aruna_data_provider_definition IS NULL);

            UPDATE layer_providers
            SET 
                definition.gbif_data_provider_definition.description = 'Access to GBIF occurrence data',
                priority = -20
            WHERE NOT ((definition).gbif_data_provider_definition IS NULL);

            UPDATE layer_providers
            SET
                definition.gfbio_abcd_data_provider_definition.description = 'Access to GFBio ABCD data',
                priority = -30
            WHERE NOT ((definition).gfbio_abcd_data_provider_definition IS NULL);

            UPDATE layer_providers
            SET 
                definition.gfbio_collections_data_provider_definition.description = 'Access to GFBio collections',
                priority = -40
            WHERE NOT ((definition).gfbio_collections_data_provider_definition IS NULL);
            
            UPDATE layer_providers
            SET 
                definition.ebv_portal_data_provider_definition.description = 'Access to EBV Portal data',
                priority = -50
            WHERE NOT ((definition).ebv_portal_data_provider_definition IS NULL);

            UPDATE layer_providers
            SET 
                definition.net_cdf_cf_data_provider_definition.description = 'Access to NetCDF CF data',
                priority = -32768
            WHERE NOT ((definition).net_cdf_cf_data_provider_definition IS NULL);

            UPDATE layer_providers
            SET
                definition.pangaea_data_provider_definition.description = 'Access to data stored in Pangaea',
                priority = -70
            WHERE NOT ((definition).pangaea_data_provider_definition IS NULL);

            UPDATE layer_providers
            SET
                definition.edr_data_provider_definition.description = 'Access to EDR data',
                priority = -80
            WHERE NOT ((definition).edr_data_provider_definition IS NULL);          


        ",
        )
        .await?;

        Ok(())
    }
}
