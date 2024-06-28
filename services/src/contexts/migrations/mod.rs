pub use crate::contexts::migrations::{
    current_schema::CurrentSchemaMigration, migration_0000_initial::Migration0000Initial,
    migration_0001_raster_stacks::Migration0001RasterStacks,
    migration_0002_dataset_listing_provider::Migration0002DatasetListingProvider,
    migration_0003_gbif_config::Migration0003GbifConfig,
    migration_0004_dataset_listing_provider_prio::Migration0004DatasetListingProviderPrio,
    migration_0005_gbif_column_selection::Migration0005GbifColumnSelection,
    migration_0006_ebv_provider::Migration0006EbvProvider,
    migration_0007_owner_role::Migration0007OwnerRole,
    migration_0008_band_names::Migration0008BandNames,
    migration_0009_oidc_tokens::Migration0009OidcTokens,
    migration_0010_delete_uploaded_datasets::Migration0010DeleteUploadedDatasets,
};
pub use database_migration::{
    initialize_database, migrate_database, DatabaseVersion, Migration, MigrationResult,
};

mod current_schema;
mod database_migration;
pub mod migration_0000_initial;
pub mod migration_0001_raster_stacks;
pub mod migration_0002_dataset_listing_provider;
pub mod migration_0003_gbif_config;
pub mod migration_0004_dataset_listing_provider_prio;
pub mod migration_0005_gbif_column_selection;
mod migration_0006_ebv_provider;
pub mod migration_0007_owner_role;
pub mod migration_0008_band_names;
pub mod migration_0009_oidc_tokens;
pub mod migration_0010_delete_uploaded_datasets;

#[cfg(test)]
mod schema_info;

#[cfg(test)]
pub(crate) use schema_info::{assert_migration_schema_eq, AssertSchemaEqPopulationConfig};

/// All migrations that are available. The migrations are applied in the order they are defined here, starting from the current version of the database.
///
/// NEW MIGRATIONS HAVE TO BE REGISTERED HERE!
///
pub fn all_migrations() -> Vec<Box<dyn Migration>> {
    vec![
        Box::new(Migration0000Initial),
        Box::new(Migration0001RasterStacks),
        Box::new(Migration0002DatasetListingProvider),
        Box::new(Migration0003GbifConfig),
        Box::new(Migration0004DatasetListingProviderPrio),
        Box::new(Migration0005GbifColumnSelection),
        Box::new(Migration0006EbvProvider),
        Box::new(Migration0007OwnerRole),
        Box::new(Migration0008BandNames),
        Box::new(Migration0009OidcTokens),
        Box::new(Migration0010DeleteUploadedDatasets),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn migrations_lead_to_ground_truth_schema() {
        assert_migration_schema_eq(
            &all_migrations(),
            include_str!("current_schema.sql"),
            AssertSchemaEqPopulationConfig {
                has_views: false,
                has_parameters: false,
                ..Default::default()
            },
        )
        .await;
    }

    #[test]
    fn versions_follow_schema() {
        for migration in all_migrations() {
            let version = migration.version();
            let (version_number, _version_name) = version.split_once('_').unwrap();
            assert_eq!(
                version_number.len(),
                4,
                "Version number {version_number} has to be 4 digits"
            );
            assert!(
                version_number.chars().all(char::is_numeric),
                "Version number {version_number} has to be numeric"
            );
        }
    }
}
