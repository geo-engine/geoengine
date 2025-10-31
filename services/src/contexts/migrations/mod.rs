use crate::contexts::migrations::migration_0032_gdal_tiles::Migration0032GdalTiles;
pub use crate::contexts::migrations::{
    current_schema::CurrentSchemaMigration,
    migration_0016_merge_providers::Migration0016MergeProviders,
    migration_0017_ml_model_tensor_shape::Migration0017MlModelTensorShape,
    migration_0018_wildlive_connector::Migration0018WildliveConnector,
    migration_0019_ml_model_no_data::Migration0019MlModelNoData,
    migration_0020_provider_permissions::Migration0020ProviderPermissions,
    migration_0021_default_permissions_for_existing_providers::Migration0021DefaultPermissionsForExistingProviders,
    migration_0022_permission_queries::Migration0022PermissionQueries,
    migration_0030_raster_result_desc::Migration0030RasterResultDesc,
    migration_0031_time_descriptor::Migration0031TimeDescriptor,
};
pub use database_migration::{
    DatabaseVersion, Migration, MigrationResult, initialize_database, migrate_database,
};

mod current_schema;
mod database_migration;
mod migration_0016_merge_providers;
mod migration_0017_ml_model_tensor_shape;
mod migration_0018_wildlive_connector;
mod migration_0019_ml_model_no_data;
mod migration_0020_provider_permissions;
mod migration_0021_default_permissions_for_existing_providers;
mod migration_0022_permission_queries;
mod migration_0030_raster_result_desc;
mod migration_0031_time_descriptor;
mod migration_0032_gdal_tiles;

#[cfg(test)]
mod schema_info;

#[cfg(test)]
pub(crate) use schema_info::{AssertSchemaEqPopulationConfig, assert_migration_schema_eq};

/// All migrations that are available. The migrations are applied in the order they are defined here, starting from the current version of the database.
///
/// NEW MIGRATIONS HAVE TO BE REGISTERED HERE!
///
pub fn all_migrations() -> Vec<Box<dyn Migration>> {
    vec![
        // Upon migration `0015_log_quota`, we did a major refactoring and removed the deprecated pro migrations.
        // Hence, we added a snapshot of the database schema at version 0015_log_quota.
        // This is the state of the database schema at commit `071ba4e636a709f05ecb18b6f01bd19f313b0c94`.
        // Furthermore, we deleted all prior migrations, so migrating from an earlier version is not possible.
        //
        // If you have a database version prior to `0015_log_quota`, you will need to migrate to `0015_log_quota` first.
        // Use commit `071ba4e636a709f05ecb18b6f01bd19f313b0c94` as a reference.
        // Then, you can migrate to the latest version.
        Box::new(Migration0016MergeProviders),
        Box::new(Migration0017MlModelTensorShape),
        Box::new(Migration0018WildliveConnector),
        Box::new(Migration0019MlModelNoData),
        Box::new(Migration0020ProviderPermissions),
        Box::new(Migration0021DefaultPermissionsForExistingProviders),
        Box::new(Migration0022PermissionQueries),
        Box::new(Migration0030RasterResultDesc),
        Box::new(Migration0031TimeDescriptor),
        Box::new(Migration0032GdalTiles),
    ]
}

#[cfg(test)]
/// Returns an iterator over all migrations that are in the range from `from` to `to`, inclusive.
///
/// # Panics
/// - If the `from` version is not found in the migrations.
/// - If the `to` version is not found in the migrations.
/// - If the `from` version is after the `to` version.
///
fn migrations_by_range(from: &str, to: &str) -> Vec<Box<dyn Migration>> {
    let migrations = all_migrations();
    let from_index = migrations
        .iter()
        .position(|m| m.version() == from)
        .expect("Migration with the given 'from' version not found");
    let to_index = migrations
        .iter()
        .position(|m| m.version() == to)
        .expect("Migration with the given 'to' version not found");
    assert!(
        from_index <= to_index,
        "The 'from' version must not be after the 'to' version"
    );
    migrations
        .into_iter()
        .skip(from_index)
        .take(to_index - from_index + 1)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[serial_test::serial]
    async fn migrations_lead_to_ground_truth_schema() {
        assert_migration_schema_eq(
            &all_migrations(),
            include_str!("current_schema.sql"),
            AssertSchemaEqPopulationConfig {
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
