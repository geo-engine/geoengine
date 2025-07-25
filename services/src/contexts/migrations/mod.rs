use crate::contexts::migrations::migration_0018_gdal_tiles::Migration0018GdalTiles;
pub use crate::contexts::migrations::{
    current_schema::CurrentSchemaMigration, migration_0015_log_quota::Migration0015LogQuota,
    migration_0016_merge_providers::Migration0016MergeProviders,
};
pub use database_migration::{
    DatabaseVersion, Migration, MigrationResult, initialize_database, migrate_database,
};

mod current_schema;
mod database_migration;
mod migration_0015_log_quota;
mod migration_0016_merge_providers;
mod migration_0017_raster_result_desc;
mod migration_0018_gdal_tiles;

#[cfg(test)]
mod schema_info;

use migration_0017_raster_result_desc::Migration0017RasterResultDesc;
#[cfg(test)]
pub(crate) use schema_info::{AssertSchemaEqPopulationConfig, assert_migration_schema_eq};

/// All migrations that are available. The migrations are applied in the order they are defined here, starting from the current version of the database.
///
/// NEW MIGRATIONS HAVE TO BE REGISTERED HERE!
///
pub fn all_migrations() -> Vec<Box<dyn Migration>> {
    vec![
        Box::new(Migration0015LogQuota), // cf. [`migration_0015_log_quota.rs`] why we start at `0015`
        Box::new(Migration0016MergeProviders),
        Box::new(Migration0017RasterResultDesc),
        Box::new(Migration0018GdalTiles),
    ]
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
