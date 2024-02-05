use crate::contexts::migrations::migration_0001_raster_stacks::Migration0001RasterStacks;

use self::migration_0000_initial::Migration0000Initial;

mod database_migration;
pub mod migration_0000_initial;
pub mod migration_0001_raster_stacks;

pub use database_migration::{migrate_database, DatabaseVersion, Migration, MigrationResult};

/// All migrations that are available. The migrations are applied in the order they are defined here, starting from the current version of the database.
///
/// NEW MIGRATIONS HAVE TO BE REGISTERED HERE!
///
pub fn all_migrations() -> Vec<Box<dyn Migration>> {
    vec![
        Box::new(Migration0000Initial),
        Box::new(Migration0001RasterStacks),
    ]
}
