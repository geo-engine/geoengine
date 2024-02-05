use crate::contexts::{Migration0000Initial, Migration0001RasterStacks};
use crate::{contexts::Migration, pro::contexts::migrations::database_migration::ProMigrationImpl};

mod database_migration;
mod migration_0000_initial;
mod migration_0001_raster_stacks;

/// Get all regular and pro migrations. This function wraps all regular migrations into a pro migration.
pub fn pro_migrations() -> Vec<Box<dyn Migration>>
where
{
    vec![
        Box::new(ProMigrationImpl::from(Migration0000Initial)),
        Box::new(ProMigrationImpl::from(Migration0001RasterStacks)),
    ]
}
