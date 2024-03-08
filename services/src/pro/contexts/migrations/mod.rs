use crate::contexts::migrations::Migration0006EbvProvider;
use crate::contexts::{
    Migration0000Initial, Migration0001RasterStacks, Migration0002DatasetListingProvider,
    Migration0003GbifConfig, Migration0004DatasetListingProviderPrio,
    Migration0005GbifColumnSelection, Migration0007OwnerRole, Migration0008BandNames,
};
use crate::pro::contexts::migrations::database_migration::NoProMigrationImpl;
use crate::{contexts::Migration, pro::contexts::migrations::database_migration::ProMigrationImpl};

mod database_migration;
mod migration_0000_initial;
mod migration_0004_dataset_listing_provider_prio;
mod migration_0007_owner_role;

/// Get all regular and pro migrations. This function wraps all regular migrations into a pro migration.
pub fn pro_migrations() -> Vec<Box<dyn Migration>>
where
{
    vec![
        Box::new(ProMigrationImpl::from(Migration0000Initial)),
        Box::new(NoProMigrationImpl::from(Migration0001RasterStacks)),
        Box::new(NoProMigrationImpl::from(
            Migration0002DatasetListingProvider,
        )),
        Box::new(NoProMigrationImpl::from(Migration0003GbifConfig)),
        Box::new(ProMigrationImpl::from(
            Migration0004DatasetListingProviderPrio,
        )),
        Box::new(NoProMigrationImpl::from(Migration0005GbifColumnSelection)),
        Box::new(NoProMigrationImpl::from(Migration0006EbvProvider)),
        Box::new(NoProMigrationImpl::from(Migration0007OwnerRole)),
        Box::new(NoProMigrationImpl::from(Migration0008BandNames)),
    ]
}
