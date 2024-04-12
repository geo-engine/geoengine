use crate::contexts::Migration;
use crate::contexts::{
    Migration0000Initial, Migration0001RasterStacks, Migration0002DatasetListingProvider,
    Migration0003GbifConfig, Migration0004DatasetListingProviderPrio,
    Migration0005GbifColumnSelection, Migration0006EbvProvider, Migration0007OwnerRole,
    Migration0008BandNames,
};
use crate::pro::contexts::migrations::database_migration::NoProMigrationImpl;

pub use crate::pro::contexts::migrations::database_migration::ProMigrationImpl;

mod current_pro_schema;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::migrations::{assert_migration_schema_eq, AssertSchemaEqPopulationConfig};

    #[tokio::test]
    async fn migrations_lead_to_ground_truth_schema() {
        assert_migration_schema_eq(
            &pro_migrations(),
            concat!(
                include_str!("../../../contexts/migrations/current_schema.sql"),
                include_str!("current_schema.sql"),
            ),
            AssertSchemaEqPopulationConfig {
                has_parameters: false,
                ..Default::default()
            },
        )
        .await;
    }
}
