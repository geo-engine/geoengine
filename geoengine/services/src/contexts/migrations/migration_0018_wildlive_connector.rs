use super::{
    Migration0017MlModelTensorShape,
    database_migration::{DatabaseVersion, Migration},
};
use crate::error::Result;
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds a new provider type into the `DataProviderDefinition` type
/// and creates a new cache tables for the Wildlive connector.
pub struct Migration0018WildliveConnector;

#[async_trait]
impl Migration for Migration0018WildliveConnector {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0017MlModelTensorShape.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0018_widlive_connector".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0018_wildlive_connector.sql"))
            .await?;

        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::migrations::database_migration::tests::create_migration_0015_snapshot;
    use crate::contexts::migrations::{Migration0016MergeProviders, migrations_by_range};
    use crate::util::postgres::DatabaseConnectionConfig;
    use crate::{config::get_config_element, contexts::migrate_database};
    use bb8_postgres::{PostgresConnectionManager, bb8::Pool};
    use tokio_postgres::NoTls;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_merges_the_pro_layer_providers_table() {
        let postgres_config = get_config_element::<crate::config::Postgres>().unwrap();
        let db_config = DatabaseConnectionConfig::from(postgres_config);
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);

        let pool = Pool::builder().max_size(1).build(pg_mgr).await.unwrap();

        let mut conn = pool.get().await.unwrap();

        // initial schema
        create_migration_0015_snapshot(&mut conn).await.unwrap();

        // insert test data on initial schema
        assert_eq!(
            conn.execute(include_str!("migration_0016_test_data.sql"), &[])
                .await
                .unwrap(),
            2
        );

        // perform this migration
        migrate_database(
            &mut conn,
            &migrations_by_range(
                &Migration0016MergeProviders.version(),
                &Migration0018WildliveConnector.version(),
            ),
        )
        .await
        .unwrap();

        // verify that entries are in the new table
        assert_eq!(
            conn.query_one("SELECT COUNT(*) FROM layer_providers", &[])
                .await
                .unwrap()
                .get::<usize, i64>(0),
            2
        );
    }
}
