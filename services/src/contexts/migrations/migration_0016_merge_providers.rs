use super::{
    Migration0015LogQuota,
    database_migration::{DatabaseVersion, Migration},
};
use crate::error::Result;
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration merges the two providers tables into one
pub struct Migration0016MergeProviders;

#[async_trait]
impl Migration for Migration0016MergeProviders {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0015LogQuota.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0016_merge_providers".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0016_merge_providers.sql"))
            .await?;

        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use crate::contexts::migrations::all_migrations;
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
        migrate_database(&mut conn, &all_migrations()[0..1])
            .await
            .unwrap();

        // insert test data on initial schema
        assert_eq!(
            conn.execute(include_str!("migration_0016_test_data.sql"), &[])
                .await
                .unwrap(),
            2
        );

        // perform this migration
        migrate_database(&mut conn, &all_migrations()[1..=1])
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
