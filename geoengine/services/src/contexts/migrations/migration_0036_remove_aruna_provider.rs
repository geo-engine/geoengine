use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0035_stac_grid::Migration0035StacGrid,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// Removes persisted Aruna providers and their obsolete database type.
pub struct Migration0036RemoveArunaProvider;

#[async_trait]
impl Migration for Migration0036RemoveArunaProvider {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0035StacGrid.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0036_remove_aruna_provider".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0036_remove_aruna_provider.sql"))
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bb8_postgres::{PostgresConnectionManager, bb8::Pool};
    use tokio_postgres::NoTls;

    use crate::{
        config::get_config_element,
        contexts::{
            migrate_database,
            migrations::{
                Migration0016MergeProviders, Migration0035StacGrid,
                database_migration::tests::create_migration_0015_snapshot, migrations_by_range,
            },
        },
        util::postgres::DatabaseConnectionConfig,
    };

    use super::*;

    #[tokio::test]
    async fn it_removes_persisted_aruna_providers() {
        let postgres_config = get_config_element::<crate::config::Postgres>().unwrap();
        let db_config = DatabaseConnectionConfig::from(postgres_config);
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);
        let pool = Pool::builder().max_size(1).build(pg_mgr).await.unwrap();
        let mut conn = pool.get().await.unwrap();

        create_migration_0015_snapshot(&mut conn).await.unwrap();
        migrate_database(
            &mut conn,
            &migrations_by_range(
                &Migration0016MergeProviders.version(),
                &Migration0035StacGrid.version(),
            ),
        )
        .await
        .unwrap();

        conn.execute(include_str!("migration_0036_test_data.sql"), &[])
            .await
            .unwrap();

        let tx = conn.transaction().await.unwrap();
        Migration0036RemoveArunaProvider.migrate(&tx).await.unwrap();
        tx.commit().await.unwrap();

        let provider_count: i64 = conn
            .query_one("SELECT COUNT(*) FROM layer_providers", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(provider_count, 0);
    }
}