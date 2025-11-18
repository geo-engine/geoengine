use super::database_migration::{DatabaseVersion, Migration};
use crate::{contexts::migrations::Migration0022PermissionQueries, error::Result};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the provider permissions
pub struct Migration0023WildliveOidc;

#[async_trait]
impl Migration for Migration0023WildliveOidc {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0022PermissionQueries.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0023_wildlive_oidc".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0023_wildlive_oidc.sql"))
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
                Migration0016MergeProviders,
                database_migration::tests::create_migration_0015_snapshot, migrations_by_range,
            },
        },
        util::postgres::DatabaseConnectionConfig,
    };

    use super::*;

    #[tokio::test]
    async fn it_migrates_with_non_empty_api_key() {
        let postgres_config = get_config_element::<crate::config::Postgres>().unwrap();
        let db_config = DatabaseConnectionConfig::from(postgres_config);
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);

        let pool = Pool::builder().max_size(1).build(pg_mgr).await.unwrap();

        let mut conn = pool.get().await.unwrap();

        // initial schema
        create_migration_0015_snapshot(&mut conn).await.unwrap();

        // perform this migration
        migrate_database(
            &mut conn,
            &migrations_by_range(
                &Migration0016MergeProviders.version(),
                &Migration0022PermissionQueries.version(),
            ),
        )
        .await
        .unwrap();

        // insert test data on initial schema
        assert_eq!(
            conn.execute(include_str!("migration_0023_test_data.sql"), &[])
                .await
                .unwrap(),
            2
        );

        // perform this migration
        Migration0023WildliveOidc
            .migrate(&conn.transaction().await.unwrap())
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
