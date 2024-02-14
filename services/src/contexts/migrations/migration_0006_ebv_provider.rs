use super::database_migration::{DatabaseVersion, Migration};
use crate::{contexts::Migration0005GbifColumnSelection, error::Result};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the gbif provider column selection
pub struct Migration0006EbvProvider;

#[async_trait]
impl Migration for Migration0006EbvProvider {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0005GbifColumnSelection.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0006_ebv_provider".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0006_ebv_provider.sql"))
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
    use geoengine_datatypes::test_data;
    use tokio_postgres::NoTls;

    use crate::contexts::migrations::all_migrations;
    use crate::{
        contexts::{migrate_database, migrations::migration_0000_initial::Migration0000Initial},
        util::config::get_config_element,
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_adds_the_field_and_sets_the_default_value() -> Result<()> {
        todo!("implement test");

        let postgres_config = get_config_element::<crate::util::config::Postgres>()?;
        let pg_mgr = PostgresConnectionManager::new(postgres_config.try_into()?, NoTls);

        let pool = Pool::builder().max_size(1).build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        // initial schema
        migrate_database(&mut conn, &[Box::new(Migration0000Initial)]).await?;

        // insert test data on initial schema
        let test_data_sql = std::fs::read_to_string(test_data!("migrations/test_data.sql"))?;
        conn.batch_execute(&test_data_sql).await?;

        // perform all migrations
        migrate_database(&mut conn, &all_migrations()[1..]).await?;

        // verify that the default value for columns has been set correctly
        let autocomplete_timeout = conn
            .query_one(
                "
                    SELECT (definition).gbif_data_provider_definition.columns
                    FROM layer_providers
                    WHERE NOT ((definition).gbif_data_provider_definition IS NULL)
                ",
                &[],
            )
            .await?
            .get::<usize, Vec<String>>(0);

        assert_eq!(
            autocomplete_timeout,
            vec![
                "gbifid".to_string(),
                "basisofrecord".to_string(),
                "scientificname".to_string()
            ]
        );

        Ok(())
    }
}
