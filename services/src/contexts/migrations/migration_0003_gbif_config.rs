use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds the autocomplete timeout field to the GBIF provider config
pub struct Migration0003GbifConfig;

#[async_trait]
impl Migration for Migration0003GbifConfig {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0002_dataset_listing_provider".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0003_gbif_config".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(
            r#"
                ALTER TYPE "GbifDataProviderDefinition"
                ADD ATTRIBUTE autocomplete_timeout int
            "#,
        )
        .await?;

        // as ALTER TYPE ADD ATTRIBUTE does not support default values we manually have to set the default value of the new field in the layer_providers table
        tx.batch_execute(
            "
                UPDATE layer_providers
                SET definition.gbif_data_provider_definition.autocomplete_timeout = 5
                WHERE NOT ((definition).gbif_data_provider_definition IS NULL)
            ",
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
    use geoengine_datatypes::test_data;
    use tokio_postgres::NoTls;

    use crate::contexts::{Migration0001RasterStacks, Migration0002DatasetListingProvider};
    use crate::{
        contexts::{migrate_database, migrations::migration_0000_initial::Migration0000Initial},
        util::config::get_config_element,
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_adds_the_field_and_sets_the_default_value() -> Result<()> {
        let postgres_config = get_config_element::<crate::util::config::Postgres>()?;
        let pg_mgr = PostgresConnectionManager::new(postgres_config.try_into()?, NoTls);

        let pool = Pool::builder().max_size(1).build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        // initial schema
        migrate_database(&mut conn, &[Box::new(Migration0000Initial)]).await?;

        // insert test data on initial schema
        let test_data_sql = std::fs::read_to_string(test_data!("migrations/test_data.sql"))?;
        conn.batch_execute(&test_data_sql).await?;

        // perform the previous migration
        migrate_database(
            &mut conn,
            &[
                Box::new(Migration0001RasterStacks),
                Box::new(Migration0002DatasetListingProvider),
            ],
        )
        .await?;

        // perform the current migrations
        migrate_database(&mut conn, &[Box::new(Migration0003GbifConfig)]).await?;

        // verify that the default value for autocomplete_timeout has been set correctly
        let autocomplete_timeout = conn
            .query_one(
                "
                    SELECT (definition).gbif_data_provider_definition.autocomplete_timeout
                    FROM layer_providers
                    WHERE NOT ((definition).gbif_data_provider_definition IS NULL)
                ",
                &[],
            )
            .await?
            .get::<usize, i32>(0);

        assert_eq!(autocomplete_timeout, 5);

        // drop the connection because the pool is limited to one connection, s.t. we can reuse the temporary schema
        drop(conn);

        Ok(())
    }
}
