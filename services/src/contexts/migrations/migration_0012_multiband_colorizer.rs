use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds the multiband raster colorizer
pub struct Migration0012MultibandColorizer;

#[async_trait]
impl Migration for Migration0012MultibandColorizer {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0011_remove_xgb".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0012_multiband_colorizer".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0012_multiband_colorizer.sql"))
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::contexts::migrations::all_migrations;
    use crate::contexts::Migration0000Initial;
    use crate::error::Result;
    use crate::{contexts::migrate_database, util::config::get_config_element};
    use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
    use geoengine_datatypes::test_data;
    use tokio_postgres::NoTls;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_replaces_rgba_with_lineargradient() -> Result<()> {
        let postgres_config = get_config_element::<crate::util::config::Postgres>()?;
        let pg_mgr = PostgresConnectionManager::new(postgres_config.try_into()?, NoTls);

        let pool = Pool::builder().max_size(1).build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        // initial schema
        migrate_database(&mut conn, &[Box::new(Migration0000Initial)]).await?;

        // insert test data on initial schema
        let test_data_sql = std::fs::read_to_string(test_data!("migrations/test_data.sql"))?;
        conn.batch_execute(&test_data_sql).await?;

        assert_eq!(
            conn.query_one(
                r#"
                    SELECT (symbology).raster.colorizer."type"::text
                    FROM layers
                    WHERE id = '78aaa2b2-7d6b-4e6a-86bf-b3cd1b63553a'
                "#,
                &[],
            )
            .await?
            .get::<usize, String>(0),
            "Rgba".to_owned(),
            "Precondition failed"
        );

        // perform this migration
        migrate_database(&mut conn, &all_migrations()[1..13]).await?;

        // verify that Rgba was replaced with LinearGradient
        assert_eq!(
            conn.query_one(
                r#"
                    SELECT (symbology).raster.raster_colorizer.band_colorizer."type"::text
                    FROM layers
                    WHERE id = '78aaa2b2-7d6b-4e6a-86bf-b3cd1b63553a'
                "#,
                &[],
            )
            .await?
            .get::<usize, String>(0),
            "LinearGradient".to_owned()
        );

        Ok(())
    }
}
