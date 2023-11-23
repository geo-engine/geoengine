use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds multi band support for result descriptors and symbologies
pub struct Migration0001RasterStacks;

#[async_trait]
impl Migration for Migration0001RasterStacks {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0000_initial".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0001_raster_stacks".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(
            r#"
            CREATE TYPE "RasterBandDescriptor" AS (
                "name" text,
                measurement "Measurement"
            );

            ALTER TYPE "RasterResultDescriptor" ADD ATTRIBUTE bands "RasterBandDescriptor"[];

            CREATE TYPE "RasterColorizerType" AS ENUM (
                'SingleBandColorizer'
                -- TODO: 'MultiBandColorizer'
            );

            CREATE TYPE "RasterColorizer" AS (
                "type" "RasterColorizerType",
                -- single band colorizer
                band bigint,
                colorizer "Colorizer"
                -- TODO: multi band colorizer
            );


            ALTER TYPE "RasterSymbology" RENAME ATTRIBUTE colorizer TO colorizer_old;
            ALTER TYPE "RasterSymbology" ADD ATTRIBUTE colorizer "RasterColorizer";
        "#,
        )
        .await?;

        // as ALTER TYPE ADD ATTRIBUTE does not support default values we manually have to
        // update the `bands` in all `RasterResultDescriptor`s

        // we use a common table expression because we cannot access nested fields in UPDATE statements
        // and only want to update the rows that actually are a raster

        tx.batch_execute(
            r#"
            WITH raster_datasets AS (
                SELECT 
                    id, 
                    (result_descriptor).raster.measurement measurement
                FROM datasets
                WHERE (result_descriptor).raster.data_type IS NOT NULL
            )
            UPDATE datasets
            SET 
                result_descriptor.raster.bands = 
                    ARRAY[('band', raster_datasets.measurement)]::"RasterBandDescriptor"[]
            FROM raster_datasets
            WHERE datasets.id = raster_datasets.id;"#,
        )
        .await?;

        // update the result descriptor of the meta data
        let raster_meta_data_types = [
            "gdal_meta_data_regular",
            "gdal_static",
            "gdal_metadata_net_cdf_cf",
            "gdal_meta_data_list",
        ];

        for raster_meta_data_type in raster_meta_data_types {
            tx.batch_execute(&format!(
                r#"
            WITH cte AS (
                SELECT 
                    id, 
                    (meta_data).{raster_meta_data_type}.result_descriptor.measurement measurement
                FROM datasets
                WHERE (meta_data).{raster_meta_data_type}.result_descriptor.data_type IS NOT NULL
            )
            UPDATE datasets
            SET meta_data.{raster_meta_data_type}.result_descriptor.bands =
                ARRAY[('band', cte.measurement)]::"RasterBandDescriptor"[]
            FROM cte
            WHERE datasets.id = cte.id;"#
            ))
            .await?;
        }

        // update symbology in project layers and collection layers
        for (layer_table, id_column) in [
            ("datasets", "id"),
            ("project_version_layers", "project_version_id"),
            ("layers", "id"),
        ] {
            tx.batch_execute(&format!(
                r#"
            WITH raster_layers AS (
                SELECT {id_column}, (symbology).raster.colorizer_old colorizer_old
                FROM {layer_table}
                WHERE symbology IS NOT NULL AND (symbology).raster.colorizer_old IS NOT NULL
            )
            UPDATE {layer_table}
            SET
                symbology.raster.colorizer.band = 0,
                symbology.raster.colorizer.colorizer = colorizer_old
            FROM raster_layers
            WHERE {layer_table}.{id_column} = raster_layers.{id_column};"#,
            ))
            .await?;
        }

        // complete new definitions of types by dropping redundant attributes
        tx.batch_execute(
            r#"
        ALTER TYPE "RasterResultDescriptor" DROP ATTRIBUTE measurement;

        ALTER TYPE "RasterSymbology" DROP ATTRIBUTE colorizer_old;
        "#,
        )
        .await?;

        Ok(())
    }
}

// TODO: add tests once the `ResultDescriptor` has the desired format (including name and units)

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
    use geoengine_datatypes::{dataset::DatasetId, test_data};
    use tokio_postgres::NoTls;

    use crate::{
        contexts::{
            migrate_database, migrations::migration_0000_initial::Migration0000Initial, PostgresDb,
        },
        datasets::listing::DatasetProvider,
        util::config::get_config_element,
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_migrates_result_descriptors() -> Result<()> {
        let postgres_config = get_config_element::<crate::util::config::Postgres>()?;
        let pg_mgr = PostgresConnectionManager::new(postgres_config.try_into()?, NoTls);

        let pool = Pool::builder().max_size(1).build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        // initial schema
        migrate_database(&mut conn, &[Box::new(Migration0000Initial)]).await?;

        // insert test data on initial schema
        let test_data_sql = std::fs::read_to_string(test_data!("migrations/test_data.sql"))?;
        conn.batch_execute(&test_data_sql).await?;

        // perform current migration
        migrate_database(&mut conn, &[Box::new(Migration0001RasterStacks)]).await?;

        // drop the connection because the pool is limited to one connection, s.t. we can reuse the temporary schema
        drop(conn);

        // create `PostgresDb` on migrated database and test methods
        let db = PostgresDb::new(pool.clone());

        // verify dataset (including result descriptor) is loaded correctly
        let _ = db
            .load_dataset(&DatasetId::from_str("6cc80129-eea4-4140-b09c-6bcfbd76ad5f").unwrap())
            .await
            .unwrap();

        Ok(())
    }
}
