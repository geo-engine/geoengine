use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

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
                SELECT id, (result_descriptor).raster.measurement measurement
                FROM datasets
                WHERE (result_descriptor).raster IS NOT NULL
            )
            UPDATE datasets
            SET result_descriptor.raster.bands = 
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
                WHERE (meta_data).{raster_meta_data_type} IS NOT NULL
            )
            UPDATE datasets
            SET meta_data.{raster_meta_data_type}.result_descriptor.bands =
                ARRAY[('band', cte.measurement)]::"RasterBandDescriptor"[]
            FROM cte
            WHERE datasets.id = cte.id;"#
            ))
            .await?;
        }

        tx.batch_execute(
            r#"
        ALTER TYPE "RasterResultDescriptor" DROP ATTRIBUTE measurement;
        "#,
        )
        .await?;

        Ok(())
    }
}

// TODO: add tests once the `ResultDescriptor` has the desired format (including name and units)
