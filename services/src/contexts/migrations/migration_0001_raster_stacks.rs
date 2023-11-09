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
        let stmt = tx
            .prepare(r#"ALTER TYPE "RasterResultDescriptor" ADD ATTRIBUTE bands INTEGER;"#)
            .await?;

        tx.execute(&stmt, &[]).await?;

        // as ALTER TYPE ADD ATTRIBUTE does not support default values we manually have to
        // update the `bands` in all `RasterResultDescriptor`s

        // we use a common table expression because we cannot access nested fields in UPDATE statements
        // and only want to update the rows that actually are a raster

        // update the result descriptor of the datasets
        let stmt = tx
            .prepare(
                r#"
            WITH raster_datasets AS (
                SELECT id 
                FROM datasets
                WHERE (result_descriptor).raster IS NOT NULL
            )
            UPDATE datasets
            SET result_descriptor.raster.bands = 1
            FROM raster_datasets
            WHERE datasets.id = raster_datasets.id;"#,
            )
            .await?;

        tx.execute(&stmt, &[]).await?;

        // update the result descriptor of the meta data
        let raster_meta_data_types = [
            "gdal_meta_data_regular",
            "gdal_static",
            "gdal_metadata_net_cdf_cf",
            "gdal_meta_data_list",
        ];

        for raster_meta_data_type in raster_meta_data_types {
            let stmt = tx
                .prepare(&format!(
                    r#"
            WITH cte AS (
                SELECT id 
                FROM datasets
                WHERE (meta_data).{raster_meta_data_type} IS NOT NULL
            )
            UPDATE datasets
            SET meta_data.{raster_meta_data_type}.result_descriptor.bands = 1
            FROM cte
            WHERE datasets.id = cte.id;"#
                ))
                .await?;

            tx.execute(&stmt, &[]).await?;
        }

        Ok(())
    }
}

// TODO: add tests once the `ResultDescriptor` has the desired format (including name and units)
