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
                'SingleBand'
                -- TODO: 'MultiBand'
            );

            CREATE TYPE "RasterColorizer" AS (
                "type" "RasterColorizerType",
                -- single band colorizer
                band bigint,
                band_colorizer "Colorizer"
                -- TODO: multi band colorizer
            );

            ALTER TYPE "RasterSymbology" ADD ATTRIBUTE raster_colorizer "RasterColorizer";
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
                SELECT {id_column}, (symbology).raster.colorizer colorizer
                FROM {layer_table}
                WHERE (symbology).raster.colorizer."type" IS NOT NULL
            )
            UPDATE {layer_table}
            SET
                symbology.raster.raster_colorizer = (
                    'SingleBand'::"RasterColorizerType",
                    0, 
                    colorizer
                )::"RasterColorizer"
            FROM raster_layers
            WHERE {layer_table}.{id_column} = raster_layers.{id_column};"#,
            ))
            .await?;
        }

        // complete new definitions of types by dropping redundant attributes
        tx.batch_execute(
            r#"
        ALTER TYPE "RasterResultDescriptor" DROP ATTRIBUTE measurement;

        ALTER TYPE "RasterSymbology" DROP ATTRIBUTE colorizer;
        "#,
        )
        .await?;

        Ok(())
    }
}

// TODO: add tests once the `ResultDescriptor` has the desired format (including name and units)

#[cfg(test)]
mod tests {
    use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
    use geoengine_datatypes::test_data;
    use serde_json::json;
    use tokio_postgres::NoTls;

    use crate::{
        contexts::{migrate_database, migrations::migration_0000_initial::Migration0000Initial},
        util::config::get_config_element,
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_migrates_result_descriptors_and_symbologies() -> Result<()> {
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

        // verify dataset (including result descriptor) is loaded correctly
        let row = conn
            .query_one(
                "
                SELECT to_json(result_descriptor),
                       to_json(meta_data),
                       to_json(symbology)
                FROM datasets
                WHERE id = '6cc80129-eea4-4140-b09c-6bcfbd76ad5f'
            ",
                &[],
            )
            .await?;

        let result_descriptor: serde_json::Value = row.get(0);
        let expected_result_descriptor = json!({
                "data_type": "U8",
                "spatial_reference": {
                    "authority": "Epsg",
                    "code": "4326"
                },
                "time": {
                    "start": 0,
                    "end": 0
                },
                "bbox": {
                    "upper_left_coordinate": {
                        "x": -180,
                        "y": -90
                    },
                    "lower_right_coordinate": {
                        "x": 180,
                        "y": 90
                    }
                },
                "resolution": {
                    "x": 0.1,
                    "y": 0.1
                },
                "bands": [
                    {
                        "name": "band",
                        "measurement": {
                            "continuous": null,
                            "classification": null
                        }
                    }
                ]
        });
        pretty_assertions::assert_eq!(
            result_descriptor,
            json!({
                "raster": expected_result_descriptor,
                "vector": null,
                "plot": null
            }),
        );

        let meta_data: serde_json::Value = row.get(1);
        pretty_assertions::assert_eq!(
            meta_data,
            json!({
                "mock_meta_data": null,
                "ogr_meta_data": null,
                "gdal_meta_data_regular": null,
                "gdal_static": {
                    "time": {
                        "start": 0,
                        "end": 0
                    },
                    "params": {
                        "file_path": "foo/bar.tiff",
                        "rasterband_channel": 0,
                        "geo_transform": {
                            "origin_coordinate": {
                                "x": 0,
                                "y": 0
                            },
                            "x_pixel_size": 0.1,
                            "y_pixel_size": 0.1
                        },
                        "width": 3600,
                        "height": 1800,
                        "file_not_found_handling": "Error",
                        "no_data_value": 0,
                        "properties_mapping": [],
                        "gdal_open_options": [],
                        "gdal_config_options": [],
                        "allow_alphaband_as_mask": false,
                        "retry": {
                            "max_retries": 0
                        }
                    },
                    "result_descriptor": expected_result_descriptor,
                    "cache_ttl": 0
                },
                "gdal_metadata_net_cdf_cf": null,
                "gdal_meta_data_list": null
            })
        );

        let symbology: serde_json::Value = row.get(2);
        pretty_assertions::assert_eq!(
            symbology,
            json!({
                "raster": {
                    "opacity": 1,
                    "raster_colorizer": {
                        "type": "SingleBand",
                        "band": 0,
                        "band_colorizer": {
                            "type": "LinearGradient",
                            "breakpoints": [
                                {
                                    "value": 0,
                                    "color": [128, 128, 128, 255]
                                }
                            ],
                            "no_data_color": [0, 0, 0, 0],
                            "over_color": [0, 0, 0, 0],
                            "under_color": [0, 0, 0, 0],
                            "default_color": null
                        }
                    }
                },
                "point": null,
                "line": null,
                "polygon": null
            })
        );

        Ok(())
    }
}
