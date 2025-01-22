use async_trait::async_trait;
use serde_json::json;
use tokio_postgres::Transaction;
use uuid::Uuid;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds band names to operators `Expression` and `RasterStacker`
pub struct Migration0008BandNames;

#[async_trait]
impl Migration for Migration0008BandNames {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0007_owner_role".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0008_band_names".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        let stmt = tx.prepare("SELECT id, workflow FROM workflows;").await?;
        let portal = tx.bind(&stmt, &[]).await?;
        let batch_size = 100;
        let mut rows = tx.query_portal(&portal, batch_size).await?;

        while !rows.is_empty() {
            for row in rows {
                let id: Uuid = row.get(0);
                let mut workflow: serde_json::Value = row.get(1);

                map_objects(&mut workflow, &migrate_expression);
                map_objects(&mut workflow, &migrate_raster_stacker);
                tx.execute(
                    "UPDATE workflows SET workflow = $1 WHERE id = $2",
                    &[&workflow, &id],
                )
                .await?;
            }
            rows = tx.query_portal(&portal, batch_size).await?;
        }

        Ok(())
    }
}

fn migrate_expression(obj: &mut serde_json::Map<String, serde_json::Value>) {
    if obj.get("type").map(|v| v.as_str()) != Some(Some("Expression")) {
        return;
    }

    let Some(params) = obj.get_mut("params").and_then(|v| v.as_object_mut()) else {
        return;
    };

    let measurement = params
        .remove("outputMeasurement")
        .unwrap_or_else(|| json!({"type": "unitless"}));

    params.insert(
        "outputBand".to_string(),
        json!({
            "name": "band",
            "measurement": measurement
        }),
    );
}

fn migrate_raster_stacker(obj: &mut serde_json::Map<String, serde_json::Value>) {
    if obj.get("type").map(|v| v.as_str()) != Some(Some("RasterStacker")) {
        return;
    }

    let Some(params) = obj.get_mut("params").and_then(|v| v.as_object_mut()) else {
        return;
    };

    params.insert(
        "renameBands".to_string(),
        json!({
            "type": "default",
        }),
    );
}

fn map_objects<F>(value: &mut serde_json::Value, f: &F)
where
    F: Fn(&mut serde_json::Map<String, serde_json::Value>),
{
    match value {
        serde_json::Value::Object(obj) => {
            f(obj);
            for (_, v) in obj {
                map_objects(v, f);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                map_objects(v, f);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::get_config_element;
    use crate::contexts::migrations::all_migrations;
    use crate::contexts::{migrate_database, Migration0000Initial};
    use crate::util::postgres::DatabaseConnectionConfig;
    use bb8_postgres::bb8::Pool;
    use bb8_postgres::PostgresConnectionManager;
    use geoengine_datatypes::test_data;
    use std::time::Duration;
    use tokio::time::timeout;
    use tokio_postgres::NoTls;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn it_does_not_get_stuck() -> Result<()> {
        let workflow = json!({});

        let postgres_config = get_config_element::<crate::config::Postgres>()?;
        let db_config = DatabaseConnectionConfig::from(postgres_config);
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);

        let pool = Pool::builder().max_size(1).build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        // initial schema
        migrate_database(&mut conn, &[Box::new(Migration0000Initial)]).await?;

        // insert test data on initial schema
        let test_data_sql = std::fs::read_to_string(test_data!("migrations/test_data.sql"))?;
        conn.batch_execute(&test_data_sql).await?;

        // perform all previous migrations
        migrate_database(&mut conn, &all_migrations()[1..8]).await?;

        let prepared = conn
            .prepare("INSERT INTO workflows VALUES ($1, $2)")
            .await?;

        for _ in 0..1000 {
            conn.execute(&prepared, &[&Uuid::new_v4(), &workflow])
                .await?;
        }

        let _ = timeout(
            Duration::from_secs(5),
            migrate_database(&mut conn, &[Box::new(Migration0008BandNames)]),
        )
        .await
        .expect("Should finish within five seconds");

        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn it_migrates_expression() {
        let mut workflow = json!({
          "id": "c078db52-2dc6-4838-ad75-340cefeab476",
          "name": "Stacked Raster",
          "description": "A raster with two bands for testing",
          "workflow": {
            "type": "Raster",
            "operator": {
              "type": "RasterStacker",
              "params": {
                "renameBands": {
                  "type": "rename",
                  "values": ["ndvi", "ndvi_masked"]
                }
              },
              "sources": {
                "rasters": [
                  {
                    "type": "GdalSource",
                    "params": {
                      "data": "ndvi"
                    }
                  },
                  {
                    "type": "Expression",
                    "params": {
                      "expression": "if A > 100 { A } else { 0 }",
                      "outputType": "U8",
                      "outputMeasurement": {
                        "type": "continuous",
                        "measurement": "NDVI",
                      },
                      "mapNoData": false
                    },
                    "sources": {
                      "raster": {
                        "type": "GdalSource",
                        "params": {
                          "data": "ndvi"
                        }
                      }
                    }
                  }
                ]
              }
            }
          }
        });

        map_objects(&mut workflow, &migrate_expression);

        assert_eq!(
            workflow,
            json!({
                "id": "c078db52-2dc6-4838-ad75-340cefeab476",
                "name": "Stacked Raster",
                "description": "A raster with two bands for testing",
                "workflow": {
                  "type": "Raster",
                  "operator": {
                    "type": "RasterStacker",
                    "params": {
                      "renameBands": {
                        "type": "rename",
                        "values": ["ndvi", "ndvi_masked"]
                      }
                    },
                    "sources": {
                      "rasters": [
                        {
                          "type": "GdalSource",
                          "params": {
                            "data": "ndvi"
                          }
                        },
                        {
                          "type": "Expression",
                          "params": {
                            "expression": "if A > 100 { A } else { 0 }",
                            "outputType": "U8",
                            "outputBand": {
                              "name": "band",
                              "measurement": {
                                "type": "continuous",
                                "measurement": "NDVI",
                              }
                            },
                            "mapNoData": false
                          },
                          "sources": {
                            "raster": {
                              "type": "GdalSource",
                              "params": {
                                "data": "ndvi"
                              }
                            }
                          }
                        }
                      ]
                    }
                  }
                }
            })
        );
    }

    #[test]
    fn it_migrates_raster_stacker() {
        let mut workflow = json!({
          "id": "c078db52-2dc6-4838-ad75-340cefeab476",
          "name": "Stacked Raster",
          "description": "A raster with two bands for testing",
          "workflow": {
            "type": "Raster",
            "operator": {
              "type": "RasterStacker",
              "params": {},
              "sources": {
                "rasters": [
                  {
                    "type": "GdalSource",
                    "params": {
                      "data": "ndvi"
                    }
                  }, {
                    "type": "GdalSource",
                    "params": {
                      "data": "ndvi"
                    }
                  }
                ]
              }
            }
          }
        });

        map_objects(&mut workflow, &migrate_raster_stacker);

        assert_eq!(
            workflow,
            json!({
              "id": "c078db52-2dc6-4838-ad75-340cefeab476",
              "name": "Stacked Raster",
              "description": "A raster with two bands for testing",
              "workflow": {
                "type": "Raster",
                "operator": {
                  "type": "RasterStacker",
                  "params": {
                    "renameBands": {
                      "type": "default",
                    }
                  },
                  "sources": {
                    "rasters": [
                      {
                        "type": "GdalSource",
                        "params": {
                          "data": "ndvi"
                        }
                      }, {
                        "type": "GdalSource",
                        "params": {
                          "data": "ndvi"
                        }
                      }
                    ]
                  }
                }
              }
            })
        );
    }
}
