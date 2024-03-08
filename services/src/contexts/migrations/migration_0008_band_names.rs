use async_trait::async_trait;
use futures::{pin_mut, TryStreamExt};
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
        "008_band_names".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        let empty_params: Vec<String> = vec![];

        let stmt = tx.prepare("SELECT id, workflow FROM workflows").await?;
        let cursor = tx.query_raw(&stmt, &empty_params).await?;

        pin_mut!(cursor);
        while let Some(row) = cursor.try_next().await? {
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
            "type": "defaultSuffix",
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
                      "type": "defaultSuffix",
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
