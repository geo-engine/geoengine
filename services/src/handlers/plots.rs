use std::time::Duration;

use crate::api::model::datatypes::TimeInterval;
use crate::error;
use crate::error::Result;
use crate::handlers::Context;
use crate::ogc::util::{parse_bbox, parse_time};
use crate::util::config;
use crate::util::parsing::parse_spatial_resolution;
use crate::util::server::connection_closed;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;
use actix_web::{web, FromRequest, HttpRequest, Responder};
use geoengine_datatypes::operations::reproject::reproject_query;
use geoengine_datatypes::plots::PlotOutputFormat;
use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution, VectorQueryRectangle};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{QueryContext, ResultDescriptor, TypedPlotQueryProcessor};
use geoengine_operators::util::abortable_query_execution;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

pub(crate) fn init_plot_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: Context,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/plot/{id}").route(web::get().to(get_plot_handler::<C>)));
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetPlot {
    #[serde(deserialize_with = "parse_bbox")]
    #[param(example = "0,-0.3,0.2,0", value_type = String)]
    pub bbox: BoundingBox2D,
    #[param(example = "EPSG:4326")]
    pub crs: Option<SpatialReference>,
    #[serde(deserialize_with = "parse_time")]
    #[param(example = "2020-01-01T00:00:00.0Z")]
    pub time: TimeInterval,
    #[serde(deserialize_with = "parse_spatial_resolution")]
    #[param(example = "0.1,0.1", value_type = String)]
    pub spatial_resolution: SpatialResolution,
}

/// Generates a plot.
///
/// # Example
///
/// 1. Create a statistics workflow.
///
/// ```text
/// POST /workflow
/// Authorization: Bearer 4f0d02f9-68e8-46fb-9362-80f862b7db54
///
/// {
///   "type": "Plot",
///   "operator": {
///     "type": "Statistics",
///     "params": {},
///     "sources": {
///       "source": [
///         {
///           "type": "MockRasterSourcei32",
///           "params": {
///             "data": [
///               {
///                 "time": {
///                   "start": -8334632851200000,
///                   "end": 8210298412799999
///                 },
///                 "tilePosition": [0,0],
///                 "globalGeoTransform": {
///                   "originCoordinate": {"x": 0.0,"y": 0.0},
///                   "xPixelSize": 1.0,
///                   "yPixelSize": -1.0
///                 },
///                 "gridArray": {
///                   "type": "grid",
///                   "innerGrid": {
///                     "shape": {
///                       "shapeArray": [3, 2]
///                     },
///                     "data": [1, 2, 3, 4, 5, 6]
///                   },
///                   "validityMask": {
///                     "shape": {
///                       "shapeArray": [3,2]
///                     },
///                     "data": [true,true,true,true,true,true]
///                   }
///                 },
///                 "properties": {
///                   "scale": null,
///                   "offset": null,
///                   "band_name": null,
///                   "properties_map": {}
///                 }
///               }
///             ],
///             "resultDescriptor": {
///               "dataType": "U8",
///               "spatialReference": "EPSG:4326",
///               "measurement": {
///                 "type": "unitless"
///               },
///               "time": null,
///               "bbox": null,
///               "resolution": null
///             }
///           }
///         }
///       ]
///     }
///   }
/// }
/// ```
/// Response:
/// ```text
/// {
///   "id": "504ed8a4-e0a4-5cef-9f91-b2ffd4a2b56b"
/// }
/// ```
///
/// 2. Generate the plot with this handler.
#[utoipa::path(
    tag = "Plots",
    get,
    path = "/plot/{id}",
    responses(
        (status = 200, description = "OK", body = WrappedPlotOutput,
            example = json!({
                "outputFormat": "JsonPlain",
                "plotType": "Statistics",
                "data": {
                    "Raster-1": {
                        "max": 6.0,
                        "mean": 3.5,
                        "min": 1.0,
                        "stddev": 1.707_825_127_659_933,
                        "validCount": 6,
                        "valueCount": 6
                    }
                }
           })
        )
    ),
    params(
        GetPlot,
        ("id", description = "Workflow id")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn get_plot_handler<C: Context>(
    req: HttpRequest,
    id: web::Path<Uuid>,
    params: web::Query<GetPlot>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let conn_closed = connection_closed(
        &req,
        config::get_config_element::<config::Plots>()?
            .request_timeout_seconds
            .map(Duration::from_secs),
    );

    let workflow = ctx
        .workflow_registry_ref()
        .load(&WorkflowId(id.into_inner()))
        .await?;

    let operator = workflow.operator.get_plot().context(error::Operator)?;

    let execution_context = ctx.execution_context(session.clone())?;

    let initialized = operator
        .initialize(&execution_context)
        .await
        .context(error::Operator)?;

    // handle request and workflow crs matching
    let workflow_spatial_ref: Option<SpatialReference> =
        initialized.result_descriptor().spatial_reference().into();
    let workflow_spatial_ref = workflow_spatial_ref.ok_or(error::Error::InvalidSpatialReference)?;

    // TODO: use a default spatial reference if it is not set?
    let request_spatial_ref: SpatialReference =
        params.crs.ok_or(error::Error::MissingSpatialReference)?;

    let query_rect = VectorQueryRectangle {
        spatial_bounds: params.bbox,
        time_interval: params.time.into(),
        spatial_resolution: params.spatial_resolution,
    };

    let query_rect = if request_spatial_ref == workflow_spatial_ref {
        Some(query_rect)
    } else {
        reproject_query(query_rect, workflow_spatial_ref, request_spatial_ref)
            .map_err(From::from)
            .context(error::Operator)?
    };

    let Some(query_rect) = query_rect else {
        return Err(error::Error::UnresolvableQueryBoundingBox2DInSrs {
            query_bbox: params.bbox.into(),
            query_srs: workflow_spatial_ref.into(),
        });
    };

    let processor = initialized.query_processor().context(error::Operator)?;

    let mut query_ctx = ctx.query_context(session)?;

    let query_abort_trigger = query_ctx.abort_trigger()?;

    let output_format = PlotOutputFormat::from(&processor);
    let plot_type = processor.plot_type();

    let data = match processor {
        TypedPlotQueryProcessor::JsonPlain(processor) => {
            let json = processor.plot_query(query_rect, &query_ctx);
            let result = abortable_query_execution(json, conn_closed, query_abort_trigger).await;
            result.context(error::Operator)?
        }
        TypedPlotQueryProcessor::JsonVega(processor) => {
            let chart = processor.plot_query(query_rect, &query_ctx);
            let chart = abortable_query_execution(chart, conn_closed, query_abort_trigger).await;
            let chart = chart.context(error::Operator)?;

            serde_json::to_value(chart).context(error::SerdeJson)?
        }
        TypedPlotQueryProcessor::ImagePng(processor) => {
            let png_bytes = processor.plot_query(query_rect, &query_ctx);
            let png_bytes =
                abortable_query_execution(png_bytes, conn_closed, query_abort_trigger).await;
            let png_bytes = png_bytes.context(error::Operator)?;

            let data_uri = format!("data:image/png;base64,{}", base64::encode(png_bytes));

            serde_json::to_value(data_uri).context(error::SerdeJson)?
        }
    };

    let output = WrappedPlotOutput {
        output_format,
        plot_type,
        data,
    };

    Ok(web::Json(output))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WrappedPlotOutput {
    output_format: PlotOutputFormat,
    plot_type: &'static str,
    #[schema(value_type = Object)]
    data: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{InMemoryContext, Session, SimpleContext};
    use crate::util::tests::{
        check_allowed_http_methods, read_body_json, read_body_string, send_test_request,
    };
    use crate::workflows::workflow::Workflow;
    use actix_web;
    use actix_web::dev::ServiceResponse;
    use actix_web::http::{header, Method};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::primitives::{DateTime, Measurement};
    use geoengine_datatypes::raster::{
        Grid2D, RasterDataType, RasterTile2D, TileInformation, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{
        ChunkByteSize, PlotOperator, RasterOperator, RasterResultDescriptor,
    };
    use geoengine_operators::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_operators::plot::{
        Histogram, HistogramBounds, HistogramParams, Statistics, StatisticsParams,
    };
    use serde_json::{json, Value};

    fn example_raster_source() -> Box<dyn RasterOperator> {
        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![RasterTile2D::new_with_tile_info(
                    geoengine_datatypes::primitives::TimeInterval::default(),
                    TileInformation {
                        global_geo_transform: TestDefault::test_default(),
                        global_tile_position: [0, 0].into(),
                        tile_size_in_pixels: [3, 2].into(),
                    },
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                        .unwrap()
                        .into(),
                )],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed()
    }

    #[tokio::test]
    async fn json() {
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = InMemoryContext::new_with_context_spec(
            tiling_specification,
            ChunkByteSize::test_default(),
        );
        let session_id = ctx.default_session_ref().await.id();

        let workflow = Workflow {
            operator: Statistics {
                params: StatisticsParams {
                    column_names: vec![],
                },
                sources: vec![example_raster_source()].into(),
            }
            .boxed()
            .into(),
        };

        let id = ctx
            .workflow_registry_ref()
            .register(workflow)
            .await
            .unwrap();

        let params = &[
            ("bbox", "0,-0.3,0.2,0"),
            ("crs", "EPSG:4326"),
            ("time", "2020-01-01T00:00:00.0Z"),
            ("spatialResolution", "0.1,0.1"),
        ];
        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/plot/{}?{}",
                id,
                &serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            read_body_json(res).await,
            json!({
                "outputFormat": "JsonPlain",
                "plotType": "Statistics",
                "data": {
                    "Raster-1": {
                        "valueCount": 6,
                        "validCount": 6,
                        "min": 1.0,
                        "max": 6.0,
                        "mean": 3.5,
                        "stddev": 1.707_825_127_659_933
                    }
                }
            })
        );
    }

    #[tokio::test]
    async fn json_vega() {
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = InMemoryContext::new_with_context_spec(
            tiling_specification,
            ChunkByteSize::test_default(),
        );
        let session_id = ctx.default_session_ref().await.id();

        let workflow = Workflow {
            operator: Histogram {
                params: HistogramParams {
                    column_name: None,
                    bounds: HistogramBounds::Values {
                        min: 0.0,
                        max: 10.0,
                    },
                    buckets: Some(4),
                    interactive: false,
                },
                sources: example_raster_source().into(),
            }
            .boxed()
            .into(),
        };

        let id = ctx.workflow_registry().register(workflow).await.unwrap();

        let params = &[
            ("bbox", "0,-0.3,0.2,0"),
            ("crs", "EPSG:4326"),
            ("time", "2020-01-01T00:00:00.0Z"),
            ("spatialResolution", "0.1,0.1"),
        ];
        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/plot/{}?{}",
                id,
                &serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        assert_eq!(res.status(), 200);

        let response = serde_json::from_str::<Value>(&read_body_string(res).await).unwrap();

        assert_eq!(response["outputFormat"], "JsonVega");
        assert_eq!(response["plotType"], "Histogram");
        assert!(response["plotType"]["metadata"].is_null());

        let vega_json: Value =
            serde_json::from_str(response["data"]["vegaString"].as_str().unwrap()).unwrap();

        assert_eq!(
            vega_json,
            json!({
                "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
                "data": {
                    "values": [{
                        "binStart": 0.0,
                        "binEnd": 2.5,
                        "Frequency": 2
                    }, {
                        "binStart": 2.5,
                        "binEnd": 5.0,
                        "Frequency": 2
                    }, {
                        "binStart": 5.0,
                        "binEnd": 7.5,
                        "Frequency": 2
                    }, {
                        "binStart": 7.5,
                        "binEnd": 10.0,
                        "Frequency": 0
                    }]
                },
                "mark": "bar",
                "encoding": {
                    "x": {
                        "field": "binStart",
                        "bin": {
                            "binned": true,
                            "step": 2.5
                        },
                        "axis": {
                            "title": ""
                        }
                    },
                    "x2": {
                        "field": "binEnd"
                    },
                    "y": {
                        "field": "Frequency",
                        "type": "quantitative"
                    }
                }
            })
        );
    }

    #[test]
    fn deserialize_get_plot() {
        let params = &[
            ("bbox", "-180,-90,180,90"),
            ("crs", "EPSG:4326"),
            ("time", "2020-01-01T00:00:00.0Z"),
            ("spatialResolution", "0.1,0.1"),
        ];

        assert_eq!(
            serde_urlencoded::from_str::<GetPlot>(&serde_urlencoded::to_string(params).unwrap())
                .unwrap(),
            GetPlot {
                bbox: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                crs: SpatialReference::epsg_4326().into(),
                time: geoengine_datatypes::primitives::TimeInterval::new(
                    DateTime::new_utc(2020, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2020, 1, 1, 0, 0, 0),
                )
                .unwrap()
                .into(),
                spatial_resolution: SpatialResolution::zero_point_one(),
            }
        );
    }

    #[tokio::test]
    async fn check_request_types() {
        async fn get_workflow_json(method: Method) -> ServiceResponse {
            let ctx = InMemoryContext::test_default();
            let session_id = ctx.default_session_ref().await.id();

            let workflow = Workflow {
                operator: Statistics {
                    params: StatisticsParams {
                        column_names: vec![],
                    },
                    sources: vec![example_raster_source()].into(),
                }
                .boxed()
                .into(),
            };

            let id = ctx
                .workflow_registry_ref()
                .register(workflow)
                .await
                .unwrap();

            let params = &[
                ("bbox", "-180,-90,180,90"),
                ("time", "2020-01-01T00:00:00.0Z"),
                ("spatial_resolution", "0.1,0.1"),
            ];
            let req = actix_web::test::TestRequest::default()
                .method(method)
                .uri(&format!(
                    "/plot/{}?{}",
                    id,
                    &serde_urlencoded::to_string(params).unwrap()
                ))
                .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
            send_test_request(req, ctx).await
        }

        check_allowed_http_methods(get_workflow_json, &[Method::GET]).await;
    }
}
