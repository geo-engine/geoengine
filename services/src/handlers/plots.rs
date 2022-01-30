use crate::error;
use crate::error::Result;
use crate::handlers::Context;
use crate::ogc::util::{parse_bbox, parse_time};
use crate::util::parsing::parse_spatial_resolution;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;
use actix_web::{web, FromRequest, Responder};
use geoengine_datatypes::operations::reproject::reproject_query;
use geoengine_datatypes::plots::PlotOutputFormat;
use geoengine_datatypes::primitives::{
    BoundingBox2D, SpatialResolution, TimeInterval, VectorQueryRectangle,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{ResultDescriptor, TypedPlotQueryProcessor};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use uuid::Uuid;

pub(crate) fn init_plot_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: Context,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/plot/{id}").route(web::get().to(get_plot_handler::<C>)));
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetPlot {
    #[serde(deserialize_with = "parse_bbox")]
    pub bbox: BoundingBox2D,
    pub crs: Option<SpatialReference>,
    #[serde(deserialize_with = "parse_time")]
    pub time: TimeInterval,
    #[serde(deserialize_with = "parse_spatial_resolution")]
    pub spatial_resolution: SpatialResolution,
}

/// Generates a [plot](WrappedPlotOutput).
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
///     "rasterSources": [
///       {
///         "type": "MockRasterSource",
///         "params": {
///           "data": [
///             {
///               "time": {
///                 "start": -8334632851200000,
///                 "end": 8210298412799999
///               },
///               "tilePosition": [0, 0],
///               "globalGeoTransform": {
///                 "originCoordinate": { "x": 0.0, "y": 0.0 },
///                 "xPixelSize": 1.0,
///                 "yPixelSize": -1.0
///               },
///               "gridArray": {
///                 "shape": {
///                   "shapeArray": [3, 2]
///                 },
///                 "data": [1, 2, 3, 4, 5, 6]
///               }
///             }
///           ],
///           "resultDescriptor": {
///             "dataType": "U8",
///             "spatialReference": "EPSG:4326",
///             "measurement": "unitless"
///           }
///         }
///       }
///     ],
///     "vectorSources": []
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
/// 2. Generate the plot.
/// ```text
/// GET /plot/504ed8a4-e0a4-5cef-9f91-b2ffd4a2b56b?bbox=-180,-90,180,90&crs=EPSG:4326&time=2020-01-01T00%3A00%3A00.0Z&spatialResolution=0.1,0.1
/// Authorization: Bearer 4f0d02f9-68e8-46fb-9362-80f862b7db54
/// ```
/// Response:
/// ```text
/// {
///   "outputFormat": "JsonPlain",
///   "plotType": "Statistics",
///   "data": [
///     {
///       "pixelCount": 6,
///       "nanCount": 0,
///       "min": 1.0,
///       "max": 6.0,
///       "mean": 3.5,
///       "stddev": 1.707825127659933
///     }
///   ]
/// }
/// ```
async fn get_plot_handler<C: Context>(
    id: web::Path<Uuid>,
    params: web::Query<GetPlot>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let workflow = ctx
        .workflow_registry_ref()
        .await
        .load(&WorkflowId(id.into_inner()))
        .await?;

    let operator = workflow.operator.get_plot().context(error::Operator)?;

    let execution_context = ctx.execution_context(session)?;

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
        time_interval: params.time,
        spatial_resolution: params.spatial_resolution,
    };

    let query_rect = if request_spatial_ref == workflow_spatial_ref {
        query_rect
    } else {
        reproject_query(query_rect, workflow_spatial_ref, request_spatial_ref)
            .map_err(From::from)
            .context(error::Operator)?
    };

    let processor = initialized.query_processor().context(error::Operator)?;

    let query_ctx = ctx.query_context()?;

    let output_format = PlotOutputFormat::from(&processor);
    let plot_type = processor.plot_type();

    let data = match processor {
        TypedPlotQueryProcessor::JsonPlain(processor) => processor
            .plot_query(query_rect, &query_ctx)
            .await
            .context(error::Operator)?,
        TypedPlotQueryProcessor::JsonVega(processor) => {
            let chart = processor
                .plot_query(query_rect, &query_ctx)
                .await
                .context(error::Operator)?;

            serde_json::to_value(&chart).context(error::SerdeJson)?
        }
        TypedPlotQueryProcessor::ImagePng(processor) => {
            let png_bytes = processor
                .plot_query(query_rect, &query_ctx)
                .await
                .context(error::Operator)?;

            let data_uri = format!("data:image/png;base64,{}", base64::encode(png_bytes));

            serde_json::to_value(&data_uri).context(error::SerdeJson)?
        }
    };

    let output = WrappedPlotOutput {
        output_format,
        plot_type,
        data,
    };

    Ok(web::Json(output))
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct WrappedPlotOutput {
    output_format: PlotOutputFormat,
    plot_type: &'static str,
    data: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{InMemoryContext, Session, SimpleContext};
    use crate::util::tests::{check_allowed_http_methods, read_body_string, send_test_request};
    use crate::workflows::workflow::Workflow;
    use actix_web;
    use actix_web::dev::ServiceResponse;
    use actix_web::http::{header, Method};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::primitives::Measurement;
    use geoengine_datatypes::raster::{Grid2D, RasterDataType, RasterTile2D, TileInformation};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{PlotOperator, RasterOperator, RasterResultDescriptor};
    use geoengine_operators::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_operators::plot::{
        Histogram, HistogramBounds, HistogramParams, Statistics, StatisticsParams,
    };
    use num_traits::AsPrimitive;
    use serde_json::json;
    use time::macros::datetime;

    fn example_raster_source() -> Box<dyn RasterOperator> {
        let no_data_value = None;

        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![RasterTile2D::new_with_tile_info(
                    TimeInterval::default(),
                    TileInformation {
                        global_geo_transform: TestDefault::test_default(),
                        global_tile_position: [0, 0].into(),
                        tile_size_in_pixels: [3, 2].into(),
                    },
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value)
                        .unwrap()
                        .into(),
                )],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed()
    }

    #[tokio::test]
    async fn json() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let workflow = Workflow {
            operator: Statistics {
                params: StatisticsParams {},
                sources: vec![example_raster_source()].into(),
            }
            .boxed()
            .into(),
        };

        let id = ctx
            .workflow_registry()
            .write()
            .await
            .register(workflow)
            .await
            .unwrap();

        let params = &[
            ("bbox", "-180,-90,180,90"),
            ("crs", "EPSG:4326"),
            ("time", "2020-01-01T00:00:00.0Z"),
            ("spatialResolution", "0.1,0.1"),
        ];
        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/plot/{}/?{}",
                id,
                &serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            read_body_string(res).await,
            json!({
                "outputFormat": "JsonPlain",
                "plotType": "Statistics",
                "data": [{
                    "pixelCount": 6,
                    "nanCount": 0,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.5,
                    "stddev": 1.707_825_127_659_933
                }]
            })
            .to_string()
        );
    }

    #[tokio::test]
    async fn json_vega() {
        let ctx = InMemoryContext::test_default();
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

        let id = ctx
            .workflow_registry()
            .write()
            .await
            .register(workflow)
            .await
            .unwrap();

        let params = &[
            ("bbox", "-180,-90,180,90"),
            ("crs", "EPSG:4326"),
            ("time", "2020-01-01T00:00:00.0Z"),
            ("spatialResolution", "0.1,0.1"),
        ];
        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/plot/{}/?{}",
                id,
                &serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            read_body_string(res).await,
            json!({
                "outputFormat": "JsonVega",
                "plotType": "Histogram",
                "data": {
                    "vegaString": "{\"$schema\":\"https://vega.github.io/schema/vega-lite/v4.json\",\"data\":{\"values\":[{\"binStart\":0.0,\"binEnd\":2.5,\"Frequency\":2},{\"binStart\":2.5,\"binEnd\":5.0,\"Frequency\":2},{\"binStart\":5.0,\"binEnd\":7.5,\"Frequency\":2},{\"binStart\":7.5,\"binEnd\":10.0,\"Frequency\":0}]},\"mark\":\"bar\",\"encoding\":{\"x\":{\"field\":\"binStart\",\"bin\":{\"binned\":true,\"step\":2.5},\"axis\":{\"title\":\"\"}},\"x2\":{\"field\":\"binEnd\"},\"y\":{\"field\":\"Frequency\",\"type\":\"quantitative\"}}}",
                    "metadata": null
                }
            })
            .to_string()
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
                time: TimeInterval::new(
                    datetime!(2020-01-01 0:00:00 UTC),
                    datetime!(2020-01-01 0:00:00 UTC),
                )
                .unwrap(),
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
                    params: StatisticsParams {},
                    sources: vec![example_raster_source()].into(),
                }
                .boxed()
                .into(),
            };

            let id = ctx
                .workflow_registry()
                .write()
                .await
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
                    "/plot/{}/?{}",
                    id,
                    &serde_urlencoded::to_string(params).unwrap()
                ))
                .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
            send_test_request(req, ctx).await
        }

        check_allowed_http_methods(get_workflow_json, &[Method::GET]).await;
    }
}
