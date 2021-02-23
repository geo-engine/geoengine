use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use uuid::Uuid;
use warp::Filter;

use geoengine_datatypes::plots::PlotOutputFormat;
use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution, TimeInterval};
use geoengine_operators::engine::{QueryRectangle, TypedPlotQueryProcessor};

use crate::contexts::Context;
use crate::error;
use crate::handlers::authenticate;
use crate::ogc::util::{parse_bbox, parse_time};
use crate::users::session::Session;
use crate::util::parsing::parse_spatial_resolution;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) struct GetPlot {
    #[serde(deserialize_with = "parse_bbox")]
    pub bbox: BoundingBox2D,
    #[serde(deserialize_with = "parse_time")]
    pub time: TimeInterval,
    #[serde(deserialize_with = "parse_spatial_resolution")]
    pub spatial_resolution: SpatialResolution,
}

pub(crate) fn get_plot_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("plot" / Uuid)
        .and(warp::get())
        .and(warp::query::query::<GetPlot>())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and_then(get_plot)
}

// TODO: move into handler once async closures are available?
async fn get_plot<C: Context>(
    id: Uuid,
    params: GetPlot,
    session: Session,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    let workflow = ctx
        .workflow_registry_ref()
        .await
        .load(&WorkflowId(id))
        .await?;

    let operator = workflow.operator.get_plot().context(error::Operator)?;

    let execution_context = ctx.execution_context(&session)?;

    let initialized = operator
        .initialize(&execution_context)
        .context(error::Operator)?;

    let processor = initialized.query_processor().context(error::Operator)?;

    let query_rect = QueryRectangle {
        bbox: params.bbox,
        time_interval: params.time,
        spatial_resolution: params.spatial_resolution,
    };

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

    Ok(warp::reply::json(&output))
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct WrappedPlotOutput {
    output_format: PlotOutputFormat,
    plot_type: &'static str,
    data: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use serde_json::json;

    use geoengine_datatypes::primitives::Measurement;
    use geoengine_datatypes::raster::{Grid2D, RasterDataType, RasterTile2D, TileInformation};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_operators::engine::{PlotOperator, RasterOperator, RasterResultDescriptor};
    use geoengine_operators::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_operators::plot::{
        Histogram, HistogramBounds, HistogramParams, Statistics, StatisticsParams,
    };

    use crate::contexts::InMemoryContext;
    use crate::handlers::handle_rejection;
    use crate::workflows::workflow::Workflow;

    use super::*;
    use crate::util::tests::{check_allowed_http_methods, create_session_helper};
    use warp::http::Response;
    use warp::hyper::body::Bytes;

    fn example_raster_source() -> Box<dyn RasterOperator> {
        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![RasterTile2D::new_with_tile_info(
                    TimeInterval::default(),
                    TileInformation {
                        global_geo_transform: Default::default(),
                        global_tile_position: [0, 0].into(),
                        tile_size_in_pixels: [3, 2].into(),
                    },
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None).unwrap(),
                )],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                },
            },
        }
        .boxed()
    }

    #[tokio::test]
    async fn json() {
        let ctx = InMemoryContext::default();
        let session = create_session_helper(&ctx).await;

        let workflow = Workflow {
            operator: Statistics {
                params: StatisticsParams {},
                raster_sources: vec![example_raster_source()],
                vector_sources: vec![],
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
        let url = format!(
            "/plot/{}/?{}",
            id,
            &serde_urlencoded::to_string(params).unwrap()
        );
        let response = warp::test::request()
            .method("GET")
            .path(&url)
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&get_plot_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(response.status(), 200, "{:?}", response.body());

        let result = String::from_utf8(response.body().to_vec()).unwrap();

        assert_eq!(
            result,
            json!({
                "output_format": "JsonPlain",
                "plot_type": "Statistics",
                "data": [{
                    "pixel_count": 6,
                    "nan_count": 0,
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
        let ctx = InMemoryContext::default();
        let session = create_session_helper(&ctx).await;

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
                raster_sources: vec![example_raster_source()],
                vector_sources: vec![],
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
        let url = format!(
            "/plot/{}/?{}",
            id,
            &serde_urlencoded::to_string(params).unwrap()
        );
        let response = warp::test::request()
            .method("GET")
            .path(&url)
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&get_plot_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(response.status(), 200, "{:?}", response.body());

        let result = String::from_utf8(response.body().to_vec()).unwrap();

        assert_eq!(
            result,
            json!({
                "output_format": "JsonVega",
                "plot_type": "Histogram",
                "data": {
                    "vega_string": "{\"$schema\":\"https://vega.github.io/schema/vega-lite/v4.json\",\"data\":{\"values\":[{\"bin_start\":0.0,\"bin_end\":2.5,\"Frequency\":2},{\"bin_start\":2.5,\"bin_end\":5.0,\"Frequency\":2},{\"bin_start\":5.0,\"bin_end\":7.5,\"Frequency\":2},{\"bin_start\":7.5,\"bin_end\":10.0,\"Frequency\":0}]},\"mark\":\"bar\",\"encoding\":{\"x\":{\"field\":\"bin_start\",\"bin\":{\"binned\":true,\"step\":2.5},\"axis\":{\"title\":\"\"}},\"x2\":{\"field\":\"bin_end\"},\"y\":{\"field\":\"Frequency\",\"type\":\"quantitative\"}}}",
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
            ("time", "2020-01-01T00:00:00.0Z"),
            ("spatial_resolution", "0.1,0.1"),
        ];

        assert_eq!(
            serde_urlencoded::from_str::<GetPlot>(&serde_urlencoded::to_string(params).unwrap())
                .unwrap(),
            GetPlot {
                bbox: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                time: TimeInterval::new(
                    NaiveDate::from_ymd(2020, 1, 1).and_hms(0, 0, 0),
                    NaiveDate::from_ymd(2020, 1, 1).and_hms(0, 0, 0),
                )
                .unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
            }
        );
    }

    #[tokio::test]
    async fn check_request_types() {
        async fn get_workflow_json(method: &str) -> Response<Bytes> {
            let ctx = InMemoryContext::default();
            let session = create_session_helper(&ctx).await;

            let workflow = Workflow {
                operator: Statistics {
                    params: StatisticsParams {},
                    raster_sources: vec![example_raster_source()],
                    vector_sources: vec![],
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
            let url = format!(
                "/plot/{}/?{}",
                id,
                &serde_urlencoded::to_string(params).unwrap()
            );
            warp::test::request()
                .method(method)
                .path(&url)
                .header(
                    "Authorization",
                    format!("Bearer {}", session.id.to_string()),
                )
                .reply(&get_plot_handler(ctx).recover(handle_rejection))
                .await
        }

        check_allowed_http_methods(get_workflow_json, &["GET"]).await;
    }
}
