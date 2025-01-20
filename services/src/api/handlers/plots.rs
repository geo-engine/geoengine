use crate::api::model::datatypes::TimeInterval;
use crate::api::ogc::util::{parse_bbox, parse_time};
use crate::config;
use crate::contexts::{ApplicationContext, SessionContext};
use crate::error;
use crate::error::Result;
use crate::util::parsing::parse_spatial_resolution;
use crate::util::server::connection_closed;
use crate::util::tests::MockQueryContext;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;
use actix_web::{web, FromRequest, HttpRequest, Responder};
use base64::Engine;
use geoengine_datatypes::operations::reproject::reproject_query;
use geoengine_datatypes::plots::PlotOutputFormat;
use geoengine_datatypes::primitives::{
    BoundingBox2D, ColumnSelection, SpatialResolution, VectorQueryRectangle,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{
    QueryContext, ResultDescriptor, TypedPlotQueryProcessor, WorkflowOperatorPath,
};
use geoengine_operators::util::abortable_query_execution;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::time::Duration;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

pub(crate) fn init_plot_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
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
    #[param(example = "EPSG:4326", value_type = Option<String>)]
    pub crs: Option<SpatialReference>,
    #[serde(deserialize_with = "parse_time")]
    #[param(example = "2020-01-01T00:00:00.0Z", value_type = String)]
    pub time: TimeInterval,
    #[serde(deserialize_with = "parse_spatial_resolution")]
    #[param(example = "0.1,0.1", value_type = String)]
    pub spatial_resolution: SpatialResolution,
}

/// Generates a plot.
///
/// # Example
///
/// 1. Upload the file `plain_data.csv` with the following content:
///
/// ```csv
/// a
/// 1
/// 2
/// ```
/// 2. Create a dataset from it using the "Plain Data" example at `/dataset`.
/// 3. Create a statistics workflow using the "Statistics Plot" example at `/workflow`.
/// 4. Generate the plot with this handler.
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
                    "a": {
                        "max": 2.0,
                        "mean": 1.5,
                        "min": 1.0,
                        "stddev": 0.5,
                        "validCount": 2,
                        "valueCount": 2
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
async fn get_plot_handler<C: ApplicationContext>(
    req: HttpRequest,
    id: web::Path<Uuid>,
    params: web::Query<GetPlot>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    let conn_closed = connection_closed(
        &req,
        config::get_config_element::<config::Plots>()?
            .request_timeout_seconds
            .map(Duration::from_secs),
    );

    let ctx = app_ctx.session_context(session);
    let workflow = ctx.db().load_workflow(&WorkflowId(id.into_inner())).await?;

    let operator = workflow.operator.get_plot()?;

    let execution_context = ctx.execution_context()?;

    let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

    let initialized = operator
        .initialize(workflow_operator_path_root, &execution_context)
        .await?;

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
        attributes: ColumnSelection::all(),
    };

    let query_rect = if request_spatial_ref == workflow_spatial_ref {
        Some(query_rect)
    } else {
        reproject_query(query_rect, workflow_spatial_ref, request_spatial_ref)?
    };

    let Some(query_rect) = query_rect else {
        return Err(error::Error::UnresolvableQueryBoundingBox2DInSrs {
            query_bbox: params.bbox.into(),
            query_srs: workflow_spatial_ref.into(),
        });
    };

    let processor = initialized.query_processor()?;

    let mut query_ctx = ctx.mock_query_context()?;

    let query_abort_trigger = query_ctx.abort_trigger()?;

    let output_format = PlotOutputFormat::from(&processor);
    let plot_type = processor.plot_type();

    let data = match processor {
        TypedPlotQueryProcessor::JsonPlain(processor) => {
            let json = processor.plot_query(query_rect.into(), &query_ctx);
            abortable_query_execution(json, conn_closed, query_abort_trigger).await?
        }
        TypedPlotQueryProcessor::JsonVega(processor) => {
            let chart = processor.plot_query(query_rect.into(), &query_ctx);
            let chart = abortable_query_execution(chart, conn_closed, query_abort_trigger).await;
            let chart = chart?;

            serde_json::to_value(chart).context(error::SerdeJson)?
        }
        TypedPlotQueryProcessor::ImagePng(processor) => {
            let png_bytes = processor.plot_query(query_rect.into(), &query_ctx);
            let png_bytes =
                abortable_query_execution(png_bytes, conn_closed, query_abort_trigger).await;
            let png_bytes = png_bytes?;

            let data_uri = format!(
                "data:image/png;base64,{}",
                base64::engine::general_purpose::STANDARD.encode(png_bytes)
            );

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
    use crate::contexts::Session;
    use crate::ge_context;
    use crate::pro::contexts::PostgresContext;
    use crate::users::UserAuth;
    use crate::util::tests::{
        check_allowed_http_methods, read_body_json, read_body_string, send_test_request,
    };
    use crate::workflows::workflow::Workflow;
    use actix_web;
    use actix_web::dev::ServiceResponse;
    use actix_web::http::{header, Method};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::primitives::CacheHint;
    use geoengine_datatypes::primitives::DateTime;
    use geoengine_datatypes::raster::{
        Grid2D, RasterDataType, RasterTile2D, TileInformation, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{
        PlotOperator, RasterBandDescriptors, RasterOperator, RasterResultDescriptor,
    };
    use geoengine_operators::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_operators::plot::{
        Histogram, HistogramBounds, HistogramBuckets, HistogramParams, Statistics, StatisticsParams,
    };
    use serde_json::{json, Value};
    use tokio_postgres::NoTls;

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
                    0,
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                        .unwrap()
                        .into(),
                    CacheHint::default(),
                )],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed()
    }

    fn json_tiling_spec() -> TilingSpecification {
        TilingSpecification::new([0.0, 0.0].into(), [3, 2].into())
    }

    #[ge_context::test(tiling_spec = "json_tiling_spec")]
    async fn json(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let workflow = Workflow {
            operator: Statistics {
                params: StatisticsParams {
                    column_names: vec![],
                    percentiles: vec![],
                },
                sources: vec![example_raster_source()].into(),
            }
            .boxed()
            .into(),
        };

        let id = app_ctx
            .session_context(session.clone())
            .db()
            .register_workflow(workflow)
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
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            read_body_json(res).await,
            json!({
                "outputFormat": "JsonPlain",
                "plotType": "Statistics",
                "data": {
                    "Raster-1": {
                        "valueCount": 24, // Note: this is caused by the query being a BoundingBox where the right and lower bounds are inclusive. This requires that the tiles that inculde the right and lower bounds are also produced.
                        "validCount": 6,
                        "min": 1.0,
                        "max": 6.0,
                        "mean": 3.5,
                        "stddev": 1.707_825_127_659_933,
                        "percentiles": []
                    }
                }
            })
        );
    }

    fn json_vega_tiling_spec() -> TilingSpecification {
        TilingSpecification::new([0.0, 0.0].into(), [3, 2].into())
    }

    #[ge_context::test(tiling_spec = "json_vega_tiling_spec")]
    async fn json_vega(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let workflow = Workflow {
            operator: Histogram {
                params: HistogramParams {
                    attribute_name: "band".to_string(),
                    bounds: HistogramBounds::Values {
                        min: 0.0,
                        max: 10.0,
                    },
                    buckets: HistogramBuckets::Number { value: 4 },
                    interactive: false,
                },
                sources: example_raster_source().into(),
            }
            .boxed()
            .into(),
        };

        let id = app_ctx
            .session_context(session.clone())
            .db()
            .register_workflow(workflow)
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
        let res = send_test_request(req, app_ctx).await;

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

    #[ge_context::test]
    async fn check_request_types(app_ctx: PostgresContext<NoTls>) {
        async fn get_workflow_json(
            app_ctx: PostgresContext<NoTls>,
            method: Method,
        ) -> ServiceResponse {
            let session = app_ctx.create_anonymous_session().await.unwrap();
            let ctx = app_ctx.session_context(session.clone());

            let session_id = session.id();

            let workflow = Workflow {
                operator: Statistics {
                    params: StatisticsParams {
                        column_names: vec![],
                        percentiles: vec![],
                    },
                    sources: vec![example_raster_source()].into(),
                }
                .boxed()
                .into(),
            };

            let id = ctx.db().register_workflow(workflow).await.unwrap();

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
            send_test_request(req, app_ctx).await
        }

        check_allowed_http_methods(
            |method| get_workflow_json(app_ctx.clone(), method),
            &[Method::GET],
        )
        .await;
    }
}
