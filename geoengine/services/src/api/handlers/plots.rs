use crate::{
    api::{
        model::datatypes::TimeInterval,
        ogc::util::{parse_bbox, parse_time},
    },
    config,
    contexts::ApplicationContext,
    error::Result,
    processing_graphs::processing_graph::ProcessingGraphId,
    queries::{plot::plot, run_query},
    util::{parsing::parse_spatial_resolution_option, server::connection_closed},
};
use actix_web::{FromRequest, HttpRequest, HttpResponse, Responder, web};
use geoengine_datatypes::{
    plots::PlotOutputFormat,
    primitives::{BoundingBox2D, SpatialResolution},
    spatial_reference::SpatialReference,
};
use serde::{Deserialize, Serialize};
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
    /// Optional resolution (in the units of `crs`) at which the plot may be computed.
    ///
    /// If set, raster inputs are read at the coarsest overview level that is not coarser than
    /// this resolution, which speeds up plots of large areas at the cost of exactness.
    /// If omitted, raster inputs are read at their native resolution.
    #[serde(default, deserialize_with = "parse_spatial_resolution_option")]
    #[param(example = "0.1,0.1", value_type = Option<String>)]
    pub spatial_resolution: Option<SpatialResolution>,
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
/// 3. Create a statistics processing graph using the "Statistics Plot" example at `/processingGraphs`.
/// 4. Generate the plot with this handler.
#[utoipa::path(
    tag = "Plots",
    get,
    path = "/plot/{id}",
    responses(
        (status = 200, description = "OK", body = WrappedPlotOutput,
            example = json!({
                "outputFormat": "JsonVega",
                "plotType": "Statistics",
                "data": {
                    "vegaSpec": {
                        "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
                        "data": {
                            "values": [{
                                "name": "a",
                                "valueCount": 2,
                                "validCount": 2,
                                "min": 1.0,
                                "max": 2.0,
                                "mean": 1.5,
                                "stddev": 0.5,
                                "percentiles": []
                            }]
                        }
                    },
                    "metadata": null
                }
           })
        )
    ),
    params(
        GetPlot,
        ("id", description = "Processing graph id")
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
    let processing_graph_id = ProcessingGraphId(id.into_inner());
    let (output, computation_id) = run_query(
        &ctx,
        conn_closed,
        plot(ctx.clone(), processing_graph_id, params.into_inner()),
    )
    .await?;

    Ok(HttpResponse::Ok()
        .append_header(computation_id)
        .json(output))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WrappedPlotOutput {
    #[schema(value_type = crate::api::model::datatypes::PlotOutputFormat)]
    pub(crate) output_format: PlotOutputFormat,
    pub(crate) plot_type: &'static str,
    #[schema(value_type = Object)]
    pub(crate) data: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::SessionContext;
    use crate::processing_graphs::registry::ProcessingGraphRegistry;
    use crate::{
        api::model::datatypes::NamedData,
        api::model::processing_graphs::{
            GdalSource, GdalSourceParameters, Histogram, HistogramBounds, HistogramBoundsValues,
            HistogramBuckets, HistogramBucketsNumber, HistogramParameters, PlotOperator,
            RasterOperator, SingleRasterOrVectorOperator, SingleRasterOrVectorSource, Statistics,
            StatisticsParameters, TypedOperator,
        },
        contexts::{PostgresContext, Session},
        ge_context,
        processing_graphs::processing_graph::ProcessingGraph,
        users::UserAuth,
        util::tests::{
            add_ndvi_to_datasets, check_allowed_http_methods, read_body_json, read_body_string,
            send_test_request,
        },
    };
    use actix_web::{
        self,
        dev::ServiceResponse,
        http::{Method, header},
    };
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::{
        primitives::DateTime, raster::TilingSpecification, spatial_reference::SpatialReference,
    };
    use serde_json::{Value, json};
    use tokio_postgres::NoTls;

    fn json_tiling_spec() -> TilingSpecification {
        TilingSpecification::new([3, 2].into())
    }

    #[ge_context::test(tiling_spec = "json_tiling_spec")]
    async fn json(app_ctx: PostgresContext<NoTls>) {
        let (_, dataset_name) = add_ndvi_to_datasets(&app_ctx).await;

        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let processing_graph = ProcessingGraph {
            operator: TypedOperator::Plot(PlotOperator::Statistics(Statistics {
                r#type: Default::default(),
                params: StatisticsParameters {
                    attributes: Default::default(),
                    percentiles: vec![],
                },
                sources: SingleRasterOrVectorSource {
                    source: SingleRasterOrVectorOperator::Raster(RasterOperator::GdalSource(
                        GdalSource {
                            r#type: Default::default(),
                            params: GdalSourceParameters {
                                data: dataset_name.into(),
                                overview_level: None,
                            },
                        },
                    )),
                },
            }))
            .try_into()
            .unwrap(),
        };

        let id = app_ctx
            .session_context(session.clone())
            .db()
            .register_processing_graph(processing_graph)
            .await
            .unwrap();

        let params = &[
            ("bbox", "51,10,52,11"),
            ("crs", "EPSG:4326"),
            ("time", "2014-05-01T00:00:00.0Z"),
            ("spatialResolution", "0.1,0.1"),
        ];
        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/plot/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(
            res.status(),
            200,
            "Error {code}: {body:?}",
            code = res.status(),
            body = read_body_string(res).await
        );

        let response = read_body_json(res).await;

        assert_eq!(response["outputFormat"], "JsonVega");
        assert_eq!(response["plotType"], "Statistics");

        let vega_json: Value = response["data"]["vegaSpec"].clone();

        assert_eq!(
            vega_json["data"]["values"],
            json!([{
                "name": "ndvi",
                "valueCount": 121, // 11*11 pixels of the query rectangle (including its edges)
                "validCount": 121,
                "min": 29.0,
                "max": 255.0,
                "mean": 224.280_991_735_537_24,
                "stddev": 73.572_163_162_468_53,
                "percentiles": []
            }])
        );
    }

    /// Registers a statistics plot on the NDVI dataset (native resolution 0.1°),
    /// queries it with `params` and returns the statistics row of the `ndvi` band.
    async fn query_ndvi_statistics(
        app_ctx: PostgresContext<NoTls>,
        params: &[(&str, &str)],
    ) -> Value {
        let (_, dataset_name) = add_ndvi_to_datasets(&app_ctx).await;

        let session = app_ctx.create_anonymous_session().await.unwrap();

        let processing_graph = ProcessingGraph {
            operator: TypedOperator::Plot(PlotOperator::Statistics(Statistics {
                r#type: Default::default(),
                params: StatisticsParameters {
                    attributes: Default::default(),
                    percentiles: vec![],
                },
                sources: SingleRasterOrVectorSource {
                    source: SingleRasterOrVectorOperator::Raster(RasterOperator::GdalSource(
                        GdalSource {
                            r#type: Default::default(),
                            params: GdalSourceParameters {
                                data: dataset_name.into(),
                                overview_level: None,
                            },
                        },
                    )),
                },
            }))
            .try_into()
            .unwrap(),
        };

        let id = app_ctx
            .session_context(session.clone())
            .db()
            .register_processing_graph(processing_graph)
            .await
            .unwrap();

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/plot/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(
            res.status(),
            200,
            "Error {code}: {body:?}",
            code = res.status(),
            body = read_body_string(res).await
        );

        let response = read_body_json(res).await;

        response["data"]["vegaSpec"]["data"]["values"][0].clone()
    }

    #[ge_context::test(tiling_spec = "json_tiling_spec")]
    async fn it_uses_native_resolution_without_spatial_resolution(app_ctx: PostgresContext<NoTls>) {
        let statistics = query_ndvi_statistics(
            app_ctx,
            &[
                ("bbox", "51,10,52,11"),
                ("crs", "EPSG:4326"),
                ("time", "2014-05-01T00:00:00.0Z"),
            ],
        )
        .await;

        assert_eq!(statistics["valueCount"], 121);
    }

    #[ge_context::test(tiling_spec = "json_tiling_spec")]
    async fn it_uses_the_data_crs_without_crs(app_ctx: PostgresContext<NoTls>) {
        let statistics = query_ndvi_statistics(
            app_ctx,
            &[("bbox", "51,10,52,11"), ("time", "2014-05-01T00:00:00.0Z")],
        )
        .await;

        // same as with `crs=EPSG:4326`, the NDVI's crs
        assert_eq!(statistics["valueCount"], 121);
    }

    #[ge_context::test(tiling_spec = "json_tiling_spec")]
    async fn it_uses_overview_for_coarse_spatial_resolution(app_ctx: PostgresContext<NoTls>) {
        let statistics = query_ndvi_statistics(
            app_ctx,
            &[
                ("bbox", "51,10,52,11"),
                ("crs", "EPSG:4326"),
                ("time", "2014-05-01T00:00:00.0Z"),
                // snaps to overview level 2, i.e., a resolution of 0.4°
                ("spatialResolution", "0.5,0.5"),
            ],
        )
        .await;

        // 4*4 pixels instead of 11*11 pixels at native resolution
        assert_eq!(statistics["valueCount"], 16);
    }

    #[ge_context::test(tiling_spec = "json_tiling_spec")]
    async fn it_uses_overview_for_coarse_spatial_resolution_in_other_crs(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let statistics = query_ndvi_statistics(
            app_ctx,
            &[
                // roughly 10°–11° E, 51°–52° N
                ("bbox", "1113194.9,6621293.7,1224514.4,6800125.5"),
                ("crs", "EPSG:3857"),
                ("time", "2014-05-01T00:00:00.0Z"),
                // roughly 0.3° at this latitude, so it snaps to overview level 1, i.e., 0.2°
                ("spatialResolution", "55000,55000"),
            ],
        )
        .await;

        // 7*7 pixels instead of 11*11 pixels at native resolution
        assert_eq!(statistics["valueCount"], 49);
    }

    fn json_vega_tiling_spec() -> TilingSpecification {
        TilingSpecification::new([3, 2].into())
    }

    #[ge_context::test(tiling_spec = "json_vega_tiling_spec")]
    #[allow(
        clippy::too_many_lines,
        reason = "The test setup is necessarily verbose"
    )]
    async fn json_vega(app_ctx: PostgresContext<NoTls>) {
        let (_, dataset_name) = add_ndvi_to_datasets(&app_ctx).await;

        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let processing_graph = ProcessingGraph {
            operator: TypedOperator::Plot(PlotOperator::Histogram(Histogram {
                r#type: Default::default(),
                params: HistogramParameters {
                    column_name: "ndvi".to_string(),
                    bounds: HistogramBounds::Values(HistogramBoundsValues {
                        min: 0.0.try_into().unwrap(),
                        max: 255.0.try_into().unwrap(),
                    }),
                    buckets: HistogramBuckets::Number(HistogramBucketsNumber {
                        r#type: Default::default(),
                        value: 4,
                    }),
                    interactive: false,
                },
                sources: SingleRasterOrVectorSource {
                    source: SingleRasterOrVectorOperator::Raster(RasterOperator::GdalSource(
                        GdalSource {
                            r#type: Default::default(),
                            params: GdalSourceParameters {
                                data: dataset_name.into(),
                                overview_level: None,
                            },
                        },
                    )),
                },
            }))
            .try_into()
            .unwrap(),
        };

        let id = app_ctx
            .session_context(session.clone())
            .db()
            .register_processing_graph(processing_graph)
            .await
            .unwrap();

        let params = &[
            ("bbox", "51,10,52,11"),
            ("crs", "EPSG:4326"),
            ("time", "2014-05-01T00:00:00.0Z"),
            ("spatialResolution", "0.1,0.1"),
        ];
        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/plot/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(
            res.status(),
            200,
            "Error {code}: {body:?}",
            code = res.status(),
            body = read_body_string(res).await
        );

        let response = serde_json::from_str::<Value>(&read_body_string(res).await).unwrap();

        assert_eq!(response["outputFormat"], "JsonVega");
        assert_eq!(response["plotType"], "Histogram");
        assert!(response["plotType"]["metadata"].is_null());

        let vega_json: Value = response["data"]["vegaSpec"].clone();

        assert_eq!(
            vega_json,
            json!({
                "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
                "data": {
                    "values": [{
                        "binStart": 0.0,
                        "binEnd": 63.75,
                        "Frequency": 18
                    }, {
                        "binStart": 63.75,
                        "binEnd": 127.5,
                        "Frequency": 0
                    }, {
                        "binStart": 127.5,
                        "binEnd": 191.25,
                        "Frequency": 0
                    }, {
                        "binStart": 191.25,
                        "binEnd": 255.0,
                        "Frequency": 103
                    }]
                },
                "mark": "bar",
                "encoding": {
                    "x": {
                        "field": "binStart",
                        "bin": {
                            "binned": true,
                            "step": 63.75
                        },
                        "axis": {
                            "title": "vegetation"
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
                spatial_resolution: Some(SpatialResolution::zero_point_one()),
            }
        );
    }

    #[ge_context::test]
    async fn check_request_types(app_ctx: PostgresContext<NoTls>) {
        async fn get_processing_graph_json(
            app_ctx: PostgresContext<NoTls>,
            method: Method,
        ) -> ServiceResponse {
            let session = app_ctx.create_anonymous_session().await.unwrap();
            let ctx = app_ctx.session_context(session.clone());

            let session_id = session.id();

            let processing_graph = ProcessingGraph {
                operator: TypedOperator::Plot(PlotOperator::Statistics(Statistics {
                    r#type: Default::default(),
                    params: StatisticsParameters {
                        attributes: Default::default(),
                        percentiles: vec![],
                    },
                    sources: SingleRasterOrVectorSource {
                        source: SingleRasterOrVectorOperator::Raster(RasterOperator::GdalSource(
                            GdalSource {
                                r#type: Default::default(),
                                params: GdalSourceParameters {
                                    data: NamedData::with_system_name("ndvi"),
                                    overview_level: None,
                                },
                            },
                        )),
                    },
                }))
                .try_into()
                .unwrap(),
            };

            let id = ctx
                .db()
                .register_processing_graph(processing_graph)
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
                    serde_urlencoded::to_string(params).unwrap()
                ))
                .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
            send_test_request(req, app_ctx).await
        }

        check_allowed_http_methods(
            |method| get_processing_graph_json(app_ctx.clone(), method),
            &[Method::GET],
        )
        .await;
    }
}
