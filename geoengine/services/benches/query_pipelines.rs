#![allow(
    clippy::unwrap_used,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

//! End-to-end WMS queries through `services`, `operators` and `datatypes`.
//!
//! Each case registers a processing graph on the local NDVI test dataset and times
//! `GetMap` requests through the full actix app (handler, processing graph, operators, PNG encoding).
//! Needs the Postgres from `Settings-default.toml` and must run from `geoengine/`; every run uses its own temporary schema.
//!
//! Compare build settings with criterion baselines, e.g. `-- --save-baseline fat` on one build and `-- --baseline fat` on the other.
//! The output hash printed per case must match between builds.

use actix_web::http::header;
use actix_web_httpauth::headers::authorization::Bearer;
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use geoengine_services::{
    contexts::{ApplicationContext, PostgresContext, Session, SessionContext},
    processing_graphs::{processing_graph::ProcessingGraph, registry::ProcessingGraphRegistry},
    users::UserSession,
    util::tests::{add_ndvi_to_datasets, admin_login, send_test_request, with_temp_context},
};
use std::{
    hash::{DefaultHasher, Hash, Hasher},
    time::Duration,
};
use tokio_postgres::NoTls;

/// lat/lon order for EPSG:4326 in WMS 1.3.0
const EUROPE_AFRICA: &str = "-40,-20,80,100";
const SMALL: &str = "20,-10,80,50";
const WORLD: &str = "-90,-180,90,180";

struct Case {
    name: &'static str,
    operator: String,
    /// `crs`, `bbox`, `width`, `height`
    query: (&'static str, &'static str, u32, u32),
}

fn ndvi() -> &'static str {
    r#"{"type": "GdalSource", "params": {"data": "NDVI"}}"#
}

fn cases() -> Vec<Case> {
    let raster = |operator: &str, source: &str| {
        format!(r#"{{"type": "{operator}", "sources": {{"raster": {source}}}, "params": "#)
    };
    vec![
        Case {
            name: "source",
            operator: ndvi().to_string(),
            query: ("EPSG:4326", EUROPE_AFRICA, 1200, 1200),
        },
        Case {
            name: "scaling",
            operator: raster("RasterScaling", ndvi())
                + r#"{"slope": {"type": "constant", "value": 2}, "offset": {"type": "constant", "value": 1},
                      "outputMeasurement": null, "scalingMode": "mulSlopeAddOffset"}}"#,
            query: ("EPSG:4326", EUROPE_AFRICA, 1200, 1200),
        },
        Case {
            name: "neighborhood_sum_5x5",
            operator: raster("NeighborhoodAggregate", ndvi())
                + r#"{"neighborhood": {"type": "rectangle", "dimensions": [5, 5]}, "aggregateFunction": "sum"}}"#,
            query: ("EPSG:4326", EUROPE_AFRICA, 1200, 1200),
        },
        Case {
            name: "interpolation_bilinear_x2",
            operator: raster("Interpolation", ndvi())
                + r#"{"interpolation": "biLinear", "outputResolution": {"type": "fraction", "x": 2, "y": 2}}}"#,
            query: ("EPSG:4326", SMALL, 1200, 1200),
        },
        Case {
            name: "downsampling_x2",
            operator: raster("Downsampling", ndvi())
                + r#"{"samplingMethod": "nearestNeighbor", "outputResolution": {"type": "fraction", "x": 2, "y": 2}}}"#,
            query: ("EPSG:4326", WORLD, 1800, 900),
        },
        Case {
            name: "temporal_mean_6_months",
            operator: raster("TemporalRasterAggregation", ndvi())
                + r#"{"aggregation": {"type": "mean", "ignoreNoData": true}, "window": {"granularity": "months", "step": 6}}}"#,
            query: ("EPSG:4326", EUROPE_AFRICA, 1200, 1200),
        },
        Case {
            name: "reprojection_3857",
            operator: format!(
                r#"{{"type": "Reprojection", "sources": {{"source": {}}}, "params": {{"targetSpatialReference": "EPSG:3857", "deriveOutSpec": "projectionBounds"}}}}"#,
                ndvi()
            ),
            query: (
                "EPSG:3857",
                "-2226389,-4865942,11131949,9349764",
                1200,
                1200,
            ),
        },
    ]
}

async fn register(
    app_ctx: &PostgresContext<NoTls>,
    session: UserSession,
    operator: &str,
) -> String {
    let graph: ProcessingGraph =
        serde_json::from_str(&format!(r#"{{"type": "Raster", "operator": {operator}}}"#)).unwrap();
    app_ctx
        .session_context(session)
        .db()
        .register_processing_graph(graph)
        .await
        .unwrap()
        .to_string()
}

/// Sends the request and returns the PNG.
async fn get_map(app_ctx: PostgresContext<NoTls>, uri: &str, token: &str) -> actix_web::web::Bytes {
    let req = actix_web::test::TestRequest::get()
        .uri(uri)
        .append_header((header::AUTHORIZATION, Bearer::new(token.to_string())));
    let res = send_test_request(req, app_ctx).await;
    let status = res.status();
    let body = actix_web::test::read_body(res).await;
    assert_eq!(status, 200, "{uri}: {}", String::from_utf8_lossy(&body));
    body
}

fn query_pipelines(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    // `with_temp_context` needs a `'static` closure, so it runs as a task that keeps the schema
    // alive until the benches are done and hands the context to criterion over a channel
    let (setup_tx, setup_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let done_rx = std::panic::AssertUnwindSafe(done_rx);
    let context = runtime.spawn(with_temp_context(move |app_ctx, _| async move {
        add_ndvi_to_datasets(&app_ctx).await;
        let session = admin_login(&app_ctx).await;
        let mut uris = Vec::new();
        for case in cases() {
            let id = register(&app_ctx, session.clone(), &case.operator).await;
            let (crs, bbox, width, height) = case.query;
            uris.push((case.name, format!(
                "/wms/{id}?request=GetMap&service=WMS&version=1.3.0&layers={id}&bbox={bbox}&width={width}&height={height}&crs={crs}&styles=&format=image/png&time=2014-01-01T00:00:00.0Z"
            )));
        }
        setup_tx.send((app_ctx, session.id().to_string(), uris)).unwrap();
        done_rx.await.unwrap();
    }));
    let (app_ctx, token, uris) = setup_rx.recv().unwrap();

    let mut group = c.benchmark_group("query_pipelines");
    group
        .sampling_mode(SamplingMode::Flat)
        .sample_size(20)
        .measurement_time(Duration::from_secs(10));
    for (name, uri) in &uris {
        let body = runtime.block_on(get_map(app_ctx.clone(), uri, &token));
        let mut hasher = DefaultHasher::new();
        body.hash(&mut hasher);
        eprintln!(
            "{name}: output {} bytes, hash {:016x}",
            body.len(),
            hasher.finish()
        );

        group.bench_function(*name, |b| {
            b.to_async(&runtime)
                .iter(|| get_map(app_ctx.clone(), uri, &token));
        });
    }
    group.finish();

    done_tx.send(()).unwrap();
    runtime.block_on(context).unwrap();
}

criterion_group!(services, query_pipelines);
criterion_main!(services);
