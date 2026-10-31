#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

//! End-to-end WMS queries through `services`, `operators` and `datatypes`.
//!
//! Each case registers a processing graph on the local NDVI test dataset and times
//! `GetMap` requests through the full actix app (handler, processing graph, operators, PNG encoding).
//! Needs the Postgres from `Settings-default.toml`; every run uses its own temporary schema.
//!
//! Env: `BENCH_ITERATIONS` (default 15), `BENCH_FILTER` (substring of the case name).

use actix_web::http::header;
use actix_web_httpauth::headers::authorization::Bearer;
use geoengine_services::{
    contexts::{ApplicationContext, Session, SessionContext},
    processing_graphs::{processing_graph::ProcessingGraph, registry::ProcessingGraphRegistry},
    util::tests::{add_ndvi_to_datasets, admin_login, send_test_request, with_temp_context},
};
use std::{
    hash::{DefaultHasher, Hash, Hasher},
    time::{Duration, Instant},
};

const WARMUP: usize = 3;

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
    app_ctx: &geoengine_services::contexts::PostgresContext<tokio_postgres::NoTls>,
    session: geoengine_services::users::UserSession,
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

fn median(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        f64::midpoint(sorted[n / 2 - 1], sorted[n / 2])
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let iterations: usize = std::env::var("BENCH_ITERATIONS")
        .ok()
        .map_or(15, |s| s.parse().unwrap());
    let filter = std::env::var("BENCH_FILTER").unwrap_or_default();

    with_temp_context(move |app_ctx, _| async move {
        add_ndvi_to_datasets(&app_ctx).await;
        let session = admin_login(&app_ctx).await;
        let token = session.id().to_string();

        println!("case,median_ms,min_ms,mad_ms,iterations,bytes,output_hash");
        for case in cases().into_iter().filter(|c| c.name.contains(&filter)) {
            let id = register(&app_ctx, session.clone(), &case.operator).await;
            let (crs, bbox, width, height) = case.query;
            let uri = format!(
                "/wms/{id}?request=GetMap&service=WMS&version=1.3.0&layers={id}&bbox={bbox}&width={width}&height={height}&crs={crs}&styles=&format=image/png&time=2014-01-01T00:00:00.0Z"
            );

            let mut times = Vec::with_capacity(iterations);
            let mut output = (0, 0);
            for i in 0..WARMUP + iterations {
                let req = actix_web::test::TestRequest::get()
                    .uri(&uri)
                    .append_header((header::AUTHORIZATION, Bearer::new(token.clone())));
                let start = Instant::now();
                let res = send_test_request(req, app_ctx.clone()).await;
                let status = res.status();
                let body = actix_web::test::read_body(res).await;
                let elapsed = start.elapsed();
                assert_eq!(status, 200, "{}: {}", case.name, String::from_utf8_lossy(&body));
                let mut hasher = DefaultHasher::new();
                body.hash(&mut hasher);
                output = (body.len(), hasher.finish());
                if i >= WARMUP {
                    times.push(elapsed);
                }
            }

            let mut ms: Vec<f64> = times.iter().map(Duration::as_secs_f64).map(|s| s * 1e3).collect();
            ms.sort_by(f64::total_cmp);
            let med = median(&ms);
            let mut dev: Vec<f64> = ms.iter().map(|t| (t - med).abs()).collect();
            dev.sort_by(f64::total_cmp);
            println!(
                "{},{med:.2},{:.2},{:.2},{iterations},{},{:016x}",
                case.name,
                ms[0],
                median(&dev),
                output.0,
                output.1
            );
        }
    })
    .await;
}
