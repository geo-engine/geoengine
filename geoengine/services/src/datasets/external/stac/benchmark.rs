//! Opt-in benchmark for STAC metadata search grids and the per-provider cell cache.
//!
//! Run from `geoengine/` with:
//! `cargo test -p geoengine-services --lib stac_grid_benchmark -- --ignored --nocapture`

use super::*;
use crate::datasets::external::stac::{
    StacAssetBand, StacDataProviderDefinition, StacProviderDataset, StacProviderDatasetBand,
    cache::{StacQueryCache, result_bytes},
};
use futures::StreamExt;
use geoengine_datatypes::{
    dataset::DataProviderId,
    primitives::{
        BandSelection, DateTime, RegularTimeDimension, SpatialPartition2D, SpatialResolution,
        TimeGranularity, TimeStep,
    },
    raster::{
        GeoTransform, GridBoundingBox2D, GridIdx2D, GridShape, RasterDataType, TileInformation,
    },
    spatial_reference::SpatialReference,
    util::Identifier,
};
use geoengine_operators::{
    engine::{MetaData, SpatialGridDescriptor},
    source::MultiBandGdalLoadingInfoQueryRectangle,
};
use httptest::{Expectation, Server, matchers::request};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id},
};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
};

const PAGE_SIZE: usize = 32;
const WORLD_EXTENT: (f64, f64, f64, f64) = (-180., -90., 180., 90.);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LookupKind {
    Fetch,
    InFlight,
    CacheHit,
}

struct LookupObservation {
    cell: GridIdx2D,
    kind: LookupKind,
}

#[derive(Default)]
struct TraceFields {
    event: Option<String>,
    row: Option<isize>,
    column: Option<isize>,
    result_id: Option<u64>,
}

impl TraceFields {
    fn cell(&self) -> GridIdx2D {
        GridIdx2D::new_y_x(
            self.row.expect("trace contains cell_y"),
            self.column.expect("trace contains cell_x"),
        )
    }
}

impl Visit for TraceFields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "event" {
            self.event = Some(value.to_owned());
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        match field.name() {
            "cell_y" => self.row = Some(value as isize),
            "cell_x" => self.column = Some(value as isize),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "result_id" {
            self.result_id = Some(value);
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

struct CacheTraceEvent {
    at: Instant,
    event: String,
    lookup_id: Option<u64>,
    cell: GridIdx2D,
    result_id: Option<u64>,
}

#[derive(Clone, Default)]
struct CacheTraceCapture(Arc<Mutex<Vec<CacheTraceEvent>>>);

impl<S> Layer<S> for CacheTraceCapture
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        if attributes.metadata().name() == "stac_cell_lookup" {
            let mut fields = TraceFields::default();
            attributes.record(&mut fields);
            context
                .span(id)
                .expect("lookup span is registered")
                .extensions_mut()
                .insert(fields.cell());
        }
    }

    fn on_event(&self, event: &Event<'_>, context: Context<'_, S>) {
        let mut events = self.0.lock().expect("trace capture mutex is available");
        let at = Instant::now();
        let mut fields = TraceFields::default();
        event.record(&mut fields);
        let Some(event_name) = fields.event.take() else {
            return;
        };
        let span = context
            .event_scope(event)
            .and_then(|mut scope| scope.find(|span| span.metadata().name() == "stac_cell_lookup"));
        let (lookup_id, cell) = if event_name == "cache_hit" {
            (None, fields.cell())
        } else {
            let span = span.expect("search events have a lookup span");
            (
                Some(span.id().into_u64()),
                *span
                    .extensions()
                    .get::<GridIdx2D>()
                    .expect("lookup span contains its cell"),
            )
        };
        events.push(CacheTraceEvent {
            at,
            event: event_name,
            lookup_id,
            cell,
            result_id: fields.result_id,
        });
    }
}

impl CacheTraceCapture {
    fn install(&self) -> tracing::subscriber::DefaultGuard {
        let subscriber = tracing_subscriber::registry().with(self.clone().with_filter(
            tracing_subscriber::filter::Targets::new().with_target(
                "geoengine_services::datasets::external::stac::cache",
                tracing::level_filters::LevelFilter::TRACE,
            ),
        ));
        // Both benchmark tests use a current-thread runtime, so this scoped
        // subscriber also captures detached tokio tasks without a global logger.
        tracing::subscriber::set_default(subscriber)
    }

    fn observations(&self) -> Vec<LookupObservation> {
        let events = self.0.lock().expect("trace capture mutex is available");
        let mut pending = BTreeMap::new();
        let mut completed = BTreeMap::new();
        let mut observations = Vec::new();
        for event in events.iter() {
            match event.event.as_str() {
                "cache_hit" => observations.push(LookupObservation {
                    cell: event.cell,
                    kind: LookupKind::CacheHit,
                }),
                "lookup_started" => {
                    pending.insert(event.lookup_id.expect("lookup has a span"), event.at);
                }
                "fetch_finished" => {
                    let owner = event.lookup_id.expect("search has a lookup span");
                    completed.insert(
                        event.result_id.expect("search has a result identity"),
                        (owner, pending[&owner], event.at),
                    );
                }
                "lookup_finished" => {
                    let lookup = event.lookup_id.expect("lookup has a span");
                    let started = pending.remove(&lookup).expect("lookup start was captured");
                    let (owner, owner_started, finished) =
                        completed[&event.result_id.expect("lookup has a result identity")];
                    // Span IDs and Arc addresses can be reused after their owners
                    // finish. The captured start time distinguishes incarnations.
                    let kind = if lookup == owner && started == owner_started {
                        LookupKind::Fetch
                    } else if started < finished {
                        LookupKind::InFlight
                    } else {
                        LookupKind::CacheHit
                    };
                    observations.push(LookupObservation {
                        cell: event.cell,
                        kind,
                    });
                }
                "fetch_started" => {}
                _ => panic!("unexpected cache trace event: {}", event.event),
            }
        }
        assert!(pending.is_empty(), "all traced lookups completed");
        observations
    }

    fn append_log(&self, log: &mut String) {
        let events = self.0.lock().expect("trace capture mutex is available");
        if let Some(first) = events.first() {
            for event in events.iter() {
                writeln!(
                    log,
                    "{:.3}ms TRACE event={} lookup={:?} cell={} result={:?}",
                    event.at.duration_since(first.at).as_secs_f64() * 1000.,
                    event.event,
                    event.lookup_id,
                    cell_key(event.cell),
                    event.result_id
                )
                .expect("writing trace text cannot fail");
            }
        }
    }
}

#[derive(Default)]
struct Counters {
    attempts: AtomicUsize,
    accepted_pages: AtomicUsize,
    limited: AtomicUsize,
    items: AtomicUsize,
    bytes: AtomicUsize,
}

#[derive(Default)]
struct RateState {
    window: Option<Instant>,
    accepted: usize,
}

#[derive(Clone)]
struct CatalogResponder {
    base_url: String,
    counters: Arc<Counters>,
    rate: Option<(usize, Arc<Mutex<RateState>>)>,
}

impl httptest::responders::Responder for CatalogResponder {
    fn respond<'a>(
        &mut self,
        req: &'a httptest::http::Request<bytes::Bytes>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = httptest::http::Response<bytes::Bytes>> + Send + 'a>,
    > {
        let this = self.clone();
        Box::pin(async move {
            this.counters.attempts.fetch_add(1, Ordering::Relaxed);
            if let Some((limit, state)) = &this.rate {
                let allowed = {
                    let mut state = state.lock().expect("rate limiter mutex is available");
                    if state
                        .window
                        .is_none_or(|at| at.elapsed() >= Duration::from_secs(1))
                    {
                        state.window = Some(Instant::now());
                        state.accepted = 0;
                    }
                    if state.accepted < *limit {
                        state.accepted += 1;
                        true
                    } else {
                        false
                    }
                };
                if !allowed {
                    this.counters.limited.fetch_add(1, Ordering::Relaxed);
                    return response(429, "{}");
                }
            }

            let params: BTreeMap<String, String> =
                url::form_urlencoded::parse(req.uri().query().unwrap_or_default().as_bytes())
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect();
            let bbox = params
                .get("bbox")
                .and_then(|s| parse_bbox(s))
                .unwrap_or(WORLD_EXTENT);
            let offset = params
                .get("offset")
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);
            let limit = params
                .get("limit")
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(PAGE_SIZE)
                .min(PAGE_SIZE);
            let catalog = matching_scene_ids(bbox);
            let page = catalog
                .iter()
                .skip(offset)
                .take(limit)
                .map(|(x, y)| item(*x, *y))
                .collect::<Vec<_>>();
            let next = (offset + page.len() < catalog.len()).then(|| {
                let mut next_params = params.clone();
                next_params.insert("offset".to_owned(), (offset + page.len()).to_string());
                let query = url::form_urlencoded::Serializer::new(String::new())
                    .extend_pairs(next_params.iter())
                    .finish();
                format!("{}{}?{}", this.base_url, req.uri().path(), query)
            });
            let links =
                next.map_or_else(Vec::new, |path| vec![json!({"rel":"next", "href": path})]);
            let body = serde_json::to_string(
                &json!({"type":"FeatureCollection", "features":page, "links":links}),
            )
            .expect("catalog response serializes");
            this.counters.accepted_pages.fetch_add(1, Ordering::Relaxed);
            this.counters.items.fetch_add(page.len(), Ordering::Relaxed);
            this.counters.bytes.fetch_add(body.len(), Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(20 + (page.len() as u64))).await;
            response(200, &body)
        })
    }
}

fn response(status: u16, body: &str) -> httptest::http::Response<bytes::Bytes> {
    httptest::http::Response::builder()
        .status(status)
        .header("content-type", "application/geo+json")
        .body(bytes::Bytes::copy_from_slice(body.as_bytes()))
        .expect("valid mock response")
}

fn parse_bbox(value: &str) -> Option<(f64, f64, f64, f64)> {
    let values = value
        .split(',')
        .map(str::parse::<f64>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    (values.len() >= 4).then(|| (values[0], values[1], values[2], values[3]))
}

fn matching_scene_ids(bbox: (f64, f64, f64, f64)) -> Vec<(i32, i32)> {
    let (min_x, min_y, max_x, max_y) = bbox;
    let x0 = min_x.floor().max(-180.) as i32;
    let x1 = max_x.ceil().min(180.) as i32;
    let y0 = min_y.floor().max(-80.) as i32;
    let y1 = max_y.ceil().min(80.) as i32;
    let mut ids = Vec::new();
    for y in y0..y1 {
        for x in x0..x1 {
            ids.push((x, y));
        }
    }
    ids
}

fn expected_scene_files(bbox: (f64, f64, f64, f64)) -> BTreeSet<String> {
    matching_scene_ids(bbox)
        .into_iter()
        .map(|(x, y)| format!("https://assets.example/scene-{x}-{y}.tif"))
        .collect()
}

fn cell_key(index: GridIdx2D) -> String {
    format!("{}:{}", index.0[0], index.0[1])
}

fn item(x: i32, y: i32) -> Value {
    let id = format!("scene-{x}-{y}");
    json!({
        "type":"Feature", "stac_version":"1.1.0", "stac_extensions":[], "id":id,
        "geometry":{"type":"Polygon", "coordinates":[[[x,y],[x+1,y],[x+1,y+1],[x,y+1],[x,y]]]},
        "bbox":[x,y,x+1,y+1], "properties":{"datetime":"2026-01-03T12:00:00Z"}, "links":[],
        "assets":{"data":{"href":format!("https://assets.example/{id}.tif"), "title":"data", "data_type":"uint16",
            "proj:code":"EPSG:4326", "proj:shape":[1,1], "proj:transform":[1,0,x,0,-1,y+1]}}
    })
}

fn provider(
    server: &Server,
    counters: Arc<Counters>,
    rate: Option<usize>,
    grid: StacGrid,
    cache_bytes: usize,
) -> StacDataProvider {
    let responder = CatalogResponder {
        base_url: server.url_str("/").trim_end_matches('/').to_owned(),
        counters,
        rate: rate.map(|limit| (limit, Arc::new(Mutex::new(RateState::default())))),
    };
    server.expect(
        Expectation::matching(request::method_path("GET", "/collections/synthetic/items"))
            .times(1..)
            .respond_with(responder),
    );
    let dataset = StacProviderDataset {
        name: "Synthetic 1 degree scenes".to_owned(),
        description: String::new(),
        data_type: RasterDataType::U16,
        resolution: SpatialResolution::new_unchecked(1., 1.),
        projection: SpatialReference::epsg_4326(),
        spatial_grid: SpatialGridDescriptor::source_from_parts(
            GeoTransform::new((-180., 90.).into(), 1., -1.),
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([179, 359]))
                .expect("world grid bounds"),
        ),
        bands: vec![StacProviderDatasetBand::new_unitless(StacAssetBand {
            asset_title: "data".to_owned(),
            band_name: None,
        })],
    };
    let mut provider = StacDataProvider::from_definition(StacDataProviderDefinition {
        id: DataProviderId::new(),
        name: "benchmark".to_owned(),
        description: String::new(),
        priority: None,
        api_url: server.url_str("/").trim_end_matches('/').to_owned(),
        collection_name: "synthetic".to_owned(),
        s3_config: None,
        authentication: None,
        time_dimension: TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(
            TimeStep {
                granularity: TimeGranularity::Days,
                step: 1,
            },
        )),
        datasets: vec![dataset],
        page_limit: PAGE_SIZE as i64,
        query_timeout_secs: 60,
        cache_ttl_secs: None,
        stac_grid: Some(grid),
    })
    .expect("benchmark provider config is valid");
    // Fresh, local cache for every scenario; cache=0 keeps in-flight coalescing.
    provider.query_cache = Arc::new(StacQueryCache::new(cache_bytes, Duration::from_hours(1)));
    provider
}

fn metadata(provider: &StacDataProvider) -> StacMultiBandMetaData {
    StacMultiBandMetaData {
        api_url: provider.api_url.clone(),
        collection_name: provider.collection_name.clone(),
        s3_config: provider.s3_config.clone(),
        time_dimension: provider.time_dimension,
        dataset: provider.datasets[0].clone(),
        page_limit: provider.page_limit,
        client: provider.client.clone(),
        cache_ttl_secs: provider.cache_ttl_secs,
        query_cache: provider.query_cache.clone(),
        grid: provider.grid,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn it_captures_inflight_sharing_and_cache_hits_from_trace_logs() {
    for capacity in [0, 100 * 1024 * 1024] {
        let logs = CacheTraceCapture::default();
        let _trace_guard = logs.install();
        let server = Server::run();
        let provider = provider(
            &server,
            Arc::new(Counters::default()),
            None,
            StacGrid {
                target_number_of_cells: 64_800,
            },
            capacity,
        );
        let meta = metadata(&provider);
        let query = || {
            MultiBandGdalLoadingInfoQueryRectangle::new(
                SpatialPartition2D::new((0.1, 0.4).into(), (0.4, 0.1).into()).expect("test bounds"),
                TimeInterval::new(
                    DateTime::new_utc(2026, 1, 3, 0, 0, 0),
                    DateTime::new_utc(2026, 1, 4, 0, 0, 0),
                )
                .expect("test day"),
                BandSelection::first(),
                true,
            )
        };
        let (first, follower) =
            tokio::join!(meta.loading_info(query()), meta.loading_info(query()));
        first.expect("search succeeds");
        follower.expect("in-flight waiter succeeds");
        meta.loading_info(query())
            .await
            .expect("subsequent lookup succeeds");
        let observations = logs.observations();
        let count = |kind| observations.iter().filter(|o| o.kind == kind).count();
        assert_eq!(observations.len(), 3);
        assert_eq!(count(LookupKind::InFlight), 1);
        assert_eq!(count(LookupKind::Fetch), if capacity == 0 { 2 } else { 1 });
        assert_eq!(count(LookupKind::CacheHit), usize::from(capacity > 0));
    }
}

fn xyz_bbox(z: u8, x: u32, y: u32) -> (f64, f64, f64, f64) {
    let n = 2_f64.powi(i32::from(z));
    let lon = |x: f64| x / n * 360. - 180.;
    let lat = |y: f64| {
        (std::f64::consts::PI * (1. - 2. * y / n))
            .sinh()
            .atan()
            .to_degrees()
    };
    (
        lon(f64::from(x)),
        lat(f64::from(y + 1)),
        lon(f64::from(x + 1)),
        lat(f64::from(y)),
    )
}

#[derive(Clone)]
struct Tile {
    x: u32,
    y: u32,
    bbox: (f64, f64, f64, f64),
    arrival_ms: u64,
}

fn viewport(z: u8, center: (f64, f64), reverse: bool) -> Vec<Tile> {
    let n = 2_f64.powi(i32::from(z));
    let x = ((center.0 + 180.) / 360. * n).floor() as u32;
    let lat = center.1.to_radians();
    let y =
        ((1. - (lat.tan() + 1. / lat.cos()).ln() / std::f64::consts::PI) / 2. * n).floor() as u32;
    let mut coordinates = Vec::new();
    for dy in -1_i64..=1 {
        for dx in -1_i64..=1 {
            coordinates.push((
                (i64::from(x) + dx).clamp(0, n as i64 - 1) as u32,
                (i64::from(y) + dy).clamp(0, n as i64 - 1) as u32,
            ));
        }
    }
    if reverse {
        // deterministic Fisher-Yates, seed 7
        let mut state = 7_u64;
        for i in (1..coordinates.len()).rev() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            coordinates.swap(i, (state as usize) % (i + 1));
        }
    } else {
        coordinates.sort_by_key(|(tx, ty)| (tx.abs_diff(x) + ty.abs_diff(y), *ty, *tx));
    }
    coordinates
        .into_iter()
        .enumerate()
        .map(|(i, (x, y))| Tile {
            x,
            y,
            bbox: xyz_bbox(z, x, y),
            arrival_ms: (i * 2) as u64,
        })
        .collect()
}

fn tile_information(tile: &Tile) -> TileInformation {
    TileInformation::new(
        GridIdx2D::new([0, 0]),
        GridShape::new([1, 1]),
        GeoTransform::new(
            (tile.bbox.0, tile.bbox.3).into(),
            tile.bbox.2 - tile.bbox.0,
            tile.bbox.1 - tile.bbox.3,
        ),
    )
}

#[derive(serde::Deserialize)]
struct BenchResult {
    zoom: u8,
    center: String,
    order: String,
    rate: String,
    mode: String,
    target_cells: usize,
    viewport_ms: f64,
    attempts: usize,
    inflight_joins: usize,
    inflight_pages_avoided: usize,
    cache_hits: usize,
    cached_file_references: usize,
    cached_unique_files: usize,
    estimated_cache_bytes: usize,
}

/// Cold working set of complete cell results, before eviction. Uses the same
/// metadata parser and byte weigher as the provider, outside the timed queries.
fn cache_footprint(
    meta: &StacMultiBandMetaData,
    cells: &BTreeMap<String, Vec<(i32, i32)>>,
) -> (usize, usize, usize) {
    let mut references = 0;
    let mut unique_files = BTreeSet::new();
    let mut bytes = 0;
    for scenes in cells.values() {
        let mut files = Vec::new();
        for &(x, y) in scenes {
            let scene: stac::Item =
                serde_json::from_value(item(x, y)).expect("mock scene is valid STAC");
            meta.process_stac_item(&scene, &mut files)
                .expect("mock asset metadata is valid");
            // The fixture has one asset/band/version per scene, so scene IDs are
            // sufficient here; production interning would need full metadata identity.
            unique_files.insert((x, y));
        }
        assert_eq!(files.len(), scenes.len());
        references += files.len();
        bytes += result_bytes(&files);
    }
    (references, unique_files.len(), bytes)
}

fn median(values: impl Iterator<Item = f64>) -> f64 {
    let mut values = values.collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        values[middle - 1].midpoint(values[middle])
    } else {
        values[middle]
    }
}

#[allow(clippy::print_stdout)]
fn print_cache_footprints(results: &[BenchResult]) {
    println!("\nMetadata working set, zoom 8 ordinary viewport (before eviction):");
    println!("\n| Cells | File references / unique files | Copies/file | Cache weight KiB |");
    println!("|-------|--------------------------------|-------------|------------------|");
    for (target, label) in [(512, "512"), (8192, "8,192"), (131_072, "131,072")] {
        let grid = results
            .iter()
            .filter(|r| {
                r.zoom == 8
                    && r.center == "ordinary"
                    && r.order == "center-out"
                    && r.rate == "unlimited"
                    && r.mode == "grid-cache"
                    && r.target_cells == target
            })
            .collect::<Vec<_>>();
        let references = median(grid.iter().map(|r| r.cached_file_references as f64));
        let unique = median(grid.iter().map(|r| r.cached_unique_files as f64));
        let copies = if unique == 0. {
            0.
        } else {
            references / unique
        };
        let kib = median(grid.iter().map(|r| r.estimated_cache_bytes as f64)) / 1024.;
        println!(
            "| {label:>5} | {references:24.0} / {unique:<3.0} | {copies:10.2}x | {kib:16.1} |"
        );
    }
    println!(
        "\nWeights estimate metadata, keys and result headers; exclude raster pixels, cache internals and allocation overhead."
    );
}

#[allow(clippy::print_stdout)]
fn print_summary(csv: &str, repeats: usize) {
    let results = csv::Reader::from_reader(csv.as_bytes())
        .deserialize::<BenchResult>()
        .collect::<Result<Vec<_>, _>>()
        .expect("benchmark CSV matches its result schema");
    // Keep this summary in the same layout as the measured-results doc comment.
    // The complete zoom/position/arrival-order matrix stays in the CSV files.
    let group = |center: &str, rate: &str, mode: &str, target: usize| {
        results
            .iter()
            .filter(|r| {
                r.zoom == 8
                    && r.order == "center-out"
                    && r.center == center
                    && r.rate == rate
                    && r.mode == mode
                    && r.target_cells == target
            })
            .collect::<Vec<_>>()
    };
    let positions = [("ordinary", "Ordinary"), ("grid-boundary", "Boundary")]
        .into_iter()
        .filter(|(center, _)| !group(center, "unlimited", "baseline", 0).is_empty())
        .collect::<Vec<_>>();
    println!(
        "\nSTAC grid benchmark: {} cases, {repeats} repetition(s), debug profile",
        results.len()
    );
    println!("Nine XYZ tiles, six concurrent jobs; local paginated STAC; metadata lookup only.");
    println!("\nMedian viewport times (ms), {repeats} debug run(s), zoom 8, center-out arrivals:");
    println!("\n| STAC limit | Position | Per-tile | 512 (default) | 8,192 | 131,072 |");
    println!("|------------|----------|----------|---------------|-------|---------|");
    for (rate, label) in [("unlimited", "Unlimited"), ("8ps", "8 pages/s")] {
        for &(center, position) in &positions {
            let timings = [
                ("baseline", 0),
                ("grid-cache", 512),
                ("grid-cache", 8192),
                ("grid-cache", 131_072),
            ]
            .map(|(mode, target)| {
                median(
                    group(center, rate, mode, target)
                        .iter()
                        .map(|r| r.viewport_ms),
                )
            });
            println!(
                "| {label:10} | {position:8} | {:8.0} | {:13.0} | {:5.0} | {:7.0} |",
                timings[0], timings[1], timings[2], timings[3]
            );
        }
    }
    println!("\n8,192 cells at 8 pages/s versus per-tile searches (including 429 retries):");
    for &(center, position) in &positions {
        let baseline = group(center, "8ps", "baseline", 0);
        let grid = group(center, "8ps", "grid-cache", 8192);
        let before = median(baseline.iter().map(|r| r.attempts as f64));
        let after = median(grid.iter().map(|r| r.attempts as f64));
        let reduction = 100. * (1. - after / before);
        let speedup = median(baseline.iter().map(|r| r.viewport_ms))
            / median(grid.iter().map(|r| r.viewport_ms));
        println!(
            "{position}: HTTP attempts {before:.0} -> {after:.0} ({reduction:.0}% fewer), viewport {speedup:.1}x faster."
        );
    }
    println!("\nTRACE-derived reuse at 8 pages/s, same zoom and arrival order:");
    println!(
        "\n| Cells | Position | Queries attached in-flight | Duplicate pages avoided | Completed-cache hits |"
    );
    println!(
        "|-------|----------|----------------------------|-------------------------|----------------------|"
    );
    for (target, label) in [(512, "512"), (8192, "8,192"), (131_072, "131,072")] {
        for &(center, position) in &positions {
            let grid = group(center, "8ps", "grid-cache", target);
            println!(
                "| {label:>5} | {position:8} | {:26.0} | {:23.0} | {:20.0} |",
                median(grid.iter().map(|r| r.inflight_joins as f64)),
                median(grid.iter().map(|r| r.inflight_pages_avoided as f64)),
                median(grid.iter().map(|r| r.cache_hits as f64))
            );
        }
    }
    println!(
        "\nAttached queries are cell lookups; avoided pages compare identical grid lookups without sharing."
    );
    println!(
        "HTTP attempts above compare the per-tile baseline; hypothetical retry savings are excluded."
    );
    print_cache_footprints(&results);
    println!(
        "The complete matrix (including zoom 10, shuffled arrivals and in-flight-only runs) is in CSV files."
    );
}

/// Benchmarks PR #1323 against per-tile searches using a local paginated STAC
/// server: nine XYZ tiles, six concurrent jobs; metadata lookup only.
///
/// Run from `geoengine/`: `STAC_BENCH_REPEATS=3 cargo test -p geoengine-services
/// --lib stac_grid_benchmark -- --ignored --nocapture`.
/// Median viewport times (ms), 3 debug run(s), zoom 8, center-out arrivals:
///
/// | STAC limit | Position | Per-tile | 512 (default) | 8,192 | 131,072 |
/// |------------|----------|----------|---------------|-------|---------|
/// | Unlimited  | Ordinary |       61 |           280 |    85 |     214 |
/// | Unlimited  | Boundary |       58 |           554 |    69 |     203 |
/// | 8 pages/s  | Ordinary |     1063 |           278 |    85 |    3221 |
/// | 8 pages/s  | Boundary |     1061 |          2560 |    69 |    3215 |
///
/// 8,192 cells at 8 pages/s versus per-tile searches (including 429 retries):
/// Ordinary: HTTP attempts 10 -> 4 (60% fewer), viewport 12.5x faster.
/// Boundary: HTTP attempts 10 -> 4 (60% fewer), viewport 15.4x faster.
///
/// The default grid improves the ordinary viewport but becomes slower near a
/// boundary because it fetches more pages. Timings are illustrative, not assertions.
///
/// TRACE-derived reuse at 8 pages/s, same zoom and arrival order:
///
/// | Cells | Position | Queries attached in-flight | Duplicate pages avoided | Completed-cache hits |
/// |-------|----------|----------------------------|-------------------------|----------------------|
/// |   512 | Ordinary |                          5 |                      25 |                    3 |
/// |   512 | Boundary |                          7 |                      35 |                    1 |
/// | 8,192 | Ordinary |                          7 |                       7 |                    1 |
/// | 8,192 | Boundary |                          7 |                       7 |                    1 |
/// | 131,072 | Ordinary |                          3 |                       3 |                    9 |
/// | 131,072 | Boundary |                          3 |                       3 |                    9 |
///
/// Attached queries are cell lookups; avoided pages compare identical grid lookups without sharing.
/// HTTP attempts above compare the per-tile baseline; hypothetical retry savings are excluded.
///
/// Metadata working set, zoom 8 ordinary viewport (before eviction):
///
/// | Cells | File references / unique files | Copies/file | Cache weight KiB |
/// |-------|--------------------------------|-------------|------------------|
/// |   512 |                      144 / 144 |       1.00x |             53.1 |
/// | 8,192 |                       64 / 49  |       1.31x |             23.9 |
/// | 131,072 |                       90 / 25  |       3.60x |             35.3 |
///
/// Weights estimate metadata, keys and result headers; exclude raster pixels, cache internals and allocation overhead.
///
/// Smaller cells duplicate metadata; larger cells overfetch. Choose by cell width,
/// latency and memory; this cold-viewport run does not measure eviction churn.
///
/// The complete matrix (including zoom 10, shuffled arrivals and in-flight-only runs) is in CSV files.
#[tokio::test(flavor = "current_thread")]
#[ignore = "opt-in STAC grid performance benchmark; starts a local mock server"]
#[allow(clippy::too_many_lines, clippy::print_stdout, clippy::print_stderr)]
async fn it_stac_grid_benchmark() {
    let out = std::env::var_os("STAC_BENCH_OUTPUT_DIR").map_or_else(
        || std::env::temp_dir().join("stac-grid-benchmark"),
        PathBuf::from,
    );
    fs::create_dir_all(&out).expect("benchmark output directory is writable");
    let repeats = std::env::var("STAC_BENCH_REPEATS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1);
    let quick = std::env::var_os("STAC_BENCH_QUICK").is_some();
    let zooms: &[u8] = if quick { &[8] } else { &[8, 10] };
    let centers = if quick {
        vec![("ordinary", (8.6, 50.1))]
    } else {
        vec![("ordinary", (8.6, 50.1)), ("grid-boundary", (0., 45.))]
    };
    let mut csv = String::from(
        "repeat,zoom,center,order,rate,mode,target_cells,cell_side_units,tile_count,total_cell_intersections,unique_cells,tile_first_ms,tile_p50_ms,tile_p95_ms,viewport_ms,attempts,accepted_pages,rate_limited,returned_items,returned_bytes,cell_fetches,inflight_joins,inflight_pages_avoided,cache_hits,cached_pages_avoided,cached_file_references,cached_unique_files,cache_duplication_factor,estimated_cache_bytes,correct\n",
    );
    let mut tile_csv = String::from(
        "repeat,zoom,center,order,rate,mode,target_cells,tile_x,tile_y,arrival_ms,dispatch_ms,completion_ms\n",
    );
    let mut trace = String::from(
        "zoom,center,order,tile_z,tile_x,tile_y,arrival_rank,arrival_ms,bbox_west,bbox_south,bbox_east,bbox_north,cells_512,cells_8192,cells_131072\n",
    );
    let mut cache_log = String::new();
    for repeat in 1..=repeats {
        for zoom in zooms.iter().copied() {
            for (center_name, center) in centers.iter().copied() {
                for reverse in [false, true] {
                    let order = if reverse {
                        "seeded-permutation"
                    } else {
                        "center-out"
                    };
                    let tiles = viewport(zoom, center, reverse);
                    for tile in &tiles {
                        let bounds = SpatialPartition2D::new(
                            (tile.bbox.0, tile.bbox.3).into(),
                            (tile.bbox.2, tile.bbox.1).into(),
                        )
                        .expect("tile bounds");
                        let projected = SpatialReference::epsg_4326()
                            .area_of_use_projected::<BoundingBox2D>()
                            .expect("EPSG:4326 extent");
                        let mut ids = Vec::new();
                        for target in [512, 8192, 131_072] {
                            let grid = StacGrid {
                                target_number_of_cells: target,
                            }
                            .for_extent(projected)
                            .expect("grid is valid");
                            let cells = grid
                                .cells_for_bbox(bounds.as_bbox())
                                .expect("cell intersections")
                                .map(|cell| cell_key(cell.index))
                                .collect::<Vec<_>>();
                            ids.push(cells.join(";"));
                        }
                        if repeat == 1 {
                            writeln!(trace, "{zoom},{center_name},{order},{zoom},{},{},{},{},{:.8},{:.8},{:.8},{:.8},{},{},{}",
                                tile.x, tile.y, tile.arrival_ms / 2, tile.arrival_ms, tile.bbox.0, tile.bbox.1, tile.bbox.2, tile.bbox.3, ids[0], ids[1], ids[2])
                                .expect("writing a trace row to a String cannot fail");
                        }
                    }
                    for rate in [None, Some(8)] {
                        for (mode, target, capacity) in [
                            ("baseline", 0_usize, 0_usize),
                            ("grid-cache", 512, 100 * 1024 * 1024),
                            ("grid-cache", 8192, 100 * 1024 * 1024),
                            ("grid-cache", 131_072, 100 * 1024 * 1024),
                            ("grid-inflight", 512, 0),
                        ] {
                            let captured_logs = CacheTraceCapture::default();
                            let _trace_guard = captured_logs.install();
                            let server = Server::run();
                            let counters = Arc::new(Counters::default());
                            let grid_target = if target == 0 { 512 } else { target as i32 };
                            let provider = provider(
                                &server,
                                counters.clone(),
                                rate,
                                StacGrid {
                                    target_number_of_cells: grid_target,
                                },
                                capacity,
                            );
                            let meta = metadata(&provider);
                            let base_url = Url::parse(&provider.api_url).expect("mock URL parses");
                            let items_url =
                                join_base_url_and_path(&base_url, "collections/synthetic/items")
                                    .expect("items URL");
                            let projected = meta
                                .dataset
                                .projection
                                .area_of_use_projected::<BoundingBox2D>()
                                .expect("dataset CRS extent");
                            let side = if target == 0 {
                                0.
                            } else {
                                projected.size_x().sqrt() * projected.size_y().sqrt()
                                    / (target as f64).sqrt()
                            };
                            let mut intersections = 0;
                            let mut unique = BTreeSet::new();
                            let mut cell_pages = BTreeMap::new();
                            let mut cell_scenes = BTreeMap::new();
                            if target > 0 {
                                for tile in &tiles {
                                    let bounds = SpatialPartition2D::new(
                                        (tile.bbox.0, tile.bbox.3).into(),
                                        (tile.bbox.2, tile.bbox.1).into(),
                                    )
                                    .expect("tile bounds");
                                    let (grid, cells) =
                                        meta.grid_cells(bounds).expect("grid query bounds");
                                    for cell in cells {
                                        intersections += 1;
                                        unique.insert(cell_key(cell.index));
                                        let footprint = grid
                                            .clip_cell_to_extent(cell.bbox)
                                            .expect("selected cell overlaps CRS extent");
                                        let bbox =
                                            stac_query_bbox(footprint, meta.dataset.projection)
                                                .expect("cell projection succeeds")
                                                .expect("cell has a STAC footprint");
                                        let scenes = matching_scene_ids((
                                            bbox.lower_left().x,
                                            bbox.lower_left().y,
                                            bbox.upper_right().x,
                                            bbox.upper_right().y,
                                        ));
                                        cell_pages.insert(
                                            cell_key(cell.index),
                                            scenes.len().div_ceil(PAGE_SIZE).max(1),
                                        );
                                        cell_scenes.insert(cell_key(cell.index), scenes);
                                    }
                                }
                            }
                            let origin = tokio::time::Instant::now();
                            let mut queue = futures::stream::iter(
                                tiles.iter().cloned().enumerate().map(|(rank, tile)| {
                                    let meta = &meta;
                                    let items_url = items_url.clone();
                                    async move {
                                        tokio::time::sleep_until(
                                            origin + Duration::from_millis(tile.arrival_ms),
                                        )
                                        .await;
                                        let dispatch = tokio::time::Instant::now()
                                            .duration_since(origin)
                                            .as_secs_f64()
                                            * 1000.;
                                        let time = TimeInterval::new(
                                            DateTime::new_utc(2026, 1, 3, 0, 0, 0),
                                            DateTime::new_utc(2026, 1, 4, 0, 0, 0),
                                        )
                                        .expect("day interval");
                                        let spatial = SpatialPartition2D::new(
                                            (tile.bbox.0, tile.bbox.3).into(),
                                            (tile.bbox.2, tile.bbox.1).into(),
                                        )
                                        .expect("tile spatial bounds");
                                        let actual = if mode == "baseline" {
                                            let bbox = BoundingBox2D::new(
                                                (tile.bbox.0, tile.bbox.1).into(),
                                                (tile.bbox.2, tile.bbox.3).into(),
                                            )
                                            .expect("tile bbox");
                                            meta.search_bbox(items_url, bbox, time)
                                                .await
                                                .expect("baseline STAC search succeeds")
                                                .into_iter()
                                                .filter(|file| {
                                                    file.time.intersects(&time)
                                                        && file
                                                            .spatial_partition
                                                            .intersects(&spatial)
                                                })
                                                .map(|file| {
                                                    file.params
                                                        .file_path
                                                        .to_string_lossy()
                                                        .into_owned()
                                                })
                                                .collect::<BTreeSet<_>>()
                                        } else {
                                            let query = MultiBandGdalLoadingInfoQueryRectangle::new(
                                                spatial,
                                                time,
                                                BandSelection::first(),
                                                true,
                                            );
                                            let info = meta
                                                .loading_info(query)
                                                .await
                                                .expect("production loading_info succeeds");
                                            info.tile_files(time, tile_information(&tile), 0)
                                                .into_iter()
                                                .map(|params| {
                                                    params.file_path.to_string_lossy().into_owned()
                                                })
                                                .collect::<BTreeSet<_>>()
                                        };
                                        let expected = expected_scene_files(tile.bbox);
                                        (
                                            rank,
                                            actual,
                                            expected,
                                            dispatch,
                                            tokio::time::Instant::now()
                                                .duration_since(origin)
                                                .as_secs_f64()
                                                * 1000.,
                                        )
                                    }
                                }),
                            )
                            .buffer_unordered(6);
                            let mut latencies = Vec::new();
                            let mut correct = true;
                            tokio::time::timeout(Duration::from_mins(1), async {
                                while let Some((rank, actual, expected, dispatch, completion)) = queue.next().await {
                                    if actual != expected { correct = false; }
                                    latencies.push(completion);
                                    writeln!(tile_csv, "{repeat},{zoom},{center_name},{order},{},{mode},{target},{},{},{},{dispatch:.3},{completion:.3}",
                                        if rate.is_some() { "8ps" } else { "unlimited" }, tiles[rank].x, tiles[rank].y, tiles[rank].arrival_ms)
                                        .expect("writing a tile row to a String cannot fail");
                                }
                            }).await.expect("benchmark case must finish within 60 seconds");
                            assert!(
                                correct,
                                "benchmark output differs from synthetic catalog: {zoom}/{center_name}/{order}/{mode}/{target}"
                            );
                            latencies.sort_by(f64::total_cmp);
                            let percentile = |p: f64| {
                                latencies[((latencies.len() - 1) as f64 * p).ceil() as usize]
                            };
                            let elapsed = origin.elapsed().as_secs_f64() * 1000.;
                            let observations = captured_logs.observations();
                            assert_eq!(observations.len(), intersections);
                            let mut cell_fetches = 0;
                            let mut fetched_pages = 0;
                            let mut inflight_joins = 0;
                            let mut inflight_pages_avoided = 0;
                            let mut cache_hits = 0;
                            let mut cached_pages_avoided = 0;
                            for observation in observations {
                                let pages = cell_pages[&cell_key(observation.cell)];
                                match observation.kind {
                                    LookupKind::Fetch => {
                                        cell_fetches += 1;
                                        fetched_pages += pages;
                                    }
                                    LookupKind::InFlight => {
                                        inflight_joins += 1;
                                        inflight_pages_avoided += pages;
                                    }
                                    LookupKind::CacheHit => {
                                        cache_hits += 1;
                                        cached_pages_avoided += pages;
                                    }
                                }
                            }
                            if target > 0 {
                                assert_eq!(
                                    fetched_pages,
                                    counters.accepted_pages.load(Ordering::Relaxed)
                                );
                                assert_eq!(
                                    cell_fetches + inflight_joins + cache_hits,
                                    intersections
                                );
                            }
                            if mode == "grid-inflight" {
                                assert_eq!(cache_hits, 0);
                            }
                            let (
                                cached_file_references,
                                cached_unique_files,
                                estimated_cache_bytes,
                            ) = if mode == "grid-cache" {
                                cache_footprint(&meta, &cell_scenes)
                            } else {
                                (0, 0, 0)
                            };
                            let cache_duplication_factor = if cached_unique_files == 0 {
                                0.
                            } else {
                                cached_file_references as f64 / cached_unique_files as f64
                            };
                            if mode == "grid-cache" {
                                assert!(
                                    estimated_cache_bytes < capacity,
                                    "cold working set fits the benchmark cache"
                                );
                                assert_eq!(
                                    cached_file_references,
                                    counters.items.load(Ordering::Relaxed)
                                );
                            }
                            let row = format!(
                                "{repeat},{zoom},{center_name},{order},{},{mode},{target},{side:.3},{},{intersections},{},{:.3},{:.3},{:.3},{elapsed:.3},{},{},{},{},{},{cell_fetches},{inflight_joins},{inflight_pages_avoided},{cache_hits},{cached_pages_avoided},{cached_file_references},{cached_unique_files},{cache_duplication_factor:.3},{estimated_cache_bytes},true\n",
                                if rate.is_some() { "8ps" } else { "unlimited" },
                                tiles.len(),
                                unique.len(),
                                latencies[0],
                                percentile(0.5),
                                percentile(0.95),
                                counters.attempts.load(Ordering::Relaxed),
                                counters.accepted_pages.load(Ordering::Relaxed),
                                counters.limited.load(Ordering::Relaxed),
                                counters.items.load(Ordering::Relaxed),
                                counters.bytes.load(Ordering::Relaxed)
                            );
                            csv.push_str(&row);
                            writeln!(cache_log, "repeat={repeat} zoom={zoom} center={center_name} order={order} rate={rate:?} mode={mode} target={target}")
                                .expect("writing the trace case header cannot fail");
                            captured_logs.append_log(&mut cache_log);
                        }
                    }
                }
            }
        }
    }
    print_summary(&csv, repeats);
    fs::write(out.join("results.csv"), csv).expect("write benchmark results");
    fs::write(out.join("tiles.csv"), tile_csv).expect("write tile timings");
    fs::write(out.join("trace.csv"), trace).expect("write tile and cell trace");
    fs::write(out.join("cache-trace.log"), cache_log).expect("write captured cache trace logs");
    eprintln!("STAC grid benchmark output: {}", out.display());
}
