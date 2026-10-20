//! Fixtures and benchmarks for STAC metadata search grids and the cell cache.
//!
//! Run from `geoengine/` with:
//! `cargo bench --profile bench-ci -p geoengine-services --bench stac_grid`

use geoengine_datatypes::{
    dataset::DataProviderId,
    primitives::{
        RegularTimeDimension, SpatialResolution, TimeDimension, TimeGranularity, TimeStep,
    },
    raster::{GeoTransform, GridBoundingBox2D, GridIdx2D, RasterDataType},
    spatial_reference::SpatialReference,
    util::Identifier,
};
use geoengine_operators::engine::SpatialGridDescriptor;
use geoengine_services::datasets::external::stac::{
    StacAssetBand, StacDataProvider, StacDataProviderDefinition, StacGrid, StacMultiBandMetaData,
    StacProviderDataset, StacProviderDatasetBand,
};
use httptest::{Expectation, Server, matchers::request};
use serde_json::{Value, json};
use std::fmt::Write as _;
use std::{
    collections::BTreeMap,
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

pub(super) const PAGE_SIZE: usize = 32;
const WORLD_EXTENT: (f64, f64, f64, f64) = (-180., -90., 180., 90.);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LookupKind {
    Fetch,
    InFlight,
    CacheHit,
}

pub(super) struct LookupObservation {
    pub(super) cell: GridIdx2D,
    pub(super) kind: LookupKind,
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
pub(super) struct CacheTraceCapture(Arc<Mutex<Vec<CacheTraceEvent>>>);

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
    pub(super) fn enabled() -> bool {
        tracing::level_filters::STATIC_MAX_LEVEL >= tracing::level_filters::LevelFilter::TRACE
    }

    pub(super) fn install(&self) -> tracing::subscriber::DefaultGuard {
        let subscriber = tracing_subscriber::registry().with(self.clone().with_filter(
            tracing_subscriber::filter::Targets::new().with_target(
                "geoengine_services::datasets::external::stac::cache",
                tracing::level_filters::LevelFilter::TRACE,
            ),
        ));
        // The benchmark uses a current-thread runtime, so this scoped
        // subscriber also captures detached tokio tasks without a global logger.
        tracing::subscriber::set_default(subscriber)
    }

    pub(super) fn observations(&self) -> Vec<LookupObservation> {
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

    pub(super) fn append_log(&self, log: &mut String) {
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
pub(super) struct Counters {
    pub(super) attempts: AtomicUsize,
    pub(super) accepted_pages: AtomicUsize,
    pub(super) limited: AtomicUsize,
    pub(super) items: AtomicUsize,
    pub(super) bytes: AtomicUsize,
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

pub(super) fn matching_scene_ids(bbox: (f64, f64, f64, f64)) -> Vec<(i32, i32)> {
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

pub(super) fn cell_key(index: GridIdx2D) -> String {
    format!("{}:{}", index.0[0], index.0[1])
}

pub(super) fn item(x: i32, y: i32) -> Value {
    let id = format!("scene-{x}-{y}");
    json!({
        "type":"Feature", "stac_version":"1.1.0", "stac_extensions":[], "id":id,
        "geometry":{"type":"Polygon", "coordinates":[[[x,y],[x+1,y],[x+1,y+1],[x,y+1],[x,y]]]},
        "bbox":[x,y,x+1,y+1], "properties":{"datetime":"2026-01-03T12:00:00Z"}, "links":[],
        "assets":{"data":{"href":format!("https://assets.example/{id}.tif"), "title":"data", "data_type":"uint16",
            "proj:code":"EPSG:4326", "proj:shape":[1,1], "proj:transform":[1,0,x,0,-1,y+1]}}
    })
}

pub(super) fn provider(
    server: &Server,
    counters: Arc<Counters>,
    rate: Option<usize>,
    grid: StacGrid,
    cache_bytes: usize,
) -> ProviderFixture {
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
            GeoTransform::new_unchecked((-180., 90.).into(), 1., -1.),
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([179, 359]))
                .expect("world grid bounds"),
        ),
        bands: vec![StacProviderDatasetBand::new_unitless(StacAssetBand {
            asset_title: "data".to_owned(),
            band_name: None,
        })],
    };
    let api_url = server.url_str("/").trim_end_matches('/').to_owned();
    let provider = StacDataProvider::from_definition(StacDataProviderDefinition {
        id: DataProviderId::new(),
        name: "benchmark".to_owned(),
        description: String::new(),
        priority: None,
        api_url: api_url.clone(),
        collection_name: "synthetic".to_owned(),
        s3_config: None,
        authentication: None,
        time_dimension: TimeDimension::Regular(RegularTimeDimension::new_with_epoch_origin(
            TimeStep {
                granularity: TimeGranularity::Days,
                step: 1,
            },
        )),
        datasets: vec![dataset.clone()],
        page_limit: PAGE_SIZE as i64,
        query_timeout_secs: 60,
        cache_ttl_secs: None,
        stac_grid: Some(grid),
    })
    .expect("benchmark provider config is valid");
    // Fresh, local cache for every scenario; cache=0 keeps in-flight coalescing.
    let provider = provider.with_query_cache_capacity(cache_bytes, Duration::from_hours(1));
    ProviderFixture {
        metadata: StacMultiBandMetaData::new(&provider, dataset.clone()),
        dataset,
        api_url,
        grid,
    }
}

pub(super) struct ProviderFixture {
    pub(super) metadata: StacMultiBandMetaData,
    pub(super) dataset: StacProviderDataset,
    pub(super) api_url: String,
    pub(super) grid: StacGrid,
}

pub(super) fn metadata(provider: &ProviderFixture) -> StacMultiBandMetaData {
    provider.metadata.clone()
}

/// Check trace classification once before measurements, when TRACE events are available.
pub(super) async fn validate_trace_capture() {
    use geoengine_datatypes::primitives::{
        BandSelection, DateTime, SpatialPartition2D, TimeInterval,
    };
    use geoengine_operators::{engine::MetaData, source::MultiBandGdalLoadingInfoQueryRectangle};

    if !CacheTraceCapture::enabled() {
        return;
    }

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
        assert!(observations.iter().all(|o| o.cell == observations[0].cell));
        assert_eq!(count(LookupKind::InFlight), 1);
        assert_eq!(count(LookupKind::Fetch), if capacity == 0 { 2 } else { 1 });
        assert_eq!(count(LookupKind::CacheHit), usize::from(capacity > 0));
    }
}
