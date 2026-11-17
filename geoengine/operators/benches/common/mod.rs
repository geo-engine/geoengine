//! Setup shared by the topic benches (`gdal_source`, `reprojection`, `expression`,
//! `bands_and_time`, `raster_cache`), `core_scaling` and `block_reads`: one raster dataset, one GDAL pool like
//! the server, the operator workloads and queries over the whole result or a part of it, run as
//! one task per query where they are concurrent, like the server does.
//!
//! The raster is the local NDVI test data (EPSG:4326, global, 6 monthly files). For local runs on
//! other data, `GEOENGINE_BENCH_RASTER=<file.json>` names a `GdalMetaDataRegular` as JSON, e.g. the
//! `metaData` of a dataset definition like `test_data/dataset_defs/ndvi.json`, with absolute file
//! paths or paths relative to `operators/`. The query times follow the data's time step:
//! `one_step` is the 4th step, `six_steps` the first six. The cases with fixed coordinates
//! (`reprojection/wms_tile`, `tile_time6`) assume global data and give empty tiles elsewhere.
//! CI never sets it, so the continuous benchmarking always measures the NDVI data.
//!
//! The benches measure workloads through typed operators, not internals, so that the same bench
//! runs before and after a change. They need no network and no database.
//!
//! `GEOENGINE_BENCH_CI=1` selects the continuous benchmarking in CI (`just backend bench-ci`,
//! see `.github/workflows/cb.yml`): only the cases marked fast, with short measurements.
//! Without it, all cases run with longer measurements, for local comparisons (`just backend bench`).
//!
//! The pools, runtime and GDAL workers are set up from the server settings, read as the server
//! reads them: `Settings-default.toml`, a local `Settings.toml` and `GEOENGINE__...` environment
//! variables, e.g. `GEOENGINE__GDAL_PROCESS_POOL__MAX_ACTIVE_PROCESSES=4`. One configuration
//! drives the server, its benches in `services` and these.
//!
//! With `GEOENGINE_BENCH_METRICS=1` (local runs only), after timing a case, `Env::bench` runs it a
//! few more times with a DEBUG subscriber and prints where the time goes: the wall time per span (the stages of the map path) and the
//! `stream_metrics` of each operator (elements, bytes, poll and wall time, dropped streams). The
//! subscriber only records during these runs, so the timed runs do not pay for it.
#![allow(
    dead_code,
    clippy::print_stderr,
    reason = "every bench uses a different part; printing is okay in benchmarks"
)]

pub mod metrics;
pub mod range_server;

use config::{Config, ConfigError, Environment, File};
use criterion::{BenchmarkGroup, Criterion, SamplingMode, measurement::WallTime};
use futures::{StreamExt, future::join_all};
use geoengine_datatypes::{
    dataset::{DataId, DatasetId, NamedData},
    primitives::{
        BandSelection, CacheTtlSeconds, RasterQueryRectangle, SpatialPartition2D,
        SpatialResolution, TimeInstance, TimeInterval, TimeStep,
    },
    raster::{GridBoundingBox2D, RasterDataType, RenameBands, TilingSpecification},
    spatial_reference::SpatialReference,
    util::Identifier,
};
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{
        GdalReadAhead, InitializedRasterOperator, MetaData, MockExecutionContext, MockQueryContext,
        MultipleRasterSources, ProcessingGraphOperatorPath, QueryContext, RasterOperator,
        RasterQueryProcessor, ResultDescriptor, SingleRasterOrVectorSource, SingleRasterSource,
        StatisticsWrappingMockExecutionContext, TypedRasterOperator, TypedRasterQueryProcessor,
    },
    meta::quota::{QuotaCheck, QuotaTracking},
    processing::{
        AggregateFunctionParams, Aggregation, BandFilter, BandFilterParams, BandsByNameOrIndex,
        DeriveOutRasterSpecsSource, Downsampling, DownsamplingMethod, DownsamplingParams,
        DownsamplingResolution, Expression, ExpressionParams, Fraction, Interpolation,
        InterpolationMethod, InterpolationParams, InterpolationResolution, NeighborhoodAggregate,
        NeighborhoodAggregateParams, NeighborhoodParams, RasterScaling, RasterScalingParams,
        RasterStacker, RasterStackerParams, Reprojection, ReprojectionParams, ScalingMode,
        SlopeOffsetSelection, TemporalRasterAggregation, TemporalRasterAggregationParameters,
    },
    source::{
        GdalMetaDataRegular, GdalProcessPool, GdalSource, GdalSourceParameters,
        gdal_worker_process::{DEFAULT_HEADER_CACHE_ENTRIES, WorkerConfig},
    },
    util::{
        WrapWithProjectionAndResample, create_rayon_thread_pool,
        gdal::create_ndvi_meta_data_with_cache_ttl,
    },
};
use rayon::ThreadPool;
use serde::de::DeserializeOwned;
use std::{collections::HashMap, fmt::Debug, path::Path, sync::Arc, time::Duration};
use uuid::Uuid;

/// The raster expression `expression` on one raster, with `f32` output.
pub fn expression(
    raster: TypedRasterOperator,
    expression: &str,
    map_no_data: bool,
) -> TypedRasterOperator {
    Expression {
        params: ExpressionParams {
            expression: expression.to_string(),
            output_type: RasterDataType::F32,
            output_band: None,
            map_no_data,
        },
        sources: SingleRasterSource { raster },
    }
    .typed()
}

pub fn reproject(source: TypedRasterOperator, target: SpatialReference) -> TypedRasterOperator {
    Reprojection {
        params: ReprojectionParams {
            target_spatial_reference: target,
            derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
        },
        sources: SingleRasterOrVectorSource {
            source: source.into(),
        },
    }
    .typed()
}

/// Reprojects to EPSG:3857, the CRS of web maps.
pub fn mercator(source: TypedRasterOperator) -> TypedRasterOperator {
    reproject(source, SpatialReference::web_mercator())
}

/// Nearest-neighbour downsampling by `factor` in x and y, like an overview.
pub fn downsample(raster: TypedRasterOperator, factor: f64) -> TypedRasterOperator {
    Downsampling {
        params: DownsamplingParams {
            sampling_method: DownsamplingMethod::NearestNeighbor,
            output_resolution: DownsamplingResolution::Fraction(Fraction {
                x: factor,
                y: factor,
            }),
            output_origin_reference: None,
        },
        sources: SingleRasterSource { raster },
    }
    .typed()
}

/// A GDAL source of `data`.
pub fn gdal_source(data: NamedData) -> TypedRasterOperator {
    GdalSource {
        params: GdalSourceParameters::new(data),
    }
    .typed()
}

/// `value * slope + offset` with constant slope and offset.
pub fn scale(raster: TypedRasterOperator, slope: f64, offset: f64) -> TypedRasterOperator {
    RasterScaling {
        params: RasterScalingParams {
            slope: SlopeOffsetSelection::Constant { value: slope },
            offset: SlopeOffsetSelection::Constant { value: offset },
            output_measurement: None,
            scaling_mode: ScalingMode::MulSlopeAddOffset,
        },
        sources: SingleRasterSource { raster },
    }
    .typed()
}

/// The sum over a `size` × `size` neighbourhood of each pixel.
pub fn neighborhood_sum(raster: TypedRasterOperator, size: usize) -> TypedRasterOperator {
    NeighborhoodAggregate {
        params: NeighborhoodAggregateParams {
            neighborhood: NeighborhoodParams::Rectangle {
                dimensions: [size, size],
            },
            aggregate_function: AggregateFunctionParams::Sum,
        },
        sources: SingleRasterSource { raster },
    }
    .typed()
}

/// Bilinear interpolation to `factor` times the resolution in x and y.
pub fn interpolate(raster: TypedRasterOperator, factor: f64) -> TypedRasterOperator {
    Interpolation {
        params: InterpolationParams {
            interpolation: InterpolationMethod::BiLinear,
            output_resolution: InterpolationResolution::Fraction(Fraction {
                x: factor,
                y: factor,
            }),
            output_origin_reference: None,
        },
        sources: SingleRasterSource { raster },
    }
    .typed()
}

/// The mean over windows of `window`, ignoring no data.
pub fn mean(raster: TypedRasterOperator, window: TimeStep) -> TypedRasterOperator {
    TemporalRasterAggregation {
        params: TemporalRasterAggregationParameters {
            aggregation: Aggregation::Mean {
                ignore_no_data: true,
            },
            window,
            window_reference: None,
            output_type: None,
        },
        sources: SingleRasterSource { raster },
    }
    .typed()
}

/// Bands `bands` of the raster, by index.
pub fn band_filter(raster: TypedRasterOperator, bands: Vec<usize>) -> TypedRasterOperator {
    BandFilter {
        params: BandFilterParams {
            bands: BandsByNameOrIndex::Index(bands),
        },
        sources: SingleRasterSource { raster },
    }
    .typed()
}

/// The rasters as bands of one raster.
pub fn stack(rasters: Vec<TypedRasterOperator>) -> TypedRasterOperator {
    RasterStacker {
        params: RasterStackerParams {
            rename_bands: RenameBands::Default,
        },
        sources: MultipleRasterSources { rasters },
    }
    .typed()
}

#[derive(Clone, Copy)]
pub enum Area {
    /// the whole result
    Extent,
    /// a square of `n` × `n` result extents with the data in the center, so most tiles are empty
    ExtentTimes(isize),
    /// the tiles that intersect this area of the result
    Part(SpatialPartition2D),
}

/// A processor and the query to run on it. Setup ran the query once, so the first timed
/// iteration is warm, and recorded the result size.
pub struct Case {
    pub processor: Arc<TypedRasterQueryProcessor>,
    pub query: RasterQueryRectangle,
    pub tiles: usize,
    pub empty: usize,
}

impl Case {
    /// Prints the result size, so that a change in what a case returns shows next to its time.
    pub fn print(&self, name: &str) {
        eprintln!("{name}: {} tiles, {} empty", self.tiles, self.empty);
    }
}

pub struct Env {
    pub runtime: tokio::runtime::Runtime,
    /// wraps every operator with the statistics wrapper, as the server's context does
    pub exe_ctx: StatisticsWrappingMockExecutionContext,
    pub query_ctx: Arc<MockQueryContext>,
    /// the raster dataset, NDVI unless `GEOENGINE_BENCH_RASTER` is set
    pub raster: NamedData,
    /// start and time step of the raster's files, for the query times
    start: TimeInstance,
    step: TimeStep,
    /// the read-ahead of the settings, for further query contexts
    read_ahead: GdalReadAhead,
}

impl Env {
    /// 512 px tiles, like the OGC API tiles, and the pools of the server settings.
    pub fn new() -> Self {
        if metrics_enabled() {
            metrics::install();
        }
        let settings = settings();
        let mut runtime = tokio::runtime::Builder::new_multi_thread();
        runtime.enable_all();
        runtime.max_blocking_threads(setting(
            &settings,
            "processing_runtime.max_blocking_threads",
            512,
        ));
        // 0 is the server's "one per core", tokio's default
        match setting(&settings, "processing_runtime.worker_threads", 0) {
            0 => {}
            threads => {
                runtime.worker_threads(threads);
            }
        }
        let runtime = runtime.build().unwrap();
        let mut exe_ctx = MockExecutionContext::new_with_tiling_spec_and_tokio_handle(
            TilingSpecification::new([512, 512].into()),
            runtime.handle(),
        );
        // one pool for all cases: pools are not shut down, so one per case would pile up workers
        exe_ctx.gdal_process_pool = GdalProcessPool::new_with_tokio_handle(
            runtime.handle(),
            setting(&settings, "gdal_process_pool.number_of_processes", 32),
            setting(&settings, "gdal_process_pool.max_active_processes", 16),
            setting(&settings, "gdal_process_pool.max_dataset_processes", 8),
            setting(
                &settings,
                "gdal_process_pool.header_cache_entries",
                DEFAULT_HEADER_CACHE_ENTRIES,
            ),
            WorkerConfig {
                // without the server's options GDAL caches 5% of the RAM per worker
                gdal_config_options: Some(
                    setting::<HashMap<String, String>>(
                        &settings,
                        "gdal_process_pool.worker.gdal_config_options",
                        HashMap::new(),
                    )
                    .into_iter()
                    .collect(),
                ),
                blas_threads: setting(&settings, "gdal_process_pool.worker.blas_threads", 1),
                ..WorkerConfig::default()
            },
        );
        // 0 is one thread per core, as in the server
        exe_ctx.thread_pool =
            create_rayon_thread_pool(setting(&settings, "query_context.rayon_threads", 0));
        let meta_data = raster_meta_data();
        let (start, step) = (meta_data.data_time.start(), meta_data.step);
        let raster = NamedData::with_system_name("raster");
        exe_ctx.add_meta_data(
            DataId::from(DatasetId::from_u128(1)),
            raster.clone(),
            Box::new(meta_data),
        );
        let read_ahead = GdalReadAhead {
            gdal_source: setting(&settings, "query_context.gdal_source_read_ahead", 8),
            multi_band_gdal_source: setting(
                &settings,
                "query_context.multi_band_gdal_source_read_ahead",
                16,
            ),
        };
        let query_ctx = Arc::new(query_ctx(
            &runtime,
            &exe_ctx,
            exe_ctx.thread_pool.clone(),
            read_ahead,
        ));
        Self {
            runtime,
            exe_ctx: StatisticsWrappingMockExecutionContext {
                inner: exe_ctx,
                cache_raster_operators: false,
            },
            query_ctx,
            raster,
            start,
            step,
            read_ahead,
        }
    }

    /// A new query context like `query_ctx`, for APIs that take one by value.
    pub fn new_query_ctx(&self) -> MockQueryContext {
        query_ctx(
            &self.runtime,
            &self.exe_ctx.inner,
            self.exe_ctx.inner.thread_pool.clone(),
            self.read_ahead,
        )
    }

    /// Registers more data under `name`, e.g. a COG of a bench, next to the raster.
    pub fn add_data<L, R, Q>(
        &mut self,
        name: &str,
        meta_data: Box<dyn MetaData<L, R, Q>>,
    ) -> NamedData
    where
        L: Send + Sync + 'static,
        R: Send + Sync + 'static + ResultDescriptor,
        Q: Send + Sync + 'static,
    {
        let data = NamedData::with_system_name(name);
        self.exe_ctx
            .inner
            .add_meta_data(DataId::from(DatasetId::new()), data.clone(), meta_data);
        data
    }

    /// A query context like `query_ctx`, but with its own Rayon pool of `threads` threads
    /// (0 = one per core).
    pub fn query_ctx_with_rayon_threads(&self, threads: usize) -> Arc<MockQueryContext> {
        Arc::new(query_ctx(
            &self.runtime,
            &self.exe_ctx.inner,
            create_rayon_thread_pool(threads),
            self.read_ahead,
        ))
    }

    /// A GDAL source of the raster dataset.
    pub fn raster(&self) -> TypedRasterOperator {
        gdal_source(self.raster.clone())
    }

    /// The 4th time step of the raster: 2014-04-01 for NDVI.
    pub fn one_step(&self) -> TimeInterval {
        TimeInterval::new_instant((self.start + self.step * 3).unwrap()).unwrap()
    }

    /// The first six time steps of the raster: 2014-01 to 2014-06 for NDVI.
    pub fn six_steps(&self) -> TimeInterval {
        TimeInterval::new(self.start, (self.start + self.step * 6).unwrap()).unwrap()
    }

    /// The mean over windows of six time steps of the raster, ignoring no data.
    pub fn six_step_mean(&self, raster: TypedRasterOperator) -> TypedRasterOperator {
        mean(raster, self.step * 6)
    }

    /// The operator as the server wraps it for a query in `srs` with `resolution`, e.g. for a WMS
    /// `GetMap` request (`WrapWithProjectionAndResample`, as in `services::queries::raster_grid`).
    pub fn resample(
        &self,
        operator: TypedRasterOperator,
        srs: SpatialReference,
        resolution: SpatialResolution,
    ) -> TypedRasterOperator {
        self.runtime.block_on(async {
            let initialized = operator
                .clone()
                .initialize(
                    ProcessingGraphOperatorPath::initialize_root(),
                    &self.exe_ctx,
                )
                .await
                .unwrap();
            WrapWithProjectionAndResample::new_create_result_descriptor(operator, initialized)
                .wrap_with_projection_and_resample(None, Some(resolution), srs, &self.exe_ctx)
                .await
                .unwrap()
                .operator
        })
    }

    /// Initializes the operator and queries all of its bands in `area` and `time`.
    pub fn case(&self, operator: TypedRasterOperator, area: Area, time: TimeInterval) -> Case {
        self.wrapped_case(operator, area, time, |initialized| initialized)
    }

    /// Like `case`, but `wrap` gets the initialized operator, e.g. to put a cache around it.
    pub fn wrapped_case(
        &self,
        operator: TypedRasterOperator,
        area: Area,
        time: TimeInterval,
        wrap: impl FnOnce(Box<dyn InitializedRasterOperator>) -> Box<dyn InitializedRasterOperator>,
    ) -> Case {
        let mut case = self.unprimed_case(operator, area, time, wrap);
        (case.tiles, case.empty) = self.runtime.block_on(count_tiles(
            &case.processor,
            case.query.clone(),
            self.query_ctx.as_ref(),
        ));
        case
    }

    /// Like `case`, but without the first query, so the first run reads cold; the counts stay 0.
    pub fn cold_case(&self, operator: TypedRasterOperator, area: Area, time: TimeInterval) -> Case {
        self.unprimed_case(operator, area, time, |initialized| initialized)
    }

    fn unprimed_case(
        &self,
        operator: TypedRasterOperator,
        area: Area,
        time: TimeInterval,
        wrap: impl FnOnce(Box<dyn InitializedRasterOperator>) -> Box<dyn InitializedRasterOperator>,
    ) -> Case {
        let initialized = wrap(
            self.runtime
                .block_on(operator.initialize(
                    ProcessingGraphOperatorPath::initialize_root(),
                    &self.exe_ctx,
                ))
                .unwrap(),
        );
        let descriptor = initialized.result_descriptor();
        let grid = descriptor
            .spatial_grid_descriptor()
            .tiling_grid_definition(self.exe_ctx.inner.tiling_specification);
        let data = grid.tiling_grid_bounds();
        let bounds = match area {
            Area::Extent => data,
            // grow by (n - 1) / 2 extents on each side, so the data stays in the center
            Area::ExtentTimes(n) => {
                let margin_y = (n - 1) / 2 * (data.y_max() - data.y_min() + 1);
                let margin_x = (n - 1) / 2 * (data.x_max() - data.x_min() + 1);
                GridBoundingBox2D::new_min_max(
                    data.y_min() - margin_y,
                    data.y_max() + margin_y,
                    data.x_min() - margin_x,
                    data.x_max() + margin_x,
                )
                .unwrap()
            }
            Area::Part(part) => grid.tiling_geo_transform().spatial_to_grid_bounds(&part),
        };
        let query = RasterQueryRectangle::new(
            bounds,
            time,
            BandSelection::new((0..descriptor.bands.count()).collect()).unwrap(),
        );
        Case {
            processor: Arc::new(initialized.query_processor().unwrap()),
            query,
            tiles: 0,
            empty: 0,
        }
    }

    /// Runs the case and returns the number of tiles and of empty tiles.
    pub async fn run(&self, case: &Case) -> (usize, usize) {
        count_tiles(&case.processor, case.query.clone(), self.query_ctx.as_ref()).await
    }

    /// Runs `concurrent` queries of the case, each as its own task, like the server does.
    pub async fn run_concurrent(&self, case: &Case, concurrent: usize) -> (usize, usize) {
        spawn_queries(
            case,
            vec![case.query.clone(); concurrent],
            self.query_ctx.clone(),
        )
        .await
    }

    /// Times the case as it is, one query per iteration.
    pub fn bench(&self, group: &mut BenchmarkGroup<WallTime>, name: &str, case: &Case) {
        case.print(name);
        group.bench_function(name, |b| {
            b.to_async(&self.runtime).iter(|| self.run(case));
        });
        self.metrics(name, || self.run(case));
    }

    /// Locally, runs `run` a few times while recording the metrics and prints their mean per run.
    pub fn metrics<F: Future>(&self, name: &str, run: impl Fn() -> F) {
        const RUNS: u32 = 5;
        // opt-in: these runs are outside Criterion's closures, so a case filter would not skip them
        if !metrics_enabled() {
            return;
        }
        metrics::record(name, RUNS, || {
            self.runtime.block_on(run());
        });
    }

    /// Clears the open datasets and block caches of the GDAL workers.
    pub fn clear_gdal_caches(&self) {
        self.runtime
            .block_on(self.exe_ctx.inner.gdal_process_pool.clear_caches())
            .unwrap();
    }
}

/// The raster's metadata: from `GEOENGINE_BENCH_RASTER` if set, else the NDVI test data. Both get
/// a cache TTL, so that the raster cache keeps the tiles.
///
/// shortcut: only `GdalMetaDataRegular` (one file per time step); read the dataset definition's
/// `MetaDataDefinition` when a case needs a static or list dataset.
fn raster_meta_data() -> GdalMetaDataRegular {
    let ttl = CacheTtlSeconds::new(3600);
    let Some(path) = std::env::var_os("GEOENGINE_BENCH_RASTER") else {
        return create_ndvi_meta_data_with_cache_ttl(ttl);
    };
    eprintln!("GEOENGINE_BENCH_RASTER = {}", path.display());
    let file = std::fs::File::open(&path).unwrap();
    let mut meta_data: GdalMetaDataRegular =
        serde_json::from_reader(std::io::BufReader::new(file)).unwrap();
    meta_data.cache_ttl = Some(ttl);
    meta_data
}

/// The server settings, read as `services::config` reads them: `Settings-default.toml`, a local
/// `Settings.toml` if present, then `GEOENGINE__SECTION__KEY` environment variables.
fn settings() -> Config {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    Config::builder()
        .add_source(File::from(dir.join("Settings-default.toml")))
        .add_source(File::from(dir.join("Settings.toml")).required(false))
        .add_source(Environment::with_prefix("geoengine").separator("__"))
        .build()
        .unwrap()
}

/// The setting `key`, or `default` while the settings do not have the key (the keys that wp1 adds,
/// until it is merged). Prints what it uses.
fn setting<T: DeserializeOwned + Debug>(settings: &Config, key: &str, default: T) -> T {
    let value = match settings.get(key) {
        Ok(value) => value,
        Err(ConfigError::NotFound(_)) => default,
        // a key with a wrong value must not silently fall back to the default
        Err(e) => panic!("{key}: {e}"),
    };
    eprintln!("{key} = {value:?}");
    value
}

/// Runs the query and returns the number of tiles and of empty tiles.
pub async fn count_tiles(
    processor: &TypedRasterQueryProcessor,
    query: RasterQueryRectangle,
    query_ctx: &dyn QueryContext,
) -> (usize, usize) {
    call_on_generic_raster_processor!(processor, p => count(p.as_ref(), query, query_ctx).await)
}

async fn count<P: RasterQueryProcessor + ?Sized>(
    processor: &P,
    query: RasterQueryRectangle,
    query_ctx: &dyn QueryContext,
) -> (usize, usize) {
    let (tiles, empty) = processor
        .raster_query(query, query_ctx)
        .await
        .unwrap()
        .fold((0, 0), |(tiles, empty), tile| async move {
            (tiles + 1, empty + usize::from(tile.unwrap().is_empty()))
        })
        .await;
    // a query without tiles means a broken case (wrong time or area), not a fast one
    assert_ne!(tiles, 0);
    (tiles, empty)
}

/// A benchmark group with flat sampling: the iterations here take milliseconds to a second, so
/// Criterion's default linear sampling would need far more time than the measurement target.
pub fn group<'a>(c: &'a mut Criterion, name: &str) -> BenchmarkGroup<'a, WallTime> {
    let mut group = c.benchmark_group(name);
    group.sampling_mode(SamplingMode::Flat);
    group
}

/// Whether this is the continuous benchmarking in CI, which skips the slow cases.
pub fn ci() -> bool {
    std::env::var_os("GEOENGINE_BENCH_CI").is_some()
}

/// Whether to record and print the metrics after each case: `GEOENGINE_BENCH_METRICS`, never in CI.
pub fn metrics_enabled() -> bool {
    !ci() && std::env::var_os("GEOENGINE_BENCH_METRICS").is_some()
}

/// Criterion settings: short in CI, where the cases take at most ~60 ms per iteration, and for
/// cases up to a second per iteration locally.
pub fn criterion() -> Criterion {
    let c = Criterion::default();
    // CI: 10 samples of 2 s after 0.5 s warm-up keep the whole set at a few minutes on the shared
    // runner; locally, more samples for smaller confidence intervals
    let c = if ci() {
        c.sample_size(10)
            .warm_up_time(Duration::from_millis(500))
            .measurement_time(Duration::from_secs(2))
    } else {
        c.sample_size(20).measurement_time(Duration::from_secs(5))
    };
    c.configure_from_args()
}

/// A query context of the server's kind: the processing runtime, the Rayon pool, the read-ahead and quota
/// tracking and checks, which the statistics wrapper of every operator needs.
fn query_ctx(
    runtime: &tokio::runtime::Runtime,
    exe_ctx: &MockExecutionContext,
    thread_pool: Arc<ThreadPool>,
    read_ahead: GdalReadAhead,
) -> MockQueryContext {
    // the query context takes the processing runtime from the current one
    let mut query_ctx = {
        let _guard = runtime.enter();
        exe_ctx.mock_query_context_test_default()
    };
    query_ctx.thread_pool = thread_pool;
    query_ctx.gdal_read_ahead = read_ahead;
    // the statistics wrapper reports work units like in the server, whose quota manager drains them
    let (quota_sender, mut quota_receiver) = tokio::sync::mpsc::unbounded_channel();
    runtime.spawn(async move { while quota_receiver.recv().await.is_some() {} });
    query_ctx.quota_tracking = Some(QuotaTracking::new(
        quota_sender,
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    ));
    query_ctx.quota_checker = Some(Box::new(NoQuota));
    query_ctx
}

/// Runs the queries on the case's processor, each as its own task, like the server does, and
/// returns the number of tiles and of empty tiles.
pub async fn spawn_queries(
    case: &Case,
    queries: Vec<RasterQueryRectangle>,
    query_ctx: Arc<MockQueryContext>,
) -> (usize, usize) {
    let tasks = queries.into_iter().map(|query| {
        let processor = case.processor.clone();
        let query_ctx = query_ctx.clone();
        tokio::spawn(async move { count_tiles(&processor, query, query_ctx.as_ref()).await })
    });
    join_all(tasks)
        .await
        .into_iter()
        .map(Result::unwrap)
        .fold((0, 0), |(tiles, empty), (t, e)| (tiles + t, empty + e))
}

/// Quota is always available.
struct NoQuota;

#[async_trait::async_trait]
impl QuotaCheck for NoQuota {
    async fn ensure_quota_available(&self) -> geoengine_operators::util::Result<()> {
        Ok(())
    }
}
