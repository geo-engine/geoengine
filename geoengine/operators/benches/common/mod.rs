//! Setup shared by the topic benches (`gdal_source`, `reprojection`, `expression`,
//! `bands_and_time`, `raster_cache`): one raster dataset, one GDAL pool like the server
//! default and queries over the whole result or a part of it.
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
#![allow(
    dead_code,
    clippy::print_stderr,
    reason = "every bench uses a different part; printing is okay in benchmarks"
)]

use config::{Config, ConfigError, Environment, File};
use criterion::{BenchmarkGroup, Criterion, SamplingMode, measurement::WallTime};
use futures::StreamExt;
use geoengine_datatypes::{
    dataset::{DataId, DatasetId, NamedData},
    primitives::{
        BandSelection, CacheTtlSeconds, RasterQueryRectangle, SpatialPartition2D, TimeInstance,
        TimeInterval, TimeStep,
    },
    raster::{GridBoundingBox2D, RenameBands, TilingSpecification},
};
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{
        InitializedRasterOperator, MockExecutionContext, MockQueryContext, MultipleRasterSources,
        ProcessingGraphOperatorPath, QueryContext, RasterOperator, RasterQueryProcessor,
        SingleRasterSource, TypedRasterOperator, TypedRasterQueryProcessor,
    },
    processing::{
        Aggregation, RasterStacker, RasterStackerParams, TemporalRasterAggregation,
        TemporalRasterAggregationParameters,
    },
    source::{
        GdalMetaDataRegular, GdalProcessPool, GdalSource, GdalSourceParameters,
        gdal_worker_process::WorkerConfig,
    },
    util::{create_rayon_thread_pool, gdal::create_ndvi_meta_data_with_cache_ttl},
};
use serde::de::DeserializeOwned;
use std::{collections::HashMap, fmt::Debug, path::Path, time::Duration};

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
    pub processor: TypedRasterQueryProcessor,
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
    pub exe_ctx: MockExecutionContext,
    pub query_ctx: MockQueryContext,
    /// the raster dataset, NDVI unless `GEOENGINE_BENCH_RASTER` is set
    pub raster: NamedData,
    /// start and time step of the raster's files, for the query times
    start: TimeInstance,
    step: TimeStep,
}

impl Env {
    /// 512 px tiles, like the OGC API tiles, and the pools of the server settings.
    pub fn new() -> Self {
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
        // the query context takes the processing runtime from the current one
        let mut query_ctx = {
            let _guard = runtime.enter();
            exe_ctx.mock_query_context_test_default()
        };
        query_ctx.thread_pool = exe_ctx.thread_pool.clone();
        Self {
            runtime,
            exe_ctx,
            query_ctx,
            raster,
            start,
            step,
        }
    }

    /// A GDAL source of the raster dataset.
    pub fn raster(&self) -> TypedRasterOperator {
        GdalSource {
            params: GdalSourceParameters::new(self.raster.clone()),
        }
        .typed()
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
        TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Mean {
                    ignore_no_data: true,
                },
                window: self.step * 6,
                window_reference: None,
                output_type: None,
            },
            sources: SingleRasterSource { raster },
        }
        .typed()
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
            .tiling_grid_definition(self.exe_ctx.tiling_specification);
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
        let processor = initialized.query_processor().unwrap();
        let (tiles, empty) =
            self.runtime
                .block_on(count_tiles(&processor, query.clone(), &self.query_ctx));
        Case {
            processor,
            query,
            tiles,
            empty,
        }
    }

    /// Runs the case and returns the number of tiles and of empty tiles.
    pub async fn run(&self, case: &Case) -> (usize, usize) {
        count_tiles(&case.processor, case.query.clone(), &self.query_ctx).await
    }

    /// Times the case as it is, one query per iteration.
    pub fn bench(&self, group: &mut BenchmarkGroup<WallTime>, name: &str, case: &Case) {
        case.print(name);
        group.bench_function(name, |b| {
            b.to_async(&self.runtime).iter(|| self.run(case));
        });
    }

    /// Clears the open datasets and block caches of the GDAL workers.
    pub fn clear_gdal_caches(&self) {
        self.runtime
            .block_on(self.exe_ctx.gdal_process_pool.clear_caches())
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
