#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

//! One 256 px EPSG:3857 map tile from a UTM COG, along the WMS path (projection and resampling
//! wrapper, PNG encoding) without loading the processing graph from the database.
//! Prints the mean wall time per stage (the spans of the map path) for each zoom level, warm and
//! cold (GDAL worker caches cleared before each tile).
//!
//! `cargo build --release --bin gdalsource-process && cargo bench --bench map_tile`

use criterion::{Criterion, criterion_group, criterion_main};
use gdal::{
    DriverManager,
    raster::{Buffer, RasterCreationOptions},
    spatial_ref::SpatialRef,
};
use geoengine_datatypes::{
    dataset::{DataId, DatasetId, NamedData},
    operations::image::RasterColorizer,
    primitives::{
        AxisAlignedRectangle, BandSelection, DateTime, RasterQueryRectangle, SpatialPartition2D,
        SpatialResolution, TimeInterval,
    },
    spatial_reference::{SpatialReference, SpatialReferenceAuthority},
    util::{Identifier, test::TestDefault},
};
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{
        ChunkByteSize, ExecutionContext, MockExecutionContext, ProcessingGraphOperatorPath,
        TypedRasterOperator,
    },
    source::{
        GdalMetaDataStatic, GdalProcessPool, GdalSource, GdalSourceParameters,
        gdal_worker_process::WorkerConfig,
    },
    util::{
        WrapWithProjectionAndResample,
        gdal::{gdal_open_dataset, gdal_parameters_from_dataset, raster_descriptor_from_dataset},
        raster_stream_to_png::{default_colorizer_gradient, raster_stream_to_png_bytes},
    },
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};
use tracing::{Instrument, Subscriber, span};
use tracing_subscriber::{Layer, filter::LevelFilter, layer::Context, prelude::*};

const TILE_SIZE: u32 = 256;
/// Sentinel-2 like: 10980² pixels of 10 m in UTM 32N, centered near Frankfurt
const COG_SIZE: usize = 10_980;
const COG_ORIGIN: (f64, f64) = (399_960., 5_600_040.);
/// The COG center in EPSG:3857
const CENTER: (f64, f64) = (931_677., 6_456_124.);
const ZOOM_LEVELS: [u8; 3] = [9, 12, 15];
const STAGE_RUNS: u32 = 20;
const WRITE_ROWS: usize = 512;

/// Sum and count of the wall time of closed spans by name
static STAGES: LazyLock<Mutex<BTreeMap<&'static str, (Duration, u32)>>> =
    LazyLock::new(Default::default);

struct StageTimes;

impl<S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>> Layer<S> for StageTimes {
    fn on_new_span(&self, _attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(Instant::now());
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let Some(start) = span.extensions().get::<Instant>().copied() else {
            return;
        };
        let mut stages = STAGES.lock().unwrap();
        let entry = stages.entry(span.name()).or_default();
        entry.0 += start.elapsed();
        entry.1 += 1;
    }
}

/// Writes a synthetic `u16` COG once and returns its path
fn utm_cog() -> PathBuf {
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join("map_tile_utm32n_cog.tif");
    if path.exists() {
        return path;
    }

    let tmp = path.with_extension("tmp.tif");
    let mut options = RasterCreationOptions::new();
    options.add_name_value("TILED", "YES").unwrap();
    options.add_name_value("COMPRESS", "DEFLATE").unwrap();
    let mut dataset = DriverManager::get_driver_by_name("GTiff")
        .unwrap()
        .create_with_band_type_with_options::<u16, _>(&tmp, COG_SIZE, COG_SIZE, 1, &options)
        .unwrap();
    dataset
        .set_spatial_ref(&SpatialRef::from_epsg(32632).unwrap())
        .unwrap();
    dataset
        .set_geo_transform(&[COG_ORIGIN.0, 10., 0., COG_ORIGIN.1, 0., -10.])
        .unwrap();
    let mut band = dataset.rasterband(1).unwrap();
    band.set_no_data_value(Some(0.)).unwrap();
    for y0 in (0..COG_SIZE).step_by(WRITE_ROWS) {
        let rows = WRITE_ROWS.min(COG_SIZE - y0);
        let data = (0..rows * COG_SIZE)
            .map(|i| {
                let (x, y) = (i % COG_SIZE, y0 + i / COG_SIZE);
                1 + ((x / 7 + y / 5) % 10_000) as u16
            })
            .collect();
        let mut buffer = Buffer::new((COG_SIZE, rows), data);
        band.write((0, y0 as isize), (COG_SIZE, rows), &mut buffer)
            .unwrap();
    }

    let mut options = RasterCreationOptions::new();
    options.add_name_value("COMPRESS", "DEFLATE").unwrap();
    options.add_name_value("BLOCKSIZE", "512").unwrap();
    dataset
        .create_copy(
            &DriverManager::get_driver_by_name("COG").unwrap(),
            &path,
            &options,
        )
        .unwrap();
    drop(dataset);
    std::fs::remove_file(tmp).unwrap();
    path
}

/// The bounds of the EPSG:3857 tile at `zoom` that contains `CENTER`
fn tile_bounds(zoom: u8) -> SpatialPartition2D {
    const HALF_WORLD: f64 = 20_037_508.342_789_244;
    let size = 2. * HALF_WORLD / f64::from(1_u32 << zoom);
    let x = ((CENTER.0 + HALF_WORLD) / size).floor();
    let y = ((HALF_WORLD - CENTER.1) / size).floor();
    SpatialPartition2D::new(
        (-HALF_WORLD + x * size, HALF_WORLD - y * size).into(),
        (-HALF_WORLD + (x + 1.) * size, HALF_WORLD - (y + 1.) * size).into(),
    )
    .unwrap()
}

/// Same steps as `queries::raster_grid` for a WMS `GetMap`, after loading the processing graph
async fn render_tile(exec_ctx: &MockExecutionContext, source: TypedRasterOperator, zoom: u8) {
    let crs = SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857);
    let bounds = tile_bounds(zoom);
    let resolution = SpatialResolution::new(
        bounds.size_x() / f64::from(TILE_SIZE),
        bounds.size_y() / f64::from(TILE_SIZE),
    )
    .unwrap();

    let wrapped = async {
        let initialized = source
            .clone()
            .initialize(ProcessingGraphOperatorPath::initialize_root(), exec_ctx)
            .await
            .unwrap();
        WrapWithProjectionAndResample::new_create_result_descriptor(source, initialized)
            .wrap_with_projection_and_resample(None, Some(resolution), crs, exec_ctx)
            .await
            .unwrap()
    }
    .instrument(tracing::debug_span!("initialize"))
    .await;

    let grid_bounds = wrapped
        .result_descriptor
        .spatial_grid_descriptor()
        .tiling_grid_definition(exec_ctx.tiling_specification())
        .tiling_spatial_grid_definition()
        .spatial_bounds_to_compatible_spatial_grid(bounds)
        .grid_bounds();
    let time = TimeInterval::new_instant(DateTime::new_utc(2024, 6, 1, 0, 0, 0)).unwrap();
    let query = RasterQueryRectangle::new(grid_bounds, time, BandSelection::first());

    let mut query_ctx = exec_ctx.mock_query_context(ChunkByteSize::test_default());
    query_ctx.new_raster_cache = None; // measure computation, not cache hits
    // TODO: use a real colorizer for the input range
    let colorizer = RasterColorizer::SingleBand {
        band: 0,
        band_colorizer: default_colorizer_gradient::<u16>(),
    };

    let (png, _) = call_on_generic_raster_processor!(
        wrapped.initialized_operator.query_processor().unwrap(),
        p => raster_stream_to_png_bytes(
            p,
            query,
            query_ctx,
            TILE_SIZE,
            TILE_SIZE,
            Some(time),
            Some(colorizer),
            Box::pin(futures::future::pending()),
        ).await
    )
    .unwrap();
    std::hint::black_box(png);
}

/// Cold: the GDAL workers close their datasets before each tile; the OS page cache stays warm
async fn clear_caches(exec_ctx: &MockExecutionContext, cold: bool) {
    if cold {
        exec_ctx.gdal_process_pool.clear_caches().await.unwrap();
    }
}

fn map_tile(c: &mut Criterion) {
    tracing_subscriber::registry()
        .with(StageTimes.with_filter(LevelFilter::DEBUG))
        .init();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();

    let cog = utm_cog();
    let dataset = gdal_open_dataset(&cog).unwrap();
    let meta_data = GdalMetaDataStatic {
        time: Some(TimeInterval::default()),
        params: gdal_parameters_from_dataset(&dataset, 1, &cog, None, None).unwrap(),
        result_descriptor: raster_descriptor_from_dataset(&dataset, 1).unwrap(),
        cache_ttl: None,
    };
    drop(dataset);

    let mut exec_ctx = MockExecutionContext::new_with_tiling_spec(TestDefault::test_default());
    // server defaults of `[gdal_process_pool]`
    exec_ctx.gdal_process_pool = GdalProcessPool::new(32, 16, 8, WorkerConfig::default());
    let name = NamedData::with_system_name("utm_cog");
    exec_ctx.add_meta_data(
        DataId::from(DatasetId::new()),
        name.clone(),
        Box::new(meta_data),
    );
    let source = TypedRasterOperator::from(GdalSource {
        params: GdalSourceParameters::new(name),
    });

    let (exec_ctx, source) = (&exec_ctx, &source);
    let mut group = c.benchmark_group("map_tile");
    for (cold, suffix) in [(false, ""), (true, "_cold")] {
        for zoom in ZOOM_LEVELS {
            group.bench_function(format!("z{zoom}{suffix}"), |b| {
                b.to_async(&runtime).iter_custom(|iters| async move {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        clear_caches(exec_ctx, cold).await;
                        let start = Instant::now();
                        render_tile(exec_ctx, source.clone(), zoom).await;
                        elapsed += start.elapsed();
                    }
                    elapsed
                });
            });

            let mut stages = BTreeMap::new();
            for _ in 0..STAGE_RUNS {
                runtime.block_on(clear_caches(exec_ctx, cold));
                STAGES.lock().unwrap().clear();
                runtime.block_on(render_tile(exec_ctx, source.clone(), zoom));
                for (name, (total, count)) in STAGES.lock().unwrap().iter() {
                    let entry: &mut (Duration, u32) = stages.entry(*name).or_default();
                    entry.0 += *total;
                    entry.1 += count;
                }
            }
            println!("map_tile/z{zoom}{suffix}: mean wall time per tile over {STAGE_RUNS} tiles");
            println!("  {:<32} {:>8} {:>12}", "span", "count", "ms");
            for (name, (total, count)) in &stages {
                println!(
                    "  {name:<32} {:>8.1} {:>12.3}",
                    f64::from(*count) / f64::from(STAGE_RUNS),
                    total.as_secs_f64() * 1000. / f64::from(STAGE_RUNS)
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, map_tile);
criterion_main!(benches);
