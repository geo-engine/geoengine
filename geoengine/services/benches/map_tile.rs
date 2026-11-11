#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

//! One 256 px EPSG:3857 map tile from a UTM COG, along the WMS path (projection and resampling
//! wrapper, PNG encoding) without loading the processing graph from the database.
//! Prints the mean wall time per stage (the spans of the map path) for each zoom level, warm and
//! cold (GDAL worker caches cleared before each tile), plus the stream metrics per operator.
//! Setup and metrics come from the operator benches' `common` module: server settings, operators
//! wrapped as in the server.
//!
//! `cargo build --release --bin gdalsource-process && cargo bench --bench map_tile`

#[path = "../../operators/benches/common/mod.rs"]
mod common;

use common::{Env, metrics};
use criterion::{Criterion, criterion_group, criterion_main};
use gdal::{
    DriverManager,
    raster::{Buffer, RasterCreationOptions},
    spatial_ref::SpatialRef,
};
use geoengine_datatypes::{
    operations::image::RasterColorizer,
    primitives::{
        AxisAlignedRectangle, BandSelection, DateTime, RasterQueryRectangle, SpatialPartition2D,
        SpatialResolution, TimeInterval,
    },
    spatial_reference::{SpatialReference, SpatialReferenceAuthority},
};
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{ExecutionContext, ProcessingGraphOperatorPath, TypedRasterOperator},
    source::{GdalMetaDataStatic, GdalSource, GdalSourceParameters},
    util::{
        WrapWithProjectionAndResample,
        gdal::{gdal_open_dataset, gdal_parameters_from_dataset, raster_descriptor_from_dataset},
        raster_stream_to_png::{default_colorizer_gradient, raster_stream_to_png_bytes},
    },
};
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tracing::Instrument;

const TILE_SIZE: u32 = 256;
/// Sentinel-2 like: 10980² pixels of 10 m in UTM 32N, centered near Frankfurt
const COG_SIZE: usize = 10_980;
const COG_ORIGIN: (f64, f64) = (399_960., 5_600_040.);
/// The COG center in EPSG:3857
const CENTER: (f64, f64) = (931_677., 6_456_124.);
const ZOOM_LEVELS: [u8; 3] = [9, 12, 15];
const STAGE_RUNS: u32 = 20;
const WRITE_ROWS: usize = 512;

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
async fn render_tile(env: &Env, source: TypedRasterOperator, zoom: u8) {
    let exec_ctx = &env.exe_ctx;
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

    let mut query_ctx = env.new_query_ctx();
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
async fn clear_caches(env: &Env, cold: bool) {
    if cold {
        env.exe_ctx
            .inner
            .gdal_process_pool
            .clear_caches()
            .await
            .unwrap();
    }
}

fn map_tile(c: &mut Criterion) {
    // the stage times are the point of this bench, so it records them without the opt-in
    metrics::install();
    let mut env = Env::new();

    let cog = utm_cog();
    let dataset = gdal_open_dataset(&cog).unwrap();
    let meta_data = GdalMetaDataStatic {
        time: Some(TimeInterval::default()),
        params: gdal_parameters_from_dataset(&dataset, 1, &cog, None, None).unwrap(),
        result_descriptor: raster_descriptor_from_dataset(&dataset, 1).unwrap(),
        cache_ttl: None,
    };
    drop(dataset);
    let source = TypedRasterOperator::from(GdalSource {
        params: GdalSourceParameters::new(env.add_data("utm_cog", Box::new(meta_data))),
    });

    let (env, source) = (&env, &source);
    let mut group = c.benchmark_group("map_tile");
    for (cold, suffix) in [(false, ""), (true, "_cold")] {
        for zoom in ZOOM_LEVELS {
            group.bench_function(format!("z{zoom}{suffix}"), |b| {
                b.to_async(&env.runtime).iter_custom(|iters| async move {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        clear_caches(env, cold).await;
                        let start = Instant::now();
                        render_tile(env, source.clone(), zoom).await;
                        elapsed += start.elapsed();
                    }
                    elapsed
                });
            });
            // clearing the caches is part of these runs, but it opens no spans or streams
            metrics::record(&format!("map_tile/z{zoom}{suffix}"), STAGE_RUNS, || {
                env.runtime.block_on(async {
                    clear_caches(env, cold).await;
                    render_tile(env, source.clone(), zoom).await;
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, map_tile);
criterion_main!(benches);
