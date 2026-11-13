#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

//! One 256 px EPSG:3857 map tile from a UTM scene, along the WMS path (projection and resampling
//! wrapper, PNG encoding) without loading the processing graph from the database.
//! With `GEOENGINE_BENCH_METRICS=1`, prints the mean wall time per stage (the spans of the map
//! path) for each case, plus the stream metrics per operator.
//!
//! - `map_tile`: a `GdalSource` on one COG band, warm and cold (GDAL worker caches cleared before
//!   each tile).
//! - `layer_patterns`: the operator patterns of the EDV layer templates (bootstrap2), each on a
//!   `MultiBandGdalSource` with one file per band, as the STAC provider delivers them. All
//!   patterns read COGs (the mosaics, Landsat, Sentinel-1 and CLMS sources); the Sentinel-2
//!   patterns also read JP2 (Sentinel-2 L2A), warm and cold.
//! - `viewport`: the session of a map client on some of the layer patterns: a 3×3 view of map tiles,
//!   then a pan by one column and by one row (15 tiles), one tile after the other or 6 at once like a
//!   browser, without and with the raster cache around every operator, as the server wraps them with
//!   `[cache] enable_new_raster_cache`. Each session starts with an empty cache, so it shows what the
//!   whole tiles that operators compute give back to the next map tiles of the same view. Before
//!   timing, it checks that the cache does not change the PNGs.
//!
//! The scenes are synthetic Sentinel-2 like `u16` bands, written once to `CARGO_TARGET_TMPDIR`
//! (the first run takes a few minutes for the JP2 files).
//! Setup and metrics come from the operator benches' `common` module: server settings, operators
//! wrapped as in the server.
//!
//! `cargo build --release --bin gdalsource-process && [GEOENGINE_BENCH_METRICS=1] cargo bench --bench map_tile [-- layer_patterns]`

#[path = "../../operators/benches/common/mod.rs"]
mod common;

use common::{Env, expression, metrics, stack};
use criterion::{
    BenchmarkGroup, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use futures::StreamExt;
use gdal::{
    DriverManager,
    raster::{Buffer, RasterCreationOptions},
    spatial_ref::SpatialRef,
};
use geoengine_datatypes::{
    dataset::NamedData,
    operations::image::{Colorizer, RasterColorizer, RgbParams, RgbaColor},
    primitives::{
        AxisAlignedRectangle, CacheTtlSeconds, DateTime, Measurement, RasterQueryRectangle,
        SpatialPartition2D, SpatialResolution, TimeInterval,
    },
    spatial_reference::{SpatialReference, SpatialReferenceAuthority},
};
use geoengine_operators::{
    cache::new_raster_cache::NewRasterCacheEnum,
    call_on_generic_raster_processor,
    engine::{
        ExecutionContext, ProcessingGraphOperatorPath, RasterBandDescriptor, RasterBandDescriptors,
        RasterOperator, SingleRasterSource, StaticMetaData, TypedRasterOperator,
    },
    processing::{BandFilter, BandFilterParams, BandsByNameOrIndex},
    source::{
        GdalMetaDataStatic, GdalSource, GdalSourceParameters, MultiBandGdalLoadingInfo,
        MultiBandGdalLoadingInfoQueryRectangle, MultiBandGdalSource, MultiBandGdalSourceParameters,
        TileFile,
    },
    util::{
        WrapWithProjectionAndResample,
        gdal::{gdal_open_dataset, gdal_parameters_from_dataset, raster_descriptor_from_dataset},
        raster_stream_to_png::{default_colorizer_gradient, raster_stream_to_png_bytes},
    },
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::Instrument;

const TILE_SIZE: u32 = 256;
/// Sentinel-2 like: 10980² pixels of 10 m in UTM 32N, centered near Frankfurt
const SCENE_SIZE: usize = 10_980;
const SCENE_ORIGIN: (f64, f64) = (399_960., 5_600_040.);
/// The scene center in EPSG:3857
const CENTER: (f64, f64) = (931_677., 6_456_124.);
const ZOOM_LEVELS: [u8; 3] = [9, 12, 15];
const STAGE_RUNS: u32 = 20;
const WRITE_ROWS: usize = 512;
/// The bands of the synthetic scene, named after the Sentinel-2 bands the templates use
const BANDS: [&str; 4] = ["B02", "B03", "B04", "B08"];
/// The size of the raster cache of a `viewport` session; it holds all of its tiles
const CACHE_BYTES: usize = 1 << 30;

#[derive(Clone, Copy)]
enum Format {
    Cog,
    Jp2,
}

/// Writes band `index` of the synthetic `u16` scene once and returns its path
fn utm_band(format: Format, index: usize) -> PathBuf {
    // shortcut: generic tiling and compression, not the exact layout of the provider files; compare
    // with a real Sentinel-2 / mosaic file if the numbers look off
    let (extension, driver, creation_options): (_, _, &[(&str, &str)]) = match format {
        Format::Cog => (
            "tif",
            "COG",
            &[("COMPRESS", "DEFLATE"), ("BLOCKSIZE", "512")],
        ),
        // lossless with 1024² tiles, as the Sentinel-2 L2A files
        Format::Jp2 => (
            "jp2",
            "JP2OpenJPEG",
            &[
                ("REVERSIBLE", "YES"),
                ("QUALITY", "100"),
                ("BLOCKXSIZE", "1024"),
                ("BLOCKYSIZE", "1024"),
                ("NUM_THREADS", "ALL_CPUS"),
            ],
        ),
    };
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let path = dir.join(format!("utm32n_band{index}.{extension}"));
    if path.exists() {
        return path;
    }

    let tmp = dir.join(format!("utm32n_band{index}.tmp.tif"));
    let mut options = RasterCreationOptions::new();
    options.add_name_value("TILED", "YES").unwrap();
    options.add_name_value("COMPRESS", "DEFLATE").unwrap();
    let mut dataset = DriverManager::get_driver_by_name("GTiff")
        .unwrap()
        .create_with_band_type_with_options::<u16, _>(&tmp, SCENE_SIZE, SCENE_SIZE, 1, &options)
        .unwrap();
    dataset
        .set_spatial_ref(&SpatialRef::from_epsg(32632).unwrap())
        .unwrap();
    dataset
        .set_geo_transform(&[SCENE_ORIGIN.0, 10., 0., SCENE_ORIGIN.1, 0., -10.])
        .unwrap();
    let mut band = dataset.rasterband(1).unwrap();
    band.set_no_data_value(Some(0.)).unwrap();
    for y0 in (0..SCENE_SIZE).step_by(WRITE_ROWS) {
        let rows = WRITE_ROWS.min(SCENE_SIZE - y0);
        let data = (0..rows * SCENE_SIZE)
            .map(|i| {
                let (x, y) = (i % SCENE_SIZE, y0 + i / SCENE_SIZE);
                // shifted per band, so band ratios vary over the scene
                1 + ((x / 7 + y / 5 + 2500 * index) % 10_000) as u16
            })
            .collect();
        let mut buffer = Buffer::new((SCENE_SIZE, rows), data);
        band.write((0, y0 as isize), (SCENE_SIZE, rows), &mut buffer)
            .unwrap();
    }

    let mut options = RasterCreationOptions::new();
    for (name, value) in creation_options {
        options.add_name_value(name, value).unwrap();
    }
    // written under another name first, so an interrupted run leaves no truncated file behind
    let part = dir.join(format!("utm32n_band{index}.part.{extension}"));
    dataset
        .create_copy(
            &DriverManager::get_driver_by_name(driver).unwrap(),
            &part,
            &options,
        )
        .unwrap();
    drop(dataset);
    std::fs::remove_file(tmp).unwrap();
    // GDAL keeps what the format cannot store (the JP2 no-data value) in a side file
    let side_file = |file: &Path| PathBuf::from(format!("{}.aux.xml", file.display()));
    if side_file(&part).exists() {
        std::fs::rename(side_file(&part), side_file(&path)).unwrap();
    }
    std::fs::rename(part, &path).unwrap();
    path
}

/// Registers the scene as one dataset with one file per band, like the STAC provider does
fn multi_band_scene(env: &mut Env, format: Format) -> NamedData {
    let mut result_descriptor = None;
    let files = (0..BANDS.len())
        .map(|index| {
            let path = utm_band(format, index);
            let dataset = gdal_open_dataset(&path).unwrap();
            result_descriptor
                .get_or_insert_with(|| raster_descriptor_from_dataset(&dataset, 1).unwrap());
            TileFile {
                time: TimeInterval::default(),
                spatial_partition: SpatialPartition2D::new(
                    SCENE_ORIGIN.into(),
                    (
                        SCENE_ORIGIN.0 + 10. * SCENE_SIZE as f64,
                        SCENE_ORIGIN.1 - 10. * SCENE_SIZE as f64,
                    )
                        .into(),
                )
                .unwrap(),
                band: index as u32,
                z_index: 0,
                params: gdal_parameters_from_dataset(&dataset, 1, &path, None, None).unwrap(),
            }
        })
        .collect();
    let mut result_descriptor = result_descriptor.unwrap();
    result_descriptor.bands = RasterBandDescriptors::new(
        BANDS
            .iter()
            .map(|name| RasterBandDescriptor::new((*name).to_string(), Measurement::Unitless))
            .collect(),
    )
    .unwrap();
    let name = match format {
        Format::Cog => "scene_cog",
        Format::Jp2 => "scene_jp2",
    };
    env.add_data(
        name,
        Box::new(
            StaticMetaData::<_, _, MultiBandGdalLoadingInfoQueryRectangle> {
                loading_info: MultiBandGdalLoadingInfo::new(
                    vec![TimeInterval::default()],
                    files,
                    None,
                ),
                result_descriptor,
                phantom: Default::default(),
            },
        ),
    )
}

fn gradient(min: f64, max: f64) -> RasterColorizer {
    Colorizer::linear_gradient(
        vec![
            (min, RgbaColor::black()).try_into().unwrap(),
            (max, RgbaColor::white()).try_into().unwrap(),
        ],
        RgbaColor::transparent(),
        RgbaColor::white(),
        RgbaColor::black(),
    )
    .unwrap()
    .into()
}

fn rgb(min: f64, max: f64) -> RasterColorizer {
    RasterColorizer::MultiBand {
        red_band: 0,
        green_band: 1,
        blue_band: 2,
        rgb_params: RgbParams {
            red_min: min,
            red_max: max,
            red_scale: 1.,
            green_min: min,
            green_max: max,
            green_scale: 1.,
            blue_min: min,
            blue_max: max,
            blue_scale: 1.,
            no_data_color: RgbaColor::transparent(),
        },
    }
}

/// The operator patterns of the bootstrap2 layer templates, with their kind of symbology. The
/// expressions are copied from the templates, on the scene's bands instead of the source's.
fn layer_patterns(data: &NamedData) -> Vec<(&'static str, TypedRasterOperator, RasterColorizer)> {
    let source = || -> TypedRasterOperator {
        MultiBandGdalSource {
            params: MultiBandGdalSourceParameters::new(data.clone()),
        }
        .typed()
    };
    let filter = |bands: &[&str]| -> TypedRasterOperator {
        BandFilter {
            params: BandFilterParams {
                bands: BandsByNameOrIndex::Name(bands.iter().map(ToString::to_string).collect()),
            },
            sources: SingleRasterSource { raster: source() },
        }
        .typed()
    };
    vec![
        // clms_clcplus_lulucf_instance/land_use
        (
            "source",
            source(),
            default_colorizer_gradient::<u16>().into(),
        ),
        // true_color (Sentinel-2, Landsat, mosaics) and false_color
        (
            "band_filter_rgb",
            filter(&["B04", "B03", "B02"]),
            rgb(0., 10_000.),
        ),
        // clms_ba_global_300m_daily_v4_cog/*
        (
            "scaled_band",
            expression(
                filter(&["B08"]),
                "if A < 0 { NODATA } else { A * 0.001 }",
                false,
            ),
            gradient(0., 10.),
        ),
        // ndvi, ndwi (Sentinel-2, Landsat, mosaics), polarization_ratio and radar_vegetation_index
        (
            "normalized_difference",
            expression(
                filter(&["B08", "B04"]),
                "(A - B) / (max(A + B, 0.00000001))",
                false,
            ),
            gradient(-1., 1.),
        ),
        // sentinel1/sar_false_color
        (
            "stacked_expressions",
            stack(vec![
                expression(
                    filter(&["B08"]),
                    "max(0.0, min(1.0, (10.0 * log10(max(A, 0.00000001)) + 25.0) / 25.0))",
                    false,
                ),
                expression(
                    filter(&["B04"]),
                    "max(0.0, min(1.0, (10.0 * log10(max(A, 0.00000001)) + 32.0) / 27.0))",
                    false,
                ),
                expression(
                    filter(&["B04", "B08"]),
                    "max(0.0, min(1.0, ((10.0 * log10(max(A, 0.00000001)) - 10.0 * log10(max(B, 0.00000001))) + 15.0) / 17.0))",
                    false,
                ),
            ]),
            rgb(0., 1.),
        ),
    ]
}

/// The bounds of the EPSG:3857 tile at `zoom`, `dx` columns right of and `dy` rows below the one
/// that contains `CENTER`
fn tile_bounds(zoom: u8, (dx, dy): (i32, i32)) -> SpatialPartition2D {
    const HALF_WORLD: f64 = 20_037_508.342_789_244;
    let size = 2. * HALF_WORLD / f64::from(1_u32 << zoom);
    let x = ((CENTER.0 + HALF_WORLD) / size).floor() + f64::from(dx);
    let y = ((HALF_WORLD - CENTER.1) / size).floor() + f64::from(dy);
    SpatialPartition2D::new(
        (-HALF_WORLD + x * size, HALF_WORLD - y * size).into(),
        (-HALF_WORLD + (x + 1.) * size, HALF_WORLD - (y + 1.) * size).into(),
    )
    .unwrap()
}

/// Same steps as `queries::raster_grid` for a WMS `GetMap`, after loading the processing graph.
/// `cache` must be set if the execution context caches the raster operators.
async fn render_tile(
    env: &Env,
    source: TypedRasterOperator,
    (zoom, offset): (u8, (i32, i32)),
    colorizer: &RasterColorizer,
    cache: Option<Arc<NewRasterCacheEnum>>,
) -> Vec<u8> {
    let exec_ctx = &env.exe_ctx;
    let crs = SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857);
    let bounds = tile_bounds(zoom, offset);
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
    // the WMS queries the bands the colorizer shows
    let query = RasterQueryRectangle::new(grid_bounds, time, colorizer.band_selection());

    let mut query_ctx = env.new_query_ctx();
    query_ctx.new_raster_cache = cache;

    let (png, _) = call_on_generic_raster_processor!(
        wrapped.initialized_operator.query_processor().unwrap(),
        p => raster_stream_to_png_bytes(
            p,
            query,
            query_ctx,
            TILE_SIZE,
            TILE_SIZE,
            Some(time),
            Some(colorizer.clone()),
            Box::pin(futures::future::pending()),
        ).await
    )
    .unwrap();
    png
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

/// Times one tile as `<group_name>/<name>`, then records its stages if the metrics are enabled
fn bench_tile(
    group: &mut BenchmarkGroup<WallTime>,
    group_name: &str,
    env: &Env,
    name: &str,
    (source, colorizer): (&TypedRasterOperator, &RasterColorizer),
    zoom: u8,
    cold: bool,
) {
    group.bench_function(name, |b| {
        b.to_async(&env.runtime).iter_custom(|iters| async move {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                clear_caches(env, cold).await;
                let start = Instant::now();
                // measure computation, not cache hits
                render_tile(env, source.clone(), (zoom, (0, 0)), colorizer, None).await;
                elapsed += start.elapsed();
            }
            elapsed
        });
    });
    // opt-in: these runs are outside Criterion's closures, so a case filter would not skip them
    if !common::metrics_enabled() {
        return;
    }
    // clearing the caches is part of these runs, but it opens no spans or streams
    metrics::record(&format!("{group_name}/{name}"), STAGE_RUNS, || {
        env.runtime.block_on(async {
            clear_caches(env, cold).await;
            render_tile(env, source.clone(), (zoom, (0, 0)), colorizer, None).await;
        });
    });
}

/// The map tiles of a client session, as offsets from the center tile: a 3×3 view, then a pan by
/// one column to the right and one row down
fn viewport_tiles() -> Vec<(i32, i32)> {
    let view = |x0: i32, y0: i32| (y0..y0 + 3).flat_map(move |y| (x0..x0 + 3).map(move |x| (x, y)));
    let mut tiles: Vec<_> = view(-1, -1).collect();
    for tile in view(0, -1).chain(view(0, 0)) {
        if !tiles.contains(&tile) {
            tiles.push(tile);
        }
    }
    tiles
}

/// Renders the tiles of a session, `parallel` at once, all through `cache`, and returns their PNGs
async fn render_session(
    env: &Env,
    (source, colorizer): (&TypedRasterOperator, &RasterColorizer),
    zoom: u8,
    parallel: usize,
    cache: Option<Arc<NewRasterCacheEnum>>,
) -> Vec<Vec<u8>> {
    futures::stream::iter(viewport_tiles())
        .map(|offset| {
            render_tile(
                env,
                source.clone(),
                (zoom, offset),
                colorizer,
                cache.clone(),
            )
        })
        .buffered(parallel)
        .collect()
        .await
}

/// Times a session as `viewport/<name>`, each with an empty cache if `cached`, then prints what the
/// cache did in one session and records the stages if the metrics are enabled
fn bench_session(
    group: &mut BenchmarkGroup<WallTime>,
    env: &Env,
    name: &str,
    layer: (&TypedRasterOperator, &RasterColorizer),
    zoom: u8,
    parallel: usize,
) {
    let cached = env.exe_ctx.cache_raster_operators;
    let new_cache = || cached.then(|| Arc::new(NewRasterCacheEnum::new_fifo(CACHE_BYTES)));
    group.bench_function(name, |b| {
        b.to_async(&env.runtime).iter_custom(|iters| async move {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                let cache = new_cache();
                let start = Instant::now();
                std::hint::black_box(render_session(env, layer, zoom, parallel, cache).await);
                elapsed += start.elapsed();
            }
            elapsed
        });
    });
    if let Some(cache) = new_cache() {
        env.runtime.block_on(render_session(
            env,
            layer,
            zoom,
            parallel,
            Some(cache.clone()),
        ));
        eprintln!("viewport/{name}: cache per session {:?}", cache.stats());
    }
    if common::metrics_enabled() {
        metrics::record(&format!("viewport/{name}"), 3, || {
            env.runtime
                .block_on(render_session(env, layer, zoom, parallel, new_cache()));
        });
    }
}

fn viewport(
    c: &mut Criterion,
    env: &mut Env,
    layers: &[(String, TypedRasterOperator, RasterColorizer)],
) {
    const ZOOMS: [u8; 2] = [12, 15];
    // as `[cache] default_ttl_seconds`: with the default of 0, nothing would be cached
    env.exe_ctx.inner.default_cache_ttl = CacheTtlSeconds::new(3600);

    for (name, source, colorizer) in layers {
        for zoom in ZOOMS {
            env.exe_ctx.cache_raster_operators = false;
            let uncached =
                env.runtime
                    .block_on(render_session(env, (source, colorizer), zoom, 1, None));
            env.exe_ctx.cache_raster_operators = true;
            let cache = Some(Arc::new(NewRasterCacheEnum::new_fifo(CACHE_BYTES)));
            let cached =
                env.runtime
                    .block_on(render_session(env, (source, colorizer), zoom, 6, cache));
            assert!(
                uncached == cached,
                "{name}/z{zoom}: the cache changes the PNGs"
            );
        }
    }

    let mut group = c.benchmark_group("viewport");
    group.sample_size(10);
    for cached in [false, true] {
        env.exe_ctx.cache_raster_operators = cached;
        let cache = if cached { "cache" } else { "no_cache" };
        for (name, source, colorizer) in layers {
            for zoom in ZOOMS {
                for parallel in [1, 6] {
                    bench_session(
                        &mut group,
                        env,
                        &format!("{name}/z{zoom}/{cache}/parallel{parallel}"),
                        (source, colorizer),
                        zoom,
                        parallel,
                    );
                }
            }
        }
    }
    group.finish();
    env.exe_ctx.cache_raster_operators = false;
}

fn map_tile(c: &mut Criterion) {
    let mut env = Env::new();

    let cog = utm_band(Format::Cog, 0);
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
    // TODO: use a real colorizer for the input range
    let colorizer: RasterColorizer = default_colorizer_gradient::<u16>().into();

    let mut group = c.benchmark_group("map_tile");
    for (cold, suffix) in [(false, ""), (true, "_cold")] {
        for zoom in ZOOM_LEVELS {
            let name = format!("z{zoom}{suffix}");
            bench_tile(
                &mut group,
                "map_tile",
                &env,
                &name,
                (&source, &colorizer),
                zoom,
                cold,
            );
        }
    }
    group.finish();

    let cog = multi_band_scene(&mut env, Format::Cog);
    let jp2 = multi_band_scene(&mut env, Format::Jp2);
    let mut group = c.benchmark_group("layer_patterns");
    group.sample_size(10);
    for (pattern, source, colorizer) in layer_patterns(&cog) {
        for zoom in ZOOM_LEVELS {
            let name = format!("{pattern}_cog/z{zoom}");
            bench_tile(
                &mut group,
                "layer_patterns",
                &env,
                &name,
                (&source, &colorizer),
                zoom,
                false,
            );
        }
    }
    // the Sentinel-2 L2A layers: true color and NDVI
    for (pattern, source, colorizer) in layer_patterns(&jp2)
        .into_iter()
        .filter(|(pattern, ..)| ["band_filter_rgb", "normalized_difference"].contains(pattern))
    {
        for (cold, suffix) in [(false, ""), (true, "_cold")] {
            for zoom in ZOOM_LEVELS {
                let name = format!("{pattern}_jp2/z{zoom}{suffix}");
                bench_tile(
                    &mut group,
                    "layer_patterns",
                    &env,
                    &name,
                    (&source, &colorizer),
                    zoom,
                    cold,
                );
            }
        }
    }
    group.finish();

    let mut layers = vec![("source_cog".to_string(), source, colorizer)];
    for (data, format, patterns) in [
        (
            &cog,
            "cog",
            ["normalized_difference", "stacked_expressions"],
        ),
        (&jp2, "jp2", ["band_filter_rgb", "normalized_difference"]),
    ] {
        layers.extend(
            layer_patterns(data)
                .into_iter()
                .filter(|(pattern, ..)| patterns.contains(pattern))
                .map(|(pattern, source, colorizer)| {
                    (format!("{pattern}_{format}"), source, colorizer)
                }),
        );
    }
    viewport(c, &mut env, &layers);
}

criterion_group!(benches, map_tile);
criterion_main!(benches);
