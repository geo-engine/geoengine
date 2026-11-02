//! GDAL source reads over HTTP ranges: window reads (as before) against reads of whole file
//! blocks through the raster cache.
//!
//! A local range server serves generated files to the GDAL workers via `/vsicurl/` and waits
//! `WP7_BENCH_LATENCY_MS` (default 20) before each answer, like an object store:
//! - a Sentinel-2 like COG: 10 980² `u16`, DEFLATE, 1 024 px blocks, overviews 2 to 16;
//! - if `WP7_BENCH_JP2` is set, a JP2 granule of the same size in 1 024 px tiles with TLM
//!   markers, opened with and without `USE_TILE_AS_BLOCK=YES`. Needs the `JP2OpenJPEG` driver,
//!   and the encoder holds the granule in memory once (about 1 GB) when it is generated.
//!
//! `WP7_BENCH_SMALL` uses 2 048² files, so that a run takes seconds (CI).
//! The files are written once to `CARGO_TARGET_TMPDIR`.
//!
//! If `WP7_BENCH_DATA` is a directory with real Sentinel-2 L2A bands, the bench serves those
//! instead. They are from tile 30UVD, 2026-11-02
//! (<https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a/items/S2B_30UVD_20261102_0_L2A>):
//! - `B04.tif`, `B08.tif`, `SCL.tif` from
//!   `https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/30/U/VD/2026/11/S2B_30UVD_20261102_0_L2A/`
//! - `B02.jp2`, `B03.jp2`, `B04.jp2`, `B08.jp2` from
//!   `https://sentinel-s2-l2a.s3.eu-central-1.amazonaws.com/tiles/30/U/VD/2026/11/2/0/R10m/`
//!
//! Multi-band cases read one file per band through the `MultiBandGdalSource`.
//!
//! Modes: `window` queries without the raster cache, `block` with it.
//! Cold cases clear the GDAL workers and the raster cache before each iteration, untimed; warm
//! cases keep both. The OS page cache is not cleared; with a local server its cost is small.
//! Each case prints the GET requests and bytes of one untimed run and a hash of its tiles. The
//! hashes must be equal across modes, the bench panics otherwise. Timed runs do not hash: on the
//! one thread that reads the stream, hashing every pixel would take most of the time.
//!
//! `cargo build --release --bin gdalsource-process && cargo bench -p geoengine-operators --bench block_reads`

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

mod util;

use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use gdal::{DriverManager, raster::Buffer, raster::RasterCreationOptions, spatial_ref::SpatialRef};
use geoengine_datatypes::{
    dataset::{DataId, DatasetId, NamedData},
    primitives::{BandSelection, RasterQueryRectangle, TimeInterval},
    raster::{
        GridBoundingBox2D, GridBoundingBoxExt, GridBounds, GridOrEmpty, GridShape2D, Pixel,
        RasterTile2D, TilingSpecification,
    },
    util::{Identifier, test::TestDefault},
};
use geoengine_operators::{
    cache::new_raster_cache::NewRasterCacheEnum,
    engine::{
        BoxRasterQueryProcessor, ChunkByteSize, InitializedRasterOperator, MetaData,
        MockExecutionContext, ProcessingGraphOperatorPath, QueryContext, QueryProcessor,
        RasterBandDescriptor, RasterBandDescriptors, RasterOperator, RasterResultDescriptor,
        StaticMetaData, TimeDescriptor, TypedRasterOperator, TypedRasterQueryProcessor,
    },
    source::{
        GdalMetaDataStatic, GdalProcessPool, GdalSource, GdalSourceParameters,
        MultiBandGdalLoadingInfo, MultiBandGdalLoadingInfoQueryRectangle, MultiBandGdalSource,
        MultiBandGdalSourceParameters, TileFile, gdal_worker_process::WorkerConfig,
    },
    util::gdal::{gdal_open_dataset, gdal_parameters_from_dataset, raster_descriptor_from_dataset},
};
use std::{
    collections::BTreeMap,
    hash::{DefaultHasher, Hash, Hasher},
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use util::range_server::RangeServer;

const DEFAULT_SIZE: usize = 10_980;
const SMALL_SIZE: usize = 2_048;
const BLOCK_SIZE: usize = 1_024;
const SCL_SIZE: usize = 5_490;
const WRITE_ROWS: usize = 512;
const RANDOM_WINDOWS: usize = 100;
const WINDOW_SIZE: isize = 256;
const PAN_TILES: usize = 200;
const CACHE_BYTES: usize = 2 << 30;

fn file_size() -> usize {
    if std::env::var_os("WP7_BENCH_SMALL").is_some() {
        SMALL_SIZE
    } else {
        DEFAULT_SIZE
    }
}

/// Writes a `u16` `GeoTIFF` row by row, then converts it with `driver` and `options` once.
fn generate(file_name: &str, driver: &str, options: &[(&str, &str)]) -> PathBuf {
    let size = file_size();
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{size}_{file_name}"));
    if path.exists() {
        return path;
    }

    let tmp = path.with_extension("tmp.tif");
    let mut tiled = RasterCreationOptions::new();
    tiled.add_name_value("TILED", "YES").unwrap();
    let mut dataset = DriverManager::get_driver_by_name("GTiff")
        .unwrap()
        .create_with_band_type_with_options::<u16, _>(&tmp, size, size, 1, &tiled)
        .unwrap();
    dataset
        .set_spatial_ref(&SpatialRef::from_epsg(32632).unwrap())
        .unwrap();
    dataset
        .set_geo_transform(&[399_960., 10., 0., 5_600_040., 0., -10.])
        .unwrap();
    let mut band = dataset.rasterband(1).unwrap();
    band.set_no_data_value(Some(0.)).unwrap();
    for y0 in (0..size).step_by(WRITE_ROWS) {
        let rows = WRITE_ROWS.min(size - y0);
        let data = (0..rows * size)
            .map(|i| {
                let (x, y) = (i % size, y0 + i / size);
                1 + ((x / 7 + y / 5) % 10_000) as u16
            })
            .collect();
        let mut buffer = Buffer::new((size, rows), data);
        band.write((0, y0 as isize), (size, rows), &mut buffer)
            .unwrap();
    }

    let mut create_options = RasterCreationOptions::new();
    for (name, value) in options {
        create_options.add_name_value(name, value).unwrap();
    }
    dataset
        .create_copy(
            &DriverManager::get_driver_by_name(driver).unwrap(),
            &path,
            &create_options,
        )
        .unwrap();
    drop(dataset);
    std::fs::remove_file(tmp).unwrap();
    path
}

fn cog() -> PathBuf {
    generate(
        "wp7_cog.tif",
        "COG",
        &[
            ("COMPRESS", "DEFLATE"),
            ("BLOCKSIZE", "1024"),
            ("OVERVIEW_RESAMPLING", "NEAREST"),
        ],
    )
}

fn jp2() -> PathBuf {
    generate(
        "wp7_granule.jp2",
        "JP2OpenJPEG",
        &[
            ("BLOCKXSIZE", "1024"),
            ("BLOCKYSIZE", "1024"),
            ("TLM", "YES"),
            ("REVERSIBLE", "YES"),
        ],
    )
}

/// Registers `file`, served by `server`, as a `GdalSource` dataset. Returns its name and the
/// largest overview level it declares.
fn add_dataset(
    exec_ctxs: &mut [MockExecutionContext],
    server: &RangeServer,
    file: &Path,
    open_options: Option<Vec<String>>,
) -> (NamedData, u32) {
    let dataset = gdal_open_dataset(file).unwrap();
    let mut params = gdal_parameters_from_dataset(&dataset, 1, file, None, open_options).unwrap();
    params.file_path = server
        .vsicurl_path(file.file_name().unwrap().to_str().unwrap())
        .into();
    let result_descriptor = raster_descriptor_from_dataset(&dataset, 1).unwrap();
    let max_overview_level = result_descriptor
        .spatial_grid
        .max_overview_level()
        .unwrap_or(1);

    let name = NamedData::with_system_name(format!("wp7_{}", DatasetId::new()));
    let id = DataId::from(DatasetId::new());
    for exec_ctx in exec_ctxs {
        exec_ctx.add_meta_data(
            id.clone(),
            name.clone(),
            Box::new(GdalMetaDataStatic {
                time: Some(TimeInterval::default()),
                params: params.clone(),
                result_descriptor: result_descriptor.clone(),
                cache_ttl: None,
            }),
        );
    }
    (name, max_overview_level)
}

fn source(name: &NamedData, overview_level: Option<u32>) -> TypedRasterOperator {
    GdalSource {
        params: GdalSourceParameters::new(name.clone()).with_overview_level(overview_level),
    }
    .typed()
}

/// Registers one file per band, served by `server`, as a `MultiBandGdalSource` dataset. The
/// files must share their grid.
fn add_multi_band_dataset(
    exec_ctxs: &mut [MockExecutionContext],
    server: &RangeServer,
    files: &[PathBuf],
    open_option: Option<&str>,
) -> NamedData {
    let mut result_descriptor =
        raster_descriptor_from_dataset(&gdal_open_dataset(&files[0]).unwrap(), 1).unwrap();
    result_descriptor.time = TimeDescriptor::new_irregular(Some(TimeInterval::default()));
    result_descriptor.bands = RasterBandDescriptors::new(
        (0..files.len() as u32)
            .map(RasterBandDescriptor::new_unitless_with_idx)
            .collect(),
    )
    .unwrap();
    let spatial_partition = result_descriptor.spatial_grid.spatial_partition();
    let tile_files: Vec<TileFile> = files
        .iter()
        .zip(0..)
        .map(|(file, band)| {
            let dataset = gdal_open_dataset(file).unwrap();
            let mut params = gdal_parameters_from_dataset(
                &dataset,
                1,
                file,
                None,
                open_option.map(|option| vec![option.into()]),
            )
            .unwrap();
            params.file_path = server
                .vsicurl_path(file.file_name().unwrap().to_str().unwrap())
                .into();
            TileFile {
                time: TimeInterval::default(),
                spatial_partition,
                band,
                z_index: 0,
                params,
            }
        })
        .collect();

    let name = NamedData::with_system_name(format!("wp7_{}", DatasetId::new()));
    let id = DataId::from(DatasetId::new());
    for exec_ctx in exec_ctxs {
        let meta_data: Box<
            dyn MetaData<
                    MultiBandGdalLoadingInfo,
                    RasterResultDescriptor,
                    MultiBandGdalLoadingInfoQueryRectangle,
                >,
        > = Box::new(StaticMetaData {
            loading_info: MultiBandGdalLoadingInfo::new(
                vec![TimeInterval::default()],
                tile_files.clone(),
                None,
            ),
            result_descriptor: result_descriptor.clone(),
            phantom: PhantomData,
        });
        exec_ctx.add_meta_data(id.clone(), name.clone(), meta_data);
    }
    name
}

fn multi_band_source(name: &NamedData) -> TypedRasterOperator {
    MultiBandGdalSource {
        params: MultiBandGdalSourceParameters::new(name.clone()),
    }
    .typed()
}

/// What a case queries: the whole scene, or a list of windows in the source's pixels.
#[derive(Clone)]
enum Query {
    Scene,
    Windows(Vec<GridBoundingBox2D>),
}

/// Runs the queries one after another and returns a hash of all tiles.
async fn run(
    exec_ctx: &MockExecutionContext,
    source: TypedRasterOperator,
    bands: u32,
    query: &Query,
    cache: Option<Arc<NewRasterCacheEnum>>,
    hash_tiles: bool,
) -> u64 {
    let processor = source
        .initialize(ProcessingGraphOperatorPath::initialize_root(), exec_ctx)
        .await
        .unwrap();
    let scene = processor
        .result_descriptor()
        .tiling_grid_definition(exec_ctx.tiling_specification)
        .tiling_grid_bounds();
    let processor = processor.query_processor().unwrap();

    let windows = match query {
        Query::Scene => vec![scene],
        // the windows are relative to the file, the query is in the tiling grid
        Query::Windows(windows) => windows
            .iter()
            .map(|window| window.shift_by_offset(scene.min_index()))
            .collect(),
    };

    let mut query_ctx = exec_ctx.mock_query_context(ChunkByteSize::test_default());
    query_ctx.new_raster_cache = cache;

    match processor {
        TypedRasterQueryProcessor::U8(processor) => {
            read_tiles(&processor, windows, bands, &query_ctx, hash_tiles).await
        }
        TypedRasterQueryProcessor::U16(processor) => {
            read_tiles(&processor, windows, bands, &query_ctx, hash_tiles).await
        }
        _ => panic!("the bench data is u8 or u16"),
    }
}

async fn read_tiles<T: Pixel + Hash>(
    processor: &BoxRasterQueryProcessor<T>,
    windows: Vec<GridBoundingBox2D>,
    bands: u32,
    query_ctx: &dyn QueryContext,
    hash_tiles: bool,
) -> u64 {
    let mut hasher = DefaultHasher::new();
    for window in windows {
        let query = RasterQueryRectangle::new(
            window,
            TimeInterval::default(),
            BandSelection::first_n(bands),
        );
        let mut stream = processor.query(query, query_ctx).await.unwrap();
        while let Some(tile) = stream.next().await {
            let tile = tile.unwrap();
            if hash_tiles {
                hash_tile(&tile, &mut hasher);
            }
            std::hint::black_box(tile);
        }
    }
    hasher.finish()
}

fn hash_tile<T: Pixel + Hash>(tile: &RasterTile2D<T>, hasher: &mut DefaultHasher) {
    tile.tile_position.inner().hash(hasher);
    tile.band.hash(hasher);
    match &tile.grid_array {
        GridOrEmpty::Grid(grid) => {
            // only valid pixels count: no-data pixels may hold any value
            for (value, valid) in grid.inner_grid.data.iter().zip(&grid.validity_mask.data) {
                (*valid).then_some(*value).hash(hasher);
            }
        }
        GridOrEmpty::Empty(_) => 0_u8.hash(hasher),
    }
}

/// `count` windows of `WINDOW_SIZE` pixels in a scene of `size` pixels, from a fixed seed.
fn random_windows(size: usize, count: usize) -> Vec<GridBoundingBox2D> {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = |bound: isize| {
        // xorshift64*: reproducible without a dependency
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        (state.wrapping_mul(0x2545_F491_4F6C_DD1D) % bound as u64) as isize
    };
    let max = size as isize - WINDOW_SIZE;
    (0..count)
        .map(|_| {
            let (y, x) = (next(max), next(max));
            GridBoundingBox2D::new([y, x], [y + WINDOW_SIZE - 1, x + WINDOW_SIZE - 1]).unwrap()
        })
        .collect()
}

/// A panning session: `count` windows of two tiles, each shifted by 3/4 of a tile.
fn panning_windows(size: usize, tile_size: usize, count: usize) -> Vec<GridBoundingBox2D> {
    let step = (tile_size * 3 / 4) as isize;
    let extent = 2 * tile_size as isize;
    let per_row = ((size as isize - extent) / step).max(1);
    (0..count as isize)
        .map(|i| {
            let (y, x) = ((i / per_row) * step, (i % per_row) * step);
            let y = y % (size as isize - extent).max(1);
            GridBoundingBox2D::new([y, x], [y + extent - 1, x + extent - 1]).unwrap()
        })
        .collect()
}

struct Case {
    name: String,
    exec_ctx_idx: usize,
    source: TypedRasterOperator,
    bands: u32,
    query: Query,
}

impl Case {
    fn new(name: String, tile_size: usize, source: TypedRasterOperator, query: Query) -> Self {
        Self {
            name,
            exec_ctx_idx: usize::from(tile_size == BLOCK_SIZE),
            source,
            bands: 1,
            query,
        }
    }

    fn bands(self, bands: u32) -> Self {
        Self { bands, ..self }
    }
}

/// Whole scenes in both tile sizes, random windows and the declared overview levels 2 and 4.
fn cog_cases(prefix: &str, data: &NamedData, max_overview_level: u32, size: usize) -> Vec<Case> {
    let mut cases = vec![
        Case::new(
            format!("{prefix}/scene_512"),
            512,
            source(data, None),
            Query::Scene,
        ),
        Case::new(
            format!("{prefix}/scene_1024"),
            BLOCK_SIZE,
            source(data, None),
            Query::Scene,
        ),
        Case::new(
            format!("{prefix}/windows_256"),
            512,
            source(data, None),
            Query::Windows(random_windows(size, RANDOM_WINDOWS)),
        ),
    ];
    // the COG driver builds overviews down to the block size, so small files have fewer
    for level in [2, 4]
        .into_iter()
        .filter(|level| *level <= max_overview_level)
    {
        cases.push(Case::new(
            format!("{prefix}/overview_{level}"),
            512,
            source(data, Some(level)),
            Query::Scene,
        ));
    }
    cases
}

/// A whole scene and a panning session, both in 512 px tiles.
fn jp2_cases(prefix: &str, source: &TypedRasterOperator, size: usize) -> Vec<Case> {
    vec![
        Case::new(
            format!("{prefix}/scene_512"),
            512,
            source.clone(),
            Query::Scene,
        ),
        Case::new(
            format!("{prefix}/pan_{PAN_TILES}"),
            512,
            source.clone(),
            Query::Windows(panning_windows(size, 512, PAN_TILES)),
        ),
    ]
}

const TILE_AS_BLOCK: [(&str, Option<&str>); 2] = [
    ("tile_as_block", Some("USE_TILE_AS_BLOCK=YES")),
    ("plain", None),
];

#[allow(clippy::too_many_lines)]
fn block_reads(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();

    let latency = Duration::from_millis(
        std::env::var("WP7_BENCH_LATENCY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20),
    );
    let size = file_size();

    let data_dir = std::env::var_os("WP7_BENCH_DATA").map(PathBuf::from);
    let server = RangeServer::start(
        data_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_TARGET_TMPDIR"))),
        latency,
    );

    // the server defaults of `[gdal_process_pool]` and `[gdal_process_pool.worker.gdal_config_options]`
    let pool = GdalProcessPool::new(
        32,
        16,
        8,
        WorkerConfig {
            gdal_config_options: Some(vec![
                ("GDAL_DISABLE_READDIR_ON_OPEN".into(), "EMPTY_DIR".into()),
                ("CPL_VSIL_CURL_CHUNK_SIZE".into(), "1048576".into()),
                ("VSI_CACHE".into(), "TRUE".into()),
                ("VSI_CACHE_SIZE".into(), "67108864".into()),
                ("GDAL_NUM_THREADS".into(), "1".into()),
            ]),
            ..WorkerConfig::default()
        },
    );
    let mut exec_ctxs: Vec<MockExecutionContext> = [512, BLOCK_SIZE]
        .into_iter()
        .map(|tile_size| {
            let mut exec_ctx = MockExecutionContext::new_with_tiling_spec(
                TilingSpecification::new(GridShape2D::new([tile_size, tile_size])),
            );
            exec_ctx.gdal_process_pool = pool.clone();
            exec_ctx
        })
        .collect();

    let mut cases = Vec::new();
    if let Some(dir) = &data_dir {
        let band = |file: &str| dir.join(file);

        let (cog, max_overview_level) =
            add_dataset(&mut exec_ctxs, &server, &band("B04.tif"), None);
        cases.extend(cog_cases("s2_cog", &cog, max_overview_level, size));
        // a classification, the data that compresses: 5 490² `u8`, 512 px blocks
        let (scl, max_overview_level) =
            add_dataset(&mut exec_ctxs, &server, &band("SCL.tif"), None);
        cases.extend(cog_cases("s2_scl", &scl, max_overview_level, SCL_SIZE));
        let cog_2band = multi_band_source(&add_multi_band_dataset(
            &mut exec_ctxs,
            &server,
            &[band("B04.tif"), band("B08.tif")],
            None,
        ));
        cases.push(
            Case::new(
                "s2_cog_2band/scene_512".into(),
                512,
                cog_2band.clone(),
                Query::Scene,
            )
            .bands(2),
        );
        cases.push(
            Case::new(
                "s2_cog_2band/windows_256".into(),
                512,
                cog_2band,
                Query::Windows(random_windows(size, RANDOM_WINDOWS)),
            )
            .bands(2),
        );

        for (suffix, open_option) in TILE_AS_BLOCK {
            let open_options = open_option.map(|option| vec![option.to_string()]);
            let (jp2, _) = add_dataset(&mut exec_ctxs, &server, &band("B04.jp2"), open_options);
            cases.extend(jp2_cases(
                &format!("s2_jp2_{suffix}"),
                &source(&jp2, None),
                size,
            ));
        }
        let jp2_4band = multi_band_source(&add_multi_band_dataset(
            &mut exec_ctxs,
            &server,
            &[
                band("B02.jp2"),
                band("B03.jp2"),
                band("B04.jp2"),
                band("B08.jp2"),
            ],
            TILE_AS_BLOCK[0].1,
        ));
        cases.extend(
            jp2_cases("s2_jp2_4band_tile_as_block", &jp2_4band, size)
                .into_iter()
                .map(|case| case.bands(4)),
        );
    } else {
        let (cog, max_overview_level) = add_dataset(&mut exec_ctxs, &server, &cog(), None);
        cases.extend(cog_cases("cog", &cog, max_overview_level, size));
        if std::env::var_os("WP7_BENCH_JP2").is_some() {
            let jp2 = jp2();
            for (suffix, open_option) in TILE_AS_BLOCK {
                let open_options = open_option.map(|option| vec![option.to_string()]);
                let (data, _) = add_dataset(&mut exec_ctxs, &server, &jp2, open_options);
                cases.extend(jp2_cases(
                    &format!("jp2_{suffix}"),
                    &source(&data, None),
                    size,
                ));
            }
        }
    }

    let cache = Arc::new(NewRasterCacheEnum::new_lru(CACHE_BYTES));
    let hashes: Mutex<BTreeMap<String, u64>> = Mutex::default();

    let mut group = c.benchmark_group("block_reads");
    group.sample_size(10);
    for case in &cases {
        let exec_ctx = &exec_ctxs[case.exec_ctx_idx];
        for (mode, use_cache) in [("window", false), ("block", true)] {
            for cold in [true, false] {
                let id = format!(
                    "{}/{mode}{}",
                    case.name,
                    if cold { "_cold" } else { "_warm" }
                );
                let (server, cache, hashes, pool) = (&server, &cache, &hashes, &pool);
                let source = case.source.clone();

                // one untimed run for the request counts and the hash
                runtime.block_on(async {
                    pool.clear_caches().await.unwrap();
                    cache.clear();
                    server.reset();
                    let hash = run(
                        exec_ctx,
                        source.clone(),
                        case.bands,
                        &case.query,
                        use_cache.then(|| cache.clone()),
                        true,
                    )
                    .await;
                    let (requests, bytes) = server.totals();
                    println!(
                        "{id}: {requests} GET/HEAD, {:.1} MB, cache {:.1} MB, hash {hash:016x}",
                        bytes as f64 / 1e6,
                        cache.byte_size() as f64 / 1e6
                    );
                    let mut hashes = hashes.lock().unwrap();
                    let expected = *hashes.entry(case.name.clone()).or_insert(hash);
                    assert_eq!(hash, expected, "{id} differs from the other mode");
                });

                group.bench_function(&id, |b| {
                    b.to_async(&runtime).iter_custom(|iters| {
                        let source = source.clone();
                        async move {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iters {
                                if cold {
                                    pool.clear_caches().await.unwrap();
                                    cache.clear();
                                }
                                let start = Instant::now();
                                std::hint::black_box(
                                    run(
                                        exec_ctx,
                                        source.clone(),
                                        case.bands,
                                        &case.query,
                                        use_cache.then(|| cache.clone()),
                                        false,
                                    )
                                    .await,
                                );
                                elapsed += start.elapsed();
                            }
                            elapsed
                        }
                    });
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, block_reads);
criterion_main!(benches);
