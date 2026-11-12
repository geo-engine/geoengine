//! GDAL source reads over HTTP ranges: window reads (as before) against reads of whole file
//! blocks through the raster cache.
//!
//! A local range server serves generated files to the GDAL workers via `/vsicurl/` and waits
//! `GEOENGINE_BENCH_LATENCY_MS` (default 20) before each answer, like an object store:
//! - a Sentinel-2 like COG: 10 980² `u16`, DEFLATE, 1 024 px blocks, overviews 2 to 16;
//! - if `GEOENGINE_BENCH_JP2` is set, a JP2 granule of the same size in 1 024 px tiles with TLM
//!   markers, opened with and without `USE_TILE_AS_BLOCK=YES`. Needs the `JP2OpenJPEG` driver,
//!   and the encoder holds the granule in memory once (about 1 GB) when it is generated.
//!
//! With `GEOENGINE_BENCH_CI` (see `common`), the files are 2 048², so that a run takes seconds.
//! The files are written once to `CARGO_TARGET_TMPDIR`.
//!
//! If `GEOENGINE_BENCH_DATA` is a directory with real Sentinel-2 L2A bands, the bench serves those
//! instead. They are from tile 30UVD, 2026-11-02
//! (<https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a/items/S2B_30UVD_20261102_0_L2A>):
//! - `B04.tif`, `B08.tif`, `SCL.tif` from
//!   `https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/30/U/VD/2026/11/S2B_30UVD_20261102_0_L2A/`
//! - `B02.jp2`, `B03.jp2`, `B04.jp2`, `B08.jp2` from
//!   `https://sentinel-s2-l2a.s3.eu-central-1.amazonaws.com/tiles/30/U/VD/2026/11/2/0/R10m/`
//!
//! Multi-band cases read one file per band through the `MultiBandGdalSource`.
//!
//! `view` cases read like a map client: 8 x 5 tiles of 256 px, each its own query, center first and
//! at most `VIEW_CONCURRENCY` at a time, the defaults of `OpenLayers`. `view_pan` then moves the view
//! four times and reads only the tiles it has not seen, after the previous view is done.
//! `view_512` and `view_512_pan` cover the same screen in 4 x 3 aligned 512 px tiles, like the
//! OGC API tiles of `eodyssey`: one source tile per request. `view_overview_4` is a view four
//! times further out. They print
//! the time from the start of a view to its first tile and to the median, p99 and last tile.
//!
//! The pool, the runtime and the GDAL options of the workers come from the server settings, as in
//! the other benches (`common`), e.g. `GEOENGINE__GDAL_PROCESS_POOL__MAX_DATASET_PROCESSES=4` or
//! `GEOENGINE__GDAL_PROCESS_POOL__WORKER__GDAL_CONFIG_OPTIONS__CPL_VSIL_CURL_CHUNK_SIZE=262144`.
//!
//! - `GEOENGINE_BENCH_CASES`: comma separated case names to run, default all. Use it rather than a
//!   criterion filter, which still does the untimed run of every case.
//! - `GEOENGINE_BENCH_MODES`: comma separated modes to run, default both. For remote files, to bound the
//!   traffic.
//! - `GEOENGINE_BENCH_CACHE_MB`: the size of the raster cache, default 2 GiB. The bench prints its peak
//!   memory at the end.
//! - `GEOENGINE_BENCH_REMOTE_JP2`, `GEOENGINE_BENCH_REMOTE_TIF`: read the files with that extension from this
//!   base URL instead of the local server, with their metadata from the local copies. The local
//!   server then counts no GETs and bytes for them.
//! - `GEOENGINE_BENCH_ROUNDS`: run each mode this many times, cold and then warm, plus one cold run for
//!   the hash, instead of letting criterion pick. For remote files, to bound the traffic.
//!
//! Modes: `window` queries without the raster cache, `block` with it.
//! Cold cases clear the GDAL workers and the raster cache before each iteration, untimed; warm
//! cases keep both. The OS page cache is not cleared; with a local server its cost is small.
//! Each case prints the requests, bytes served and distinct bytes per file, and the pool and cache
//! counts of one untimed run with a hash of its tiles, and the counts and worker CPU per timed
//! iteration. The
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

mod common;

use common::{Env, range_server::RangeServer};
use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use gdal::{DriverManager, raster::Buffer, raster::RasterCreationOptions, spatial_ref::SpatialRef};
use geoengine_datatypes::{
    dataset::{DatasetId, NamedData},
    primitives::{BandSelection, CacheTtlSeconds, RasterQueryRectangle, TimeInterval},
    raster::{GridBoundingBox2D, GridBoundingBoxExt, GridBounds, GridOrEmpty, Pixel, RasterTile2D},
    util::Identifier,
};
use geoengine_operators::{
    cache::new_raster_cache::{CacheStats, NewRasterCacheEnum},
    engine::{
        BoxRasterQueryProcessor, InitializedRasterOperator, MetaData, ProcessingGraphOperatorPath,
        QueryContext, QueryProcessor, RasterBandDescriptor, RasterBandDescriptors, RasterOperator,
        RasterResultDescriptor, StaticMetaData, TimeDescriptor, TypedRasterOperator,
        TypedRasterQueryProcessor,
    },
    source::{
        GdalMetaDataStatic, GdalProcessPool, GdalSource, GdalSourceParameters,
        MultiBandGdalLoadingInfo, MultiBandGdalLoadingInfoQueryRectangle, MultiBandGdalSource,
        MultiBandGdalSourceParameters, TileFile, gdal_worker_process::PoolRequestStats,
    },
    util::gdal::{gdal_open_dataset, gdal_parameters_from_dataset, raster_descriptor_from_dataset},
};
use std::{
    collections::{BTreeMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

const DEFAULT_SIZE: usize = 10_980;
const SMALL_SIZE: usize = 2_048;
const SCL_SIZE: usize = 5_490;
const WRITE_ROWS: usize = 512;
const RANDOM_WINDOWS: usize = 100;
const WINDOW_SIZE: isize = 256;
const PAN_TILES: usize = 200;
const VIEW_TILE_SIZE: isize = 256;
const VIEW_TILES: (isize, isize) = (5, 8);
const VIEW_PAN_MOVES: isize = 4;
const VIEW_CONCURRENCY: usize = 16;
const CACHE_BYTES: usize = 2 << 30;

fn file_size() -> usize {
    if common::ci() {
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
        "cog.tif",
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
        "granule.jp2",
        "JP2OpenJPEG",
        &[
            ("BLOCKXSIZE", "1024"),
            ("BLOCKYSIZE", "1024"),
            ("TLM", "YES"),
            ("REVERSIBLE", "YES"),
        ],
    )
}

/// The path of `file` on its remote server if one is set for its extension, else on `server`.
fn vsicurl_path(server: &RangeServer, file: &Path) -> String {
    let name = file.file_name().unwrap().to_str().unwrap();
    let extension = file.extension().unwrap().to_str().unwrap().to_uppercase();
    match std::env::var(format!("GEOENGINE_BENCH_REMOTE_{extension}")) {
        Ok(base) => format!("/vsicurl/{base}/{name}"),
        Err(_) => server.vsicurl_path(name),
    }
}

/// Registers `file`, served by `server`, as a `GdalSource` dataset. Returns its name and the
/// largest overview level it declares.
fn add_dataset(
    env: &mut Env,
    server: &RangeServer,
    file: &Path,
    open_options: Option<Vec<String>>,
) -> (NamedData, u32) {
    let dataset = gdal_open_dataset(file).unwrap();
    let mut params = gdal_parameters_from_dataset(&dataset, 1, file, None, open_options).unwrap();
    params.file_path = vsicurl_path(server, file).into();
    let result_descriptor = raster_descriptor_from_dataset(&dataset, 1).unwrap();
    let max_overview_level = result_descriptor
        .spatial_grid
        .max_overview_level()
        .unwrap_or(1);

    let name = env.add_data(
        &format!("file_{}", DatasetId::new()),
        Box::new(GdalMetaDataStatic {
            time: Some(TimeInterval::default()),
            params,
            result_descriptor,
            cache_ttl: None,
        }),
    );
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
    env: &mut Env,
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
            params.file_path = vsicurl_path(server, file).into();
            TileFile {
                time: TimeInterval::default(),
                spatial_partition,
                band,
                z_index: 0,
                params,
            }
        })
        .collect();

    let meta_data: Box<
        dyn MetaData<
                MultiBandGdalLoadingInfo,
                RasterResultDescriptor,
                MultiBandGdalLoadingInfoQueryRectangle,
            >,
    > = Box::new(StaticMetaData {
        loading_info: MultiBandGdalLoadingInfo::new(
            vec![TimeInterval::default()],
            tile_files,
            None,
        ),
        result_descriptor,
        phantom: PhantomData,
    });
    env.add_data(&format!("files_{}", DatasetId::new()), meta_data)
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
    /// Views one after another, each a list of tile queries in the order a client starts them.
    Views(Vec<Vec<GridBoundingBox2D>>),
}

/// When the tiles of the views were done, measured from the start of their view.
#[derive(Default)]
struct ViewTimes {
    first: Vec<Duration>,
    tiles: Vec<Duration>,
}

impl ViewTimes {
    fn print_and_clear(&mut self, id: &str) {
        if self.tiles.is_empty() {
            return;
        }
        self.first.sort_unstable();
        self.tiles.sort_unstable();
        let at = |quantile: f64| self.tiles[((self.tiles.len() - 1) as f64 * quantile) as usize];
        println!(
            "{id}: first tile {:.1?}, median tile {:.1?}, p99 tile {:.1?}, last tile {:.1?}",
            self.first[self.first.len() / 2],
            at(0.5),
            at(0.99),
            at(1.),
        );
        *self = Self::default();
    }
}

/// User and system time of the child processes.
fn worker_cpu_seconds() -> f64 {
    let me = std::process::id().to_string();
    let mut ticks = 0_u64;
    for entry in std::fs::read_dir("/proc").unwrap().flatten() {
        let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        let Some((_, rest)) = stat.rsplit_once(") ") else {
            continue;
        };
        let fields: Vec<&str> = rest.split(' ').collect();
        if fields.get(1) == Some(&me.as_str()) {
            ticks += fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
        }
    }
    // `CLK_TCK` is 100 on Linux, whatever the kernel's tick rate
    ticks as f64 / 100.
}

/// What runs asked of the server, the pool and the cache.
#[derive(Clone, Copy)]
struct Counts {
    requests: u64,
    gets: u64,
    bytes: u64,
    pool: PoolRequestStats,
    cache: CacheStats,
}

impl Counts {
    fn now(server: &RangeServer, pool: &GdalProcessPool, cache: &NewRasterCacheEnum) -> Self {
        let (requests, bytes) = server.totals();
        Self {
            requests,
            gets: server.gets(),
            bytes,
            pool: pool.request_stats(),
            cache: cache.stats(),
        }
    }

    fn since(self, earlier: Self) -> Self {
        Self {
            requests: self.requests - earlier.requests,
            gets: self.gets - earlier.gets,
            bytes: self.bytes - earlier.bytes,
            pool: PoolRequestStats {
                reads: self.pool.reads - earlier.pool.reads,
                reads_joined: self.pool.reads_joined - earlier.pool.reads_joined,
                headers: self.pool.headers - earlier.pool.headers,
                headers_joined: self.pool.headers_joined - earlier.pool.headers_joined,
            },
            cache: CacheStats {
                hits: self.cache.hits - earlier.cache.hits,
                misses: self.cache.misses - earlier.cache.misses,
                stored: self.cache.stored - earlier.cache.stored,
                inserts_joined: self.cache.inserts_joined - earlier.cache.inserts_joined,
                reads_joined: self.cache.reads_joined - earlier.cache.reads_joined,
            },
        }
    }

    fn print(self, id: &str, label: &str, iterations: u64) {
        if iterations == 0 {
            return; // filtered out
        }
        let per = |count: u64| count as f64 / iterations as f64;
        println!(
            "{id} {label}: {:.1} GET, {:.1} HEAD, {:.1} MB, pool {:.1} reads +{:.1} joined, \
             {:.1} headers +{:.1} joined, cache {:.1} hits, {:.1} misses, {:.1} stored +{:.1} joined, {:.1} block reads joined",
            per(self.gets),
            per(self.requests - self.gets),
            per(self.bytes) / 1e6,
            per(self.pool.reads),
            per(self.pool.reads_joined),
            per(self.pool.headers),
            per(self.pool.headers_joined),
            per(self.cache.hits),
            per(self.cache.misses),
            per(self.cache.stored),
            per(self.cache.inserts_joined),
            per(self.cache.reads_joined),
        );
    }
}

/// Runs the queries one after another and returns a hash of all tiles.
async fn run(
    env: &Env,
    source: TypedRasterOperator,
    bands: u32,
    query: &Query,
    cache: Option<Arc<NewRasterCacheEnum>>,
    hash_tiles: bool,
    view_times: &Mutex<ViewTimes>,
) -> u64 {
    let processor = source
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &env.exe_ctx)
        .await
        .unwrap();
    let scene = processor
        .result_descriptor()
        .tiling_grid_definition(env.exe_ctx.inner.tiling_specification)
        .tiling_grid_bounds();
    let processor = processor.query_processor().unwrap();

    // the windows are relative to the file, the query is in the tiling grid
    let in_grid = |windows: &[GridBoundingBox2D]| -> Vec<GridBoundingBox2D> {
        windows
            .iter()
            .map(|window| window.shift_by_offset(scene.min_index()))
            .collect()
    };
    let new_query_ctx = || {
        let mut query_ctx = env.new_query_ctx();
        query_ctx.new_raster_cache.clone_from(&cache);
        query_ctx
    };

    if let Query::Views(views) = query {
        let views = views.iter().map(|view| in_grid(view)).collect();
        return match processor {
            TypedRasterQueryProcessor::U8(processor) => {
                read_views(
                    processor,
                    views,
                    bands,
                    new_query_ctx,
                    hash_tiles,
                    view_times,
                )
                .await
            }
            TypedRasterQueryProcessor::U16(processor) => {
                read_views(
                    processor,
                    views,
                    bands,
                    new_query_ctx,
                    hash_tiles,
                    view_times,
                )
                .await
            }
            _ => panic!("the bench data is u8 or u16"),
        };
    }

    let windows = match query {
        Query::Windows(windows) => in_grid(windows),
        Query::Scene | Query::Views(_) => vec![scene],
    };
    let query_ctx = new_query_ctx();

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

/// Reads each view like a map client: every window is a query in its own task, as for a request
/// to the server, and they start in order. The hash does not depend on which query ends first.
async fn read_views<T: Pixel + Hash, C: QueryContext + 'static>(
    processor: BoxRasterQueryProcessor<T>,
    views: Vec<Vec<GridBoundingBox2D>>,
    bands: u32,
    new_query_ctx: impl Fn() -> C,
    hash_tiles: bool,
    view_times: &Mutex<ViewTimes>,
) -> u64 {
    let processor = Arc::new(processor);
    let mut hasher = DefaultHasher::new();
    for view in views {
        let start = Instant::now();
        let mut results: Vec<(usize, u64, Duration)> =
            futures::stream::iter(view.into_iter().enumerate().map(|(index, window)| {
                let (processor, query_ctx) = (processor.clone(), new_query_ctx());
                tokio::spawn(async move {
                    let hash =
                        read_tiles(&processor, vec![window], bands, &query_ctx, hash_tiles).await;
                    (index, hash, start.elapsed())
                })
            }))
            .buffer_unordered(VIEW_CONCURRENCY)
            .map(Result::unwrap)
            .collect()
            .await;

        results.sort_unstable_by_key(|(index, ..)| *index);
        let mut times = view_times.lock().unwrap();
        times
            .first
            .extend(results.iter().map(|(.., done)| *done).min());
        times.tiles.extend(results.iter().map(|(.., done)| *done));
        for (_, hash, _) in results {
            hash.hash(&mut hasher);
        }
    }
    hasher.finish()
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

/// A view around the center of a file with `size` pixels and the views after moving it, each with
/// the tiles it has not seen, center first. Unaligned views (`gis`, `OpenLayers` WMS) request
/// 256 px tiles that do not line up with the blocks of the file; aligned ones (`eodyssey`, OGC
/// API tiles) request the server's 512 px tiles. Both cover the same screen and move alike.
fn views(size: usize, moves: isize, aligned: bool) -> Vec<Vec<GridBoundingBox2D>> {
    let (tile, (offset_y, offset_x)) = if aligned {
        (512, (0, 0))
    } else {
        (VIEW_TILE_SIZE, (137, 91))
    };
    let (rows, columns) = (
        (VIEW_TILES.0 * VIEW_TILE_SIZE + tile - 1) / tile,
        (VIEW_TILES.1 * VIEW_TILE_SIZE + tile - 1) / tile,
    );
    let center = size as isize / 2;
    let mut seen = HashSet::new();
    (0..=moves)
        .map(|moved| {
            let (center_y, center_x) = (
                center + moved * VIEW_TILE_SIZE,
                center + 2 * moved * VIEW_TILE_SIZE,
            );
            let (first_row, first_column) = (
                (center_y - rows * tile / 2) / tile,
                (center_x - columns * tile / 2) / tile,
            );
            let mut view: Vec<(isize, isize)> = (first_row..first_row + rows)
                .flat_map(|row| {
                    (first_column..first_column + columns).map(move |column| (row, column))
                })
                .filter(|tile| seen.insert(*tile))
                .collect();
            let distance = |(row, column): (isize, isize)| {
                let y = row * tile + tile / 2 - center_y;
                let x = column * tile + tile / 2 - center_x;
                y * y + x * x
            };
            view.sort_by_key(|tile| distance(*tile));
            view.into_iter()
                .map(|(row, column)| {
                    // the offsets are not multiples of a block size, like the grid of a client
                    let (y, x) = (row * tile + offset_y, column * tile + offset_x);
                    GridBoundingBox2D::new([y, x], [y + tile - 1, x + tile - 1]).unwrap()
                })
                // a level smaller than the view only has the tiles inside of it
                .filter(|window| {
                    window.y_min() >= 0
                        && window.x_min() >= 0
                        && window.y_max() < size as isize
                        && window.x_max() < size as isize
                })
                .collect()
        })
        .collect()
}

struct Case {
    name: String,
    source: TypedRasterOperator,
    bands: u32,
    query: Query,
}

impl Case {
    fn new(name: String, source: TypedRasterOperator, query: Query) -> Self {
        Self {
            name,
            source,
            bands: 1,
            query,
        }
    }

    fn bands(self, bands: u32) -> Self {
        Self { bands, ..self }
    }
}

/// The whole scene, random windows and the declared overview levels 2 and 4.
fn cog_cases(prefix: &str, data: &NamedData, max_overview_level: u32, size: usize) -> Vec<Case> {
    let mut cases = vec![
        Case::new(
            format!("{prefix}/scene_512"),
            source(data, None),
            Query::Scene,
        ),
        Case::new(
            format!("{prefix}/windows_256"),
            source(data, None),
            Query::Windows(random_windows(size, RANDOM_WINDOWS)),
        ),
    ];
    cases.extend(view_cases(prefix, &source(data, None), size));
    if max_overview_level >= 4 {
        cases.push(Case::new(
            format!("{prefix}/view_overview_4"),
            source(data, Some(4)),
            Query::Views(views(size / 4, 0, false)),
        ));
    }
    // the COG driver builds overviews down to the block size, so small files have fewer
    for level in [2, 4]
        .into_iter()
        .filter(|level| *level <= max_overview_level)
    {
        cases.push(Case::new(
            format!("{prefix}/overview_{level}"),
            source(data, Some(level)),
            Query::Scene,
        ));
    }
    cases
}

/// A whole scene and a panning session.
fn jp2_cases(prefix: &str, source: &TypedRasterOperator, size: usize) -> Vec<Case> {
    let mut cases = vec![
        Case::new(format!("{prefix}/scene_512"), source.clone(), Query::Scene),
        Case::new(
            format!("{prefix}/pan_{PAN_TILES}"),
            source.clone(),
            Query::Windows(panning_windows(size, 512, PAN_TILES)),
        ),
    ];
    cases.extend(view_cases(prefix, source, size));
    cases
}

/// A first view and a panning session of a map client, unaligned and aligned.
fn view_cases(prefix: &str, source: &TypedRasterOperator, size: usize) -> Vec<Case> {
    [("view", false), ("view_512", true)]
        .into_iter()
        .flat_map(|(name, aligned)| {
            [
                Case::new(
                    format!("{prefix}/{name}"),
                    source.clone(),
                    Query::Views(views(size, 0, aligned)),
                ),
                Case::new(
                    format!("{prefix}/{name}_pan"),
                    source.clone(),
                    Query::Views(views(size, VIEW_PAN_MOVES, aligned)),
                ),
            ]
        })
        .collect()
}

const TILE_AS_BLOCK: [(&str, Option<&str>); 2] = [
    ("tile_as_block", Some("USE_TILE_AS_BLOCK=YES")),
    ("plain", None),
];

#[allow(clippy::too_many_lines)]
fn block_reads(c: &mut Criterion) {
    let mut env = Env::new();
    let pool = env.exe_ctx.inner.gdal_process_pool.clone();

    let latency = Duration::from_millis(
        std::env::var("GEOENGINE_BENCH_LATENCY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20),
    );
    let size = file_size();

    let data_dir = std::env::var_os("GEOENGINE_BENCH_DATA").map(PathBuf::from);
    let server = RangeServer::start(
        data_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_TARGET_TMPDIR"))),
        latency,
    );

    let mut cases = Vec::new();
    if let Some(dir) = &data_dir {
        let band = |file: &str| dir.join(file);

        let (cog, max_overview_level) = add_dataset(&mut env, &server, &band("B04.tif"), None);
        cases.extend(cog_cases("s2_cog", &cog, max_overview_level, size));
        // a classification, the data that compresses: 5 490² `u8`, 512 px blocks
        let (scl, max_overview_level) = add_dataset(&mut env, &server, &band("SCL.tif"), None);
        cases.extend(cog_cases("s2_scl", &scl, max_overview_level, SCL_SIZE));
        let cog_2band = multi_band_source(&add_multi_band_dataset(
            &mut env,
            &server,
            &[band("B04.tif"), band("B08.tif")],
            None,
        ));
        cases.push(
            Case::new(
                "s2_cog_2band/scene_512".into(),
                cog_2band.clone(),
                Query::Scene,
            )
            .bands(2),
        );
        cases.push(
            Case::new(
                "s2_cog_2band/windows_256".into(),
                cog_2band,
                Query::Windows(random_windows(size, RANDOM_WINDOWS)),
            )
            .bands(2),
        );

        for (suffix, open_option) in TILE_AS_BLOCK {
            let open_options = open_option.map(|option| vec![option.to_string()]);
            let (jp2, _) = add_dataset(&mut env, &server, &band("B04.jp2"), open_options);
            cases.extend(jp2_cases(
                &format!("s2_jp2_{suffix}"),
                &source(&jp2, None),
                size,
            ));
            cases.push(Case::new(
                format!("s2_jp2_{suffix}/view_overview_4"),
                source(&jp2, Some(4)),
                Query::Views(views(size / 4, 0, false)),
            ));
        }
        let jp2_4band = multi_band_source(&add_multi_band_dataset(
            &mut env,
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
        let (cog, max_overview_level) = add_dataset(&mut env, &server, &cog(), None);
        cases.extend(cog_cases("cog", &cog, max_overview_level, size));
        if std::env::var_os("GEOENGINE_BENCH_JP2").is_some() {
            let jp2 = jp2();
            for (suffix, open_option) in TILE_AS_BLOCK {
                let open_options = open_option.map(|option| vec![option.to_string()]);
                let (data, _) = add_dataset(&mut env, &server, &jp2, open_options);
                cases.extend(jp2_cases(
                    &format!("jp2_{suffix}"),
                    &source(&data, None),
                    size,
                ));
            }
        }
    }

    let cache_bytes = std::env::var("GEOENGINE_BENCH_CACHE_MB")
        .map_or(CACHE_BYTES, |mb| mb.parse::<usize>().unwrap() << 20);
    // blocks live longer than a run, so that warm runs stay warm
    let cache = Arc::new(
        NewRasterCacheEnum::new_lru(cache_bytes)
            .with_min_block_ttl(CacheTtlSeconds::new(3600))
            .with_block_reads(true),
    );
    let hashes: Mutex<BTreeMap<String, u64>> = Mutex::default();

    let mut group = common::group(c, "block_reads");
    // a cold iteration takes up to seconds
    group.sample_size(10);
    let only = std::env::var("GEOENGINE_BENCH_CASES").unwrap_or_default();
    let rounds: Option<u32> = std::env::var("GEOENGINE_BENCH_ROUNDS")
        .ok()
        .map(|rounds| rounds.parse().unwrap());
    let modes = std::env::var("GEOENGINE_BENCH_MODES").unwrap_or_default();
    let view_times: Mutex<ViewTimes> = Mutex::default();
    for case in &cases {
        if !only.is_empty() && !only.split(',').any(|name| case.name == name) {
            continue;
        }
        for (mode, use_cache) in [("window", false), ("block", true)] {
            if !modes.is_empty() && !modes.split(',').any(|name| mode == name) {
                continue;
            }
            for cold in [true, false] {
                let id = format!(
                    "{}/{mode}{}",
                    case.name,
                    if cold { "_cold" } else { "_warm" }
                );
                let (env, server, cache, hashes, pool) = (&env, &server, &cache, &hashes, &pool);
                let view_times = &view_times;
                let source = case.source.clone();

                if let Some(rounds) = rounds {
                    env.runtime.block_on(async {
                        let run_once = |hash_tiles| {
                            run(
                                env,
                                source.clone(),
                                case.bands,
                                &case.query,
                                use_cache.then(|| cache.clone()),
                                hash_tiles,
                                view_times,
                            )
                        };
                        let clear = || async {
                            pool.clear_caches().await.unwrap();
                            cache.clear();
                        };
                        if cold {
                            clear().await;
                            let hash = run_once(true).await;
                            *view_times.lock().unwrap() = ViewTimes::default();
                            println!("{id}: hash {hash:016x}");
                            let mut hashes = hashes.lock().unwrap();
                            let expected = *hashes.entry(case.name.clone()).or_insert(hash);
                            assert_eq!(hash, expected, "{id} differs from the other mode");
                        }
                        let before = Counts::now(server, pool, cache);
                        let mut seconds = Vec::new();
                        for _ in 0..rounds {
                            if cold {
                                clear().await;
                            }
                            let start = Instant::now();
                            run_once(false).await;
                            seconds.push(start.elapsed().as_secs_f64());
                        }
                        println!("{id}: rounds {seconds:.3?} s");
                        Counts::now(server, pool, cache).since(before).print(
                            &id,
                            "per round",
                            u64::from(rounds),
                        );
                        view_times.lock().unwrap().print_and_clear(&id);
                    });
                    continue;
                }

                // one untimed run for the request counts and the hash
                env.runtime.block_on(async {
                    pool.clear_caches().await.unwrap();
                    cache.clear();
                    server.reset();
                    let before = Counts::now(server, pool, cache);
                    let hash = run(
                        env,
                        source.clone(),
                        case.bands,
                        &case.query,
                        use_cache.then(|| cache.clone()),
                        true,
                        view_times,
                    )
                    .await;
                    *view_times.lock().unwrap() = ViewTimes::default();
                    let run_counts = Counts::now(server, pool, cache).since(before);
                    run_counts.print(&id, "untimed run", 1);
                    server.print_coverage(&id);
                    println!(
                        "{id}: cache {:.1} MB, hash {hash:016x}",
                        cache.byte_size() as f64 / 1e6
                    );
                    let mut hashes = hashes.lock().unwrap();
                    let expected = *hashes.entry(case.name.clone()).or_insert(hash);
                    assert_eq!(hash, expected, "{id} differs from the other mode");
                });

                let before_timed = Counts::now(server, pool, cache);
                let timed_iterations = AtomicU64::new(0);
                let cpu_before = worker_cpu_seconds();
                group.bench_function(&id, |b| {
                    b.to_async(&env.runtime).iter_custom(|iters| {
                        let source = source.clone();
                        let timed_iterations = &timed_iterations;
                        async move {
                            timed_iterations.fetch_add(iters, Ordering::Relaxed);
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iters {
                                if cold {
                                    pool.clear_caches().await.unwrap();
                                    cache.clear();
                                }
                                let start = Instant::now();
                                std::hint::black_box(
                                    run(
                                        env,
                                        source.clone(),
                                        case.bands,
                                        &case.query,
                                        use_cache.then(|| cache.clone()),
                                        false,
                                        view_times,
                                    )
                                    .await,
                                );
                                elapsed += start.elapsed();
                            }
                            elapsed
                        }
                    });
                });
                let iterations = timed_iterations.into_inner();
                Counts::now(server, pool, cache).since(before_timed).print(
                    &id,
                    "timed, per iteration",
                    iterations,
                );
                if iterations > 0 {
                    println!(
                        "{id}: worker CPU {:.2} s per iteration",
                        (worker_cpu_seconds() - cpu_before) / iterations as f64
                    );
                }
                view_times.lock().unwrap().print_and_clear(&id);
            }
        }
    }
    group.finish();
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    if let Some(peak) = status.lines().find(|line| line.starts_with("VmHWM")) {
        println!("bench process {peak}");
    }
}

criterion_group!(name = benches; config = common::criterion(); targets = block_reads);
criterion_main!(benches);
