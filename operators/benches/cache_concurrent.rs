use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::future::join_all;
use geoengine_datatypes::primitives::CacheHint;
use geoengine_datatypes::primitives::{DateTime, SpatialPartition2D, SpatialResolution};
use geoengine_datatypes::raster::RasterProperties;
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, TimeInterval},
    raster::{Grid, RasterTile2D},
    util::test::TestDefault,
};
use geoengine_operators::{
    engine::CanonicOperatorName,
    pro::cache::shared_cache::{AsyncCache, SharedCache},
};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde_json::json;

static WRITTEN_ELEMENTS: AtomicUsize = AtomicUsize::new(0);

enum Measurement {
    Read(ReadMeasurement),
    Write(WriteMeasurement),
}

struct ReadMeasurement {
    read_query_ms: u128,
}

struct WriteMeasurement {
    insert_query_ms: u128,
    insert_tile_ms: u128,
    finish_query_ms: u128,
}

async fn write_cache(tile_cache: &SharedCache, op_name: CanonicOperatorName) -> WriteMeasurement {
    let tile = RasterTile2D::<u8> {
        time: TimeInterval::new_unchecked(1, 1),
        tile_position: [-1, 0].into(),
        global_geo_transform: TestDefault::test_default(),
        grid_array: Grid::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
            .unwrap()
            .into(),
        properties: RasterProperties::default(),
        cache_hint: CacheHint::max_duration(),
    };

    let start = std::time::Instant::now();

    let query_id = <SharedCache as AsyncCache<RasterTile2D<u8>>>::insert_query(
        tile_cache,
        &op_name,
        &query_rect(),
    )
    .await
    .unwrap();

    let insert_query_s = start.elapsed().as_millis();

    let start = std::time::Instant::now();

    tile_cache
        .insert_query_element(&op_name, &query_id, tile)
        .await
        .unwrap();

    let insert_tile_s = start.elapsed().as_millis();

    let start = std::time::Instant::now();

    #[allow(clippy::unit_arg)]
    black_box(
        <SharedCache as AsyncCache<RasterTile2D<u8>>>::finish_query(
            tile_cache, &op_name, &query_id,
        )
        .await
        .unwrap(),
    );

    let finish_query_s = start.elapsed().as_millis();

    WRITTEN_ELEMENTS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    WriteMeasurement {
        insert_query_ms: insert_query_s,
        insert_tile_ms: insert_tile_s,
        finish_query_ms: finish_query_s,
    }
}

async fn read_cache(tile_cache: &SharedCache, op_no: usize) -> ReadMeasurement {
    // read from one of the previously written queries at random.
    // as it is not predictable which queries are already written, this means the benchmark may run differently each times
    let mut rng: SmallRng = SeedableRng::seed_from_u64(op_no as u64);

    let op = op(rng.gen_range(0..WRITTEN_ELEMENTS.load(Ordering::SeqCst)));

    let query = query_rect();

    let start = std::time::Instant::now();

    let res = black_box(
        <SharedCache as AsyncCache<RasterTile2D<u8>>>::query_cache(tile_cache, &op, &query).await,
    );
    res.unwrap();

    let read_query_s = start.elapsed().as_millis();

    ReadMeasurement {
        read_query_ms: read_query_s,
    }
}

fn query_rect() -> RasterQueryRectangle {
    RasterQueryRectangle {
        spatial_bounds: SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into()),
        time_interval: TimeInterval::new_instant(DateTime::new_utc(2014, 3, 1, 0, 0, 0)).unwrap(),
        spatial_resolution: SpatialResolution::one(),
    }
}

fn op(idx: usize) -> CanonicOperatorName {
    CanonicOperatorName::new_unchecked(&json!({
        "type": "GdalSource",
        "params": {
            "data": idx
        }
    }))
}

#[derive(Debug, Clone, Copy)]
enum Operation {
    Read,
    Write,
}

async fn cache_access(
    tile_cache: Arc<SharedCache>,
    op_no: usize,
    operation: Operation,
) -> Measurement {
    match operation {
        Operation::Read => Measurement::Read(read_cache(&tile_cache, op_no).await),
        Operation::Write => Measurement::Write(write_cache(&tile_cache, op(op_no)).await),
    }
}

// generate read/writes according to the writes_per_read
// e.g. 0.5 -> read, write, read, write
// e.g. 0.25 -> read, read, write, read, read, write
fn generate_operations(simultaneous_queries: usize, writes_per_read: f64) -> Vec<Operation> {
    let mut operations = vec![];

    let mut counter = 0;

    let (ratio, operation) = if writes_per_read <= 0.5 {
        (writes_per_read, (Operation::Write, Operation::Read))
    } else {
        (1.0 - writes_per_read, (Operation::Read, Operation::Write))
    };

    let threshold = (1.0 / ratio) as usize;

    for _ in 0..simultaneous_queries {
        counter = (counter + 1) % threshold;
        let operation = if counter == 0 {
            operation.0
        } else {
            operation.1
        };
        operations.push(operation);
    }

    operations
}

async fn run_bench(simultaneous_queries: usize, writes_per_read: f64) {
    // cache without limits, because we do not care about eviction here
    let tile_cache = Arc::new(SharedCache::test_default());

    // pre-fill the query cache
    write_cache(&tile_cache, op(0)).await;

    let operations = generate_operations(simultaneous_queries, writes_per_read);

    let futures = operations
        .into_iter()
        .enumerate()
        .map(|(op_no, operation)| tokio::spawn(cache_access(tile_cache.clone(), op_no, operation)));

    let res = join_all(futures).await;
    let res = res.into_iter().collect::<Result<Vec<_>, _>>().unwrap();

    let mut reads = vec![];
    let mut writes = vec![];

    for r in res {
        match r {
            Measurement::Read(r) => reads.push(r),
            Measurement::Write(w) => writes.push(w),
        }
    }

    for read in reads {
        println!(
            "{},{},query_cache,{}",
            simultaneous_queries, writes_per_read, read.read_query_ms
        );
    }

    for write in writes {
        println!(
            "{},{},insert_query,{}",
            simultaneous_queries, writes_per_read, write.insert_query_ms
        );

        println!(
            "{},{},insert_tile,{}",
            simultaneous_queries, writes_per_read, write.insert_tile_ms
        );

        println!(
            "{}, {}, finish_query, {}",
            simultaneous_queries, writes_per_read, write.finish_query_ms
        );
    }
}

/// This benchmark investigates the performance of the query cache under concurrent read/write access.
// #[tokio::main]
#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
    let simultaneous_queries = [10_000, 100_000, 1_000_000];
    let writes_per_reads = [0., 0.25, 0.50, 0.75, 1.]; // 0.0 = only reads, 1.0 = only writes

    let repititions = 1;

    println!("queries,writes_per_read,operation,duration");
    for simultaneous_queries in simultaneous_queries.iter() {
        for writes_per_read in writes_per_reads.iter() {
            for _ in 0..repititions {
                run_bench(*simultaneous_queries, *writes_per_read).await;
            }
        }
    }
}
