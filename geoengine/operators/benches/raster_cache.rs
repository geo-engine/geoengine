//! Benchmarks queries through the new raster cache operator: cold, with the cache cleared before
//! every run, and warm, with every tile cached. It is not part of the CI benchmarks.
//!
//! The source is the local NDVI test data, so the GDAL workers keep the file open: cold runs
//! measure the cache operator and its source queries, not remote reads.
//!
//! Run with `cargo bench -p geoengine-operators --bench raster_cache`.

#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

use futures::StreamExt;
use geoengine_datatypes::util::test::TestDefault;
use geoengine_datatypes::{
    primitives::{BandSelection, DateTime, RasterQueryRectangle, TimeInterval},
    raster::{GridBoundingBox2D, TilingSpecification},
};
use geoengine_operators::{
    cache::new_raster_cache::{NewRasterCacheEnum, RasterCacheOperator},
    engine::{
        ChunkByteSize, InitializedRasterOperator, MockExecutionContext,
        ProcessingGraphOperatorPath, QueryProcessor, RasterOperator,
    },
    source::{GdalSource, GdalSourceParameters},
    util::gdal::add_ndvi_dataset,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

const RUNS: usize = 20;
const CACHE_BYTES: usize = 1 << 30;

fn main() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        for tile_size in [256, 512] {
            let mut exec_ctx = MockExecutionContext::new_with_tiling_spec(
                TilingSpecification::new([tile_size, tile_size].into()),
            );
            let ndvi = add_ndvi_dataset(&mut exec_ctx);
            let source = GdalSource {
                params: GdalSourceParameters::new(ndvi),
            }
            .typed()
            .initialize(ProcessingGraphOperatorPath::initialize_root(), &exec_ctx)
            .await
            .unwrap();
            let cached = RasterCacheOperator::wrap_operator(source);
            let processor = cached.query_processor().unwrap().get_u8().unwrap();

            let cache = Arc::new(NewRasterCacheEnum::new_lru(CACHE_BYTES));
            let mut query_ctx = exec_ctx.mock_query_context(ChunkByteSize::test_default());
            query_ctx.new_raster_cache = Some(cache.clone());

            let query = RasterQueryRectangle::new(
                GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
                TimeInterval::new_instant(DateTime::new_utc(2014, 1, 1, 0, 0, 0)).unwrap(),
                BandSelection::first(),
            );

            for (case, cold) in [("cold", true), ("warm", false)] {
                let mut times = Vec::with_capacity(RUNS);
                let mut tiles = 0;
                for _ in 0..RUNS {
                    if cold {
                        cache.clear();
                    }
                    let start = Instant::now();
                    let mut stream = processor.query(query.clone(), &query_ctx).await.unwrap();
                    tiles = 0;
                    while let Some(tile) = stream.next().await {
                        tile.unwrap();
                        tiles += 1;
                    }
                    times.push(start.elapsed());
                }
                times.sort();
                println!(
                    "ndvi/tiles_{tile_size}/{case}: {tiles} tiles, min {:.1} ms, median {:.1} ms ({RUNS} runs)",
                    ms(times[0]),
                    ms(times[RUNS / 2]),
                );
            }
        }
    });
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.
}
