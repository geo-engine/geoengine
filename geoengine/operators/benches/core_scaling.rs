#![allow(
    clippy::unwrap_used,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

//! Tiles/s of map path operators on the bench raster (see `common`; NDVI by default) over the Rayon pool size and the number
//! of concurrent queries, with the setup of the topic benches (`common`): the server settings,
//! operators wrapped as in the server, one task per query. The raster cache is not used.
//!
//! `cargo bench -p geoengine-operators --bench core_scaling`

mod common;

use common::{Area, Env, downsample, expression, mercator};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

const RAYON_THREADS: [usize; 7] = [1, 2, 4, 8, 16, 32, 64];
const CONCURRENT_QUERIES: [usize; 2] = [1, 8];

fn core_scaling(c: &mut Criterion) {
    let env = Env::new();
    // all tiles of the six months of the dataset
    let cases = [
        (
            "ndvi_expression",
            // NDVI-like arithmetic on a single band
            env.case(
                expression(env.raster(), "(A - 100) / (A + 100)", false),
                Area::Extent,
                env.six_steps(),
            ),
        ),
        (
            "downsample_3x3",
            env.case(downsample(env.raster(), 3.), Area::Extent, env.six_steps()),
        ),
        (
            "reproject_3857",
            env.case(mercator(env.raster()), Area::Extent, env.six_steps()),
        ),
    ];

    for (name, case) in &cases {
        case.print(name);
        let mut group = c.benchmark_group(format!("core_scaling/{name}"));
        group.sample_size(10);

        for threads in RAYON_THREADS {
            let query_ctx = env.query_ctx_with_rayon_threads(threads);
            for concurrent in CONCURRENT_QUERIES {
                group.throughput(Throughput::Elements((case.tiles * concurrent) as u64));
                group.bench_function(
                    BenchmarkId::new(format!("queries_{concurrent}"), format!("rayon_{threads}")),
                    |b| {
                        b.to_async(&env.runtime).iter(|| {
                            common::spawn_queries(
                                case,
                                vec![case.query.clone(); concurrent],
                                query_ctx.clone(),
                            )
                        });
                    },
                );
            }
        }
        group.finish();
    }
}

criterion_group!(benches, core_scaling);
criterion_main!(benches);
