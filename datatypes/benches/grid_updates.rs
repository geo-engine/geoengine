use criterion::{black_box, criterion_group, criterion_main, Criterion};
use geoengine_datatypes::raster::{
    Grid, GridIdx, GridIdx1D, GridIdx2D, GridShape, UpdateIndexedElements,
    UpdateIndexedElementsParallel, UpdateIndexedElementsParallel2dOptimized,
};

fn update_indexed_elements_1d(c: &mut Criterion) {
    let thread_nums = [1, 2, 4, 8, 16, 32];

    let grid_shape = GridShape::from([512 * 512]);
    let grid = Grid::new_filled(grid_shape, 123);

    let lin_idx_map_fn = |idx: usize, element: u32| (element * 321) % (idx + 1) as u32;
    let grid_idx_map_fn = |GridIdx([x]): GridIdx1D, element: u32| (element * 321) % (x + 1) as u32;

    for thread_num in thread_nums {
        let group_name = format!("UpdateIndexedElements 1D {thread_num} threads");

        let mut group = c.benchmark_group(&group_name);

        let pool_builder = rayon::ThreadPoolBuilder::new();
        let pool = pool_builder.num_threads(thread_num).build().unwrap();

        rayon::spawn_fifo(|| {
            let _best_number = 40 + 2;
        });

        group.bench_function("update_indexed_elements usize", |b| {
            b.iter(|| {
                let mut grid = grid.clone();

                black_box({
                    let map_fn = lin_idx_map_fn;
                    grid.update_indexed_elements(map_fn);
                })
            })
        });

        group.bench_function("update_indexed_elements GridIdx1D", |b| {
            b.iter(|| {
                let mut grid = grid.clone();

                black_box({
                    let map_fn = grid_idx_map_fn;
                    let _ = grid.update_indexed_elements(map_fn);
                })
            })
        });

        group.bench_function("update_indexed_elements_parallel usize", |b| {
            pool.install(|| {
                b.iter(|| {
                    let mut grid = grid.clone();

                    black_box({
                        let map_fn = lin_idx_map_fn;
                        grid.update_indexed_elements_parallel(map_fn);
                    })
                })
            })
        });

        group.bench_function("update_indexed_elements_parallel GridIdx1D", |b| {
            pool.install(|| {
                b.iter(|| {
                    let mut grid = grid.clone();

                    black_box({
                        let map_fn = grid_idx_map_fn;
                        let _ = grid.update_indexed_elements_parallel(map_fn);
                    })
                })
            })
        });
    }
}

fn update_indexed_elements_2d(c: &mut Criterion) {
    let thread_nums = [1, 2, 4, 8, 16, 32];

    let grid_shape = GridShape::from([512, 512]);
    let grid = Grid::new_filled(grid_shape, 123);

    let lin_idx_map_fn = |idx: usize, element: u32| (element * 321) % (idx + 1) as u32;
    let grid_idx_map_fn =
        |GridIdx([y, x]): GridIdx2D, element: u32| (element * 321) % (y * 512 + x + 1) as u32;

    for thread_num in thread_nums {
        let group_name = format!("UpdateIndexedElements 2D {thread_num} threads");

        let mut group = c.benchmark_group(&group_name);

        let pool_builder = rayon::ThreadPoolBuilder::new();
        let pool = pool_builder.num_threads(thread_num).build().unwrap();

        rayon::spawn_fifo(|| {
            let _best_number = 40 + 2;
        });

        group.bench_function("update_indexed_elements usize", |b| {
            b.iter(|| {
                let mut grid = grid.clone();

                black_box({
                    let map_fn = lin_idx_map_fn;
                    grid.update_indexed_elements(map_fn);
                })
            })
        });

        group.bench_function("update_indexed_elements GridIdx2D", |b| {
            b.iter(|| {
                let mut grid = grid.clone();

                black_box({
                    let map_fn = grid_idx_map_fn;
                    grid.update_indexed_elements(map_fn);
                })
            })
        });

        group.bench_function("update_indexed_elements_parallel usize", |b| {
            pool.install(|| {
                b.iter(|| {
                    let mut grid = grid.clone();

                    black_box({
                        let map_fn = lin_idx_map_fn;
                        grid.update_indexed_elements_parallel(map_fn);
                    })
                })
            })
        });

        group.bench_function("update_indexed_elements_parallel GridIdx2D", |b| {
            pool.install(|| {
                b.iter(|| {
                    let mut grid = grid.clone();

                    black_box({
                        let map_fn = grid_idx_map_fn;
                        grid.update_indexed_elements_parallel(map_fn);
                    })
                })
            })
        });

        group.bench_function(
            "update_indexed_elements_parallel GridIdx2D optimized-y",
            |b| {
                pool.install(|| {
                    b.iter(|| {
                        let mut grid = grid.clone();

                        black_box({
                            let map_fn = grid_idx_map_fn;
                            grid.update_indexed_elements_parallel_2d_optimized(map_fn);
                        })
                    })
                })
            },
        );
    }
}

criterion_group!(
    benches,
    update_indexed_elements_1d,
    update_indexed_elements_2d,
);
criterion_main!(benches);
