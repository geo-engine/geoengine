#![feature(int_log)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use geoengine_datatypes::raster::{
    Grid, GridIdx, GridIdx1D, GridIdx2D, GridShape, UpdateIndexedElements,
    UpdateIndexedElementsParallel,
};

#[allow(clippy::unit_arg)]
fn update_indexed_elements_1d_simple(c: &mut Criterion) {
    let grid_shape = GridShape::from([512 * 512]);
    let grid = Grid::new_filled(grid_shape, 123);

    let lin_idx_map_fn = |idx: usize, element: u32| (element * 321) % (idx + 1) as u32;
    let grid_idx_map_fn = |GridIdx([x]): GridIdx1D, element: u32| (element * 321) % (x + 1) as u32;

    let group_name = "UpdateIndexedElements 1D simple";

    let mut group = c.benchmark_group(group_name);

    group.bench_function("update_indexed_elements usize", |b| {
        b.iter(|| {
            let mut grid = grid.clone();

            black_box(grid.update_indexed_elements(lin_idx_map_fn))
        })
    });

    group.bench_function("update_indexed_elements GridIdx1D", |b| {
        b.iter(|| {
            let mut grid = grid.clone();

            black_box(grid.update_indexed_elements(grid_idx_map_fn))
        })
    });

    group.finish();
}

#[allow(clippy::unit_arg)]
fn update_indexed_elements_1d(c: &mut Criterion) {
    let cpu_cores = std::thread::available_parallelism().unwrap();
    let thread_nums = (0..=cpu_cores.log2()).map(|exp| 2_usize.pow(exp)); //[1, 2, 4, 8, 16, 32];

    let grid_shape = GridShape::from([512 * 512]);
    let grid = Grid::new_filled(grid_shape, 123);

    let lin_idx_map_fn = |idx: usize, element: u32| (element * 321) % (idx + 1) as u32;
    let grid_idx_map_fn = |GridIdx([x]): GridIdx1D, element: u32| (element * 321) % (x + 1) as u32;

    for thread_num in thread_nums {
        let group_name = format!("UpdateIndexedElements 1D parallel ({thread_num})");

        let mut group = c.benchmark_group(&group_name);

        let pool_builder = rayon::ThreadPoolBuilder::new();
        let pool = pool_builder.num_threads(thread_num).build().unwrap();

        rayon::spawn_fifo(|| {
            let _best_number = 40 + 2;
        });

        group.bench_function("update_indexed_elements_parallel usize", |b| {
            pool.install(|| {
                b.iter(|| {
                    let mut grid = grid.clone();

                    black_box(grid.update_indexed_elements_parallel(lin_idx_map_fn));
                })
            })
        });

        group.bench_function("update_indexed_elements_parallel GridIdx1D", |b| {
            pool.install(|| {
                b.iter(|| {
                    let mut grid = grid.clone();

                    black_box(grid.update_indexed_elements_parallel(grid_idx_map_fn));
                })
            })
        });
    }
}

#[allow(clippy::unit_arg)]
fn update_indexed_elements_2d_simple(c: &mut Criterion) {
    let grid_shape = GridShape::from([512, 512]);
    let grid = Grid::new_filled(grid_shape, 123);

    let lin_idx_map_fn = |idx: usize, element: u32| (element * 321) % (idx + 1) as u32;
    let grid_idx_map_fn =
        |GridIdx([y, x]): GridIdx2D, element: u32| (element * 321) % (y * 512 + x + 1) as u32;

    let group_name = "UpdateIndexedElements 2D simple";

    let mut group = c.benchmark_group(group_name);

    group.bench_function("update_indexed_elements usize", |b| {
        b.iter(|| {
            let mut grid = grid.clone();

            black_box(grid.update_indexed_elements(lin_idx_map_fn));
        })
    });

    group.bench_function("update_indexed_elements GridIdx2D", |b| {
        b.iter(|| {
            let mut grid = grid.clone();

            black_box(grid.update_indexed_elements(grid_idx_map_fn));
        })
    });

    group.finish();
}

#[allow(clippy::unit_arg)]
fn update_indexed_elements_2d(c: &mut Criterion) {
    let cpu_cores = std::thread::available_parallelism().unwrap();
    let thread_nums = (0..=cpu_cores.log2()).map(|exp| 2_usize.pow(exp)); //[1, 2, 4, 8, 16, 32];

    let grid_shape = GridShape::from([512, 512]);
    let grid = Grid::new_filled(grid_shape, 123);

    let lin_idx_map_fn = |idx: usize, element: u32| (element * 321) % (idx + 1) as u32;
    let grid_idx_map_fn =
        |GridIdx([y, x]): GridIdx2D, element: u32| (element * 321) % (y * 512 + x + 1) as u32;

    for thread_num in thread_nums {
        let group_name = format!("UpdateIndexedElements 2D parallel ({thread_num})");

        let mut group = c.benchmark_group(&group_name);

        let pool_builder = rayon::ThreadPoolBuilder::new();
        let pool = pool_builder.num_threads(thread_num).build().unwrap();

        rayon::spawn_fifo(|| {
            let _best_number = 40 + 2;
        });

        group.bench_function("update_indexed_elements_parallel usize", |b| {
            pool.install(|| {
                b.iter(|| {
                    let mut grid = grid.clone();

                    black_box(grid.update_indexed_elements_parallel(lin_idx_map_fn));
                })
            })
        });

        group.bench_function("update_indexed_elements_parallel GridIdx2D", |b| {
            pool.install(|| {
                b.iter(|| {
                    let mut grid = grid.clone();

                    black_box(grid.update_indexed_elements_parallel(grid_idx_map_fn))
                })
            })
        });

        group.finish();
    }
}

criterion_group!(
    benches,
    update_indexed_elements_1d_simple,
    update_indexed_elements_1d,
    update_indexed_elements_2d_simple,
    update_indexed_elements_2d,
);
criterion_main!(benches);
