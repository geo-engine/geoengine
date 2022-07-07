use criterion::{black_box, criterion_group, criterion_main, Criterion};
use geoengine_datatypes::raster::{
    Grid1D, GridShape, MapIndexedElements, MapIndexedElementsParallel, Grid, GridIdx2D, GridIdx, MapIndexedElementsParallel2D,
};

fn grid_map_linear_indexed_elements_1d(c: &mut Criterion) {
    let mut group = c.benchmark_group("MapIndexedElements_Grid1D");

    let grid_shape = GridShape::from([64*64]);
    let grid = Grid1D::new_filled(grid_shape, 123);

    group.bench_function("Grid 1D", |b| {
        b.iter(|| {
            let grid = grid.clone();

            black_box({
                let map_fn = |idx: usize, element: u16| { (element * 321) % (idx + 1) as u16};
                let _ = grid.map_indexed_elements(map_fn);
            })
        })
    });

    let thread_nums = [1, 2, 4, 8];

    for thread_num in thread_nums {
        let id = format!("Grid 1D parallel ({thread_num})");

        let pool_builder = rayon::ThreadPoolBuilder::new();
        let pool = pool_builder.num_threads(thread_num).build().unwrap();

        rayon::spawn_fifo(|| {
            let _best_number = 40 + 2;
        });

        group.bench_function(id.as_str(), |b| {
            pool.install(|| {
                b.iter(|| {
                    let grid = grid.clone();

                    black_box({
                        let map_fn = |idx: usize, element: u16| { (element * 321) % (idx + 1) as u16};
                        let _ = grid.map_indexed_elements_parallel(map_fn);
                    })
                })
            })
        });
    }

    group.finish();
}

fn grid_map_linear_indexed_elements_2d_usize(c: &mut Criterion) {
    let mut group = c.benchmark_group("MapIndexedElements_Grid2D_usize");

    let grid_shape = GridShape::from([64,64]);
    let grid = Grid::new_filled(grid_shape, 123);

    group.bench_function("Grid 2D", |b| {
        b.iter(|| {
            let grid = grid.clone();

            black_box({
                let map_fn = |idx: usize, element: u16| {  (element * 321) % (idx + 1) as u16};
                let _ = grid.map_indexed_elements(map_fn);
            })
        })
    });

    let thread_nums = [1, 2, 4, 8];

    for thread_num in thread_nums {
        let id = format!("Grid 2D parallel ({thread_num})");

        let pool_builder = rayon::ThreadPoolBuilder::new();
        let pool = pool_builder.num_threads(thread_num).build().unwrap();

        rayon::spawn_fifo(|| {
            let _best_number = 40 + 2;
        });

        group.bench_function(id.as_str(), |b| {
            pool.install(|| {
                b.iter(|| {
                    let grid = grid.clone();

                    black_box({
                        let map_fn = |idx: usize, element: u16| {  (element * 321) % (idx + 1) as u16};
                        let _ = grid.map_indexed_elements_parallel(map_fn);
                    })
                })
            })
        });
    }

    group.finish();
}

fn grid_map_2d_indexed_elements_2d_grididx(c: &mut Criterion) {
    let mut group = c.benchmark_group("MapIndexedElements_Grid2D_GridIdx2D");

    let grid_shape = GridShape::from([64,64]);
    let grid = Grid::new_filled(grid_shape, 123);

    group.bench_function("Grid 2D", |b| {
        b.iter(|| {
            let grid = grid.clone();

            black_box({
                let map_fn = |GridIdx([y,x]): GridIdx2D, element: u16| {(element * 321 + 1) % (y * 64 + x + 1) as u16};
                let _ = grid.map_indexed_elements(map_fn);
            })
        })
    });

    let thread_nums = [1, 2, 4, 8];

    for thread_num in thread_nums {
        let id = format!("Grid 2D parallel ({thread_num})");

        let pool_builder = rayon::ThreadPoolBuilder::new();
        let pool = pool_builder.num_threads(thread_num).build().unwrap();

        rayon::spawn_fifo(|| {
            let _best_number = 40 + 2;
        });

        group.bench_function(id.as_str(), |b| {
            pool.install(|| {
                b.iter(|| {
                    let grid = grid.clone();

                    black_box({
                        let map_fn = |GridIdx([y,x]): GridIdx2D, element: u16| {(element * 321) % (y * 64 + x + 1) as u16};
                        let _ = grid.map_indexed_elements_parallel(map_fn);
                    })
                })
            })
        });
    }
    group.finish();

}

    fn grid_map_2d_indexed_elements_2d_grididx_selfmade(c: &mut Criterion) {
        let mut group = c.benchmark_group("MapIndexedElements_Grid2D_GridIdx2D");
    
        let grid_shape = GridShape::from([64,64]);
        let grid = Grid::new_filled(grid_shape, 123);
    
        group.bench_function("Grid 2D", |b| {
            b.iter(|| {
                let grid = grid.clone();
    
                black_box({
                    let map_fn = |GridIdx([y,x]): GridIdx2D, element: u16| {(element * 321 + 1) % (y * 64 + x + 1) as u16};
                    let _ = grid.map_indexed_elements(map_fn);
                })
            })
        });
    
        let thread_nums = [1, 2, 4, 8];
    
        for thread_num in thread_nums {
            let id = format!("Grid 2D parallel ({thread_num})");
    
            let pool_builder = rayon::ThreadPoolBuilder::new();
            let pool = pool_builder.num_threads(thread_num).build().unwrap();
    
            rayon::spawn_fifo(|| {
                let _best_number = 40 + 2;
            });
    
            group.bench_function(id.as_str(), |b| {
                pool.install(|| {
                    b.iter(|| {
                        let grid = grid.clone();
    
                        black_box({
                            let map_fn = |GridIdx([y,x]): GridIdx2D, element: u16| {(element * 321) % (y * 64 + x + 1) as u16};
                            let _ = grid.map_indexed_elements_parallel2d(map_fn);
                        })
                    })
                })
            });
        }
    

    group.finish();
}


criterion_group!(benches, grid_map_linear_indexed_elements_1d, grid_map_linear_indexed_elements_2d_usize, grid_map_2d_indexed_elements_2d_grididx, grid_map_2d_indexed_elements_2d_grididx_selfmade);
criterion_main!(benches);
