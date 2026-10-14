use criterion::{criterion_group, criterion_main};

#[path = "stac_grid/mod.rs"]
mod stac_grid;

use stac_grid::stac_grid_benchmark;

criterion_group!(benches, stac_grid_benchmark);
criterion_main!(benches);
