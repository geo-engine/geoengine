//! WMS `GetMap` load on a server pinned to N cores; see `tile_load` for the setup and variables.

mod tile_load;

use criterion::{Criterion, criterion_group, criterion_main};

fn wms_load(c: &mut Criterion) {
    tile_load::run(c, "wms_load", false);
}

criterion_group!(benches, wms_load);
criterion_main!(benches);
