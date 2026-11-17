//! OGC API tiles load, the same tiles as `wms_load` on a server pinned to N cores; see `tile_load` for the setup and variables.

mod tile_load;

use criterion::{Criterion, criterion_group, criterion_main};

fn ogc_tiles_load(c: &mut Criterion) {
    tile_load::run(c, "ogc_tiles_load", true);
}

criterion_group!(benches, ogc_tiles_load);
criterion_main!(benches);
