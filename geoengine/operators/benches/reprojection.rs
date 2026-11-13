//! Raster reprojection of the bench raster (see `common`; NDVI in EPSG:4326) to EPSG:3857: the whole extent, a
//! chain of two reprojections, a downsampled overview, a WMS tile and the cases where sub-queries of a tile share work:
//! several bands of a tile, and several time steps of a single tile, like a time slider.
//!
//! The group `resampling` queries the raster at another resolution, as the server wraps it for a
//! WMS `GetMap` request (`WrapWithProjectionAndResample`). The resolutions come from the request
//! with non-square pixels of #1436 (0.176° × 0.088° on the 0.1° NDVI): `mixed` is coarser in x and
//! finer in y, `coarser` and `finer` are the square resolutions of its two axes. The `_tile` cases
//! query the one output tile that contains 10°E 50°N, so all three return the same number of
//! pixels and their times compare directly.
//!
//! `cargo bench -p geoengine-operators --bench reprojection`
#![allow(clippy::unwrap_used, reason = "okay in benchmarks")]

mod common;

use common::{Area, Env, downsample, mercator, reproject, stack};
use criterion::{Criterion, criterion_group, criterion_main};
use geoengine_datatypes::{
    primitives::{Coordinate2D, SpatialPartition2D, SpatialResolution},
    spatial_reference::SpatialReference,
};
fn reprojection(c: &mut Criterion) {
    let env = Env::new();
    let mut cases = vec![
        // a WMS GetMap tile at zoom 4 over Europe; the EPSG:3857 coordinates assume global data
        (
            "wms_tile",
            env.case(
                mercator(env.raster()),
                Area::Part(SpatialPartition2D::new_unchecked(
                    Coordinate2D::new(0., 7_514_065.6),
                    Coordinate2D::new(2_504_688.5, 5_009_377.1),
                )),
                env.one_step(),
            ),
        ),
        // a time slider on the single output tile that contains 10°E 50°N: a 2 m square, so the
        // query selects exactly that tile
        (
            "tile_time6",
            env.case(
                mercator(env.raster()),
                Area::Part(SpatialPartition2D::new_unchecked(
                    Coordinate2D::new(1_113_194., 6_446_277.),
                    Coordinate2D::new(1_113_196., 6_446_275.),
                )),
                env.six_steps(),
            ),
        ),
    ];
    // Local only (`just backend bench`), skipped in CI (`GEOENGINE_BENCH_CI`): the whole-extent
    // cases take up to a second per iteration, beyond the ~60 ms budget of a CI case. The tile
    // cases above cover the reprojection in CI.
    if !common::ci() {
        cases.extend([
            (
                "extent",
                env.case(mercator(env.raster()), Area::Extent, env.one_step()),
            ),
            // the inner reprojection runs once per sub-query of the outer one
            (
                "chained",
                env.case(
                    reproject(mercator(env.raster()), SpatialReference::epsg_4326()),
                    Area::Extent,
                    env.one_step(),
                ),
            ),
            // a zoomed-out map of the whole world: downsampling nested over the reprojection
            (
                "overview",
                env.case(
                    downsample(mercator(env.raster()), 4.),
                    Area::Extent,
                    env.one_step(),
                ),
            ),
            // sub-queries of one tile that differ only by band
            (
                "bands3",
                env.case(
                    mercator(stack(vec![env.raster(), env.raster(), env.raster()])),
                    Area::Extent,
                    env.one_step(),
                ),
            ),
        ]);
    }

    let mut group = common::group(c, "reprojection");
    for (name, case) in &cases {
        env.bench(&mut group, name, case);
    }
    group.finish();
}

/// A tiny area around 10°E 50°N, so the query selects exactly the output tile that contains it.
fn tile_at_10e_50n() -> Area {
    Area::Part(SpatialPartition2D::new_unchecked(
        Coordinate2D::new(10., 50.000_001),
        Coordinate2D::new(10.000_001, 50.),
    ))
}

fn resampling(c: &mut Criterion) {
    let env = Env::new();
    let wgs84 = SpatialReference::epsg_4326();
    let resolutions = [
        ("mixed", SpatialResolution::new_unchecked(0.176, 0.088)),
        ("coarser", SpatialResolution::new_unchecked(0.176, 0.176)),
        ("finer", SpatialResolution::new_unchecked(0.088, 0.088)),
    ];
    let mut cases = Vec::new();
    for (name, resolution) in resolutions {
        cases.push((
            format!("{name}_tile"),
            env.case(
                env.resample(env.raster(), wgs84, resolution),
                tile_at_10e_50n(),
                env.one_step(),
            ),
        ));
    }
    // the `wms_tile` of `reprojection` with pixels that are coarser in x and finer in y than the
    // ~11 km of the reprojected raster: reprojection and resampling in one wrapper
    cases.push((
        "mixed_wms_3857".to_string(),
        env.case(
            env.resample(
                env.raster(),
                SpatialReference::web_mercator(),
                SpatialResolution::new_unchecked(19_568., 4_892.),
            ),
            Area::Part(SpatialPartition2D::new_unchecked(
                Coordinate2D::new(0., 7_514_065.6),
                Coordinate2D::new(2_504_688.5, 5_009_377.1),
            )),
            env.one_step(),
        ),
    ));
    // local only, see `reprojection`
    if !common::ci() {
        for (name, resolution) in resolutions {
            cases.push((
                format!("{name}_extent"),
                env.case(
                    env.resample(env.raster(), wgs84, resolution),
                    Area::Extent,
                    env.one_step(),
                ),
            ));
        }
    }

    let mut group = common::group(c, "resampling");
    for (name, case) in &cases {
        env.bench(&mut group, name, case);
    }
    group.finish();
}

criterion_group!(name = benches; config = common::criterion(); targets = reprojection, resampling);
criterion_main!(benches);
