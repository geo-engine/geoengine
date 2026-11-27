//! Raster reprojection of the bench raster (see `common`; NDVI in EPSG:4326) to EPSG:3857: the whole extent, a
//! chain of two reprojections, a downsampled overview, a WMS tile and the cases where sub-queries of a tile share work:
//! several bands of a tile, and several time steps of a single tile, like a time slider.
//!
//! The group `resampling` queries the raster at another resolution, as the server wraps it for a
//! WMS `GetMap` request (`AdaptToGrid`). The resolutions come from the request
//! with non-square pixels of #1436 (0.176° × 0.088° on the 0.1° NDVI): `mixed` is coarser in x and
//! finer in y, `coarser` and `finer` are the square resolutions of its two axes. The `_tile` cases
//! query the one output tile that contains 10°E 50°N, so all three return the same number of
//! pixels and their times compare directly.
//!
//! `cargo bench -p geoengine-operators --bench reprojection`
#![allow(clippy::unwrap_used, reason = "okay in benchmarks")]

mod common;

use common::{Area, Case, Env, downsample, mercator, reproject, stack};
use criterion::{Criterion, criterion_group, criterion_main};
use geoengine_datatypes::{
    primitives::{Coordinate2D, SpatialPartition2D, SpatialResolution},
    spatial_reference::SpatialReference,
};
use geoengine_operators::processing::{WarpDownsampling, WarpUpsampling};
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

/// The resampling methods besides nearest neighbour for the `resolutions` of `resampling` in `area`,
/// named `<resolution>_<area_name>_<method>`. The tile has all of them, the extent only the
/// cheaper ones for coarser and finer outputs.
fn method_cases(
    env: &Env,
    resolutions: &[(&str, SpatialResolution)],
    area: impl Fn() -> Area,
    area_name: &str,
) -> Vec<(String, Case)> {
    let methods = [
        (
            "mixed",
            "average",
            WarpDownsampling::Average,
            WarpUpsampling::Nearest,
        ),
        (
            "coarser",
            "average",
            WarpDownsampling::Average,
            WarpUpsampling::Nearest,
        ),
        (
            "coarser",
            "mode",
            WarpDownsampling::Mode,
            WarpUpsampling::Nearest,
        ),
        (
            "finer",
            "bilinear",
            WarpDownsampling::Nearest,
            WarpUpsampling::Bilinear,
        ),
    ];
    let on_extent = [("coarser", "average"), ("finer", "bilinear")];
    methods
        .into_iter()
        .filter(|(name, method, _, _)| area_name == "tile" || on_extent.contains(&(*name, *method)))
        .map(|(name, method, downsampling, upsampling)| {
            let (_, resolution) = resolutions
                .iter()
                .find(|(resolution_name, _)| *resolution_name == name)
                .unwrap();
            (
                format!("{name}_{area_name}_{method}"),
                env.case(
                    env.resample_with_methods(
                        env.raster(),
                        SpatialReference::epsg_4326(),
                        *resolution,
                        (downsampling, upsampling),
                    ),
                    area(),
                    env.one_step(),
                ),
            )
        })
        .collect()
}

/// The `wms_tile` of `reprojection` with pixels that are coarser in x and finer in y than the
/// ~11 km of the reprojected raster, and with pixels that are finer in both axes: reprojection and
/// resampling in one wrapper, with nearest neighbour and with the other methods.
fn wms_3857_cases(env: &Env) -> Vec<(String, Case)> {
    let tile = || {
        Area::Part(SpatialPartition2D::new_unchecked(
            Coordinate2D::new(0., 7_514_065.6),
            Coordinate2D::new(2_504_688.5, 5_009_377.1),
        ))
    };
    let cases = [
        (
            "mixed_wms_3857",
            (19_568., 4_892.),
            WarpDownsampling::Nearest,
            WarpUpsampling::Nearest,
        ),
        (
            "mixed_wms_3857_average",
            (19_568., 4_892.),
            WarpDownsampling::Average,
            WarpUpsampling::Nearest,
        ),
        (
            "finer_wms_3857",
            (4_892., 4_892.),
            WarpDownsampling::Nearest,
            WarpUpsampling::Nearest,
        ),
        (
            "finer_wms_3857_bilinear",
            (4_892., 4_892.),
            WarpDownsampling::Nearest,
            WarpUpsampling::Bilinear,
        ),
    ];
    cases
        .into_iter()
        .map(|(name, (x, y), downsampling, upsampling)| {
            (
                name.to_string(),
                env.case(
                    env.resample_with_methods(
                        env.raster(),
                        SpatialReference::web_mercator(),
                        SpatialResolution::new_unchecked(x, y),
                        (downsampling, upsampling),
                    ),
                    tile(),
                    env.one_step(),
                ),
            )
        })
        .collect()
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
    cases.extend(method_cases(&env, &resolutions, tile_at_10e_50n, "tile"));
    cases.extend(wms_3857_cases(&env));
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

    if !common::ci() {
        cases.extend(method_cases(&env, &resolutions, || Area::Extent, "extent"));
    }

    let mut group = common::group(c, "resampling");
    for (name, case) in &cases {
        env.bench(&mut group, name, case);
    }
    group.finish();
}

criterion_group!(name = benches; config = common::criterion(); targets = reprojection, resampling);
criterion_main!(benches);
