//! Raster reprojection of the bench raster (see `common`; NDVI in EPSG:4326) to EPSG:3857: the whole extent, a
//! chain of two reprojections, a WMS tile and the cases where sub-queries of a tile share work:
//! several bands of a tile, and several time steps of a single tile, like a time slider.
//!
//! `cargo bench -p geoengine-operators --bench reprojection`
#![allow(clippy::unwrap_used, reason = "okay in benchmarks")]

mod common;

use common::{Area, Env, stack};
use criterion::{Criterion, criterion_group, criterion_main};
use geoengine_datatypes::{
    primitives::{Coordinate2D, SpatialPartition2D},
    spatial_reference::SpatialReference,
};
use geoengine_operators::{
    engine::{RasterOperator, SingleRasterOrVectorSource, TypedRasterOperator},
    processing::{DeriveOutRasterSpecsSource, Reprojection, ReprojectionParams},
};

fn reproject(source: TypedRasterOperator, target: SpatialReference) -> TypedRasterOperator {
    Reprojection {
        params: ReprojectionParams {
            target_spatial_reference: target,
            derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
        },
        sources: SingleRasterOrVectorSource {
            source: source.into(),
        },
    }
    .typed()
}

fn mercator(source: TypedRasterOperator) -> TypedRasterOperator {
    reproject(source, SpatialReference::web_mercator())
}

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

criterion_group!(name = benches; config = common::criterion(); targets = reprojection);
criterion_main!(benches);
