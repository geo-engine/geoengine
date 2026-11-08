//! The raster expression: the per-pixel work, several bands, `map_no_data` on empty tiles and
//! initializing an expression that was compiled before, like every request of a processing graph.
//!
//! The first compilation of each expression happens before the measurement.
//!
//! `cargo bench -p geoengine-operators --bench expression`
#![allow(clippy::unwrap_used, reason = "okay in benchmarks")]

mod common;

use common::{Area, Env, count_tiles, one_month, stack};
use criterion::{Criterion, criterion_group, criterion_main};
use geoengine_datatypes::raster::{GridBoundingBox2D, RasterDataType};
use geoengine_operators::{
    engine::{
        ProcessingGraphOperatorPath, RasterOperator, SingleRasterSource, TypedRasterOperator,
    },
    processing::{Expression, ExpressionParams},
};

fn expression(
    raster: TypedRasterOperator,
    expression: &str,
    map_no_data: bool,
) -> TypedRasterOperator {
    Expression {
        params: ExpressionParams {
            expression: expression.to_string(),
            output_type: RasterDataType::F32,
            output_band: None,
            map_no_data,
        },
        sources: SingleRasterSource { raster },
    }
    .typed()
}

fn expression_bench(c: &mut Criterion) {
    let env = Env::new();
    let scale = || expression(env.ndvi(), "A / 255", false);
    let extent = env.case(scale(), Area::Extent, one_month());
    let bands2 = env.case(
        expression(
            stack(vec![env.ndvi(), env.ndvi()]),
            "(A - B) / (A + B + 1)",
            false,
        ),
        Area::Extent,
        one_month(),
    );
    // 8 of 9 tiles are empty, but `map_no_data` must compute them
    let map_no_data = env.case(
        expression(env.ndvi(), "if A IS NODATA { 0 } else { A }", true),
        Area::ExtentTimes(3),
        one_month(),
    );
    let first_tile = extent.query.select_spatial_bounds(
        GridBoundingBox2D::new_min_max(
            extent.query.spatial_bounds().y_min(),
            extent.query.spatial_bounds().y_min() + 511,
            extent.query.spatial_bounds().x_min(),
            extent.query.spatial_bounds().x_min() + 511,
        )
        .unwrap(),
    );

    let mut group = common::group(c, "expression");
    for (name, case) in [
        ("extent", &extent),
        ("bands2_extent", &bands2),
        ("map_no_data_x3_empty", &map_no_data),
    ] {
        env.bench(&mut group, name, case);
    }

    group.bench_function("initialize_and_tile", |b| {
        b.to_async(&env.runtime).iter(|| async {
            let processor = scale()
                .initialize(ProcessingGraphOperatorPath::initialize_root(), &env.exe_ctx)
                .await
                .unwrap()
                .query_processor()
                .unwrap();
            count_tiles(&processor, first_tile.clone(), &env.query_ctx).await
        });
    });
    group.finish();
}

criterion_group!(name = benches; config = common::criterion(); targets = expression_bench);
criterion_main!(benches);
