use std::cell::Cell;
use std::str::FromStr;

use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{BandSelection, RasterQueryRectangle, TimeInstance, TimeInterval},
    raster::{GridBoundingBox2D, GridShape2D, GridSize},
};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rayon::ThreadPoolBuilder;

mod default_impl {
    use super::*;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::{
        engine::{
            MockExecutionContext, RasterOperator, RasterQueryProcessor, WorkflowOperatorPath,
        },
        source::{GdalDatasetParameters, GdalSource, GdalSourceParameters},
        util::gdal::create_ndvi_meta_data,
    };
    use rand::RngExt;

    fn random_grid_bounds(
        output_shape: GridShape2D,
        params: &GdalDatasetParameters,
        rng: &mut impl Rng,
    ) -> GridBoundingBox2D {
        let tile_x = output_shape.axis_size_x();
        let tile_y = output_shape.axis_size_y();

        let max_start_x = params.width - tile_x;
        let max_start_y = params.height - tile_y;
        let start_x = rng.random_range(0..=max_start_x);
        let start_y = rng.random_range(0..=max_start_y);

        GridBoundingBox2D::new_unchecked(
            [start_y as isize, start_x as isize],
            [
                (start_y + tile_y - 1) as isize,
                (start_x + tile_x - 1) as isize,
            ],
        )
    }

    fn make_query_rectangles(
        count: usize,
        output_shape: GridShape2D,
        params: &GdalDatasetParameters,
        time_interval: TimeInterval,
        seed: u64,
    ) -> Vec<RasterQueryRectangle> {
        let mut rng = SmallRng::seed_from_u64(seed);
        (0..count)
            .map(|_| {
                let grid_bounds = random_grid_bounds(output_shape, params, &mut rng);
                RasterQueryRectangle::new(grid_bounds, time_interval, BandSelection::first())
            })
            .collect()
    }

    fn ensure_global_rayon_pool() {
        let _ = ThreadPoolBuilder::new().build_global();
    }

    fn bench_with_shape(
        c: &mut Criterion,
        bench_name: &str,
        output_shape: GridShape2D,
        time_interval: TimeInterval,
    ) {
        ensure_global_rayon_pool();
        let meta = create_ndvi_meta_data();
        let params = meta.params.clone();
        let query_rectangles = make_query_rectangles(512, output_shape, &params, time_interval, 0);
        let query_idx = Cell::new(0);

        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context(TestDefault::test_default());

        let ndvi_id = geoengine_operators::util::gdal::add_ndvi_dataset(&mut exe_ctx);

        let runtime = tokio::runtime::Runtime::new().expect("runtime creation should work");
        let processor = runtime.block_on(async {
            let op = GdalSource {
                params: GdalSourceParameters {
                    data: ndvi_id.clone(),
                    overview_level: None,
                },
            }
            .boxed();

            let initialized = op
                .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
                .await
                .expect("GdalSource should initialize");

            initialized
                .query_processor()
                .expect("query processor should be available")
                .get_u8()
                .expect("ndvi should be u8")
        });

        c.bench_function(bench_name, |b| {
            b.to_async(&runtime).iter(|| {
                let idx = query_idx.get();
                query_idx.set((idx + 1) % query_rectangles.len());
                let query = query_rectangles[idx].clone();
                async {
                    let tiles = processor
                        .raster_query(query, &query_ctx)
                        .await
                        .expect("query should succeed")
                        .collect::<Vec<_>>()
                        .await;
                    std::hint::black_box(tiles);
                }
            });
        });
    }

    pub fn bench(c: &mut Criterion) {
        let meta = create_ndvi_meta_data();

        // One time interval
        let single_ts = TimeInterval::new_unchecked(
            // time step is months for this dataset
            TimeInstance::from_str("2014-01-01T00:00:00.000Z").expect("it shoudl be valid"),
            TimeInstance::from_str("2014-02-01T00:00:00.000Z").expect("it should be valid"),
        );

        // multiple time slices
        let multi_ts = meta.data_time;

        bench_with_shape(
            c,
            "gdal_source_operator_default_small_single_ts",
            [8, 8].into(),
            single_ts,
        );
        bench_with_shape(
            c,
            "gdal_source_operator_default_small_multi_ts",
            [8, 8].into(),
            multi_ts,
        );
        bench_with_shape(
            c,
            "gdal_source_operator_default_large_single_ts",
            [256, 256].into(),
            single_ts,
        );
        bench_with_shape(
            c,
            "gdal_source_operator_default_large_multi_ts",
            [256, 256].into(),
            multi_ts,
        );
    }
}

fn gdal_source_operator_default(c: &mut Criterion) {
    default_impl::bench(c);
}

criterion_group!(benches, gdal_source_operator_default,);
criterion_main!(benches);
