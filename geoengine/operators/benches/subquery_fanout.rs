#![allow(clippy::unwrap_used, unused_mut, reason = "okay in benchmarks")]

use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use futures::{
    FutureExt, StreamExt,
    future::{BoxFuture, Ready, ready},
};
use geoengine_datatypes::{
    primitives::{BandSelection, Coordinate2D, RasterQueryRectangle, TimeInterval, TimeStep},
    raster::{
        BoundedGrid, GeoTransform, Grid, GridBoundingBox2D, GridShape, GridSize, RasterTile2D,
        SpatialGridDefinition, TilingSpecification,
    },
    util::test::TestDefault,
};
use geoengine_operators::{
    adapters::{FoldTileAccu, FoldTileAccuMut, RasterSubQueryAdapter, SubQueryTileAggregator},
    engine::{
        MockExecutionContext, RasterBandDescriptors, RasterOperator, RasterResultDescriptor,
        SpatialGridDescriptor, TimeDescriptor, WorkflowOperatorPath,
    },
    mock::{MockRasterSource, MockRasterSourceParams},
};
use rayon::ThreadPool;
use std::hint::black_box;
use std::sync::Arc;

#[derive(Clone)]
struct HeavySubQuery;

#[derive(Clone)]
struct HeavyAccu {
    tile: geoengine_datatypes::raster::TileInformation,
    data: Vec<u8>,
    pool: Arc<ThreadPool>,
}

#[async_trait]
impl FoldTileAccu for HeavyAccu {
    type RasterType = u8;

    async fn into_tile(self) -> geoengine_operators::util::Result<RasterTile2D<u8>> {
        Ok(RasterTile2D::new_with_tile_info(
            TimeInterval::new_unchecked(0, 5),
            self.tile,
            0,
            Grid::new(self.tile.tile_size_in_pixels, self.data)
                .unwrap()
                .into(),
            Default::default(),
        ))
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

impl FoldTileAccuMut for HeavyAccu {
    fn set_time(&mut self, _: TimeInterval) {}
    fn set_cache_hint(&mut self, _: geoengine_datatypes::primitives::CacheHint) {}
}

fn heavy_fold(
    accu: HeavyAccu,
    _: RasterTile2D<u8>,
) -> Ready<geoengine_operators::util::Result<HeavyAccu>> {
    ready(Ok(accu))
}

impl<'a> SubQueryTileAggregator<'a, u8> for HeavySubQuery {
    type FoldFuture = Ready<geoengine_operators::util::Result<HeavyAccu>>;
    type FoldMethod = fn(HeavyAccu, RasterTile2D<u8>) -> Self::FoldFuture;
    type TileAccu = HeavyAccu;
    type TileAccuFuture = BoxFuture<'a, geoengine_operators::util::Result<HeavyAccu>>;

    fn new_fold_accu(
        &self,
        tile_info: geoengine_datatypes::raster::TileInformation,
        _: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        let pool = pool.clone();
        async move {
            let tile_size = tile_info.tile_size_in_pixels;
            let data = geoengine_operators::util::spawn_blocking_with_thread_pool(
                pool.clone(),
                move || {
                    let mut data = vec![0_u8; tile_size.number_of_elements()];
                    for (index, value) in data.iter_mut().enumerate() {
                        let mut x = index as u32;
                        for _ in 0..32 {
                            x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                        }
                        *value = x as u8;
                    }
                    data
                },
            )
            .await
            .unwrap();
            Ok(HeavyAccu {
                tile: tile_info,
                data,
                pool,
            })
        }
        .boxed()
    }

    fn fold_method(&self) -> Self::FoldMethod {
        heavy_fold
    }
}

fn setup(
    runtime: &tokio::runtime::Runtime,
) -> (
    geoengine_operators::engine::BoxRasterQueryProcessor<u8>,
    geoengine_operators::engine::MockQueryContext,
    RasterQueryRectangle,
    geoengine_datatypes::raster::TilingStrategy,
) {
    let tiling_specification = TilingSpecification::new([512, 512].into());
    let result_descriptor = RasterResultDescriptor {
        data_type: geoengine_datatypes::raster::RasterDataType::U8,
        spatial_reference: geoengine_datatypes::spatial_reference::SpatialReference::epsg_4326()
            .into(),
        time: TimeDescriptor::new_regular_with_epoch(None, TimeStep::millis(5).unwrap()),
        spatial_grid: SpatialGridDescriptor::new_source(SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(0., 8192.), 1., -1.),
            GridShape::new_2d(8192, 8192).bounding_box(),
        )),
        bands: RasterBandDescriptors::new_single_band(),
    };

    let data = vec![RasterTile2D {
        time: TimeInterval::new_unchecked(0, 5),
        tile_position: [0, 0].into(),
        band: 0,
        global_geo_transform: TestDefault::test_default(),
        grid_array: Grid::new([512, 512].into(), vec![1_u8; 262_144])
            .unwrap()
            .into(),
        properties: Default::default(),
        cache_hint: Default::default(),
    }];

    let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);
    let source = MockRasterSource {
        params: MockRasterSourceParams {
            data,
            result_descriptor: result_descriptor.clone(),
        },
    }
    .boxed();
    let initialized = runtime
        .block_on(source.initialize(WorkflowOperatorPath::initialize_root(), &execution_context))
        .unwrap();
    let processor = initialized.query_processor().unwrap().get_u8().unwrap();
    let query_context = execution_context.mock_query_context_test_default();
    let tiling_grid = result_descriptor.tiling_grid_definition(tiling_specification);
    let tiling_strategy = tiling_grid.generate_data_tiling_strategy();
    let query = RasterQueryRectangle::new(
        GridBoundingBox2D::new([0, 0], [8191, 8191]).unwrap(),
        TimeInterval::new_unchecked(0, 5),
        BandSelection::first(),
    );

    (processor, query_context, query, tiling_strategy)
}

async fn run(
    processor: &geoengine_operators::engine::BoxRasterQueryProcessor<u8>,
    query_context: &geoengine_operators::engine::MockQueryContext,
    query: RasterQueryRectangle,
    tiling_strategy: geoengine_datatypes::raster::TilingStrategy,
) {
    let time_stream = processor
        .time_query(query.time_interval(), query_context)
        .await
        .unwrap();
    let adapter = RasterSubQueryAdapter::new(
        processor,
        query,
        tiling_strategy,
        query_context,
        HeavySubQuery,
        time_stream,
    );
    let result = adapter.map(Result::unwrap).collect::<Vec<_>>().await;
    assert_eq!(result.len(), 256);
    black_box(result);
}

fn subquery_fanout_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    let (processor, mut query_context, query, tiling_strategy) = setup(&runtime);

    #[cfg(not(feature = "new-subquery-adapter"))]
    {
        c.bench_function("subquery_old", |b| {
            b.to_async(&runtime)
                .iter(|| run(&processor, &query_context, query.clone(), tiling_strategy));
        });
    }

    #[cfg(feature = "new-subquery-adapter")]
    for factor in 1..=8 {
        query_context.tile_scheduler = geoengine_operators::engine::TileScheduler::fixed(factor);
        c.bench_function(&format!("subquery_fanout_{factor}"), |b| {
            b.to_async(&runtime)
                .iter(|| run(&processor, &query_context, query.clone(), tiling_strategy));
        });
    }
}

criterion_group!(subquery, subquery_fanout_benchmark);
criterion_main!(subquery);
