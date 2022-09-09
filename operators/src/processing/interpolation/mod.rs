use std::marker::PhantomData;
use std::sync::Arc;

use crate::adapters::{
    FoldTileAccu, FoldTileAccuMut, RasterSubQueryAdapter, SubQueryTileAggregator,
};
use crate::engine::{
    ExecutionContext, InitializedRasterOperator, Operator, QueryContext, QueryProcessor,
    RasterOperator, RasterQueryProcessor, RasterResultDescriptor, SingleRasterSource,
    TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{Future, FutureExt, TryFuture, TryFutureExt};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, Coordinate2D, RasterQueryRectangle, SpatialPartition2D,
    SpatialPartitioned, SpatialResolution, TimeInstance, TimeInterval,
};
use geoengine_datatypes::raster::{
    Bilinear, Blit, EmptyGrid2D, GeoTransform, GridOrEmpty, GridSize, InterpolationAlgorithm,
    NearestNeighbor, Pixel, RasterTile2D, TileInformation, TilingSpecification,
};
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use snafu::{ensure, Snafu};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InterpolationParams {
    pub interpolation: InterpolationMethod,
    pub input_resolution: InputResolution,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum InputResolution {
    Value(SpatialResolution),
    Native,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub enum InterpolationMethod {
    NearestNeighbor,
    BiLinear,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum InterpolationError {
    #[snafu(display(
        "The input resolution was defined as `native` but the source resolution is unknown.",
    ))]
    UnknownInputResolution,
}

pub type Interpolation = Operator<InterpolationParams, SingleRasterSource>;

#[typetag::serde]
#[async_trait]
impl RasterOperator for Interpolation {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let raster_source = self.sources.raster.initialize(context).await?;
        let in_descriptor = raster_source.result_descriptor();

        ensure!(
            matches!(self.params.input_resolution, InputResolution::Value(_))
                || in_descriptor.resolution.is_some(),
            error::UnknownInputResolution
        );

        let input_resolution = if let InputResolution::Value(res) = self.params.input_resolution {
            res
        } else {
            in_descriptor.resolution.expect("checked in ensure")
        };

        let out_descriptor = RasterResultDescriptor {
            spatial_reference: in_descriptor.spatial_reference,
            data_type: in_descriptor.data_type,
            measurement: in_descriptor.measurement.clone(),
            bbox: in_descriptor.bbox,
            time: in_descriptor.time,
            resolution: None, // after interpolation the resolution is uncapped
        };

        let initialized_operator = InitializedInterpolation {
            result_descriptor: out_descriptor,
            raster_source,
            interpolation_method: self.params.interpolation,
            input_resolution,
            tiling_specification: context.tiling_specification(),
        };

        Ok(initialized_operator.boxed())
    }
}

pub struct InitializedInterpolation {
    result_descriptor: RasterResultDescriptor,
    raster_source: Box<dyn InitializedRasterOperator>,
    interpolation_method: InterpolationMethod,
    input_resolution: SpatialResolution,
    tiling_specification: TilingSpecification,
}

impl InitializedRasterOperator for InitializedInterpolation {
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_processor = self.raster_source.query_processor()?;

        let res = call_on_generic_raster_processor!(
            source_processor, p => match self.interpolation_method  {
                InterpolationMethod::NearestNeighbor => InterploationProcessor::<_,_, NearestNeighbor>::new(
                        p,
                        self.input_resolution,
                        self.tiling_specification,
                    ).boxed()
                    .into(),
                InterpolationMethod::BiLinear =>InterploationProcessor::<_,_, Bilinear>::new(
                        p,
                        self.input_resolution,
                        self.tiling_specification,
                    ).boxed()
                    .into(),
            }
        );

        Ok(res)
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

pub struct InterploationProcessor<Q, P, I>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
    I: InterpolationAlgorithm<P>,
{
    source: Q,
    input_resolution: SpatialResolution,
    tiling_specification: TilingSpecification,
    interpolation: PhantomData<I>,
}

impl<Q, P, I> InterploationProcessor<Q, P, I>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
    I: InterpolationAlgorithm<P>,
{
    pub fn new(
        source: Q,
        input_resolution: SpatialResolution,
        tiling_specification: TilingSpecification,
    ) -> Self {
        Self {
            source,
            input_resolution,
            tiling_specification,
            interpolation: PhantomData,
        }
    }
}

#[async_trait]
impl<Q, P, I> QueryProcessor for InterploationProcessor<Q, P, I>
where
    Q: QueryProcessor<Output = RasterTile2D<P>, SpatialBounds = SpatialPartition2D>,
    P: Pixel,
    I: InterpolationAlgorithm<P>,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        // do not interpolate if the source resolution is already fine enough
        if query.spatial_resolution.x >= self.input_resolution.x
            && query.spatial_resolution.y >= self.input_resolution.y
        {
            return self.source.query(query, ctx).await;
        }

        let sub_query = InterpolationSubQuery::<_, P, I> {
            input_resolution: self.input_resolution,
            fold_fn: fold_future,
            tiling_specification: self.tiling_specification,
            phantom: PhantomData,
            _phantom_pixel_type: PhantomData,
        };

        Ok(RasterSubQueryAdapter::<'a, P, _, _>::new(
            &self.source,
            query,
            self.tiling_specification,
            ctx,
            sub_query,
        )
        .filter_and_fill())
    }
}

#[derive(Debug, Clone)]
pub struct InterpolationSubQuery<F, T, I> {
    input_resolution: SpatialResolution,
    fold_fn: F,
    tiling_specification: TilingSpecification,
    phantom: PhantomData<I>,
    _phantom_pixel_type: PhantomData<T>,
}

impl<'a, T, FoldM, FoldF, I> SubQueryTileAggregator<'a, T> for InterpolationSubQuery<FoldM, T, I>
where
    T: Pixel,
    FoldM: Send + Sync + 'a + Clone + Fn(InterpolationAccu<T, I>, RasterTile2D<T>) -> FoldF,
    FoldF: Send + TryFuture<Ok = InterpolationAccu<T, I>, Error = crate::error::Error>,
    I: InterpolationAlgorithm<T>,
{
    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    type TileAccu = InterpolationAccu<T, I>;
    type TileAccuFuture = BoxFuture<'a, Result<Self::TileAccu>>;

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        create_accu(
            tile_info,
            query_rect,
            pool.clone(),
            self.tiling_specification,
        )
        .boxed()
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        // enlarge the spatial bounds in order to have the neighbor pixels for the interpolation
        let spatial_bounds = tile_info.spatial_partition();
        let enlarge: Coordinate2D = (self.input_resolution.x, -self.input_resolution.y).into();
        let spatial_bounds = SpatialPartition2D::new(
            spatial_bounds.upper_left(),
            spatial_bounds.lower_right() + enlarge,
        )?;

        Ok(Some(RasterQueryRectangle {
            spatial_bounds,
            time_interval: TimeInterval::new_instant(start_time)?,
            spatial_resolution: self.input_resolution,
        }))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

#[derive(Clone, Debug)]
pub struct InterpolationAccu<T: Pixel, I: InterpolationAlgorithm<T>> {
    pub output_info: TileInformation,
    pub input_tile: RasterTile2D<T>,
    pub pool: Arc<ThreadPool>,
    phantom: PhantomData<I>,
}

impl<T: Pixel, I: InterpolationAlgorithm<T>> InterpolationAccu<T, I> {
    pub fn new(
        input_tile: RasterTile2D<T>,
        output_info: TileInformation,
        pool: Arc<ThreadPool>,
    ) -> Self {
        InterpolationAccu {
            input_tile,
            output_info,
            pool,
            phantom: Default::default(),
        }
    }
}

#[async_trait]
impl<T: Pixel, I: InterpolationAlgorithm<T>> FoldTileAccu for InterpolationAccu<T, I> {
    type RasterType = T;

    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        // now that we collected all the input tile pixels we perform the actual interpolation

        let output_tile = crate::util::spawn_blocking_with_thread_pool(self.pool, move || {
            I::interpolate(&self.input_tile, &self.output_info)
        })
        .await??;

        Ok(output_tile)
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

impl<T: Pixel, I: InterpolationAlgorithm<T>> FoldTileAccuMut for InterpolationAccu<T, I> {
    fn tile_mut(&mut self) -> &mut RasterTile2D<T> {
        &mut self.input_tile
    }
}

pub fn create_accu<T: Pixel, I: InterpolationAlgorithm<T>>(
    tile_info: TileInformation,
    query_rect: RasterQueryRectangle,
    pool: Arc<ThreadPool>,
    tiling_specification: TilingSpecification,
) -> impl Future<Output = Result<InterpolationAccu<T, I>>> {
    // create an accumulator as a single tile that fits all the input tiles
    crate::util::spawn_blocking(move || {
        let tiling = tiling_specification.strategy(
            query_rect.spatial_resolution.x,
            -query_rect.spatial_resolution.y,
        );

        let origin_coordinate = tiling
            .tile_information_iterator(query_rect.spatial_bounds)
            .next()
            .expect("a query contains at least one tile")
            .spatial_partition()
            .upper_left();

        let geo_transform = GeoTransform::new(
            origin_coordinate,
            query_rect.spatial_resolution.x,
            -query_rect.spatial_resolution.y,
        );

        let bbox = tiling.tile_grid_box(query_rect.spatial_bounds);

        let shape = [
            bbox.axis_size_y() * tiling.tile_size_in_pixels.axis_size_y(),
            bbox.axis_size_x() * tiling.tile_size_in_pixels.axis_size_x(),
        ];

        // create a non-aligned (w.r.t. the tiling specification) grid by setting the origin to the top-left of the tile and the tile-index to [0, 0]
        let grid = EmptyGrid2D::new(shape.into());

        let input_tile = RasterTile2D::new(
            query_rect.time_interval,
            [0, 0].into(),
            geo_transform,
            GridOrEmpty::from(grid),
        );

        InterpolationAccu::new(input_tile, tile_info, pool)
    })
    .map_err(From::from)
}

pub fn fold_future<T, I>(
    accu: InterpolationAccu<T, I>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<InterpolationAccu<T, I>>>
where
    T: Pixel,
    I: InterpolationAlgorithm<T>,
{
    crate::util::spawn_blocking(|| fold_impl(accu, tile)).then(|x| async move {
        match x {
            Ok(r) => r,
            Err(e) => Err(e.into()),
        }
    })
}

pub fn fold_impl<T, I>(
    mut accu: InterpolationAccu<T, I>,
    tile: RasterTile2D<T>,
) -> Result<InterpolationAccu<T, I>>
where
    T: Pixel,
    I: InterpolationAlgorithm<T>,
{
    // get the time now because it is not known when the accu was created
    accu.input_tile.time = tile.time;

    // TODO: add a skip if both tiles are empty?

    // copy all input tiles into the accu to have all data for interpolation
    let mut accu_input_tile = accu.input_tile.into_materialized_tile();
    accu_input_tile.blit(tile)?;

    Ok(InterpolationAccu::new(
        accu_input_tile.into(),
        accu.output_info,
        accu.pool,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::{
            Measurement, RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval,
        },
        raster::{
            Grid2D, GridOrEmpty, RasterDataType, RasterTile2D, TileInformation, TilingSpecification,
        },
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use crate::{
        engine::{MockExecutionContext, MockQueryContext, RasterOperator, RasterResultDescriptor},
        mock::{MockRasterSource, MockRasterSourceParams},
    };

    #[tokio::test]
    async fn nearest_neighbor_operator() -> Result<()> {
        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [2, 2].into(),
        ));

        let raster = make_raster();

        let operator = Interpolation {
            params: InterpolationParams {
                interpolation: InterpolationMethod::NearestNeighbor,
                input_resolution: InputResolution::Value(SpatialResolution::one()),
            },
            sources: SingleRasterSource { raster },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await?;

        let processor = operator.query_processor()?.get_i8().unwrap();

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 2.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 20),
            spatial_resolution: SpatialResolution::zero_point_five(),
        };
        let query_ctx = MockQueryContext::test_default();

        let result_stream = processor.query(query_rect, &query_ctx).await?;

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>()?;

        let mut times: Vec<TimeInterval> = vec![TimeInterval::new_unchecked(0, 10); 8];
        times.append(&mut vec![TimeInterval::new_unchecked(10, 20); 8]);

        let data = vec![
            vec![1, 2, 5, 6],
            vec![2, 3, 6, 7],
            vec![3, 4, 7, 8],
            vec![4, 0, 8, 0],
            vec![5, 6, 0, 0],
            vec![6, 7, 0, 0],
            vec![7, 8, 0, 0],
            vec![8, 0, 0, 0],
            vec![8, 7, 4, 3],
            vec![7, 6, 3, 2],
            vec![6, 5, 2, 1],
            vec![5, 0, 1, 0],
            vec![4, 3, 0, 0],
            vec![3, 2, 0, 0],
            vec![2, 1, 0, 0],
            vec![1, 0, 0, 0],
        ];

        let valid = vec![
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true, false, true, false],
            vec![true, true, false, false],
            vec![true, true, false, false],
            vec![true, true, false, false],
            vec![true, false, false, false],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true, false, true, false],
            vec![true, true, false, false],
            vec![true, true, false, false],
            vec![true, true, false, false],
            vec![true, false, false, false],
        ];

        for (i, tile) in result.into_iter().enumerate() {
            let tile = tile.into_materialized_tile();
            assert_eq!(tile.time, times[i]);
            assert_eq!(tile.grid_array.inner_grid.data, data[i]);
            assert_eq!(tile.grid_array.validity_mask.data, valid[i]);
        }

        Ok(())
    }

    fn make_raster() -> Box<dyn RasterOperator> {
        // test raster:
        // [0, 10)
        // || 1 | 2 || 3 | 4 ||
        // || 5 | 6 || 7 | 8 ||
        //
        // [10, 20)
        // || 8 | 7 || 6 | 5 ||
        // || 4 | 3 || 2 | 1 ||
        let raster_tiles = vec![
            RasterTile2D::<i8>::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([2, 2].into(), vec![1, 2, 5, 6]).unwrap()),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([2, 2].into(), vec![3, 4, 7, 8]).unwrap()),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([2, 2].into(), vec![8, 7, 4, 3]).unwrap()),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([2, 2].into(), vec![6, 5, 2, 1]).unwrap()),
            ),
        ];

        MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed()
    }
}
