use std::marker::PhantomData;
use std::sync::Arc;

use crate::adapters::{
    FoldTileAccu, FoldTileAccuMut, RasterSubQueryAdapter, SubQueryTileAggregator,
};
use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources, Operator,
    OperatorName, QueryContext, QueryProcessor, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, SingleRasterSource, TypedRasterQueryProcessor, WorkflowOperatorPath,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{Future, FutureExt, TryFuture, TryFutureExt};
use geoengine_datatypes::primitives::{BandSelection, CacheHint};
use geoengine_datatypes::primitives::{
    RasterQueryRectangle, RasterSpatialQueryRectangle, SpatialResolution, TimeInstance,
    TimeInterval,
};
use geoengine_datatypes::raster::{
    Bilinear, ChangeGridBounds, GeoTransform, GridBlit, GridBoundingBox2D, GridOrEmpty,
    InterpolationAlgorithm, NearestNeighbor, Pixel, RasterTile2D, TileInformation,
    TilingSpecification,
};
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use snafu::{ensure, Snafu};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InterpolationParams {
    pub interpolation: InterpolationMethod,
    pub output_resolution: InputResolution,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum InputResolution {
    Resolution(SpatialResolution),
    Fraction(f64), // FIXME: Must be > 1
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
        "The input resolution was defined as `source` but the source resolution is unknown.",
    ))]
    UnknownInputResolution,
    #[snafu(display("The fraction used to interpolate must be >= 1, was {f}."))]
    FractionMustBeOneOrLarger { f: f64 },
    #[snafu(display("The output resolution must be higher than the input resolution."))]
    OutputMustBeHigherResolutionThanInput {
        input: SpatialResolution,
        output: SpatialResolution,
    },
}

pub type Interpolation = Operator<InterpolationParams, SingleRasterSource>;

impl OperatorName for Interpolation {
    const TYPE_NAME: &'static str = "Interpolation";
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Interpolation {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        let initialized_sources = self.sources.initialize_sources(path, context).await?;
        let raster_source = initialized_sources.raster;
        let in_descriptor = raster_source.result_descriptor();

        let in_geo_transform = in_descriptor.tiling_geo_transform();
        let in_pixel_bounds = in_descriptor.tiling_pixel_bounds();

        let output_resolution = match self.params.output_resolution {
            InputResolution::Resolution(res) => {
                ensure!(
                    res.x.abs() <= in_geo_transform.x_pixel_size().abs(),
                    error::OutputMustBeHigherResolutionThanInput {
                        input: in_geo_transform.spatial_resolution(),
                        output: res
                    }
                );
                ensure!(
                    res.y.abs() <= in_geo_transform.y_pixel_size().abs(),
                    error::OutputMustBeHigherResolutionThanInput {
                        input: in_geo_transform.spatial_resolution(),
                        output: res
                    }
                );
                res
            }

            InputResolution::Fraction(f) => {
                ensure!(f >= 1.0, error::FractionMustBeOneOrLarger { f });

                SpatialResolution::new(
                    in_geo_transform.x_pixel_size() / f,
                    in_geo_transform.y_pixel_size().abs() / f,
                )
                .expect("the input resolution is valid")
            }
        };

        let output_geo_transform = GeoTransform::new(
            in_geo_transform.origin_coordinate,
            output_resolution.x,
            -output_resolution.y,
        );

        let out_pixel_bounds = output_geo_transform
            .spatial_to_grid_bounds(&in_geo_transform.grid_to_spatial_bounds(&in_pixel_bounds));

        let out_descriptor = RasterResultDescriptor {
            spatial_reference: in_descriptor.spatial_reference,
            data_type: in_descriptor.data_type,
            time: in_descriptor.time,
            geo_transform_x: output_geo_transform,
            pixel_bounds_x: out_pixel_bounds,
            bands: in_descriptor.bands.clone(),
        };

        let initialized_operator = InitializedInterpolation {
            name,
            output_result_descriptor: out_descriptor,
            raster_source,
            interpolation_method: self.params.interpolation,
            tiling_specification: context.tiling_specification(),
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(Interpolation);
}

pub struct InitializedInterpolation {
    name: CanonicOperatorName,
    output_result_descriptor: RasterResultDescriptor,
    raster_source: Box<dyn InitializedRasterOperator>,
    interpolation_method: InterpolationMethod,
    tiling_specification: TilingSpecification,
}

impl InitializedRasterOperator for InitializedInterpolation {
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_processor = self.raster_source.query_processor()?;

        let res = call_on_generic_raster_processor!(
            source_processor, p => match self.interpolation_method  {
                InterpolationMethod::NearestNeighbor => InterploationProcessor::<_,_, NearestNeighbor>::new(
                        p,
                        self.output_result_descriptor.clone(),
                        self.tiling_specification,
                    ).boxed()
                    .into(),
                InterpolationMethod::BiLinear =>InterploationProcessor::<_,_, Bilinear>::new(
                        p,
                        self.output_result_descriptor.clone(),
                        self.tiling_specification,
                    ).boxed()
                    .into(),
            }
        );

        Ok(res)
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.output_result_descriptor
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }
}

pub struct InterploationProcessor<Q, P, I>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
    I: InterpolationAlgorithm<GridBoundingBox2D, P>,
{
    source: Q,
    out_result_descriptor: RasterResultDescriptor,
    tiling_specification: TilingSpecification,
    interpolation: PhantomData<I>,
}

impl<Q, P, I> InterploationProcessor<Q, P, I>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
    I: InterpolationAlgorithm<GridBoundingBox2D, P>,
{
    pub fn new(
        source: Q,
        out_result_descriptor: RasterResultDescriptor,
        tiling_specification: TilingSpecification,
    ) -> Self {
        Self {
            source,
            out_result_descriptor,
            tiling_specification,
            interpolation: PhantomData,
        }
    }
}

#[async_trait]
impl<Q, P, I> QueryProcessor for InterploationProcessor<Q, P, I>
where
    Q: QueryProcessor<
        Output = RasterTile2D<P>,
        SpatialQuery = RasterSpatialQueryRectangle,
        Selection = BandSelection,
        ResultDescription = RasterResultDescriptor,
    >,
    P: Pixel,
    I: InterpolationAlgorithm<GridBoundingBox2D, P>,
{
    type Output = RasterTile2D<P>;
    type SpatialQuery = RasterSpatialQueryRectangle;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        // do not interpolate if the source resolution is already fine enough

        let out_geo_transform = self.out_result_descriptor.tiling_geo_transform();
        let in_geo_transform = self.source.result_descriptor().tiling_geo_transform();

        // TODO: This should not be necessary since we already checked this in the initialization
        ensure!(
            out_geo_transform.x_pixel_size().abs() <= in_geo_transform.x_pixel_size().abs(),
            error::OutputMustBeHigherResolutionThanInput {
                input: in_geo_transform.spatial_resolution(),
                output: out_geo_transform.spatial_resolution(),
            },
        );
        ensure!(
            out_geo_transform.y_pixel_size().abs() <= in_geo_transform.y_pixel_size().abs(),
            error::OutputMustBeHigherResolutionThanInput {
                input: in_geo_transform.spatial_resolution(),
                output: out_geo_transform.spatial_resolution(),
            },
        );

        // if the output resolution is the same as the input resolution, we can just forward the query
        if out_geo_transform.spatial_resolution() == in_geo_transform.spatial_resolution() {
            return self.source.query(query, ctx).await;
        }

        // This is the tiling strategy we want to fill
        let tiling_strategy = self.tiling_specification.strategy(out_geo_transform);

        let sub_query = InterpolationSubQuery::<_, P, I> {
            input_geo_transform: in_geo_transform,
            output_geo_transform: out_geo_transform,
            fold_fn: fold_future,
            tiling_specification: self.tiling_specification,
            phantom: PhantomData,
            _phantom_pixel_type: PhantomData,
        };

        Ok(RasterSubQueryAdapter::<'a, P, _, _>::new(
            &self.source,
            query,
            tiling_strategy,
            ctx,
            sub_query,
        )
        .filter_and_fill(
            crate::adapters::FillerTileCacheExpirationStrategy::DerivedFromSurroundingTiles,
        ))
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.out_result_descriptor
    }
}

#[derive(Debug, Clone)]
pub struct InterpolationSubQuery<F, T, I> {
    input_geo_transform: GeoTransform,
    output_geo_transform: GeoTransform, // TODO remove because in adapter?
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
    I: InterpolationAlgorithm<GridBoundingBox2D, T>,
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
            self.input_geo_transform,
            self.output_geo_transform,
            tile_info,
            &query_rect,
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
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        // enlarge the spatial bounds in order to have the neighbor pixels for the interpolation

        let tile_pixel_bounds = tile_info.global_pixel_bounds();
        let tile_spatial_bounds = self
            .output_geo_transform
            .grid_to_spatial_bounds(&tile_pixel_bounds);
        let input_pixel_bounds = self
            .input_geo_transform
            .spatial_to_grid_bounds(&tile_spatial_bounds);
        let enlarged_input_pixel_bounds = GridBoundingBox2D::new(
            [input_pixel_bounds.y_min(), input_pixel_bounds.x_min()],
            [
                input_pixel_bounds.y_max() + 1,
                input_pixel_bounds.x_max() + 1,
            ],
        )
        .expect("max bounds must be larger then min bounds already");

        Ok(Some(RasterQueryRectangle::new_with_grid_bounds(
            enlarged_input_pixel_bounds,
            TimeInterval::new_instant(start_time)?,
            BandSelection::new_single(band_idx),
        )))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

#[derive(Clone, Debug)]
pub struct InterpolationAccu<T: Pixel, I: InterpolationAlgorithm<GridBoundingBox2D, T>> {
    pub output_info: TileInformation,
    pub input_tile: GridOrEmpty<GridBoundingBox2D, T>,
    pub input_geo_transform: GeoTransform,
    pub time: TimeInterval,
    pub cache_hint: CacheHint,
    pub pool: Arc<ThreadPool>,
    phantom: PhantomData<I>,
}

impl<T: Pixel, I: InterpolationAlgorithm<GridBoundingBox2D, T>> InterpolationAccu<T, I> {
    pub fn new(
        input_tile: GridOrEmpty<GridBoundingBox2D, T>,
        input_geo_transform: GeoTransform,
        time: TimeInterval,
        cache_hint: CacheHint,
        output_info: TileInformation,
        pool: Arc<ThreadPool>,
    ) -> Self {
        InterpolationAccu {
            input_tile,
            input_geo_transform,
            time,
            cache_hint,
            output_info,
            pool,
            phantom: Default::default(),
        }
    }
}

#[async_trait]
impl<T: Pixel, I: InterpolationAlgorithm<GridBoundingBox2D, T>> FoldTileAccu
    for InterpolationAccu<T, I>
{
    type RasterType = T;

    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        // now that we collected all the input tile pixels we perform the actual interpolation

        let output_tile = crate::util::spawn_blocking_with_thread_pool(self.pool, move || {
            I::interpolate(
                self.input_geo_transform,
                &self.input_tile,
                self.output_info.global_geo_transform,
                self.output_info.global_pixel_bounds(),
            )
        })
        .await??;

        let output_tile = RasterTile2D::new_with_tile_info(
            self.time,
            self.output_info,
            0,
            output_tile.unbounded(),
            self.cache_hint,
        );

        Ok(output_tile)
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

impl<T: Pixel, I: InterpolationAlgorithm<GridBoundingBox2D, T>> FoldTileAccuMut
    for InterpolationAccu<T, I>
{
    fn set_time(&mut self, time: TimeInterval) {
        self.time = time;
    }

    fn set_cache_hint(&mut self, cache_hint: CacheHint) {
        self.cache_hint = cache_hint;
    }
}

pub fn create_accu<T: Pixel, I: InterpolationAlgorithm<GridBoundingBox2D, T>>(
    input_geo_transform: GeoTransform,
    output_geo_transform: GeoTransform,
    tile_info: TileInformation,
    query_rect: &RasterQueryRectangle,
    pool: Arc<ThreadPool>,
    _tiling_specification: TilingSpecification,
) -> impl Future<Output = Result<InterpolationAccu<T, I>>> {
    let query_rect = query_rect.clone();

    // create an accumulator as a single tile that fits all the input tiles
    let time_interval = query_rect.time_interval;

    crate::util::spawn_blocking(move || {
        let tile_pixel_bounds = tile_info.global_pixel_bounds();
        let tile_spatial_bounds = output_geo_transform.grid_to_spatial_bounds(&tile_pixel_bounds);
        let input_pixel_bounds = input_geo_transform.spatial_to_grid_bounds(&tile_spatial_bounds);
        let enlarged_input_pixel_bounds = GridBoundingBox2D::new(
            [input_pixel_bounds.y_min(), input_pixel_bounds.x_min()],
            [
                input_pixel_bounds.y_max() + 1,
                input_pixel_bounds.x_max() + 1,
            ],
        )
        .expect("max bounds must be larger then min bounds already");

        // create a non-aligned (w.r.t. the tiling specification) grid by setting the origin to the top-left of the tile and the tile-index to [0, 0]
        let grid = GridOrEmpty::new_empty(enlarged_input_pixel_bounds);

        InterpolationAccu::new(
            grid,
            input_geo_transform,
            time_interval,
            CacheHint::max_duration(),
            tile_info,
            pool,
        )
    })
    .map_err(From::from)
}

pub fn fold_future<T, I>(
    accu: InterpolationAccu<T, I>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<InterpolationAccu<T, I>>>
where
    T: Pixel,
    I: InterpolationAlgorithm<GridBoundingBox2D, T>,
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
    I: InterpolationAlgorithm<GridBoundingBox2D, T>,
{
    // get the time now because it is not known when the accu was created
    accu.set_time(tile.time);
    accu.cache_hint.merge_with(&tile.cache_hint);

    // TODO: add a skip if both tiles are empty?

    // copy all input tiles into the accu to have all data for interpolation
    let in_tile = &tile.into_inner_positioned_grid();

    accu.input_tile.grid_blit_from(in_tile);

    Ok(accu)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::{Coordinate2D, RasterQueryRectangle, SpatialResolution, TimeInterval},
        raster::{
            Grid2D, GridOrEmpty, RasterDataType, RasterTile2D, RenameBands, TileInformation,
            TilingSpecification,
        },
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use crate::{
        engine::{
            MockExecutionContext, MockQueryContext, MultipleRasterSources, RasterBandDescriptors,
            RasterOperator, RasterResultDescriptor,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
        processing::{RasterStacker, RasterStackerParams},
    };

    #[tokio::test]
    async fn nearest_neighbor_operator() -> Result<()> {
        let exe_ctx =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([2, 2].into()));

        // test raster:
        // [0, 10)
        // || 1 | 2 || 3 | 4 ||
        // || 5 | 6 || 7 | 8 ||
        //
        // [10, 20)
        // || 8 | 7 || 6 | 5 ||
        // || 4 | 3 || 2 | 1 ||

        // exptected raster:
        // [0, 10)
        // ||1 | 1 || 2 | 2 ||
        // ||1 | 1 || 2 | 2 ||
        // ||5 | 5 || 6 | 6 ||
        // ||5 | 5 || 6 | 6 ||

        let raster = make_raster(CacheHint::max_duration());

        let operator =
            Interpolation {
                params: InterpolationParams {
                    interpolation: InterpolationMethod::NearestNeighbor,
                    output_resolution: InputResolution::Resolution(
                        SpatialResolution::zero_point_five(),
                    ),
                },
                sources: SingleRasterSource { raster },
            }
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await?;

        let processor = operator.query_processor()?.get_i8().unwrap();

        let query_rect = RasterQueryRectangle::new_with_grid_bounds(
            GridBoundingBox2D::new([-4, 0], [-1, 7]).unwrap(),
            TimeInterval::new_unchecked(0, 20),
            BandSelection::first(),
        );
        let query_ctx = MockQueryContext::test_default();

        let result_stream = processor.query(query_rect, &query_ctx).await?;

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>()?;

        let mut times: Vec<TimeInterval> = vec![TimeInterval::new_unchecked(0, 10); 8];
        times.append(&mut vec![TimeInterval::new_unchecked(10, 20); 8]);

        let data = vec![
            vec![1; 4],
            vec![2; 4],
            vec![3; 4],
            vec![4; 4],
            vec![5; 4],
            vec![6; 4],
            vec![7; 4],
            vec![8; 4],
            vec![8; 4],
            vec![7; 4],
            vec![6; 4],
            vec![5; 4],
            vec![4; 4],
            vec![3; 4],
            vec![2; 4],
            vec![1; 4],
        ];

        let valid = vec![
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
        ];

        for (i, tile) in result.into_iter().enumerate() {
            let tile = tile.into_materialized_tile();
            assert_eq!(tile.time, times[i]);
            assert_eq!(tile.grid_array.validity_mask.data, valid[i]);
            assert_eq!(tile.grid_array.inner_grid.data, data[i]);
        }

        Ok(())
    }

    fn make_raster(cache_hint: CacheHint) -> Box<dyn RasterOperator> {
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
                0,
                GridOrEmpty::from(Grid2D::new([2, 2].into(), vec![1, 2, 5, 6]).unwrap()),
                cache_hint,
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([2, 2].into(), vec![3, 4, 7, 8]).unwrap()),
                cache_hint,
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([2, 2].into(), vec![8, 7, 4, 3]).unwrap()),
                cache_hint,
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [2, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                0,
                GridOrEmpty::from(Grid2D::new([2, 2].into(), vec![6, 5, 2, 1]).unwrap()),
                cache_hint,
            ),
        ];

        MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    geo_transform_x: GeoTransform::new(Coordinate2D::new(0., 0.), 1.0, -1.0),
                    pixel_bounds_x: GridBoundingBox2D::new_min_max(-2, -1, 0, 3).unwrap(),
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed()
    }

    #[tokio::test]
    async fn it_attaches_cache_hint() -> Result<()> {
        let exe_ctx =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([2, 2].into()));

        let cache_hint = CacheHint::seconds(1234);
        let raster = make_raster(cache_hint);

        let operator =
            Interpolation {
                params: InterpolationParams {
                    interpolation: InterpolationMethod::NearestNeighbor,
                    output_resolution: InputResolution::Resolution(
                        SpatialResolution::zero_point_five(),
                    ),
                },
                sources: SingleRasterSource { raster },
            }
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await?;

        let processor = operator.query_processor()?.get_i8().unwrap();

        let query_rect = RasterQueryRectangle::new_with_grid_bounds(
            GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 20),
            BandSelection::first(),
        );
        let query_ctx = MockQueryContext::test_default();

        let result_stream = processor.query(query_rect, &query_ctx).await?;

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>()?;

        for tile in result {
            // dbg!(tile.time, tile.grid_array);
            assert_eq!(tile.cache_hint.expires(), cache_hint.expires());
        }

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_interpolates_multiple_bands() -> Result<()> {
        let exe_ctx =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([2, 2].into()));
        let operator =
            Interpolation {
                params: InterpolationParams {
                    interpolation: InterpolationMethod::NearestNeighbor,
                    output_resolution: InputResolution::Resolution(
                        SpatialResolution::zero_point_five(),
                    ),
                },
                sources: SingleRasterSource {
                    raster: RasterStacker {
                        params: RasterStackerParams {
                            rename_bands: RenameBands::Default,
                        },
                        sources: MultipleRasterSources {
                            rasters: vec![
                                make_raster(CacheHint::max_duration()),
                                make_raster(CacheHint::max_duration()),
                            ],
                        },
                    }
                    .boxed(),
                },
            }
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await?;

        let processor = operator.query_processor()?.get_i8().unwrap();

        let query_rect = RasterQueryRectangle::new_with_grid_bounds(
            GridBoundingBox2D::new([-4, 0], [-1, 7]).unwrap(),
            TimeInterval::new_unchecked(0, 20),
            [0, 1].try_into().unwrap(),
        );
        let query_ctx = MockQueryContext::test_default();

        let result_stream = processor.query(query_rect, &query_ctx).await?;

        let result: Vec<Result<RasterTile2D<i8>>> = result_stream.collect().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>()?;

        let mut times: Vec<TimeInterval> = vec![TimeInterval::new_unchecked(0, 10); 8];
        times.append(&mut vec![TimeInterval::new_unchecked(10, 20); 8]);

        let times = times
            .clone()
            .into_iter()
            .zip(times)
            .flat_map(|(a, b)| vec![a, b])
            .collect::<Vec<_>>();

        let data = vec![
            vec![1; 4],
            vec![2; 4],
            vec![3; 4],
            vec![4; 4],
            vec![5; 4],
            vec![6; 4],
            vec![7; 4],
            vec![8; 4],
            vec![8; 4],
            vec![7; 4],
            vec![6; 4],
            vec![5; 4],
            vec![4; 4],
            vec![3; 4],
            vec![2; 4],
            vec![1; 4],
        ];

        let valid = vec![
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
            vec![true; 4],
        ];

        let data = data
            .clone()
            .into_iter()
            .zip(data)
            .flat_map(|(a, b)| vec![a, b])
            .collect::<Vec<_>>();

        let valid = valid
            .clone()
            .into_iter()
            .zip(valid)
            .flat_map(|(a, b)| vec![a, b])
            .collect::<Vec<_>>();

        for (i, tile) in result.into_iter().enumerate() {
            let tile = tile.into_materialized_tile();
            assert_eq!(tile.time, times[i]);
            assert_eq!(tile.grid_array.inner_grid.data, data[i]);
            assert_eq!(tile.grid_array.validity_mask.data, valid[i]);
        }

        Ok(())
    }
}
