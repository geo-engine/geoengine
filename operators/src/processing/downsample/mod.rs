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
use geoengine_datatypes::primitives::{BandSelection, CacheHint, Coordinate2D};
use geoengine_datatypes::primitives::{RasterQueryRectangle, SpatialResolution, TimeInterval};
use geoengine_datatypes::raster::{
    ChangeGridBounds, GeoTransform, GridBoundingBox2D, GridContains, GridIdx2D, GridIndexAccess,
    GridOrEmpty, Pixel, RasterTile2D, TileInformation, TilingSpecification,
    UpdateIndexedElementsParallel,
};
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use snafu::{Snafu, ensure};
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub struct DownsamplingParams {
    pub sampling_method: DownsamplingMethod,
    pub output_resolution: DownsamplingResolution,
    pub output_origin_reference: Option<Coordinate2D>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum DownsamplingResolution {
    Resolution(SpatialResolution),
    Fraction { x: f64, y: f64 },
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub enum DownsamplingMethod {
    NearestNeighbor,
    // Mean,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum DownsamplingError {
    #[snafu(display("The fraction used to downsample must be >= 1, was {f}."))]
    FractionMustBeOneOrLarger { f: f64 },
    #[snafu(display("The output resolution must be higher than the input resolution."))]
    OutputMustBeLowerResolutionThanInput {
        input: SpatialResolution,
        output: SpatialResolution,
    },
}

pub type Downsampling = Operator<DownsamplingParams, SingleRasterSource>;

impl OperatorName for Downsampling {
    const TYPE_NAME: &'static str = "Downsampling";
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Downsampling {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);
        let initialized_source = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?;
        InitializedDownsampling::new_with_source_and_params(
            name,
            path,
            initialized_source.raster,
            self.params,
            context.tiling_specification(),
        )
        .map(InitializedRasterOperator::boxed)
    }

    span_fn!(Downsampling);
}

pub struct InitializedDownsampling<O: InitializedRasterOperator> {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    output_result_descriptor: RasterResultDescriptor,
    raster_source: O,
    sampling_method: DownsamplingMethod,
    tiling_specification: TilingSpecification,
}

impl<O: InitializedRasterOperator> InitializedDownsampling<O> {
    pub fn new_with_source_and_params(
        name: CanonicOperatorName,
        path: WorkflowOperatorPath,
        raster_source: O,
        params: DownsamplingParams,
        tiling_specification: TilingSpecification,
    ) -> Result<Self> {
        let in_descriptor = raster_source.result_descriptor();

        let in_spatial_grid = in_descriptor.spatial_grid_descriptor();

        let output_resolution = match params.output_resolution {
            DownsamplingResolution::Resolution(res) => {
                ensure!(
                    res.x.abs() >= in_spatial_grid.spatial_resolution().x.abs(),
                    error::OutputMustBeLowerResolutionThanInput {
                        input: in_spatial_grid.spatial_resolution(),
                        output: res
                    }
                );
                ensure!(
                    res.y.abs() >= in_spatial_grid.spatial_resolution().y.abs(), // TODO: allow neg y size in SpatialResolution
                    error::OutputMustBeLowerResolutionThanInput {
                        input: in_spatial_grid.spatial_resolution(),
                        output: res
                    }
                );
                res
            }

            DownsamplingResolution::Fraction { x, y } => {
                ensure!(x >= 1.0, error::FractionMustBeOneOrLarger { f: x });
                ensure!(y >= 1.0, error::FractionMustBeOneOrLarger { f: y });

                SpatialResolution::new_unchecked(
                    in_spatial_grid.spatial_resolution().x * x,
                    in_spatial_grid.spatial_resolution().y.abs() * y, // TODO: allow negative size
                )
            }
        };

        let output_gspatial_grid = if let Some(oc) = params.output_origin_reference {
            in_spatial_grid
                .with_moved_origin_to_nearest_grid_edge(oc)
                .replace_origin(oc)
                .with_changed_resolution(output_resolution)
        } else {
            in_spatial_grid.with_changed_resolution(output_resolution)
        };

        let out_descriptor = RasterResultDescriptor {
            spatial_reference: in_descriptor.spatial_reference,
            data_type: in_descriptor.data_type, // TODO: datatype depends on resample method!
            time: in_descriptor.time,
            spatial_grid: output_gspatial_grid,
            bands: in_descriptor.bands.clone(),
        };

        Ok(InitializedDownsampling {
            name,
            path,
            output_result_descriptor: out_descriptor,
            raster_source,
            sampling_method: params.sampling_method,
            tiling_specification,
        })
    }
}

impl<O: InitializedRasterOperator> InitializedRasterOperator for InitializedDownsampling<O> {
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_processor = self.raster_source.query_processor()?;

        let res = call_on_generic_raster_processor!(
            source_processor, p => match self.sampling_method  {
                DownsamplingMethod::NearestNeighbor => DownsampleProcessor::<_,_>::new(
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

    fn name(&self) -> &'static str {
        Downsampling::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }
}

pub struct DownsampleProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Copy,
{
    source: Q,
    out_result_descriptor: RasterResultDescriptor,
    tiling_specification: TilingSpecification,
}

impl<Q, P> DownsampleProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Copy,
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
        }
    }
}

#[async_trait]
impl<Q, P> RasterQueryProcessor for DownsampleProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    type RasterType = P;

    async fn _time_query<'a>(
        &'a self,
        query: geoengine_datatypes::primitives::TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<geoengine_datatypes::primitives::TimeInterval>>> {
        self.source.time_query(query, ctx).await
    }
}

#[async_trait]
impl<Q, P> QueryProcessor for DownsampleProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = GridBoundingBox2D;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        // do not interpolate if the source resolution is already fine enough

        let in_spatial_grid = self.source.result_descriptor().spatial_grid_descriptor();
        let out_spatial_grid = self.result_descriptor().spatial_grid_descriptor();

        // if the output resolution is the same as the input resolution, we can just forward the query // TODO: except the origin changes?
        if in_spatial_grid == out_spatial_grid {
            return self.source.query(query, ctx).await;
        }

        let tiling_grid_definition =
            out_spatial_grid.tiling_grid_definition(ctx.tiling_specification());
        // This is the tiling strategy we want to fill
        let tiling_strategy: geoengine_datatypes::raster::TilingStrategy =
            tiling_grid_definition.generate_data_tiling_strategy();

        let sub_query = DownsampleSubQuery::<_, P> {
            input_geo_transform: in_spatial_grid
                .tiling_grid_definition(ctx.tiling_specification())
                .tiling_geo_transform(),
            output_geo_transform: tiling_grid_definition.tiling_geo_transform(),
            fold_fn: fold_future,
            tiling_specification: self.tiling_specification,
            _phantom_pixel_type: PhantomData,
        };

        let time_stream = self.time_query(query.time_interval(), ctx).await?;

        Ok(Box::pin(RasterSubQueryAdapter::<'a, P, _, _, _>::new(
            &self.source,
            query,
            tiling_strategy,
            ctx,
            sub_query,
            time_stream,
        )))
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.out_result_descriptor
    }
}

#[derive(Debug, Clone)]
pub struct DownsampleSubQuery<F, T> {
    input_geo_transform: GeoTransform,
    output_geo_transform: GeoTransform,
    fold_fn: F,
    tiling_specification: TilingSpecification,
    _phantom_pixel_type: PhantomData<T>,
}

impl<'a, T, FoldM, FoldF> SubQueryTileAggregator<'a, T> for DownsampleSubQuery<FoldM, T>
where
    T: Pixel,
    FoldM: Send + Sync + 'a + Clone + Fn(DownsampleAccu<T>, RasterTile2D<T>) -> FoldF,
    FoldF: Send + TryFuture<Ok = DownsampleAccu<T>, Error = crate::error::Error>,
{
    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    type TileAccu = DownsampleAccu<T>;
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
        time: TimeInterval,
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        let out_tile_pixel_bounds = tile_info.global_pixel_bounds();
        //.intersection(&query_rect.spatial_query.grid_bounds());
        //let out_tile_pixel_bounds = out_tile_pixel_bounds;
        let out_tile_spatial_bounds = self
            .output_geo_transform
            .grid_to_spatial_bounds(&out_tile_pixel_bounds);
        let input_pixel_bounds = self
            .input_geo_transform
            .spatial_to_grid_bounds(&out_tile_spatial_bounds);

        Ok(Some(RasterQueryRectangle::new(
            input_pixel_bounds,
            time,
            BandSelection::new_single(band_idx),
        )))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

#[derive(Clone, Debug)]
pub struct DownsampleAccu<T: Pixel> {
    pub output_tile_info: TileInformation,
    pub output_grid: GridOrEmpty<GridBoundingBox2D, T>,
    pub input_global_geo_transform: GeoTransform,

    pub time: Option<TimeInterval>,
    pub cache_hint: CacheHint,
    pub pool: Arc<ThreadPool>,
}

impl<T: Pixel> DownsampleAccu<T> {
    pub fn new(
        output_tile_info: TileInformation,
        input_global_geo_transform: GeoTransform,
        time: Option<TimeInterval>,
        cache_hint: CacheHint,
        pool: Arc<ThreadPool>,
    ) -> Self {
        DownsampleAccu {
            output_tile_info,
            output_grid: GridOrEmpty::new_empty_shape(output_tile_info.global_pixel_bounds()),
            input_global_geo_transform,
            time,
            cache_hint,
            pool,
        }
    }
}

#[async_trait]
impl<T: Pixel> FoldTileAccu for DownsampleAccu<T> {
    type RasterType = T;

    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        // TODO: later do conversation of accu into tile here

        let output_tile = RasterTile2D::new_with_tile_info(
            self.time.expect("there is at least one input"),
            self.output_tile_info,
            0, // TODO: need band?
            self.output_grid.unbounded(),
            self.cache_hint,
        );

        Ok(output_tile)
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

impl<T: Pixel> FoldTileAccuMut for DownsampleAccu<T> {
    fn set_time(&mut self, time: TimeInterval) {
        self.time = Some(time);
    }

    fn set_cache_hint(&mut self, cache_hint: CacheHint) {
        self.cache_hint = cache_hint;
    }
}

pub fn create_accu<T: Pixel>(
    input_geo_transform: GeoTransform,
    _output_geo_transform: GeoTransform,
    tile_info: TileInformation,
    _query_rect: &RasterQueryRectangle,
    pool: Arc<ThreadPool>,
    _tiling_specification: TilingSpecification,
) -> impl Future<Output = Result<DownsampleAccu<T>>> + use<T> {
    crate::util::spawn_blocking(move || {
        DownsampleAccu::new(
            tile_info,
            input_geo_transform,
            None,
            CacheHint::max_duration(),
            pool.clone(),
        )
    })
    .map_err(From::from)
}

pub fn fold_future<T>(
    accu: DownsampleAccu<T>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<DownsampleAccu<T>>>
where
    T: Pixel,
{
    crate::util::spawn_blocking_with_thread_pool(accu.pool.clone(), || fold_impl(accu, tile)).then(
        |x| async move {
            match x {
                Ok(r) => Ok(r),
                Err(e) => Err(e.into()),
            }
        },
    )
}

pub fn fold_impl<T>(mut accu: DownsampleAccu<T>, tile: RasterTile2D<T>) -> DownsampleAccu<T>
where
    T: Pixel,
{
    // get the time now because it is not known when the accu was created
    accu.set_time(tile.time);
    accu.cache_hint.merge_with(&tile.cache_hint);

    // TODO: add a skip if both tiles are empty?
    if tile.is_empty() {
        // TODO: and ignore no-data.
        return accu;
    }

    // copy all input tiles into the accu to have all data for interpolation
    let mut accu_tile = accu.output_grid.into_materialized_masked_grid();
    let in_tile_grid = tile.into_inner_positioned_grid();
    let accu_geo_transform = accu.output_tile_info.global_geo_transform;
    let in_geo_transform = accu.input_global_geo_transform;

    let map_fn = |grid_idx: GridIdx2D, current_value: Option<T>| -> Option<T> {
        let accu_pixel_coord = accu_geo_transform.grid_idx_to_pixel_center_coordinate_2d(grid_idx); // use center coordinate similar to ArcGIS
        let source_pixel_idx = in_geo_transform.coordinate_to_grid_idx_2d(accu_pixel_coord);

        if in_tile_grid.contains(&source_pixel_idx) {
            in_tile_grid.get_at_grid_index_unchecked(source_pixel_idx)
        } else {
            current_value
        }
    };

    accu_tile.update_indexed_elements_parallel(map_fn);

    accu.output_grid = accu_tile.into();

    accu
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        ChunkByteSize, MockExecutionContext, RasterBandDescriptors, SpatialGridDescriptor,
        TimeDescriptor,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use futures::StreamExt;
    use geoengine_datatypes::primitives::TimeStep;
    use geoengine_datatypes::raster::{Grid, GridShape2D, RasterDataType};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn nearest_neighbor_4() {
        // In this test, 2x2 tiles with 4x4 pixels are downsampled using nearest neighbor to one tile with 4x4 pixels. The resolution is now 1/2 of the original resolution.
        // The test uses the following input:
        //
        // _1, _2, _3, _4 | 21, 22, 23, 24
        // _5, _6, _7, _8 | 25, 26, 27, 28
        // _9, 10, 11, 12 | 29, 30, 31, 32
        // 13, 14, 15, 16 | 33, 34, 35, 36
        // ---------------+---------------
        // 41, 42, 43, 44 | 61, 62, 63, 64
        // 45, 46, 47, 48 | 65, 66, 67, 68
        // 49, 50, 51, 52 | 69, 70, 71, 72
        // 53, 54, 55, 56 | 73, 74, 75, 76
        //
        // The input is downsampled to:
        //
        // _6. _8, 26, 28
        // 14, 16, 33, 36
        // 46, 48, 66, 68
        // 54, 56, 74, 76
        //
        // The center of each pixel is mapped to a coordinate. Then, for this coordinate the nearest pixel center is selected.
        // In this case, we have the special case that the pixel center of the target pixel hits the edge between two original pixels.
        // The pixel which "owns" the edge is selected because pixels are defiend from the upper left edge.
        // E.g. _6 is selected for the first pixel since it owns the edge between _1, _2, _5_ and _6.

        let in_geo_transform = GeoTransform::new(Coordinate2D::new(0.0, 0.0), 1.0, -1.0);
        let out_geo_transform = GeoTransform::new(Coordinate2D::new(0.0, 0.0), 2.0, -2.0);
        let tile_size_in_pixels = GridShape2D {
            shape_array: [4, 4],
        };

        let exe_ctx = MockExecutionContext::new_with_tiling_spec_and_thread_count(
            TilingSpecification::new(tile_size_in_pixels),
            8,
        );

        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size_in_pixels,
                    vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                )
                .unwrap()
                .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size_in_pixels,
                    vec![
                        21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
                    ],
                )
                .unwrap()
                .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [1, 0].into(),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size_in_pixels,
                    vec![
                        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56,
                    ],
                )
                .unwrap()
                .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [1, 1].into(),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size_in_pixels,
                    vec![
                        61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76,
                    ],
                )
                .unwrap()
                .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(
                Some(
                    TimeInterval::new(
                        data.first().unwrap().time.start(),
                        data.last().unwrap().time.end(),
                    )
                    .unwrap(),
                ),
                TimeStep::millis(5).unwrap(),
            ),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                in_geo_transform,
                GridBoundingBox2D::new_min_max(0, 7, 0, 7).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let downsampler = Downsampling {
            params: DownsamplingParams {
                sampling_method: DownsamplingMethod::NearestNeighbor,
                output_origin_reference: None,
                output_resolution: DownsamplingResolution::Resolution(SpatialResolution {
                    x: out_geo_transform.x_pixel_size(),
                    y: out_geo_transform.y_pixel_size().abs(),
                }),
            },
            sources: SingleRasterSource { raster: mrs1 },
        }
        .boxed();

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(0, 3, 0, 3).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            [0].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context(ChunkByteSize::test_default());

        let op = downsampler
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 1);

        let tile = result[0].as_ref().unwrap();
        let grid = tile.grid_array.clone().into_materialized_masked_grid();
        // _6. _8, 26, 28
        // 14, 16, 33, 36
        // 46, 48, 66, 68
        // 54, 56, 74, 76
        assert_eq!(
            grid.inner_grid.data,
            &[6, 8, 26, 28, 14, 16, 34, 36, 46, 48, 66, 68, 54, 56, 74, 76]
        );
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn nearest_neighbor_3() {
        // In this test, 3x3 tiles with 3x3 pixels are downsampled using nearest neighbor to one tile with 3x3 pixels. The resolution is now 1/3 of the original resolution.
        // The test uses the following input:
        //
        // _0, _1, _2 | 10, 11, 12 | 20, 21, 22
        // _3, _4, _5 | 13, 14, 15 | 23, 24, 25
        // _6, _7, _8 | 16, 17, 18 | 26, 27, 28
        // -----------+------------+-----------
        // 30, 31, 32 | 40, 41, 42 | 50, 51, 52
        // 33, 34, 35 | 43, 44, 45 | 53, 54, 55
        // 36, 37, 38 | 46, 47, 48 | 56, 57, 58
        // -----------+------------+-----------
        // 60, 61, 62 | 70, 71, 72 | 80, 81, 82
        // 63, 64, 65 | 73, 74, 75 | 83, 84, 85
        // 66, 67, 68 | 76, 77, 78 | 86, 87, 88
        //
        // The input is downsampled to:
        //
        // _4. 14, 24
        // 34, 44, 54
        // 64, 74, 84
        //
        // The center of each pixel is mapped to a coordinate. Then, for this coordinate the nearest pixel center is selected.
        // In this case, each pixel corresponds to the center of the corresponding tile.
        // E.g. _4 is selected for the first pixel since it is in the center of the tile.

        let in_geo_transform = GeoTransform::new(Coordinate2D::new(0.0, 0.0), 1.0, -1.0);
        let out_geo_transform = GeoTransform::new(Coordinate2D::new(0.0, 0.0), 3.0, -3.0);
        let tile_size_in_pixels = GridShape2D {
            shape_array: [3, 3],
        };

        let exe_ctx = MockExecutionContext::new_with_tiling_spec_and_thread_count(
            TilingSpecification::new(tile_size_in_pixels),
            8,
        );

        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(tile_size_in_pixels, vec![0, 1, 2, 3, 4, 5, 6, 7, 8])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size_in_pixels,
                    vec![10, 11, 12, 13, 14, 15, 16, 17, 18],
                )
                .unwrap()
                .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 2].into(),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size_in_pixels,
                    vec![20, 21, 22, 23, 24, 25, 26, 27, 28],
                )
                .unwrap()
                .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [1, 0].into(),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size_in_pixels,
                    vec![30, 31, 32, 33, 34, 35, 36, 37, 38],
                )
                .unwrap()
                .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [1, 1].into(),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size_in_pixels,
                    vec![40, 41, 42, 43, 44, 45, 46, 47, 48],
                )
                .unwrap()
                .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [1, 2].into(),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size_in_pixels,
                    vec![50, 51, 52, 53, 54, 55, 56, 57, 58],
                )
                .unwrap()
                .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [2, 0].into(),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size_in_pixels,
                    vec![60, 61, 62, 63, 64, 65, 66, 67, 68],
                )
                .unwrap()
                .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [2, 1].into(),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size_in_pixels,
                    vec![70, 71, 72, 73, 74, 75, 76, 77, 78],
                )
                .unwrap()
                .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [2, 2].into(),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size_in_pixels,
                    vec![80, 81, 82, 83, 84, 85, 86, 87, 88],
                )
                .unwrap()
                .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(
                Some(
                    TimeInterval::new(
                        data.first().unwrap().time.start(),
                        data.last().unwrap().time.end(),
                    )
                    .unwrap(),
                ),
                TimeStep::millis(5).unwrap(),
            ),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                in_geo_transform,
                GridBoundingBox2D::new_min_max(0, 8, 0, 8).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let downsampler = Downsampling {
            params: DownsamplingParams {
                sampling_method: DownsamplingMethod::NearestNeighbor,
                output_origin_reference: None,
                output_resolution: DownsamplingResolution::Resolution(SpatialResolution {
                    x: out_geo_transform.x_pixel_size(),
                    y: out_geo_transform.y_pixel_size().abs(),
                }),
            },
            sources: SingleRasterSource { raster: mrs1 },
        }
        .boxed();

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(0, 2, 0, 2).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            [0].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context(ChunkByteSize::test_default());

        let op = downsampler
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 1);

        let tile = result[0].as_ref().unwrap();
        let grid = tile.grid_array.clone().into_materialized_masked_grid();
        // _4. 14, 24
        // 34, 44, 54
        // 64, 74, 84

        assert_eq!(grid.inner_grid.data, &[4, 14, 24, 34, 44, 54, 64, 74, 84]);
    }
}
