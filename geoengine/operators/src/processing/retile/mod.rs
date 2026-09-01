use crate::adapters::{
    FoldTileAccu, FoldTileAccuMut, RasterSubQueryAdapter, SubQueryTileAggregator,
};
use crate::engine::{
    CanonicOperatorName, InitializedRasterOperator, InitializedSources, Operator, OperatorName,
    QueryContext, QueryProcessor, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
    SingleRasterSource, SpatialGridDescriptor, TypedRasterQueryProcessor, WorkflowOperatorPath,
};
use crate::optimization::OptimizationError;
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, TryFutureExt};
use geoengine_datatypes::primitives::{
    BandSelection, CacheHint, Coordinate2D, RasterQueryRectangle, SpatialResolution, TimeInterval,
};
use geoengine_datatypes::raster::{
    ChangeGridBounds, GeoTransform, GridBoundingBox2D, GridContains, GridIdx2D, GridIndexAccess,
    GridOrEmpty, Pixel, RasterTile2D, SpatialGridDefinition, TileInformation, TileSize, TilingGrid,
    TilingSpecification, TilingStrategy, UpdateIndexedElementsParallel,
};
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

/// Re-tiles raster data from one grid layout to another.
///
/// Re-aligns raster tiles between pixel-aligned grids without resampling.
///

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReTileParams {
    /// Override the output tile size. If `None`, the global tiling specification's tile size is used.
    pub tile_size: Option<TileSize>,
    /// Override the output tiling origin. If `None`, the dataset's own geo-transform origin is used
    /// (i.e. tiles are aligned to the data's native grid).
    pub origin: Option<Coordinate2D>,
}

pub type ReTile = Operator<ReTileParams, SingleRasterSource>;

impl OperatorName for ReTile {
    const TYPE_NAME: &'static str = "ReTile";
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for ReTile {
    /// Initializes the `ReTile` operator.
    ///
    /// Derives the output tiling specification from the input dataset's spatial grid:
    /// - **origin**: from `ReTileParams::origin`, or the input dataset's geo-transform origin
    /// - **tile size**: from `ReTileParams::tile_size`, or the global tiling spec
    ///
    /// Computes the output spatial grid (same spatial extent and resolution as input, but
    /// aligned to the new tiling origin) and stores it in the result descriptor.
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);
        let initialized_source = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?;

        let in_descriptor = initialized_source.raster.result_descriptor();
        let in_spatial_grid = in_descriptor.spatial_grid_descriptor();

        let output_origin = self
            .params
            .origin
            .unwrap_or_else(|| in_spatial_grid.geo_transform().origin_coordinate);

        let output_tile_size = self
            .params
            .tile_size
            .unwrap_or_else(|| context.tiling_specification().tile_size);

        if output_tile_size.axis_size_y() == 0 || output_tile_size.axis_size_x() == 0 {
            return Err(crate::error::Error::InvalidTileSize {
                tile_size: output_tile_size,
            });
        }

        let tiling_spec = TilingSpecification::new(output_tile_size, output_origin);

        let output_tiling_grid = TilingGrid::from_spatial_grid_with_origin(
            in_spatial_grid.spatial_grid,
            output_origin,
            output_tile_size,
        )
        .ok_or_else(|| crate::error::Error::ReTileGridMismatch {
            input: in_spatial_grid.spatial_grid,
            output: SpatialGridDefinition::new(
                GeoTransform::new(
                    output_origin,
                    in_spatial_grid.geo_transform().x_pixel_size(),
                    in_spatial_grid.geo_transform().y_pixel_size(),
                ),
                GridBoundingBox2D::new_unchecked([0, 0], [0, 0]),
            ),
        })?;
        let output_spatial_grid = SpatialGridDescriptor::new_source(
            output_tiling_grid.to_spatial_grid(),
            output_tile_size,
        );

        let out_descriptor = RasterResultDescriptor {
            spatial_reference: in_descriptor.spatial_reference,
            data_type: in_descriptor.data_type,
            time: in_descriptor.time,
            spatial_grid: output_spatial_grid,
            bands: in_descriptor.bands.clone(),
        };

        Ok(InitializedReTile {
            name,
            path,
            output_result_descriptor: out_descriptor,
            raster_source: initialized_source.raster,
            tiling_specification: tiling_spec,
        }
        .boxed())
    }

    span_fn!(ReTile);
}

/// The initialized (ready-to-execute) form of the `ReTile` operator.
///
/// Stores the output result descriptor (with the re-aligned spatial grid),
/// the source operator, and the output tiling specification for use in query
/// processing and optimization.
pub struct InitializedReTile<O: InitializedRasterOperator> {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    output_result_descriptor: RasterResultDescriptor,
    raster_source: O,
    tiling_specification: TilingSpecification,
}

impl<O: InitializedRasterOperator> InitializedRasterOperator for InitializedReTile<O> {
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_processor = self.raster_source.query_processor()?;

        let res = call_on_generic_raster_processor!(
            source_processor,
            p => ReTileProcessor::new(
                p,
                self.output_result_descriptor.clone(),
                self.tiling_specification,
            )
            .boxed()
            .into()
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
        ReTile::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<Box<dyn RasterOperator>, OptimizationError> {
        Ok(ReTile {
            params: ReTileParams {
                tile_size: Some(self.tiling_specification.tile_size),
                origin: Some(self.tiling_specification.tiling_origin_reference()),
            },
            sources: SingleRasterSource {
                raster: self.raster_source.optimize(target_resolution)?,
            },
        }
        .boxed())
    }
}

/// The query-time processor for `ReTile`. Holds the source query processor and the
/// output tiling specification. Delegates to `RasterSubQueryAdapter` for the actual
/// tile-level re-mapping.
pub struct ReTileProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Copy,
{
    source: Q,
    out_result_descriptor: RasterResultDescriptor,
    tiling_specification: TilingSpecification,
}

impl<Q, P> ReTileProcessor<Q, P>
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
impl<Q, P> RasterQueryProcessor for ReTileProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    type RasterType = P;

    async fn _time_query<'a>(
        &'a self,
        query: TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<TimeInterval>>> {
        self.source.time_query(query, ctx).await
    }
}

#[async_trait]
impl<Q, P> QueryProcessor for ReTileProcessor<Q, P>
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
        let in_spatial_grid = self.source.result_descriptor().spatial_grid_descriptor();
        let out_spatial_grid = self.result_descriptor().spatial_grid_descriptor();

        if in_spatial_grid == out_spatial_grid {
            return self.source.query(query, ctx).await;
        }

        // Use the stored tiling specification (with the correct output origin)
        // instead of the context's global tiling spec.
        // Similarly, derive the input geo transform from the source's spatial grid directly.
        let tiling_strategy = TilingStrategy::new(
            self.tiling_specification.tile_size,
            out_spatial_grid.geo_transform(),
        );

        let input_geo_transform = in_spatial_grid.geo_transform();

        let output_geo_transform = out_spatial_grid.geo_transform();

        let sub_query = ReTileSubQuery {
            input_geo_transform,
            output_geo_transform,
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

/// Sub-query implementation that bridges the output tile grid to input queries.
///
/// For each output tile, this struct:
/// 1. **Computes the spatial bounds** of the output tile using the output geo-transform.
/// 2. **Converts those bounds to input pixel coordinates** using the input geo-transform,
///    producing a query rectangle for the source operator.
/// 3. **Provides the fold function** (`re_tile_fold`) that maps input data to output pixels.
///
/// Both geo-transforms have the same pixel size (resolution) — only the origin differs.
/// This means the mapping is a direct pixel-by-pixel copy, not an interpolation.
#[derive(Debug, Clone)]
pub struct ReTileSubQuery<T> {
    /// The geo-transform of the source (input) tiles — used to map spatial coordinates
    /// back to input pixel indices.
    input_geo_transform: GeoTransform,
    /// The geo-transform of the output tiles — used to determine spatial bounds for
    /// each output tile and to compute the pixel center coordinate for the mapping.
    output_geo_transform: GeoTransform,
    _phantom_pixel_type: PhantomData<T>,
}

impl<'a, T> SubQueryTileAggregator<'a, T> for ReTileSubQuery<T>
where
    T: Pixel,
{
    type FoldFuture = BoxFuture<'a, Result<ReTileAccu<T>>>;
    type FoldMethod = fn(ReTileAccu<T>, RasterTile2D<T>) -> BoxFuture<'a, Result<ReTileAccu<T>>>;
    type TileAccu = ReTileAccu<T>;
    type TileAccuFuture = BoxFuture<'a, Result<Self::TileAccu>>;

    /// Creates a fresh accumulator for one output tile.
    ///
    /// The output grid starts empty (all no-data). As input tiles are folded in,
    /// their values are mapped to the correct output pixel positions.
    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        let output_grid = GridOrEmpty::new_empty_shape(tile_info.global_pixel_bounds());
        let input_geo_transform = self.input_geo_transform;
        let pool = pool.clone();
        Box::pin(async move {
            Ok(ReTileAccu {
                output_tile_info: tile_info,
                output_grid,
                input_geo_transform,
                time: None,
                cache_hint: CacheHint::max_duration(),
                pool,
            })
        })
    }

    /// Determines which input region to query for a given output tile.
    ///
    /// Converts the output tile's pixel bounds to spatial coordinates via the output
    /// geo-transform, then converts those spatial bounds to input pixel bounds via
    /// the input geo-transform. The resulting query rectangle tells the source
    /// operator which pixels to return.
    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        time: TimeInterval,
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        let out_tile_pixel_bounds = tile_info.global_pixel_bounds();
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
        re_tile_fold
    }
}

/// Accumulator for the fold-based re-tiling.
///
/// Created for each output tile. Starts as an empty (no-data) grid of the output tile's
/// size. Input tiles are folded into it by mapping each output pixel to its source pixel
/// in the input tile and copying the value.
#[derive(Clone, Debug)]
pub struct ReTileAccu<T: Pixel> {
    pub output_tile_info: TileInformation,
    pub output_grid: GridOrEmpty<GridBoundingBox2D, T>,
    pub input_geo_transform: GeoTransform,
    pub time: Option<TimeInterval>,
    pub cache_hint: CacheHint,
    pub pool: Arc<ThreadPool>,
}

#[async_trait]
impl<T: Pixel> FoldTileAccu for ReTileAccu<T> {
    type RasterType = T;

    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        let time = self
            .time
            .ok_or_else(|| crate::error::Error::InvalidOperatorSpec {
                reason: "ReTile: no input tiles were folded".into(),
            })?;
        let output_tile = RasterTile2D::new_with_tile_info(
            time,
            self.output_tile_info,
            0,
            self.output_grid.unbounded(),
            self.cache_hint,
        );
        Ok(output_tile)
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

impl<T: Pixel> FoldTileAccuMut for ReTileAccu<T> {
    fn set_time(&mut self, time: TimeInterval) {
        self.time = Some(time);
    }

    fn set_cache_hint(&mut self, cache_hint: CacheHint) {
        self.cache_hint = cache_hint;
    }
}

/// Fold function that re-maps input tile data to the output grid.
///
/// For each pixel in the output accumulator, computes the corresponding input pixel
/// coordinate by:
/// 1. Getting the output pixel's center coordinate from `accu_geo_transform`.
/// 2. Converting that coordinate to an input pixel index via `input_geo_transform`.
/// 3. If the input tile contains that pixel, copying the value; otherwise keeping
///    the current accumulator value (initially no-data).
///
/// This is a nearest-neighbor / identity mapping — no interpolation is performed.
/// Since input and output have the same pixel size, each output pixel maps to
/// exactly one input pixel (or nothing, at dataset edges).
pub fn re_tile_fold<T: Pixel>(
    mut accu: ReTileAccu<T>,
    tile: RasterTile2D<T>,
) -> BoxFuture<'static, Result<ReTileAccu<T>>> {
    crate::util::spawn_blocking_with_thread_pool(accu.pool.clone(), move || {
        accu.set_time(tile.time);
        accu.cache_hint.merge_with(&tile.cache_hint);

        if tile.is_empty() {
            return accu;
        }

        // Source tile is in input_geo_transform space.
        // Output tile is in output_geo_transform space.
        // Same pixel size, so mapping is a simple coordinate lookup.
        let mut accu_tile = accu.output_grid.into_materialized_masked_grid();
        let in_tile_grid = tile.into_inner_positioned_grid();
        let accu_geo_transform = accu.output_tile_info.global_geo_transform;
        let in_geo_transform = accu.input_geo_transform;

        let map_fn = |grid_idx: GridIdx2D, current_value: Option<T>| -> Option<T> {
            // `grid_idx` is the global pixel index of the output grid: the accumulator
            // is positioned at `tile_info.global_pixel_bounds()`, so its indices are global.
            let accu_pixel_coord =
                accu_geo_transform.grid_idx_to_pixel_center_coordinate_2d(grid_idx);
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
    })
    .map_err(Into::into)
    .boxed()
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
    use geoengine_datatypes::raster::{Grid, GridShape2D, RasterDataType, TileIdx, TileSize};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;

    #[tokio::test]
    async fn retile_same_origin_passthrough() {
        let in_geo_transform = GeoTransform::new(Coordinate2D::new(0.0, 0.0), 1.0, -1.0);

        let exe_ctx = MockExecutionContext::new_with_tiling_spec_and_thread_count(
            TilingSpecification::with_zero_origin(TileSize::new_y_x(4, 4)),
            8,
        );

        let data: Vec<RasterTile2D<u8>> = vec![RasterTile2D {
            time: TimeInterval::new_unchecked(0, 5),
            tile_position: TileIdx::new_y_x(0, 0),
            band: 0,
            global_geo_transform: in_geo_transform,
            grid_array: Grid::new(
                GridShape2D {
                    shape_array: [4, 4],
                },
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            )
            .unwrap()
            .into(),
            properties: Default::default(),
            cache_hint: CacheHint::default(),
        }];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(Some(TimeInterval::new_unchecked(0, 5))),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                in_geo_transform,
                GridBoundingBox2D::new_min_max(0, 3, 0, 3).unwrap(),
                TileSize::new_y_x(4, 4),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data,
                result_descriptor,
            },
        }
        .boxed();

        let retile = ReTile {
            params: ReTileParams {
                tile_size: None,
                origin: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(0, 3, 0, 3).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            [0].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context(ChunkByteSize::test_default());

        let op = retile
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
        assert_eq!(
            grid.inner_grid.data,
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn retile_different_origin() {
        // Input: origin (0,0), 2x2 tiles of 4x4 pixels each → 8x8 pixel grid
        // Output: origin (2, -2), same tile size
        let in_geo_transform = GeoTransform::new(Coordinate2D::new(0.0, 0.0), 1.0, -1.0);
        let tile_size = TileSize::new_y_x(4, 4);

        let exe_ctx = MockExecutionContext::new_with_tiling_spec_and_thread_count(
            TilingSpecification::with_zero_origin(tile_size),
            8,
        );

        // 2x2 input tiles covering [0,7] x [0,7]
        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(0, 0),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size.grid_shape(),
                    vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                )
                .unwrap()
                .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(0, 1),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size.grid_shape(),
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
                tile_position: TileIdx::new_y_x(1, 0),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size.grid_shape(),
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
                tile_position: TileIdx::new_y_x(1, 1),
                band: 0,
                global_geo_transform: in_geo_transform,
                grid_array: Grid::new(
                    tile_size.grid_shape(),
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
            time: TimeDescriptor::new_irregular(Some(TimeInterval::new_unchecked(0, 5))),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                in_geo_transform,
                GridBoundingBox2D::new_min_max(0, 7, 0, 7).unwrap(),
                TileSize::new_y_x(4, 4),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data,
                result_descriptor,
            },
        }
        .boxed();

        let new_origin = Coordinate2D::new(2.0, -2.0);
        let retile = ReTile {
            params: ReTileParams {
                tile_size: None,
                origin: Some(new_origin),
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(0, 7, 0, 7).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            [0].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context(ChunkByteSize::test_default());

        let op = retile
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

        // Output tile (0,0) with origin (2, -2) covers input pixels [2,5] x [2,5]:
        //   (0,0)→(2,2)=11  (0,1)→(2,3)=12  (0,2)→(2,4)=29  (0,3)→(2,5)=30
        //   (1,0)→(3,2)=15  (1,1)→(3,3)=16  (1,2)→(3,4)=33  (1,3)→(3,5)=34
        //   (2,0)→(4,2)=43  (2,1)→(4,3)=44  (2,2)→(4,4)=61  (2,3)→(4,5)=62
        //   (3,0)→(5,2)=47  (3,1)→(5,3)=48  (3,2)→(5,4)=65  (3,3)→(5,5)=66
        //
        // Input tile (1,1) data layout (4x4):
        //   row 0: 61, 62, 63, 64   (global pixels row 4, cols 4-7)
        //   row 1: 65, 66, 67, 68   (global pixels row 5, cols 4-7)
        //   row 2: 69, 70, 71, 72   (global pixels row 6, cols 4-7)
        //   row 3: 73, 74, 75, 76   (global pixels row 7, cols 4-7)
        let expected_tile_data = vec![
            11, 12, 29, 30, 15, 16, 33, 34, 43, 44, 61, 62, 47, 48, 65, 66,
        ];

        assert!(!result.is_empty(), "should have at least one tile");

        let matching = result.iter().find(|t| {
            let tile = t.as_ref().unwrap();
            let grid = tile.grid_array.clone().into_materialized_masked_grid();
            grid.inner_grid.data == expected_tile_data
        });

        assert!(
            matching.is_some(),
            "Expected tile with data {:?} not found. Got tiles: {:?}",
            expected_tile_data,
            result
                .iter()
                .map(|t| t
                    .as_ref()
                    .unwrap()
                    .grid_array
                    .clone()
                    .into_materialized_masked_grid()
                    .inner_grid
                    .data)
                .collect::<Vec<_>>()
        );
    }
}
