//! The `AddTileOverlap` operator equips every output tile with an overlap
//! (halo) around its core region.
//!
//! For each core tile the source is sub-queried for the tile's *data* bounds
//! (core expanded by the halo). Neighbor data is blitted into a padded
//! accumulator; regions beyond the dataset extent remain no-data. The output
//! descriptor declares the halo so downstream operators can discriminate.
//!
//! Typical use: ML segmentation models whose convolutions require more input
//! pixels than they produce. Pair with `RemoveTileOverlap` to crop the halo.

use crate::{
    adapters::{FoldTileAccu, RasterSubQueryAdapter, SubQueryTileAggregator},
    engine::InitializedSources,
    engine::{
        CanonicOperatorName, ExecutionContext, InitializedRasterOperator, Operator, OperatorName,
        QueryContext, QueryProcessor, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
        SingleRasterSource, TypedRasterQueryProcessor, WorkflowOperatorPath,
    },
    optimization::OptimizationError,
    util,
};
use async_trait::async_trait;
use futures::future::{Ready, ready};
use futures::stream::BoxStream;
use geoengine_datatypes::primitives::{
    BandSelection, CacheHint, RasterQueryRectangle, SpatialResolution, TimeInterval,
};
use geoengine_datatypes::raster::{
    ChangeGridBounds, GridBlit, GridBoundingBox2D, GridOrEmpty, Pixel, RasterTile2D,
    TileInformation, TileOverlap,
};
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::sync::Arc;

/// Parameters of [`AddTileOverlap`]: the halo size per axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddTileOverlapParams {
    pub overlap: TileOverlap,
}

impl AddTileOverlapParams {
    pub fn new(overlap: TileOverlap) -> Self {
        Self { overlap }
    }
}

/// This `QueryProcessor` adds an overlap halo to all tiles of its input raster.
pub type AddTileOverlap = Operator<AddTileOverlapParams, SingleRasterSource>;

impl OperatorName for AddTileOverlap {
    const TYPE_NAME: &'static str = "AddTileOverlap";
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for AddTileOverlap {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> util::Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        let source = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?
            .raster;

        let in_descriptor = source.result_descriptor();
        let overlap = self.params.overlap;

        // the halo must be strictly smaller than the core on each axis
        ensure!(
            overlap.is_valid_for_tile_size(in_descriptor.tiling_grid_definition().tile_size),
            crate::error::InvalidOverlap { overlap }
        );

        // v1 restriction: stacking halos on halos is rejected; remove the
        // existing overlap first to define a new one
        in_descriptor.ensure_no_tile_overlap(AddTileOverlap::TYPE_NAME)?;

        let mut result_descriptor = in_descriptor.clone();
        result_descriptor.spatial_grid = result_descriptor.spatial_grid.with_tile_overlap(overlap);

        Ok(Box::new(InitializedAddTileOverlap {
            name,
            path,
            params: self.params,
            result_descriptor,
            raster_source: source,
        }))
    }

    span_fn!(AddTileOverlap);
}

/// Initialized form of the `AddTileOverlap` operator.
pub struct InitializedAddTileOverlap<O: InitializedRasterOperator> {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    #[allow(dead_code)]
    params: AddTileOverlapParams,
    result_descriptor: RasterResultDescriptor,
    raster_source: O,
}

impl<O: InitializedRasterOperator> InitializedRasterOperator for InitializedAddTileOverlap<O> {
    fn query_processor(&self) -> util::Result<TypedRasterQueryProcessor> {
        let source_processor = self.raster_source.query_processor()?;

        let res = call_on_generic_raster_processor!(
            source_processor,
            p => AddTileOverlapProcessor::new(
                p,
                self.result_descriptor.clone(),
            )
            .boxed()
            .into()
        );

        Ok(res)
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        AddTileOverlap::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<Box<dyn RasterOperator>, OptimizationError> {
        Ok(AddTileOverlap {
            params: AddTileOverlapParams {
                overlap: self.result_descriptor.spatial_grid.tile_overlap(),
            },
            sources: SingleRasterSource {
                raster: self.raster_source.optimize(target_resolution)?,
            },
        }
        .boxed())
    }
}

/// Query-time processor: wraps the source stream with padded tiles.
pub struct AddTileOverlapProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    source: Q,
    out_result_descriptor: RasterResultDescriptor,
}

impl<Q, P> AddTileOverlapProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    pub fn new(source: Q, out_result_descriptor: RasterResultDescriptor) -> Self {
        Self {
            source,
            out_result_descriptor,
        }
    }
}

#[async_trait]
impl<Q, P> RasterQueryProcessor for AddTileOverlapProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    type RasterType = P;

    async fn _time_query<'a>(
        &'a self,
        query: TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> util::Result<BoxStream<'a, util::Result<TimeInterval>>> {
        self.source.time_query(query, ctx).await
    }
}

#[async_trait]
impl<Q, P> QueryProcessor for AddTileOverlapProcessor<Q, P>
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
    ) -> util::Result<BoxStream<'a, util::Result<Self::Output>>> {
        // the out descriptor's strategy carries the declared overlap, so the
        // adapter enumerates cores and this aggregator pads them
        let tiling_strategy = self.out_result_descriptor.tiling_strategy();

        debug_assert_eq!(
            tiling_strategy.overlap,
            self.out_result_descriptor.spatial_grid.tile_overlap()
        );

        let time_stream = self.time_query(query.time_interval(), ctx).await?;

        Ok(Box::pin(RasterSubQueryAdapter::<'a, P, _, _, _>::new(
            &self.source,
            query,
            tiling_strategy,
            ctx,
            OverlapAggregator,
            time_stream,
        )))
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.out_result_descriptor
    }
}

/// Sub-query aggregator that pads each enumerated core tile with neighbor data.
///
/// The accumulator grid spans the tile's *data* bounds; the fold blits every
/// intersecting source tile into it. Missing neighbors stay no-data.
#[derive(Debug, Clone, Copy, Default)]
struct OverlapAggregator;

impl<P> SubQueryTileAggregator<'_, P> for OverlapAggregator
where
    P: Pixel,
{
    type FoldFuture = Ready<util::Result<Self::TileAccu>>;
    type FoldMethod = fn(Self::TileAccu, RasterTile2D<P>) -> Self::FoldFuture;
    type TileAccu = OverlapAccu<P>;
    type TileAccuFuture = Ready<util::Result<Self::TileAccu>>;

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        ready(Ok(OverlapAccu {
            tile_info,
            grid: GridOrEmpty::new_empty_shape(tile_info.data_pixel_bounds()),
            time: query_rect.time_interval(),
            cache_hint: CacheHint::max_duration(),
            pool: pool.clone(),
        }))
    }

    /// Sub-query the *data* bounds instead of only the core bounds.
    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        time: TimeInterval,
        band_idx: u32,
    ) -> util::Result<Option<RasterQueryRectangle>> {
        Ok(Some(RasterQueryRectangle::new(
            tile_info.data_pixel_bounds(),
            time,
            band_idx.into(),
        )))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        |mut accu, tile| {
            // time and cache hint are taken from the last folded tile
            let time = tile.time;
            let cache_hint = tile.cache_hint;
            if !tile.is_empty() {
                accu.grid.grid_blit_from(&tile.into_inner_positioned_grid());
            }
            accu.time = time;
            accu.cache_hint = cache_hint;
            ready(Ok(accu))
        }
    }
}

/// Accumulator holding the padded tile under construction.
#[derive(Clone, Debug)]
struct OverlapAccu<P: Pixel> {
    tile_info: TileInformation,
    grid: GridOrEmpty<GridBoundingBox2D, P>,
    time: TimeInterval,
    cache_hint: CacheHint,
    pool: Arc<ThreadPool>,
}

#[async_trait]
impl<P: Pixel> FoldTileAccu for OverlapAccu<P> {
    type RasterType = P;

    async fn into_tile(self) -> util::Result<RasterTile2D<Self::RasterType>> {
        Ok(RasterTile2D::new_with_tile_info(
            self.time,
            self.tile_info,
            0,
            self.grid.unbounded(),
            self.cache_hint,
        ))
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        MockExecutionContext, RasterBandDescriptors, SpatialGridDescriptor, TimeDescriptor,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use futures::StreamExt;
    use geoengine_datatypes::primitives::TimeInterval;
    use geoengine_datatypes::raster::{
        BoundedGrid, GeoTransform, Grid2D, GridBounds, GridIdx2D, GridIndexAccess, GridShape2D,
        TileIdx, TileSize,
    };
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
    use geoengine_datatypes::util::test::TestDefault;

    /// Four constant-valued core tiles of 2x2 pixels:
    ///
    /// ```text
    /// 1 | 2
    /// -----
    /// 3 | 4
    /// ```
    fn mock_raster_source() -> Box<MockRasterSource<f64>> {
        let geo_transform = GeoTransform::new((0., 0.).into(), 1., -1.);
        let tile_shape = GridShape2D::new_2d(2, 2);

        let data = [[0usize, 0], [0, 1], [1, 0], [1, 1]]
            .into_iter()
            .map(|tile_position| {
                RasterTile2D::new(
                    TimeInterval::default(),
                    TileIdx::new_y_x(tile_position[0] as isize, tile_position[1] as isize),
                    0,
                    geo_transform,
                    Grid2D::new_filled(
                        tile_shape,
                        (tile_position[0] * 2 + tile_position[1] + 1) as f64,
                    )
                    .into(),
                    CacheHint::default(),
                )
            })
            .collect();

        Box::new(MockRasterSource {
            params: MockRasterSourceParams {
                data,
                result_descriptor: RasterResultDescriptor {
                    data_type: geoengine_datatypes::raster::RasterDataType::F64,
                    spatial_reference: SpatialReferenceOption::SpatialReference(
                        SpatialReference::epsg_4326(),
                    ),
                    time: TimeDescriptor::new_irregular(Some(TimeInterval::default())),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        geo_transform,
                        GridBoundingBox2D::new_min_max(0, 3, 0, 3).unwrap(),
                        TileSize::new(2, 2),
                    ),
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        })
    }

    fn mock_execution_context() -> MockExecutionContext {
        let mut context = MockExecutionContext::test_default();
        // mock tiles are 2x2 pixels
        context.tiling_specification.tile_size = TileSize(GridShape2D::new_2d(2, 2));
        context
    }

    async fn overlapped_tiles(overlap: TileOverlap) -> util::Result<Vec<RasterTile2D<f64>>> {
        let operator = AddTileOverlap {
            params: AddTileOverlapParams { overlap },
            sources: SingleRasterSource {
                raster: mock_raster_source(),
            },
        }
        .boxed();

        let execution_context = mock_execution_context();
        let initialized = operator
            .initialize(
                crate::engine::WorkflowOperatorPath::initialize_root(),
                &execution_context,
            )
            .await?;

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(0, 3, 0, 3).unwrap(),
            TimeInterval::default(),
            BandSelection::first(),
        );

        let query_ctx = execution_context.mock_query_context_test_default();
        let processor = initialized.query_processor()?.get_f64().unwrap();
        let mut stream = processor.query(query_rect, &query_ctx).await?;
        let mut tiles = Vec::new();
        while let Some(tile) = stream.next().await {
            tiles.push(tile?);
        }
        tiles.sort_by_key(|t| *t.tile_position.0.inner());
        Ok(tiles)
    }

    #[tokio::test]
    async fn it_pads_interior_tiles_with_neighbor_data() {
        let tiles = overlapped_tiles(TileOverlap::new(1, 1)).await.unwrap();

        assert_eq!(tiles.len(), 4);
        for tile in &tiles {
            assert_eq!(tile.overlap, TileOverlap::new(1, 1));
            // padded shape: core + halo on every side
            assert_eq!(tile.grid_array.shape_ref(), &GridShape2D::new_2d(4, 4));
        }
    }

    #[tokio::test]
    async fn it_fills_halo_from_neighbors_and_border_with_no_data() {
        let tiles = overlapped_tiles(TileOverlap::new(1, 1)).await.unwrap();

        let top_left = &tiles[0];
        assert_eq!(top_left.tile_position, TileIdx::new_y_x(0, 0));

        // local pixel (y, x) maps to global (-1 + y, -1 + x); values follow the
        // four-constant-tile layout, no-data beyond the dataset extent
        let expected: [[Option<f64>; 4]; 4] = [
            [None, None, None, None],
            [None, Some(1.), Some(1.), Some(2.)],
            [None, Some(1.), Some(1.), Some(2.)],
            [None, Some(3.), Some(3.), Some(4.)],
        ];
        for (y, row) in expected.iter().enumerate() {
            for (x, value) in row.iter().enumerate() {
                let pixel = top_left
                    .get_at_grid_index(GridIdx2D::new_y_x(y as isize, x as isize))
                    .unwrap();
                assert_eq!(pixel, *value, "at local ({y},{x})");
            }
        }
    }

    #[tokio::test]
    async fn it_keeps_georeference_of_padded_data() {
        let tiles = overlapped_tiles(TileOverlap::new(1, 1)).await.unwrap();

        let top_left = &tiles[0];
        // data starts one halo pixel before the core anchor
        assert_eq!(
            top_left.bounding_box().min_index(),
            GridIdx2D::new_y_x(-1, -1)
        );
        assert_eq!(
            top_left.bounding_box().max_index(),
            GridIdx2D::new_y_x(2, 2)
        );
        // the core anchor is unaffected by the overlap
        assert_eq!(
            top_left.tile_information().core_pixel_bounds().min_index(),
            GridIdx2D::new_y_x(0, 0)
        );
    }

    #[tokio::test]
    async fn it_rejects_overlapping_inputs() {
        // wrap an already-overlapped stream in a second AddTileOverlap
        let inner = AddTileOverlap {
            params: AddTileOverlapParams {
                overlap: TileOverlap::new(1, 1),
            },
            sources: SingleRasterSource {
                raster: mock_raster_source(),
            },
        }
        .boxed();

        let outer = AddTileOverlap {
            params: AddTileOverlapParams {
                overlap: TileOverlap::new(1, 1),
            },
            sources: SingleRasterSource { raster: inner },
        }
        .boxed();

        let execution_context = mock_execution_context();
        let result = outer
            .initialize(
                crate::engine::WorkflowOperatorPath::initialize_root(),
                &execution_context,
            )
            .await;

        let err = result.err().expect("must reject overlapping input");
        assert!(
            err.to_string()
                .contains("does not support overlapping tiles"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn it_rejects_overlap_larger_than_core() {
        let operator = AddTileOverlap {
            params: AddTileOverlapParams {
                overlap: TileOverlap::new(3, 1), // core is only 2x2
            },
            sources: SingleRasterSource {
                raster: mock_raster_source(),
            },
        }
        .boxed();

        let execution_context = mock_execution_context();
        let result = operator
            .initialize(
                crate::engine::WorkflowOperatorPath::initialize_root(),
                &execution_context,
            )
            .await;

        let err = result.err().expect("must reject too-large overlap");
        assert!(
            err.to_string().contains("Invalid tile overlap"),
            "unexpected error: {err}"
        );
    }
}
