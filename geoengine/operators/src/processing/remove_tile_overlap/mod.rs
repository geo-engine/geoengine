//! The `RemoveTileOverlap` operator crops a tile's overlap halo.
//!
//! It is the inverse of [`AddTileOverlap`]: each tile's data grid shrinks
//! symmetrically by the removed halo while its core region and georeference
//! stay untouched. Removing the full overlap restores plain core tiles, which
//! every downstream operator accepts.
//!
//! Typical use: after an ML segmentation model has run on padded tiles, crop
//! the model output back to cores so tiles can be written or rendered without
//! double counting.

use crate::{
    engine::{
        CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources,
        Operator, OperatorName, QueryContext, QueryProcessor, RasterOperator, RasterQueryProcessor,
        RasterResultDescriptor, SingleRasterSource, TypedRasterQueryProcessor,
        WorkflowOperatorPath,
    },
    error,
    optimization::OptimizationError,
    util,
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use geoengine_datatypes::primitives::{
    BandSelection, RasterQueryRectangle, SpatialResolution, TimeInterval,
};
use geoengine_datatypes::raster::{GridBoundingBox2D, Pixel, RasterTile2D, TileOverlap};
use serde::{Deserialize, Serialize};
use snafu::ensure;

/// Parameters of [`RemoveTileOverlap`].
///
/// `amount` is the halo cropped from *every side* of each tile. `None` removes
/// all available overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveTileOverlapParams {
    pub amount: Option<TileOverlap>,
}

impl RemoveTileOverlapParams {
    pub fn new(amount: Option<TileOverlap>) -> Self {
        Self { amount }
    }
}

/// This `QueryProcessor` crops the overlap halo of all tiles of its input raster.
pub type RemoveTileOverlap = Operator<RemoveTileOverlapParams, SingleRasterSource>;

impl OperatorName for RemoveTileOverlap {
    const TYPE_NAME: &'static str = "RemoveTileOverlap";
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for RemoveTileOverlap {
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
        let available = in_descriptor.spatial_grid_descriptor().tile_overlap();
        // default: strip the halo completely
        let removed = self.params.amount.unwrap_or(available);

        ensure!(
            removed.y <= available.y && removed.x <= available.x,
            error::NotEnoughTileOverlap {
                available,
                requested: removed
            }
        );

        let mut result_descriptor = in_descriptor.clone();
        result_descriptor.spatial_grid =
            result_descriptor
                .spatial_grid
                .with_tile_overlap(TileOverlap::new(
                    available.y - removed.y,
                    available.x - removed.x,
                ));

        Ok(Box::new(InitializedRemoveTileOverlap {
            name,
            path,
            removed,
            result_descriptor,
            raster_source: source,
        }))
    }

    span_fn!(RemoveTileOverlap);
}

/// Initialized form of the `RemoveTileOverlap` operator.
pub struct InitializedRemoveTileOverlap<O: InitializedRasterOperator> {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    removed: TileOverlap,
    result_descriptor: RasterResultDescriptor,
    raster_source: O,
}

impl<O: InitializedRasterOperator> InitializedRasterOperator for InitializedRemoveTileOverlap<O> {
    fn query_processor(&self) -> util::Result<TypedRasterQueryProcessor> {
        let source_processor = self.raster_source.query_processor()?;

        let res = call_on_generic_raster_processor!(
            source_processor,
            p => RemoveTileOverlapProcessor::new(
                p,
                self.result_descriptor.clone(),
                self.removed,
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
        RemoveTileOverlap::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<Box<dyn RasterOperator>, OptimizationError> {
        Ok(RemoveTileOverlap {
            // keep the resolved amount so optimization does not change semantics
            params: RemoveTileOverlapParams {
                amount: Some(self.removed),
            },
            sources: SingleRasterSource {
                raster: self.raster_source.optimize(target_resolution)?,
            },
        }
        .boxed())
    }
}

/// Query-time processor: crops each streamed tile by the resolved halo.
pub struct RemoveTileOverlapProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    source: Q,
    out_result_descriptor: RasterResultDescriptor,
    removed: TileOverlap,
}

impl<Q, P> RemoveTileOverlapProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    pub fn new(
        source: Q,
        out_result_descriptor: RasterResultDescriptor,
        removed: TileOverlap,
    ) -> Self {
        Self {
            source,
            out_result_descriptor,
            removed,
        }
    }
}

#[async_trait]
impl<Q, P> RasterQueryProcessor for RemoveTileOverlapProcessor<Q, P>
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
impl<Q, P> QueryProcessor for RemoveTileOverlapProcessor<Q, P>
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
        use futures::StreamExt;

        if self.removed.is_zero() {
            return self.source.query(query, ctx).await;
        }

        let removed = self.removed;
        let stream = self.source.query(query, ctx).await?;
        Ok(Box::pin(
            stream.map(move |tile| Ok(tile?.crop_overlap(removed)?)),
        ))
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.out_result_descriptor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        MockExecutionContext, RasterBandDescriptors, SpatialGridDescriptor, TimeDescriptor,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use crate::processing::add_tile_overlap::{AddTileOverlap, AddTileOverlapParams};
    use geoengine_datatypes::primitives::{CacheHint, TimeInterval};
    use geoengine_datatypes::raster::{
        GeoTransform, Grid2D, GridIdx2D, GridIndexAccess, GridShape2D, TileIdx, TileSize,
        TilesEqualIgnoringCacheHint,
    };
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
    use geoengine_datatypes::util::test::TestDefault;

    /// The same four constant core tiles as in the `add_tile_overlap` tests:
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
                        TileSize::new_y_x(2, 2),
                    ),
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        })
    }

    fn mock_execution_context() -> MockExecutionContext {
        let mut context = MockExecutionContext::test_default();
        // mock tiles are 2x2 pixels
        context.tiling_specification.tile_size = TileSize::new_y_x(2, 2);
        context
    }

    async fn query_operator(
        operator: Box<dyn RasterOperator>,
    ) -> util::Result<Vec<RasterTile2D<f64>>> {
        use futures::StreamExt;

        let execution_context = mock_execution_context();
        let query_ctx = execution_context.mock_query_context_test_default();

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

        let processor = initialized.query_processor()?.get_f64().unwrap();
        let mut stream = processor.query(query_rect, &query_ctx).await?;
        let mut tiles = Vec::new();
        while let Some(tile) = stream.next().await {
            tiles.push(tile?);
        }
        tiles.sort_by_key(|t| t.tile_position.grid_idx().0);
        Ok(tiles)
    }

    async fn remove_tiles(
        amount: Option<TileOverlap>,
        source: Box<dyn RasterOperator>,
    ) -> util::Result<Vec<RasterTile2D<f64>>> {
        query_operator(
            RemoveTileOverlap {
                params: RemoveTileOverlapParams { amount },
                sources: SingleRasterSource { raster: source },
            }
            .boxed(),
        )
        .await
    }

    fn padded_source(overlap: TileOverlap) -> Box<dyn RasterOperator> {
        AddTileOverlap {
            params: AddTileOverlapParams { overlap },
            sources: SingleRasterSource {
                raster: mock_raster_source(),
            },
        }
        .boxed()
    }

    #[tokio::test]
    async fn it_restores_core_tiles_after_padding() {
        // add a halo and strip it again: must reproduce the core tiles exactly
        let restored = remove_tiles(None, padded_source(TileOverlap::new(1, 1)))
            .await
            .unwrap();

        let original = query_operator(mock_raster_source()).await.unwrap();

        assert_eq!(original.len(), 4);
        assert!(original.tiles_equal_ignoring_cache_hint(&restored));
    }

    #[tokio::test]
    async fn it_removes_partial_overlap_and_keeps_remainder() {
        // remove only the x halo of a 1x1 halo tile
        let tiles = remove_tiles(
            Some(TileOverlap::new(0, 1)),
            padded_source(TileOverlap::new(1, 1)),
        )
        .await
        .unwrap();

        for tile in &tiles {
            assert_eq!(tile.overlap, TileOverlap::new(1, 0));
            // y keeps both halos, x loses both
            assert_eq!(tile.grid_array.shape_ref(), &GridShape2D::new_2d(4, 2));
        }

        // the top-left tile keeps its top no-data row and its data core
        let top_left = &tiles[0];
        let pixel = top_left
            .get_at_grid_index(GridIdx2D::new_y_x(0, 1))
            .unwrap();
        assert_eq!(pixel, None); // halo row above the dataset extent
        let pixel = top_left
            .get_at_grid_index(GridIdx2D::new_y_x(1, 1))
            .unwrap();
        assert_eq!(pixel, Some(1.)); // core value survived the crop
    }

    #[tokio::test]
    async fn it_rejects_removal_beyond_available_overlap() {
        let result = RemoveTileOverlap {
            params: RemoveTileOverlapParams {
                amount: Some(TileOverlap::new(1, 1)),
            },
            sources: SingleRasterSource {
                raster: mock_raster_source(), // has no overlap at all
            },
        }
        .boxed();

        let execution_context = mock_execution_context();
        let err = result
            .initialize(
                crate::engine::WorkflowOperatorPath::initialize_root(),
                &execution_context,
            )
            .await
            .err()
            .expect("must reject removal beyond available overlap");

        assert!(
            err.to_string().contains("Not enough tile overlap"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn it_passes_through_when_nothing_to_remove() {
        let tiles = remove_tiles(None, mock_raster_source()).await.unwrap();

        assert_eq!(tiles.len(), 4);
        for tile in &tiles {
            assert_eq!(tile.overlap, TileOverlap::zero());
            assert_eq!(tile.grid_array.shape_ref(), &GridShape2D::new_2d(2, 2));
        }
    }
}
