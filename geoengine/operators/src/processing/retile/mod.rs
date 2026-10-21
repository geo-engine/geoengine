use crate::adapters::{
    FoldTileAccu, FoldTileAccuMut, RasterSubQueryAdapter, SubQueryTileAggregator,
};
use crate::engine::{QueryContext, QueryProcessor, RasterQueryProcessor, RasterResultDescriptor};
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, TryFutureExt};
use geoengine_datatypes::primitives::{
    BandSelection, CacheHint, RasterQueryRectangle, TimeInterval,
};
use geoengine_datatypes::raster::{
    ChangeGridBounds, GeoTransform, GridBoundingBox2D, GridContains, GridIdx2D, GridIndexAccess,
    GridOrEmpty, Pixel, RasterTile2D, TileInformation, TilingSpecification, TilingStrategy,
    UpdateIndexedElementsParallel,
};
use rayon::ThreadPool;
use std::marker::PhantomData;
use std::sync::Arc;

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
        query_rect: RasterQueryRectangle,
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
                // An output tile can fall outside every input's extent (e.g. the
                // union grid of a `RasterStacker`), in which case the fold sees no
                // input tiles. Fall back to the query interval like
                // `AddTileOverlap` rather than failing the query.
                time: Some(query_rect.time_interval()),
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
        let time = self.time.unwrap_or_default();
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

    use geoengine_datatypes::raster::{TileIdx, TileSize};

    use geoengine_datatypes::util::test::TestDefault;

    #[tokio::test]
    async fn it_folds_to_an_empty_tile_when_no_input_tiles_arrive() {
        use geoengine_datatypes::raster::GridSize;

        let accu = ReTileAccu::<u8> {
            output_tile_info: TileInformation {
                global_geo_transform: GeoTransform::test_default(),
                tile_position: TileIdx::new_y_x(0, 0),
                tile_size: TileSize::new_unchecked(4, 4),
            },
            output_grid: GridOrEmpty::new_empty_shape(
                GridBoundingBox2D::new_min_max(0, 3, 0, 3).unwrap(),
            ),
            input_geo_transform: GeoTransform::test_default(),
            time: None,
            cache_hint: CacheHint::max_duration(),
            pool: std::sync::Arc::new(rayon::ThreadPoolBuilder::new().build().unwrap()),
        };

        let tile = accu.into_tile().await.expect("must not fail");

        assert_eq!(tile.time, TimeInterval::default());
        assert_eq!(tile.grid_array.shape_ref().axis_size_y(), 4);
        assert_eq!(tile.grid_array.shape_ref().axis_size_x(), 4);
    }
}
