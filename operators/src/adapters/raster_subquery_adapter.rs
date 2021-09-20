use crate::engine::{QueryContext, QueryProcessor, RasterQueryProcessor, RasterQueryRectangle};
use crate::error;
use crate::util::Result;
use futures::future::BoxFuture;
use futures::Stream;
use futures::{
    ready,
    stream::{BoxStream, TryFold},
    FutureExt, TryFuture, TryStreamExt,
};
use futures::{stream::FusedStream, Future};
use geoengine_datatypes::operations::reproject::ReprojectClipped;
use geoengine_datatypes::primitives::{SpatialPartition2D, SpatialPartitioned};
use geoengine_datatypes::raster::{EmptyGrid2D, GridOrEmpty};
use geoengine_datatypes::{
    error::Error::{GridIndexOutOfBounds, InvalidGridIndex},
    operations::reproject::{
        project_coordinates_fail_tolerant, CoordinateProjection, CoordinateProjector, Reproject,
    },
    primitives::{SpatialResolution, TimeInterval},
    raster::{
        grid_idx_iter_2d, BoundedGrid, EmptyGrid, Grid2D, MaterializedRasterTile2D, NoDataValue,
        TilingSpecification,
    },
    spatial_reference::SpatialReference,
};
use geoengine_datatypes::{
    primitives::{Coordinate2D, TimeInstance},
    raster::{
        Blit, CoordinatePixelAccess, GridIdx2D, GridIndexAccessMut, Pixel, RasterTile2D,
        TileInformation,
    },
};

use log::debug;
use pin_project::pin_project;
use std::task::Poll;

use std::pin::Pin;

pub trait FoldTileAccu {
    type RasterType: Pixel;
    fn into_tile(self) -> RasterTile2D<Self::RasterType>;
}

pub trait FoldTileAccuMut: FoldTileAccu {
    fn tile_mut(&mut self) -> &mut RasterTile2D<Self::RasterType>;
}

impl<T: Pixel> FoldTileAccu for RasterTile2D<T> {
    type RasterType = T;

    fn into_tile(self) -> RasterTile2D<Self::RasterType> {
        self
    }
}

pub type RasterFold<'a, T, FoldFuture, FoldMethod, FoldTileAccu> =
    TryFold<BoxStream<'a, Result<RasterTile2D<T>>>, FoldFuture, FoldTileAccu, FoldMethod>;

pub type RasterFoldOption<'a, T, FoldFuture, FoldMethod, FoldTileAccu> =
    Option<RasterFold<'a, T, FoldFuture, FoldMethod, FoldTileAccu>>;

type QueryFuture<'a, T> = BoxFuture<'a, Result<BoxStream<'a, Result<RasterTile2D<T>>>>>;

/// This adapter allows to generate a tile stream using sub-querys.
/// This is done using a `TileSubQuery`.
/// The sub-query is resolved for each produced tile.
#[pin_project(project = RasterSubQueryAdapterProjection)]
pub struct RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<PixelType>,
{
    /// The `QueryRectangle` the adapter is queried with
    query_rect: RasterQueryRectangle,
    /// This `TimeInstance` is the point in time currently queried in the sub-query
    time_start: TimeInstance,
    /// This `TimeInstance` is the latest point in time seen from the tiles produced by the sub-query
    time_end: Option<TimeInstance>,
    /// The `TileInformation` of all tiles the produces stream will contain
    tiles_to_produce: Vec<TileInformation>, // TODO: change to IntoIterator<Item = TileInformation>
    current_spatial_tile: usize, // TODO: change into current_tile_iterator
    /// The `RasterQueryProcessor` to answer the sub-queries
    source: &'a RasterProcessorType,
    /// The `QueryContext` to use for sub-queries
    query_ctx: &'a dyn QueryContext,
    /// This is the `Future` which creates the sub-query stream
    #[pin]
    running_query: Option<QueryFuture<'a, PixelType>>,
    /// This is the `Future` which flattens the sub-query streams into single tiles
    #[pin]
    running_fold: RasterFoldOption<
        'a,
        PixelType,
        SubQuery::FoldFuture,
        SubQuery::FoldMethod,
        SubQuery::TileAccu,
    >,

    /// remember when the operator is done
    ended: bool,
    /// The `SubQuery` defines what this adapter does.
    sub_query: SubQuery,

    no_data_value: Option<PixelType>,
}

impl<'a, PixelType, RasterProcessorType, SubQuery>
    RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<PixelType>,
{
    /// Creates a new `RasterOverlapAdapter` and initialize all the internal things.
    pub fn new(
        source: &'a RasterProcessorType,
        query_rect: RasterQueryRectangle,
        tiling_spec: TilingSpecification,
        query_ctx: &'a dyn QueryContext,
        sub_query: SubQuery,
        no_data_value: Option<PixelType>,
    ) -> Self {
        let tiling_strat = tiling_spec.strategy(
            query_rect.spatial_resolution.x,
            -query_rect.spatial_resolution.y,
        );

        let tiles_to_produce: Vec<TileInformation> = tiling_strat
            .tile_information_iterator(query_rect.spatial_bounds)
            .collect();

        Self {
            source,
            query_rect,
            tiles_to_produce,
            current_spatial_tile: 0,
            time_start: query_rect.time_interval.start(),
            time_end: None,
            running_query: None,
            running_fold: None,
            query_ctx,
            ended: false,
            sub_query,
            no_data_value,
        }
    }
}

impl<PixelType, RasterProcessorType, SubQuery> FusedStream
    for RasterSubQueryAdapter<'_, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType:
        QueryProcessor<Output = RasterTile2D<PixelType>, SpatialBounds = SpatialPartition2D>,
    SubQuery: SubQueryTileAggregator<PixelType>,
{
    fn is_terminated(&self) -> bool {
        self.ended
    }
}

impl<'a, PixelType, RasterProcessorType, SubQuery> Stream
    for RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType:
        QueryProcessor<Output = RasterTile2D<PixelType>, SpatialBounds = SpatialPartition2D>,
    SubQuery: SubQueryTileAggregator<PixelType>,
{
    type Item = Result<RasterTile2D<PixelType>>;

    #[allow(clippy::too_many_lines)]
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.is_terminated() {
            return Poll::Ready(None);
        }

        let mut this = self.project();

        if !(this.query_rect.time_interval.is_instant()
            && *this.time_start == this.query_rect.time_interval.start())
            && *this.time_start >= this.query_rect.time_interval.end()
        {
            *this.ended = true;
            return Poll::Ready(None);
        }

        let current_spatial_tile_info = this.tiles_to_produce[*this.current_spatial_tile];

        let fold_tile_spec = TileInformation {
            tile_size_in_pixels: current_spatial_tile_info.tile_size_in_pixels,
            global_tile_position: current_spatial_tile_info.global_tile_position,
            global_geo_transform: current_spatial_tile_info.global_geo_transform,
        };

        let tile_query_rectangle = this.sub_query.tile_query_rectangle(
            fold_tile_spec,
            *this.query_rect,
            *this.time_start,
        )?;

        let tile_accu_result = if let Some(tile_query_rectangle) = tile_query_rectangle {
            if this.running_query.as_ref().is_none() && this.running_fold.as_ref().is_none() {
                // there is no query and no stream pending
                debug!("New running_query for: {:?}", &tile_query_rectangle);

                let tile_query_stream = this
                    .source
                    .query(tile_query_rectangle, *this.query_ctx)
                    .boxed();

                this.running_query.set(Some(tile_query_stream));
            }

            let query_future_result =
                if let Some(query_future) = this.running_query.as_mut().as_pin_mut() {
                    // TODO: match block?
                    let query_result: Result<BoxStream<'a, Result<RasterTile2D<PixelType>>>> =
                        ready!(query_future.poll(cx));
                    Some(query_result)
                } else {
                    None
                };

            this.running_query.set(None);

            match query_future_result {
                Some(Ok(tile_query_stream)) => {
                    let tile_folding_accu = this
                        .sub_query
                        .new_fold_accu(fold_tile_spec, tile_query_rectangle)?;

                    debug!("New running_fold: {:?}", &tile_query_rectangle);
                    let tile_folding_stream =
                        tile_query_stream.try_fold(tile_folding_accu, this.sub_query.fold_method());

                    this.running_fold.set(Some(tile_folding_stream));
                }
                Some(Err(err)) => {
                    debug!("Tile fold stream returned error: {:?}", &err);
                    *this.ended = true;
                    return Poll::Ready(Some(Err(err)));
                }
                None => {} // there is no result but there might be a running fold...
            }

            let future_result = match this.running_fold.as_mut().as_pin_mut() {
                Some(fut) => {
                    ready!(fut.poll(cx))
                }
                None => {
                    debug!("running_fold is None");
                    return Poll::Ready(None); // should initialize next tile query?
                }
            };

            // set the running future to None --> will create a new one in the next call
            this.running_fold.set(None);

            let tile_accu_result: Result<RasterTile2D<PixelType>> =
                future_result.map(FoldTileAccu::into_tile);

            tile_accu_result
        } else {
            // no sub query query rectangle was produced for the current tile, thus output an empty tile
            Ok(RasterTile2D::new_with_tile_info(
                this.query_rect.time_interval,
                fold_tile_spec,
                GridOrEmpty::Empty(EmptyGrid2D::<PixelType>::new(
                    fold_tile_spec.tile_size_in_pixels,
                    // TODO: check if zero makes sense as default value or if we should return an error
                    this.no_data_value.unwrap_or(PixelType::zero()),
                )),
            ))
        };

        // if we produced a tile: get the end of the current time slot (must be the same for all tiles in the slot)
        if let Ok(ref r) = tile_accu_result {
            debug!("time_end: {:?} --> {:?}", &this.time_end, &r.time.end());
            this.time_end.replace(r.time.end());
        }

        // make tile progress
        debug!(
            "current_spatial_tile: {:?} --> {:?}",
            *this.current_spatial_tile,
            *this.current_spatial_tile + 1
        );
        *this.current_spatial_tile += 1;
        // check if we iterated through all the tiles
        if *this.current_spatial_tile >= this.tiles_to_produce.len() {
            // reset to the first tile and move time forward
            *this.current_spatial_tile = 0;
            // make time progress
            if let Some(ref time_end) = this.time_end {
                debug!("time_start: {:?} --> {:?}", &this.time_start, *time_end);
                *this.time_start = *time_end;
                *this.time_end = None;
            } else {
                // we produced only error tiles for a time slot and do not know how to progress in time => end stream
                // TODO: discuss whether we should generate an additional time slot (t_end, eot) with error tiles, s.t. the query time interval is answered completely
                *this.ended = true;
            }
        }

        match tile_accu_result {
            Ok(tile_accu) => Poll::Ready(Some(Ok(tile_accu))),
            Err(err) => {
                *this.ended = true;
                Poll::Ready(Some(Err(err)))
            }
        }
    }
}

pub fn fold_by_blit_impl<T>(accu: RasterTile2D<T>, tile: RasterTile2D<T>) -> Result<RasterTile2D<T>>
where
    T: Pixel,
{
    let mut accu_tile = accu.into_tile();
    let t_union = accu_tile.time.union(&tile.time)?;

    accu_tile.time = t_union;

    if tile.grid_array.is_empty() && accu_tile.no_data_value() == tile.no_data_value() {
        return Ok(accu_tile);
    }

    let mut materialized_accu_tile = accu_tile.into_materialized_tile();

    match materialized_accu_tile.blit(tile) {
        Ok(_) => Ok(materialized_accu_tile.into()),
        Err(_error) => {
            // Ignore lookup errors
            //dbg!(
            //    "Skipping non-overlapping area tiles in blit method. This schould not happen but the MockSource produces all tiles!!!",
            //    error
            //);
            Ok(materialized_accu_tile.into())
        }
    }
}

#[allow(dead_code)]
pub fn fold_by_blit_future<T>(
    accu: RasterTile2D<T>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<RasterTile2D<T>>>
where
    T: Pixel,
{
    tokio::task::spawn_blocking(|| fold_by_blit_impl(accu, tile)).then(async move |x| match x {
        Ok(r) => r,
        Err(e) => Err(e.into()),
    })
}

#[allow(dead_code)]
pub fn fold_by_coordinate_lookup_future<T>(
    accu: TileWithProjectionCoordinates<T>,
    tile: RasterTile2D<T>,
) -> impl TryFuture<Ok = TileWithProjectionCoordinates<T>, Error = error::Error>
where
    T: Pixel,
{
    tokio::task::spawn_blocking(|| fold_by_coordinate_lookup_impl(accu, tile)).then(
        async move |x| match x {
            Ok(r) => r,
            Err(e) => Err(e.into()),
        },
    )
}

#[allow(dead_code)]
#[allow(clippy::type_complexity)]
#[allow(clippy::needless_pass_by_value)]
pub fn fold_by_coordinate_lookup_impl<T>(
    accu: TileWithProjectionCoordinates<T>,
    tile: RasterTile2D<T>,
) -> Result<TileWithProjectionCoordinates<T>>
where
    T: Pixel,
{
    let mut accu = accu;
    let t_union = accu.accu_tile.time.union(&tile.time)?;

    accu.tile_mut().time = t_union;

    if tile.grid_array.is_empty() {
        return Ok(accu);
    }

    let TileWithProjectionCoordinates { accu_tile, coords } = accu;

    let mut materialized_accu_tile = accu_tile.into_materialized_tile(); //in a fold chain the real materialization should only happen once. All other calls will be simple conversions.

    match insert_projected_pixels(&mut materialized_accu_tile, &tile, coords.iter()) {
        Ok(_) => Ok(TileWithProjectionCoordinates {
            accu_tile: materialized_accu_tile.into(),
            coords,
        }),
        Err(error) => Err(error),
    }
}

/// This method takes two tiles and a map from `GridIdx2D` to `Coordinate2D`. Then for all `GridIdx2D` we set the values from the corresponding coordinate in the source tile.
pub fn insert_projected_pixels<'a, T: Pixel, I: Iterator<Item = &'a (GridIdx2D, Coordinate2D)>>(
    target: &mut MaterializedRasterTile2D<T>,
    source: &RasterTile2D<T>,
    local_target_idx_source_coordinate_map: I,
) -> Result<()> {
    // TODO: it would be better to run the pixel wise stuff in insert_projected_pixels in parallel...
    for (idx, coord) in local_target_idx_source_coordinate_map {
        match source.pixel_value_at_coord(*coord) {
            Ok(px_value) => target.set_at_grid_index(*idx, px_value)?,
            Err(e) => match e {
                // Ignore errors where a coordinate is not inside a source tile. This is by design.
                GridIndexOutOfBounds {
                    index: _,
                    min_index: _,
                    max_index: _,
                }
                | InvalidGridIndex {
                    grid_index: _,
                    description: _,
                } => {}
                _ => return Err(error::Error::DataType { source: e }),
            },
        }
    }

    Ok(())
}

/// This trait defines the behavior of the `RasterOverlapAdapter`.
pub trait SubQueryTileAggregator<T>: Send
where
    T: Pixel,
{
    type FoldFuture: TryFuture<Ok = Self::TileAccu, Error = error::Error>;
    type FoldMethod: Clone + Fn(Self::TileAccu, RasterTile2D<T>) -> Self::FoldFuture;
    type TileAccu: FoldTileAccu<RasterType = T> + Clone + Send;

    /// The no-data-value to use in the resulting `RasterTile2D`
    fn result_no_data_value(&self) -> Option<T>;
    /// The initial fill-value of the accumulator (`RasterTile2D`).
    fn initial_fill_value(&self) -> T;

    /// This method generates a new accumulator which is used to fold the `Stream` of `RasterTile2D` of a sub-query.
    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
    ) -> Result<Self::TileAccu>;

    /// This method generates `Some(QueryRectangle)` for a tile-specific sub-query or `None` if the `query_rect` cannot be translated.
    /// In the latter case an `EmptyTile` will be produced for the sub query instead of querying the source.
    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>>;

    /// This method generates the method which combines the accumulator and each tile of the sub-query stream in the `TryFold` stream adapter.
    fn fold_method(&self) -> Self::FoldMethod;

    fn into_raster_overlap_adapter<'a, S>(
        self,
        source: &'a S,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
        tiling_specification: TilingSpecification,
        no_data_value: Option<T>,
    ) -> RasterSubQueryAdapter<'a, T, S, Self>
    where
        S: RasterQueryProcessor<RasterType = T>,
        Self: Sized,
    {
        RasterSubQueryAdapter::<'a, T, S, Self>::new(
            source,
            query,
            tiling_specification,
            ctx,
            self,
            no_data_value,
        )
    }
}

#[derive(Debug, Clone)]
pub struct TileSubQueryIdentity<F> {
    fold_fn: F,
}

impl<T, FoldM, FoldF> SubQueryTileAggregator<T> for TileSubQueryIdentity<FoldM>
where
    T: Pixel,
    FoldM: Send + Clone + Fn(RasterTile2D<T>, RasterTile2D<T>) -> FoldF,
    FoldF: TryFuture<Ok = RasterTile2D<T>, Error = error::Error>,
{
    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    type TileAccu = RasterTile2D<T>;

    fn result_no_data_value(&self) -> Option<T> {
        Some(T::from_(0))
    }

    fn initial_fill_value(&self) -> T {
        T::from_(0)
    }

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
    ) -> Result<Self::TileAccu> {
        let output_raster = Grid2D::new_filled(
            tile_info.tile_size_in_pixels,
            self.initial_fill_value(),
            self.result_no_data_value(),
        );
        Ok(RasterTile2D::new_with_tile_info(
            query_rect.time_interval,
            tile_info,
            output_raster.into(),
        ))
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        Ok(Some(RasterQueryRectangle {
            spatial_bounds: tile_info.spatial_partition(),
            time_interval: TimeInterval::new_instant(start_time)?,
            spatial_resolution: query_rect.spatial_resolution,
        }))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

#[derive(Debug, Clone)]
pub struct TileWithProjectionCoordinates<T> {
    accu_tile: RasterTile2D<T>,
    coords: Vec<(GridIdx2D, Coordinate2D)>,
}

impl<T: Pixel> FoldTileAccu for TileWithProjectionCoordinates<T> {
    type RasterType = T;

    fn into_tile(self) -> RasterTile2D<Self::RasterType> {
        self.accu_tile
    }
}

impl<T: Pixel> FoldTileAccuMut for TileWithProjectionCoordinates<T> {
    fn tile_mut(&mut self) -> &mut RasterTile2D<Self::RasterType> {
        &mut self.accu_tile
    }
}

#[derive(Debug)]
pub struct TileReprojectionSubQuery<T, F> {
    pub in_srs: SpatialReference,
    pub out_srs: SpatialReference,
    pub no_data_and_fill_value: T,
    pub fold_fn: F,
    pub in_spatial_res: SpatialResolution,
}

impl<T, FoldM, FoldF> SubQueryTileAggregator<T> for TileReprojectionSubQuery<T, FoldM>
where
    T: Pixel,
    FoldM: Send + Clone + Fn(TileWithProjectionCoordinates<T>, RasterTile2D<T>) -> FoldF,
    FoldF: Send + TryFuture<Ok = TileWithProjectionCoordinates<T>, Error = error::Error>,
{
    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    type TileAccu = TileWithProjectionCoordinates<T>;

    fn result_no_data_value(&self) -> Option<T> {
        Some(self.no_data_and_fill_value)
    }

    fn initial_fill_value(&self) -> T {
        self.no_data_and_fill_value
    }

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
    ) -> Result<Self::TileAccu> {
        let output_raster =
            EmptyGrid::new(tile_info.tile_size_in_pixels, self.no_data_and_fill_value);

        // generate a projector which transforms wgs84 into the projection we want to produce.
        let valid_bounds_proj =
            CoordinateProjector::from_known_srs(SpatialReference::epsg_4326(), self.out_srs)?;

        // transform the bounds of the input srs (coordinates are in wgs84) into the output projection.
        // TODO check if  there is a better / smarter way to check if the coordinates are valid.
        let valid_bounds = self
            .in_srs
            .area_of_use::<SpatialPartition2D>()?
            .reproject_clipped(&valid_bounds_proj)?;

        // get all pixel idxs and coordinates.
        let idxs: Vec<GridIdx2D> = grid_idx_iter_2d(&output_raster.bounding_box()).collect();
        let idx_coords: Vec<(GridIdx2D, Coordinate2D)> = idxs
            .iter()
            .map(|&i| {
                (
                    i,
                    tile_info
                        .tile_geo_transform()
                        .grid_idx_to_upper_left_coordinate_2d(i),
                )
            })
            .collect();

        // check if the tile to fill is contained by the valid bounds of the input projection
        let idx_coords = if valid_bounds.contains(&tile_info.spatial_partition()) {
            // use all pixel coordinates
            idx_coords
        } else if valid_bounds.intersects(&tile_info.spatial_partition()) {
            // filter the coordinates to only contain valid ones
            idx_coords
                .into_iter()
                .filter(|(_, c)| valid_bounds.contains_coordinate(c))
                .collect()
        } else {
            // fastpath to skip filter
            vec![]
        };

        // unzip to use batch projection
        let (idxs, coords): (Vec<_>, Vec<_>) = idx_coords.into_iter().unzip();

        // project
        let proj = CoordinateProjector::from_known_srs(self.out_srs, self.in_srs)?;
        let projected_coords = project_coordinates_fail_tolerant(&coords, &proj);

        // zip idx and projected coordinates
        let coords: Vec<(GridIdx2D, Coordinate2D)> = idxs
            .into_iter()
            .zip(projected_coords.into_iter())
            .filter_map(|(i, c)| c.map(|c| (i, c)))
            .collect();

        Ok(TileWithProjectionCoordinates {
            accu_tile: RasterTile2D::new_with_tile_info(
                query_rect.time_interval,
                tile_info,
                output_raster.into(),
            ),
            coords,
        })
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        let proj = CoordinateProjector::from_known_srs(self.out_srs, self.in_srs)?;

        let spatial_bounds = tile_info
            .spatial_partition()
            .intersection(&query_rect.spatial_partition())
            .expect("should not be empty")
            .reproject(&proj);

        if let Ok(spatial_bounds) = spatial_bounds {
            Ok(Some(RasterQueryRectangle {
                spatial_bounds,
                time_interval: TimeInterval::new_instant(start_time)?,
                spatial_resolution: self.in_spatial_res,
            }))
        } else {
            // output query rectangle is not valid in source projection => produce empty tile
            Ok(None)
        }
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{Measurement, SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::{Grid, GridShape, RasterDataType},
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use crate::engine::{RasterOperator, RasterResultDescriptor};
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use futures::StreamExt;
    use num_traits::AsPrimitive;

    #[tokio::test]
    async fn identity() {
        let no_data_value = Some(0);
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 1.).into(), (3., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };

        let query_ctx = MockQueryContext::default();
        let tiling_strat = exe_ctx.tiling_specification;

        let op = mrs1.initialize(&exe_ctx).await.unwrap();

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let a = RasterSubQueryAdapter::new(
            &qp,
            query_rect,
            tiling_strat,
            &query_ctx,
            TileSubQueryIdentity {
                fold_fn: fold_by_blit_future,
            },
            no_data_value,
        );
        let res = a
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        assert_eq!(data, res);
    }

    #[tokio::test]
    async fn identity_projection() {
        let projection = SpatialReference::epsg_4326();
        let no_data_value = Some(0);

        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 1.).into(), (3., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };

        let query_ctx = MockQueryContext::default();
        let tiling_strat = exe_ctx.tiling_specification;

        let op = mrs1.initialize(&exe_ctx).await.unwrap();

        let raster_res_desc: &RasterResultDescriptor = op.result_descriptor();

        let no_data_v = raster_res_desc
            .no_data_value
            .map(AsPrimitive::as_)
            .expect("must be 0 since we specified it above");

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let state_gen = TileReprojectionSubQuery {
            in_srs: projection,
            out_srs: projection,
            no_data_and_fill_value: no_data_v,
            fold_fn: fold_by_coordinate_lookup_future,
            in_spatial_res: query_rect.spatial_resolution,
        };
        let a = RasterSubQueryAdapter::new(
            &qp,
            query_rect,
            tiling_strat,
            &query_ctx,
            state_gen,
            no_data_value,
        );
        let res = a
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        assert_eq!(data, res);
    }
}
