use crate::engine::{QueryContext, QueryRectangle, RasterQueryProcessor};
use crate::error;
use crate::util::Result;
use futures::Stream;
use futures::{
    ready,
    stream::{BoxStream, TryFold},
    FutureExt, TryFuture, TryStreamExt,
};
use futures::{stream::FusedStream, Future};
use geoengine_datatypes::{
    error::Error::{GridIndexOutOfBounds, InvalidGridIndex},
    operations::reproject::{
        project_coordinates_fail_tolarant, CoordinateProjection, CoordinateProjector, Reproject,
    },
    primitives::{SpatialBounded, SpatialResolution, TimeInterval},
    raster::{grid_idx_iter_2d, BoundedGrid, Grid2D, TilingSpecification},
    spatial_reference::SpatialReference,
};
use geoengine_datatypes::{
    primitives::{Coordinate2D, TimeInstance},
    raster::{
        Blit, CoordinatePixelAccess, GridIdx2D, GridIndexAccessMut, Pixel, RasterTile2D,
        TileInformation,
    },
};

use pin_project::pin_project;
use std::task::Poll;

use std::pin::Pin;

pub type RasterFold<'a, T, FoldFuture, FoldMethod, FoldCompanion> = TryFold<
    BoxStream<'a, Result<RasterTile2D<T>>>,
    FoldFuture,
    (RasterTile2D<T>, FoldCompanion),
    FoldMethod,
>;

pub type RasterFoldOption<'a, T, FoldFuture, FoldMethod, FoldCompanion> =
    Option<RasterFold<'a, T, FoldFuture, FoldMethod, FoldCompanion>>;

/// This adapter allows to generate a tile stream using sub-querys.
/// This is done using a `TileSubQuery`.
/// The sub-query is resolved for each produced tile.
#[pin_project(project = RasterOverlapAdapterProjection)]
pub struct RasterOverlapAdapter<'a, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: TileSubQuery<PixelType>,
{
    /// The `QueryRectangle` the adapter is queried with
    query_rect: QueryRectangle,
    /// This `TimeInstance` is the point in time currently queried in the sub-query
    time_start: Option<TimeInstance>,
    /// This `TimeInstance` is the latest point in time seen from the tiles produced by the sub-query
    time_end: Option<TimeInstance>,
    /// The `TileInformation` of all tiles the produces stream will contain
    tiles_to_produce: Vec<TileInformation>, // TODO: change to IntoIterator<Item = TileInformation>
    current_spatial_tile: usize, // TODO: change into current_tile_iterator
    /// The `RasterQueryProcessor` to answer the sub-queries
    source: &'a RasterProcessorType,
    /// The `QueryContext` to use for sub-queries
    query_ctx: &'a dyn QueryContext,
    /// This is the `Future` which flattens the sub-query streams into single tiles
    #[pin]
    running_future: RasterFoldOption<
        'a,
        PixelType,
        SubQuery::FoldFuture,
        SubQuery::FoldMethod,
        SubQuery::FoldCompanion,
    >,
    /// remember when the operator is done
    ended: bool,
    /// The `TileSubQuery` defined what this adapter does.
    sub_query: SubQuery,
}

impl<'a, PixelType, RasterProcessorType, SubQuery>
    RasterOverlapAdapter<'a, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: TileSubQuery<PixelType>,
{
    /// Creates a new `RasterOverlapAdapter` and initialize all the internal things.
    pub fn new(
        source: &'a RasterProcessorType,
        query_rect: QueryRectangle,
        tiling_spec: TilingSpecification,
        query_ctx: &'a dyn QueryContext,
        sub_query: SubQuery,
    ) -> Self {
        let tiling_strat = tiling_spec.strategy(
            query_rect.spatial_resolution.x,
            -query_rect.spatial_resolution.y,
        );

        let tx: Vec<TileInformation> = tiling_strat
            .tile_information_iterator(query_rect.bbox)
            .collect();

        Self {
            source,
            query_rect,
            tiles_to_produce: tx,
            current_spatial_tile: 0,
            time_start: Some(query_rect.time_interval.start()),
            time_end: None,
            running_future: None,
            query_ctx,
            ended: false,
            sub_query,
        }
    }
}

impl<PixelType, RasterProcessorType, SubQuery> FusedStream
    for RasterOverlapAdapter<'_, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: TileSubQuery<PixelType>,
{
    fn is_terminated(&self) -> bool {
        self.ended
    }
}

impl<'a, PixelType, RasterProcessorType, SubQuery> Stream
    for RasterOverlapAdapter<'a, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: TileSubQuery<PixelType>,
{
    type Item = Result<RasterTile2D<PixelType>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.is_terminated() {
            return Poll::Ready(None);
        }

        let mut this = self.project();

        if this.running_future.as_ref().is_none() {
            // there is no future running / stream processing
            if *this.current_spatial_tile >= this.tiles_to_produce.len() {
                // we iterated through all the tiles, now we need to move time forward
                *this.current_spatial_tile = 0;

                // make time progress
                let (n_t_start, n_t_end) = match (*this.time_start, *this.time_end) {
                    (Some(t_start), Some(t_end)) if t_start == t_end => (Some(t_start + 1), None),
                    (Some(t_start), Some(t_end)) if t_start < t_end => (Some(t_end), None),
                    (_, _) => (None, None),
                };

                *this.time_start = n_t_start;
                *this.time_end = n_t_end;
            }

            if let Some(t_start) = *this.time_start {
                if t_start >= this.query_rect.time_interval.end() {
                    *this.ended = true;
                } else {
                    let current_spatial_tile_info =
                        this.tiles_to_produce[*this.current_spatial_tile];

                    let fold_tile_spec = TileInformation {
                        tile_size_in_pixels: current_spatial_tile_info.tile_size_in_pixels,
                        global_tile_position: current_spatial_tile_info.global_tile_position,
                        global_geo_transform: current_spatial_tile_info.global_geo_transform,
                    };

                    let tqr = this.sub_query.tile_query_rectangle(
                        fold_tile_spec,
                        *this.query_rect,
                        t_start,
                    )?;

                    // TODO: this schould also be a future. We can chain it with the query if we find a way to store it in the running future
                    let accu = this.sub_query.new_fold_accu(fold_tile_spec, tqr)?;

                    let qs = this.source.raster_query(tqr, *this.query_ctx)?;

                    let ttf = qs.try_fold(accu, this.sub_query.fold_method());

                    this.running_future.set(Some(ttf));
                }
            }
        }

        let rv = match this.running_future.as_mut().as_pin_mut() {
            Some(fut) => ready!(fut.poll(cx)),
            None => return Poll::Ready(None),
        };

        let r = match rv {
            Ok(tile) => tile,
            Err(err) => return Poll::Ready(Some(Err(err))),
        };

        // set the running future to None --> will create a new one in the next call
        this.running_future.set(None);

        // update the end_time from the produced tile
        let t_end = r.0.time.end();
        let _old_t_end = this.time_end.replace(t_end);

        *this.current_spatial_tile += 1;

        Poll::Ready(Some(Ok(r.0)))
    }
}

pub fn fold_by_blit_impl<T>(
    accu: (RasterTile2D<T>, ()),
    tile: RasterTile2D<T>,
) -> Result<(RasterTile2D<T>, ())>
where
    T: Pixel,
{
    let (mut accu_tile, unused) = accu;
    let t_union = accu_tile.time.union(&tile.time)?;
    match accu_tile.blit(tile) {
        Ok(_) => {
            accu_tile.time = t_union;
            Ok((accu_tile, unused))
        }
        Err(error) => {
            dbg!(
                "Skipping non-overlapping area tiles in blit method. This schould not happen but the MockSource produces all tiles!!!",
                error
            );
            Ok((accu_tile, unused))
        }
    }
}

#[allow(dead_code)]
pub fn fold_by_blit_future<T>(
    accu: (RasterTile2D<T>, ()),
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<(RasterTile2D<T>, ())>>
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
    accu: (RasterTile2D<T>, Vec<(GridIdx2D, Coordinate2D)>),
    tile: RasterTile2D<T>,
) -> impl TryFuture<Ok = (RasterTile2D<T>, Vec<(GridIdx2D, Coordinate2D)>), Error = error::Error>
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
#[allow(clippy::clippy::needless_pass_by_value)]
pub fn fold_by_coordinate_lookup_impl<T>(
    accu: (RasterTile2D<T>, Vec<(GridIdx2D, Coordinate2D)>),
    tile: RasterTile2D<T>,
) -> Result<(RasterTile2D<T>, Vec<(GridIdx2D, Coordinate2D)>)>
where
    T: Pixel,
{
    let (mut accu_tile, accu_companion) = accu;
    let t_union = accu_tile.time.union(&tile.time)?;

    match insert_projected_pixels(&mut accu_tile, &tile, accu_companion.iter()) {
        Ok(_) => {
            accu_tile.time = t_union;
            Ok((accu_tile, accu_companion))
        }
        Err(error) => Err(error),
    }
}

/// This method takes two tiles and a map from `GridIdx2D` to `Coordinate2D`. Then for all `GridIdx2D` we set the values from the corresponding coordinate in the source tile.
pub fn insert_projected_pixels<'a, T: Pixel, I: Iterator<Item = &'a (GridIdx2D, Coordinate2D)>>(
    target: &mut RasterTile2D<T>,
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
pub trait TileSubQuery<T>: Send
where
    T: Pixel,
{
    type FoldFuture: TryFuture<Ok = (RasterTile2D<T>, Self::FoldCompanion), Error = error::Error>;
    type FoldMethod: Clone
        + Fn((RasterTile2D<T>, Self::FoldCompanion), RasterTile2D<T>) -> Self::FoldFuture;
    type FoldCompanion: Clone + Send;

    /// The no-data-value to use in the resulting `RasterTile2D`
    fn result_no_data_value(&self) -> Option<T>;
    /// The initial fill-value of the accumulator (`RasterTile2D`).
    fn initial_fill_value(&self) -> T;

    /// This method geneerates a new accumulator which is used to fold the `Stream` of `RasterTile2D` of a sub-query.
    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: QueryRectangle,
    ) -> Result<(RasterTile2D<T>, Self::FoldCompanion)>;

    /// This method generates a `QueryRectangle` for a tile-specific sub-query
    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: QueryRectangle,
        start_time: TimeInstance,
    ) -> Result<QueryRectangle>;

    /// This method generates the method which combines the accumulator and each tile of the sub-query stream in the `TryFold` stream adapter.
    fn fold_method(&self) -> Self::FoldMethod;
}

#[derive(Debug, Clone)]
pub struct TileSubQueryIdentity<F> {
    fold_fn: F,
}

impl<T, FoldM, FoldF> TileSubQuery<T> for TileSubQueryIdentity<FoldM>
where
    T: Pixel,
    FoldM: Send + Clone + Fn((RasterTile2D<T>, ()), RasterTile2D<T>) -> FoldF,
    FoldF: TryFuture<Ok = (RasterTile2D<T>, ()), Error = error::Error>,
{
    fn result_no_data_value(&self) -> Option<T> {
        Some(T::from_(0))
    }

    fn initial_fill_value(&self) -> T {
        T::from_(0)
    }

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: QueryRectangle,
    ) -> Result<(RasterTile2D<T>, ())> {
        let output_raster = Grid2D::new_filled(
            tile_info.tile_size_in_pixels,
            self.initial_fill_value(),
            self.result_no_data_value(),
        );
        Ok((
            RasterTile2D::new_with_tile_info(query_rect.time_interval, tile_info, output_raster),
            (),
        ))
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: QueryRectangle,
        start_time: TimeInstance,
    ) -> Result<QueryRectangle> {
        Ok(QueryRectangle {
            bbox: tile_info.spatial_bounds(),
            spatial_resolution: query_rect.spatial_resolution,
            time_interval: TimeInterval::new_instant(start_time),
        })
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }

    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    type FoldCompanion = ();
}

#[derive(Debug)]
pub struct TileReprojectionSubQuery<T, F> {
    pub in_srs: SpatialReference,
    pub out_srs: SpatialReference,
    pub no_data_and_fill_value: T,
    pub fold_fn: F,
    pub in_spatial_res: SpatialResolution,
}

impl<T, FoldM, FoldF> TileSubQuery<T> for TileReprojectionSubQuery<T, FoldM>
where
    T: Pixel,
    FoldM: Send
        + Clone
        + Fn((RasterTile2D<T>, Vec<(GridIdx2D, Coordinate2D)>), RasterTile2D<T>) -> FoldF,
    FoldF: Send
        + TryFuture<Ok = (RasterTile2D<T>, Vec<(GridIdx2D, Coordinate2D)>), Error = error::Error>,
{
    fn result_no_data_value(&self) -> Option<T> {
        Some(self.no_data_and_fill_value)
    }

    fn initial_fill_value(&self) -> T {
        self.no_data_and_fill_value
    }

    #[allow(clippy::clippy::type_complexity)]
    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: QueryRectangle,
    ) -> Result<(RasterTile2D<T>, Vec<(GridIdx2D, Coordinate2D)>)> {
        let output_raster = Grid2D::new_filled(
            tile_info.tile_size_in_pixels,
            self.initial_fill_value(),
            self.result_no_data_value(),
        );
        let idxs: Vec<GridIdx2D> = grid_idx_iter_2d(&output_raster.bounding_box()).collect();
        let coords: Vec<Coordinate2D> = idxs
            .iter()
            .map(|&i| tile_info.tile_geo_transform().grid_idx_to_coordinate_2d(i))
            .collect();

        let proj = CoordinateProjector::from_known_srs(self.in_srs, self.out_srs)?;
        let projected_coords = project_coordinates_fail_tolarant(&coords, &proj);

        let accu_companion: Vec<(GridIdx2D, Coordinate2D)> = idxs
            .into_iter()
            .zip(projected_coords.into_iter())
            .filter_map(|(i, c)| c.map(|c| (i, c)))
            .collect();

        Ok((
            RasterTile2D::new_with_tile_info(query_rect.time_interval, tile_info, output_raster),
            accu_companion,
        ))
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        _query_rect: QueryRectangle,
        start_time: TimeInstance,
    ) -> Result<QueryRectangle> {
        let proj = CoordinateProjector::from_known_srs(self.in_srs, self.out_srs)?;

        Ok(QueryRectangle {
            bbox: tile_info.spatial_bounds().reproject(&proj)?,
            spatial_resolution: self.in_spatial_res,
            time_interval: TimeInterval::new_instant(start_time),
        })
    }

    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    type FoldCompanion = Vec<(GridIdx2D, Coordinate2D)>;

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{BoundingBox2D, Measurement, SpatialResolution, TimeInterval},
        raster::{Grid, GridShape, RasterDataType},
        spatial_reference::SpatialReference,
    };

    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use crate::engine::{RasterOperator, RasterResultDescriptor};
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use futures::StreamExt;

    #[tokio::test]
    async fn identity() {
        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], Some(0)).unwrap(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                },
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (3., 1.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };

        let query_ctx = MockQueryContext {
            chunk_byte_size: 1024 * 1024,
        };
        let tiling_strat = exe_ctx.tiling_specification;

        let op = mrs1.initialize(&exe_ctx).unwrap();

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let a = RasterOverlapAdapter::new(
            &qp,
            query_rect,
            tiling_strat,
            &query_ctx,
            TileSubQueryIdentity {
                fold_fn: fold_by_blit_future,
            },
        );
        let res = a
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        assert_eq!(data, res)
    }

    #[tokio::test]
    async fn identity_projection() {
        let projection = SpatialReference::new(
            geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
            4326,
        );

        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], Some(0)).unwrap(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                },
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (3., 1.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };

        let query_ctx = MockQueryContext {
            chunk_byte_size: 1024 * 1024,
        };
        let tiling_strat = exe_ctx.tiling_specification;

        let op = mrs1.initialize(&exe_ctx).unwrap();

        // would be nice to get from result descriptor
        let no_data_v = 0_u8; // op.result_descriptor().no_data_value;

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let state_gen = TileReprojectionSubQuery {
            in_srs: projection,
            out_srs: projection,
            no_data_and_fill_value: no_data_v,
            fold_fn: fold_by_coordinate_lookup_future,
            in_spatial_res: query_rect.spatial_resolution,
        };
        let a = RasterOverlapAdapter::new(&qp, query_rect, tiling_strat, &query_ctx, state_gen);
        let res = a
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        assert_eq!(data, res)
    }
}
