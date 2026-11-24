use super::ApproxProjection;
use super::raster_subquery_reprojection::with_projector;
use crate::engine::{
    Footprint, FootprintBounds, FootprintPrecision, QueryContext, RasterQueryProcessor,
    RasterResultDescriptor, unknown_footprints,
};
use crate::error;
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{Future, stream::FusedStream};
use futures::{
    FutureExt, TryFuture, TryStreamExt, ready,
    stream::{BoxStream, TryFold},
};
use futures::{Stream, StreamExt};
use geoengine_datatypes::primitives::TimeInterval;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, Coordinate2D, RasterQueryRectangle, SpatialPartition2D,
};
use geoengine_datatypes::primitives::{BandSelectionIter, CacheHint, CacheTtlSeconds};
use geoengine_datatypes::raster::{
    GeoTransform, GridBoundingBox2D, GridIntersection, GridOrEmpty,
    TileInformationBandCrossProductIter, TilingSpecification, TilingStrategy,
};
use geoengine_datatypes::raster::{Pixel, RasterTile2D, TileInformation};
use geoengine_datatypes::spatial_reference::{
    CoordinateProjection, DefaultCoordinateProjector, SpatialReference,
};
use geoengine_datatypes::util::NonNegativeFiniteF64;
use itertools::Itertools;
use rayon::ThreadPool;
use std::pin::Pin;

use pin_project::pin_project;

use std::sync::Arc;
use std::task::Poll;

#[async_trait]
pub trait FoldTileAccu {
    type RasterType: Pixel;
    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>>;
    fn thread_pool(&self) -> &Arc<ThreadPool>;
}

pub trait FoldTileAccuMut: FoldTileAccu {
    fn set_time(&mut self, new_time: TimeInterval);
    fn set_cache_hint(&mut self, new_cache_hint: CacheHint);
}

pub type RasterFold<'a, T, FoldFuture, FoldMethod, FoldTileAccu> =
    TryFold<BoxStream<'a, Result<RasterTile2D<T>>>, FoldFuture, FoldTileAccu, FoldMethod>;

type QueryAccuFuture<'a, T, A> = BoxFuture<'a, Result<(BoxStream<'a, Result<RasterTile2D<T>>>, A)>>;

type IntoTileFuture<'a, T> = BoxFuture<'a, Result<RasterTile2D<T>>>;

/// This adapter allows to generate a tile stream using sub-querys.
/// This is done using a `TileSubQuery`.
/// The sub-query is resolved for each produced tile.

#[pin_project(project=StateInnerProjection)]
#[derive(Debug, Clone)]
enum StateInner<A, B, C, D> {
    ProgressTileBand {
        time: TimeInterval,
    },
    CreateNextTime,
    // PollingTime(#[pin] N), not needed?
    CreateNextQuery {
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    },
    RunningQuery {
        #[pin]
        query_with_accu: A,
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    },
    RunningFold {
        #[pin]
        fold: B,
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    },
    RunningIntoTile {
        #[pin]
        into_tile: D,
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    },
    ReturnResult {
        result: Option<C>,
        /// The TTL of the empty tile that is returned without a result.
        empty_cache_ttl: CacheTtlSeconds,
        time: TimeInterval,
        tile: TileInformation,
        band: u32,
    },
    Ended,
}

/// This type is needed to stop Clippy from complaining about a very complex type in the `RasterSubQueryAdapter` struct.
type StateInnerType<'a, P, FoldFuture, FoldMethod, TileAccu> = StateInner<
    QueryAccuFuture<'a, P, TileAccu>,
    RasterFold<'a, P, FoldFuture, FoldMethod, TileAccu>,
    RasterTile2D<P>,
    IntoTileFuture<'a, P>,
>;

/// This adapter allows to generate a tile stream using sub-querys.
/// This is done using a `TileSubQuery`.
/// The sub-query is resolved for each produced tile.
#[pin_project(project = RasterSubQueryAdapterProjection)]
pub struct RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType>,
{
    /// The time steps to produce, each with where the source may have data. Without footprints of the
    /// source, the time steps of the caller with unknown bounds, so no tile is skipped.
    footprints: BoxStream<'a, Result<Footprint>>,
    /// The `RasterQueryProcessor` to answer the sub-queries
    source_processor: &'a RasterProcessorType,
    /// The `QueryContext` to use for sub-queries
    query_ctx: &'a dyn QueryContext,
    /// The `QueryRectangle` the adapter is queried with
    query_rect_to_answer: RasterQueryRectangle,
    /// The pixel bounds of the source data. Sub-queries outside of it produce empty tiles without querying the source.
    source_data_bounds: GridBoundingBox2D,
    /// The tiles of the query, to find the area of the source they read
    tiling_strategy: TilingStrategy,

    /// The `SubQuery` defines what this adapter does.
    sub_query: SubQuery,

    /// This `TimeInterval` is the time currently worked on
    current_time: TimeInterval,

    /// Where the source has data in the current time step.
    current_bounds: FootprintBounds,
    /// how long a tile skipped because of `current_bounds` may be cached
    current_cache_ttl: CacheTtlSeconds,

    band_tile_iter: TileInformationBandCrossProductIter,

    /// This current state of the adapter
    #[pin]
    state: StateInnerType<
        'a,
        PixelType,
        SubQuery::FoldFuture,
        SubQuery::FoldMethod,
        SubQuery::TileAccu,
    >,
}

impl<'a, PixelType, RasterProcessor, SubQuery>
    RasterSubQueryAdapter<'a, PixelType, RasterProcessor, SubQuery>
where
    PixelType: Pixel,
    RasterProcessor: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType>,
{
    pub fn new(
        source_processor: &'a RasterProcessor,
        query_rect_to_answer: RasterQueryRectangle,
        tiling_strategy: TilingStrategy,
        query_ctx: &'a dyn QueryContext,
        sub_query: SubQuery,
        time_stream: impl Stream<Item = Result<TimeInterval>> + Send + 'a,
    ) -> Self {
        let tile_iter = tiling_strategy
            .tile_information_iterator_from_pixel_bounds(query_rect_to_answer.spatial_bounds());
        let band_iter = BandSelectionIter::new(query_rect_to_answer.attributes().clone());
        let band_tile_iter = TileInformationBandCrossProductIter::new(tile_iter, band_iter);
        let source_data_bounds = source_data_bounds(source_processor, tiling_strategy);

        Self {
            current_time: TimeInterval::default(), // This is overwritten in the first poll_next call!
            footprints: unknown_footprints(time_stream.boxed()),
            current_bounds: FootprintBounds::Unknown,
            current_cache_ttl: CacheTtlSeconds::max(),
            query_rect_to_answer,
            source_data_bounds,
            tiling_strategy,
            band_tile_iter,
            query_ctx,
            source_processor,
            state: StateInner::CreateNextTime,
            sub_query,
        }
    }

    /// An adapter whose output has the time steps of its source. With footprint skipping on in the query
    /// context, it asks the source where it has data in the area the tiles read and skips the tiles without.
    pub async fn with_source_time_steps(
        source_processor: &'a RasterProcessor,
        query_rect_to_answer: RasterQueryRectangle,
        tiling_strategy: TilingStrategy,
        query_ctx: &'a dyn QueryContext,
        sub_query: SubQuery,
    ) -> Result<Self> {
        let time = query_rect_to_answer.time_interval();
        let mut adapter = Self::new(
            source_processor,
            query_rect_to_answer,
            tiling_strategy,
            query_ctx,
            sub_query,
            futures::stream::empty(),
        );

        if query_ctx.footprint_skipping()
            && let Some(source_query) = adapter.source_query_rectangle()?
        {
            let footprints = source_processor
                .footprint_query(source_query, FootprintPrecision::Coverage, query_ctx)
                .await?;
            return Ok(adapter.with_footprints(footprints));
        }

        adapter.footprints =
            unknown_footprints(source_processor.time_query(time, query_ctx).await?);
        Ok(adapter)
    }

    /// Produces the time steps of `footprints` instead of the time stream and skips the tiles where they
    /// have no data. They must be the footprints of the source in its pixel space, which have the time
    /// steps of the source.
    #[must_use]
    pub(crate) fn with_footprints(mut self, footprints: BoxStream<'a, Result<Footprint>>) -> Self {
        self.footprints = footprints;
        self
    }

    /// The area of the source that the tiles of the query together read, for all bands and the whole
    /// time of the query. `None` if no tile reads the source.
    fn source_query_rectangle(&self) -> Result<Option<RasterQueryRectangle>> {
        let query = &self.query_rect_to_answer;
        let tiles = tile_source_bounds(&self.sub_query, self.tiling_strategy, query)?;
        Ok(
            hull(tiles.iter().map(|(_, source_bounds)| *source_bounds)).map(|bounds| {
                RasterQueryRectangle::new(bounds, query.time_interval(), query.attributes().clone())
            }),
        )
    }

    pub fn box_pin(self) -> BoxStream<'a, Result<RasterTile2D<PixelType>>>
    where
        SubQuery: Send + 'static,
    {
        Box::pin(self)
    }
}

/// The tiles of `query` that read the source, each with the area of the source it reads.
fn tile_source_bounds<'a, PixelType, SubQuery>(
    sub_query: &SubQuery,
    tiling_strategy: TilingStrategy,
    query: &RasterQueryRectangle,
) -> Result<Vec<(TileInformation, GridBoundingBox2D)>>
where
    PixelType: Pixel,
    SubQuery: SubQueryTileAggregator<'a, PixelType>,
{
    let Some(band) = query.attributes().as_slice().first().copied() else {
        return Ok(Vec::new());
    };
    let mut tiles = Vec::new();
    for tile in tiling_strategy.tile_information_iterator_from_pixel_bounds(query.spatial_bounds())
    {
        // the sub-queries of a tile differ in time and band, not in space
        if let Some(tile_query) =
            sub_query.tile_query_rectangle(tile, query.clone(), query.time_interval(), band)?
        {
            tiles.push((tile, tile_query.spatial_bounds()));
        }
    }
    Ok(tiles)
}

fn hull(boxes: impl IntoIterator<Item = GridBoundingBox2D>) -> Option<GridBoundingBox2D> {
    boxes.into_iter().reduce(|hull, bounds| {
        GridBoundingBox2D::new_unchecked(
            [
                hull.y_min().min(bounds.y_min()),
                hull.x_min().min(bounds.x_min()),
            ],
            [
                hull.y_max().max(bounds.y_max()),
                hull.x_max().max(bounds.x_max()),
            ],
        )
    })
}

/// The pixel bounds of the data of `source`, on its tiling grid.
fn source_data_bounds<P: RasterQueryProcessor>(
    source: &P,
    tiling_strategy: TilingStrategy,
) -> GridBoundingBox2D {
    descriptor_data_bounds(source.raster_result_descriptor(), tiling_strategy)
}

/// The pixel bounds of the data a raster of `descriptor` describes, on its tiling grid.
fn descriptor_data_bounds(
    descriptor: &RasterResultDescriptor,
    tiling_strategy: TilingStrategy,
) -> GridBoundingBox2D {
    descriptor
        .spatial_grid_descriptor()
        .tiling_grid_definition(TilingSpecification::new(
            tiling_strategy.tile_size_in_pixels,
        ))
        .tiling_spatial_grid_definition()
        .grid_bounds()
}

/// The footprints of a processor that builds its tiles with a [`RasterSubQueryAdapter`] over the
/// time steps of `source` (see [`RasterSubQueryAdapter::with_source_time_steps`]).
///
/// The source is asked where it has data in the area the tiles of `query` read, which are those of
/// the data of `output`, the result of the processor. Above `footprint_tiles_limit` of these tiles,
/// each time step of the source may have data anywhere. `output_bounds`
/// tells for the boxes of the source of one time step in which pixels of the processor their data
/// can end up (see [`source_box_mapping`]), or `None` if it cannot tell; then data can be
/// anywhere in the query.
#[allow(clippy::too_many_arguments)]
pub async fn subquery_footprints<'a, PixelType, RasterProcessor, SubQuery>(
    source: &'a RasterProcessor,
    output: &RasterResultDescriptor,
    query: RasterQueryRectangle,
    tiling_strategy: TilingStrategy,
    sub_query: &SubQuery,
    precision: FootprintPrecision,
    query_ctx: &'a dyn QueryContext,
    output_bounds: impl Fn(&[GridBoundingBox2D]) -> Option<Vec<GridBoundingBox2D>> + Send + 'a,
) -> Result<BoxStream<'a, Result<Footprint>>>
where
    PixelType: Pixel,
    RasterProcessor: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType>,
{
    let query_bounds = query.spatial_bounds();
    let data_bounds = source_data_bounds(source, tiling_strategy);
    // a tile outside the data of the output reads nothing, so only those within are visited
    let visited = query_bounds.intersection(&descriptor_data_bounds(output, tiling_strategy));
    if visited.is_some_and(|visited| {
        tiling_strategy.num_tiles_intersecting_grid_bounds(visited)
            > query_ctx.footprint_tiles_limit()
    }) {
        // too many tiles to visit: the data may be anywhere in each time step
        let time_steps = source.time_query(query.time_interval(), query_ctx).await?;
        return Ok(time_steps
            .map_ok(|time| Footprint {
                time,
                bounds: FootprintBounds::Unknown,
                cache_ttl: CacheTtlSeconds::max(),
            })
            .boxed());
    }
    let source_bounds = match visited {
        Some(visited) => hull(
            tile_source_bounds(
                sub_query,
                tiling_strategy,
                &RasterQueryRectangle::new(
                    visited,
                    query.time_interval(),
                    query.attributes().clone(),
                ),
            )?
            .into_iter()
            .map(|(_, source_bounds)| source_bounds),
        ),
        None => None,
    }
    .and_then(|source_bounds| source_bounds.intersection(&data_bounds));

    let Some(source_bounds) = source_bounds else {
        // no tile reads data of the source, so no time step has data
        let time_steps = source.time_query(query.time_interval(), query_ctx).await?;
        return Ok(time_steps
            .map_ok(|time| Footprint {
                time,
                bounds: FootprintBounds::empty(),
                // the bounds of the data of the source do not change
                cache_ttl: CacheTtlSeconds::max(),
            })
            .boxed());
    };

    let footprints = source
        .footprint_query(
            RasterQueryRectangle::new(
                source_bounds,
                query.time_interval(),
                query.attributes().clone(),
            ),
            precision,
            query_ctx,
        )
        .await?;
    let max_boxes = query_ctx.footprint_boxes_per_step_limit();

    Ok(footprints
        .map_ok(
            move |Footprint {
                      time,
                      bounds,
                      cache_ttl,
                  }| {
                let bounds = match bounds {
                    FootprintBounds::Unknown => FootprintBounds::Unknown,
                    FootprintBounds::Boxes(source_boxes) => match output_bounds(&source_boxes) {
                        Some(boxes) => FootprintBounds::from_boxes(
                            boxes
                                .iter()
                                .filter_map(|bounds| bounds.intersection(&query_bounds)),
                            max_boxes,
                        ),
                        None => FootprintBounds::from_boxes([query_bounds], max_boxes),
                    },
                };
                Footprint {
                    time,
                    bounds,
                    cache_ttl,
                }
            },
        )
        .boxed())
}

/// The pixels of a processor in which the data of `source_box`, a box of pixels of its source,
/// can end up, or `None` if a corner or edge of the box cannot be projected.
///
/// `source_margin` is how many pixels of the source an output pixel reads beyond the one under
/// it, in y and x, e.g. one for a bilinear interpolation. With another spatial reference,
/// `projection` projects from the source into the grid of `geo_transform`; the edges of the box
/// bend there, so their bounds are found with [`ApproxProjection::line_bounds`] and grown by its
/// error. The result is rounded outwards by one pixel.
fn output_bounds_of_source_box<P: CoordinateProjection>(
    source_box: &GridBoundingBox2D,
    source_margin: [isize; 2],
    source_geo_transform: &GeoTransform,
    geo_transform: &GeoTransform,
    projection: Option<&ApproxProjection<'_, P>>,
    source_poles: &[Coordinate2D],
) -> Option<GridBoundingBox2D> {
    let [margin_y, margin_x] = source_margin;
    let area = source_geo_transform.grid_to_spatial_bounds(&GridBoundingBox2D::new_unchecked(
        [source_box.y_min() - margin_y, source_box.x_min() - margin_x],
        [source_box.y_max() + margin_y, source_box.x_max() + margin_x],
    ));

    let (area, error_px) = match projection {
        None => (area, 0),
        Some(projection) => {
            // around a pole the edges do not bound the interior: a box around the South Pole in a polar
            // projection reaches -90° only inside, not on its edges
            let contains = |pole: &Coordinate2D| {
                (area.upper_left().x..=area.lower_right().x).contains(&pole.x)
                    && (area.lower_right().y..=area.upper_left().y).contains(&pole.y)
            };
            if source_poles.iter().any(contains) {
                return None;
            }
            let corners = [
                area.upper_left(),
                area.upper_right(),
                area.lower_right(),
                area.lower_left(),
            ];
            let mut bounds: Option<(Coordinate2D, Coordinate2D)> = None;
            for (start, end) in corners.into_iter().circular_tuple_windows() {
                let (edge_min, edge_max) = projection.line_bounds(start, end)?;
                bounds = Some(match bounds {
                    None => (edge_min, edge_max),
                    Some((min, max)) => (
                        Coordinate2D::new(min.x.min(edge_min.x), min.y.min(edge_min.y)),
                        Coordinate2D::new(max.x.max(edge_max.x), max.y.max(edge_max.y)),
                    ),
                });
            }
            let (min, max) = bounds?;
            (
                SpatialPartition2D::new((min.x, max.y).into(), (max.x, min.y).into()).ok()?,
                projection.line_bounds_error_px().ceil() as isize,
            )
        }
    };

    let bounds = geo_transform.spatial_to_grid_bounds(&area);
    let grow = error_px + 1;
    Some(GridBoundingBox2D::new_unchecked(
        [bounds.y_min() - grow, bounds.x_min() - grow],
        [bounds.y_max() + grow, bounds.x_max() + grow],
    ))
}

/// [`output_bounds_of_source_box`] for the boxes of one time step, as [`subquery_footprints`]
/// takes it. `projection` is the spatial reference of the source, the one of the processor and
/// the largest error of the projection in pixels (see [`ApproxProjection`]), if they differ.
/// The North and South Pole in `srs`, as far as it can show them; a Mercator projection cannot.
fn poles_in(srs: SpatialReference) -> Vec<Coordinate2D> {
    with_projector(SpatialReference::epsg_4326(), srs, |projector| {
        [Coordinate2D::new(0., 90.), Coordinate2D::new(0., -90.)]
            .into_iter()
            .filter_map(|pole| projector.project_coordinate(pole).ok())
            .filter(|pole| pole.x.is_finite() && pole.y.is_finite())
            .collect()
    })
    .unwrap_or_default()
}

pub fn source_box_mapping(
    source_margin: [isize; 2],
    source_geo_transform: GeoTransform,
    geo_transform: GeoTransform,
    projection: Option<(SpatialReference, SpatialReference, NonNegativeFiniteF64)>,
) -> impl Fn(&[GridBoundingBox2D]) -> Option<Vec<GridBoundingBox2D>> + Send {
    move |source_boxes: &[GridBoundingBox2D]| {
        let map = |projection: Option<&ApproxProjection<'_, DefaultCoordinateProjector>>,
                   source_poles: &[Coordinate2D]| {
            source_boxes
                .iter()
                .map(|source_box| {
                    output_bounds_of_source_box(
                        source_box,
                        source_margin,
                        &source_geo_transform,
                        &geo_transform,
                        projection,
                        source_poles,
                    )
                })
                .collect()
        };
        match projection {
            None => map(None, &[]),
            // a projector cannot move between threads, so the thread's own one is used
            Some((source_srs, srs, max_error_px)) => {
                let source_poles = poles_in(source_srs);
                with_projector(source_srs, srs, |projector| {
                    map(
                        Some(&ApproxProjection::new(
                            projector,
                            &geo_transform,
                            max_error_px,
                        )),
                        &source_poles,
                    )
                })
                .ok()?
            }
        }
    }
}

impl<'a, PixelType, RasterProcessorType, SubQuery> FusedStream
    for RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType> + 'static,
{
    fn is_terminated(&self) -> bool {
        matches!(self.state, StateInner::Ended)
    }
}

impl<'a, PixelType, RasterProcessorType, SubQuery> Stream
    for RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType> + 'static,
{
    type Item = Result<RasterTile2D<PixelType>>;

    /**************************************************************************************************************************************
     * This method uses the `StateInner` enum to keep track of the current state
     *
     * There are two cases aka transition flows that are valid:
     *  a) CreateNextQuery -> ReturnResult
     *  b) CreateNextQuery -> RunningQuery -> RunningFold -> ReturnResult
     *
     * In case a) a valid `QueryRectangle` for the target tile is produced and a stream is queryed and folded to produce a new tile.
     * In case b) no valid `QueryRectange` is produced. Therefore, all async steps are skipped and None is produced instead of a tile.
     *
     * When all tiles are queried the state transitions from ReturnResult to Ended.
     *
     * In case an Error occures the state is set to Ended AND the method returns Poll::Ready(Some(Err))).
     *************************************************************************************************************************************/
    #[allow(clippy::too_many_lines)]
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();

        // check if we ended in a previous call
        if matches!(*this.state, StateInner::Ended) {
            return Poll::Ready(None);
        }

        // check if we ended in a previous call
        if matches!(*this.state, StateInner::ProgressTileBand { .. }) {
            // The state is pinned. Project it to get access to the query stored in the context.
            let time = if let StateInnerProjection::ProgressTileBand { time } =
                this.state.as_mut().project()
            {
                *time
            } else {
                // we already checked that the state is `StateInner::RunningQuery` so this case can not happen.
                unreachable!()
            };

            if let Some((next_tile, next_band)) = this.band_tile_iter.next() {
                this.state.set(StateInner::CreateNextQuery {
                    time,
                    band: next_band,
                    tile: next_tile,
                });
            } else {
                this.state.set(StateInner::CreateNextTime);
            }
        }

        if matches!(*this.state, StateInner::CreateNextTime) {
            match ready!(this.footprints.poll_next_unpin(cx)) {
                None => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(None);
                }
                Some(Err(e)) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
                Some(Ok(Footprint {
                    time,
                    bounds,
                    cache_ttl,
                })) => {
                    *this.current_time = time;
                    *this.current_bounds = bounds;
                    *this.current_cache_ttl = cache_ttl;
                    this.band_tile_iter.reset();
                    let (tile, band) = this
                        .band_tile_iter
                        .next()
                        .expect("There must be at least one band/tile");
                    this.state
                        .set(StateInner::CreateNextQuery { time, band, tile });
                }
            }
        }

        // first generate a new query.
        if matches!(*this.state, StateInner::CreateNextQuery { .. }) {
            let (time, band, tile) =
                if let StateInnerProjection::CreateNextQuery { time, band, tile } =
                    this.state.as_mut().project()
                {
                    (*time, *band, *tile)
                } else {
                    // we already checked that the state is `StateInner::RunningQuery` so this case can not happen.
                    unreachable!()
                };

            match this.sub_query.tile_query_rectangle(
                tile,
                this.query_rect_to_answer.clone(),
                time,
                band,
            ) {
                Ok(Some(raster_query_rect))
                    if raster_query_rect
                        .spatial_bounds()
                        .intersection(this.source_data_bounds)
                        .is_some()
                        && this
                            .current_bounds
                            .intersects(&raster_query_rect.spatial_bounds()) =>
                {
                    let tile_query_stream_fut = this
                        .source_processor
                        .raster_query(raster_query_rect.clone(), *this.query_ctx);

                    let tile_folding_accu_fut = this.sub_query.new_fold_accu(
                        tile,
                        raster_query_rect,
                        this.query_ctx.thread_pool(),
                    );

                    let joined_future =
                        async { futures::try_join!(tile_query_stream_fut, tile_folding_accu_fut) }
                            .boxed();

                    this.state.set(StateInner::RunningQuery {
                        query_with_accu: joined_future,
                        band,
                        tile,
                        time,
                    });
                }
                // the source has data there, but not in this time step: empty as long as the footprint holds
                Ok(Some(raster_query_rect))
                    if raster_query_rect
                        .spatial_bounds()
                        .intersection(this.source_data_bounds)
                        .is_some() =>
                {
                    this.state.set(StateInner::ReturnResult {
                        result: None,
                        empty_cache_ttl: *this.current_cache_ttl,
                        band,
                        tile,
                        time,
                    });
                }
                // no query or no source data => an empty tile, which no footprint decided
                Ok(Some(_) | None) => this.state.set(StateInner::ReturnResult {
                    result: None,
                    empty_cache_ttl: CacheTtlSeconds::max(),
                    band,
                    tile,
                    time,
                }),
                Err(e) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        // A query was issued, so we check whether it is finished
        // To work in this scope we first check if the state is the one we expect. We want to set the state in this scope so we can not borrow it here!
        if matches!(*this.state, StateInner::RunningQuery { .. }) {
            // The state is pinned. Project it to get access to the query stored in the context.
            let (time, band, tile, query_with_accu) = if let StateInnerProjection::RunningQuery {
                query_with_accu,
                tile,
                band,
                time,
            } = this.state.as_mut().project()
            {
                (*time, *band, *tile, query_with_accu)
            } else {
                // we already checked that the state is `StateInner::RunningQuery` so this case can not happen.
                unreachable!()
            };

            match ready!(query_with_accu.poll(cx)) {
                Ok((query, tile_folding_accu)) => {
                    let tile_folding_stream =
                        query.try_fold(tile_folding_accu, this.sub_query.fold_method());

                    this.state.set(StateInner::RunningFold {
                        fold: tile_folding_stream,
                        time,
                        tile,
                        band,
                    });
                }
                Err(e) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        // We are waiting for/expecting the result of the fold.
        // This block uses the same check and project pattern as above.
        if matches!(*this.state, StateInner::RunningFold { .. }) {
            let (tile, band, time, fold) = if let StateInnerProjection::RunningFold {
                fold,
                time,
                tile,
                band,
            } = this.state.as_mut().project()
            {
                (*tile, *band, *time, fold)
            } else {
                unreachable!()
            };

            match ready!(fold.poll(cx)) {
                Ok(tile_accu) => {
                    let into_tile = tile_accu.into_tile();
                    this.state.set(StateInner::RunningIntoTile {
                        into_tile,
                        time,
                        tile,
                        band,
                    });
                }
                Err(e) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        // We are waiting for/expecting the result of `into_tile` method.
        // This block uses the same check and project pattern as above.
        if matches!(*this.state, StateInner::RunningIntoTile { .. }) {
            let (time, band, tile, into_tile) = if let StateInnerProjection::RunningIntoTile {
                into_tile,
                time,
                tile,
                band,
            } = this.state.as_mut().project()
            {
                (*time, *band, *tile, into_tile)
            } else {
                unreachable!()
            };

            match ready!(into_tile.poll(cx)) {
                Ok(mut result_tile) => {
                    // set the tile band to the running index, that is because output bands always start at zero and are consecutive, independent of the input bands.
                    result_tile.band = band;
                    this.state.set(StateInner::ReturnResult {
                        result: Some(result_tile),
                        empty_cache_ttl: CacheTtlSeconds::max(),
                        time,
                        tile,
                        band,
                    });
                }
                Err(e) => {
                    this.state.set(StateInner::Ended);
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        debug_assert!(
            matches!(*this.state, StateInner::ReturnResult { .. }),
            "Must be in 'ReturnResult' state at this point!"
        );
        // At this stage we are in ReturnResult state. Either from a running fold or because the tile query rect was not valid.
        // This block uses the check and project pattern as above.
        let (tile, band, time, result, empty_cache_ttl) =
            if let StateInnerProjection::ReturnResult {
                result,
                empty_cache_ttl,
                time,
                tile,
                band,
            } = this.state.as_mut().project()
            {
                (*tile, *band, *time, result.take(), *empty_cache_ttl)
            } else {
                unreachable!()
            };
        // In the next poll we need to produce a new tile (if nothing else happens)
        this.state.set(StateInner::ProgressTileBand { time });

        let result_tile = result.unwrap_or_else(|| {
            RasterTile2D::new_with_tile_info(
                time,
                tile,
                band,
                GridOrEmpty::new_empty_shape(tile.tile_size_in_pixels),
                empty_cache_ttl.into(),
            )
        });

        Poll::Ready(Some(Ok(result_tile)))
    }
}

/// This trait defines the behavior of the `RasterOverlapAdapter`.
pub trait SubQueryTileAggregator<'a, T>: Send
where
    T: Pixel,
{
    type FoldFuture: Send + TryFuture<Ok = Self::TileAccu, Error = error::Error>;
    type FoldMethod: 'a
        + Send
        + Sync
        + Clone
        + Fn(Self::TileAccu, RasterTile2D<T>) -> Self::FoldFuture;
    type TileAccu: FoldTileAccu<RasterType = T> + Clone + Send;
    type TileAccuFuture: Send + Future<Output = Result<Self::TileAccu>>;

    /// This method generates a new accumulator which is used to fold the `Stream` of `RasterTile2D` of a sub-query.
    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture;

    /// This method generates `Some(QueryRectangle)` for a tile-specific sub-query or `None` if the `query_rect` cannot be translated.
    /// In the latter case an `EmptyTile` will be produced for the sub query instead of querying the source.
    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        time: TimeInterval,
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        Ok(Some(RasterQueryRectangle::new(
            tile_info.global_pixel_bounds(),
            time,
            band_idx.into(),
        )))
    }

    /// This method generates the method which combines the accumulator and each tile of the sub-query stream in the `TryFold` stream adapter.
    fn fold_method(&self) -> Self::FoldMethod;

    fn into_raster_subquery_adapter<S, G>(
        self,
        source: &'a S,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
        tiling_strategy: TilingStrategy,
        time_stream: G,
    ) -> RasterSubQueryAdapter<'a, T, S, Self>
    where
        S: RasterQueryProcessor<RasterType = T>,
        G: Stream<Item = Result<TimeInterval>> + Send + 'a,
        Self: Sized,
    {
        RasterSubQueryAdapter::<'a, T, S, Self>::new(
            source,
            query,
            tiling_strategy,
            ctx,
            self,
            time_stream,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::spatial_reference::SpatialReferenceAuthority;

    #[test]
    fn it_maps_a_source_box_around_a_pole_to_the_whole_query() {
        let south_polar = SpatialReference::new(SpatialReferenceAuthority::Epsg, 3031);
        // 1 km pixels from -2000 km to 2000 km around the South Pole, into a grid of 0.1°
        let mapping = source_box_mapping(
            [0, 0],
            GeoTransform::new_unchecked((-2_000_000., 2_000_000.).into(), 1000., -1000.),
            GeoTransform::new_unchecked((-180., 90.).into(), 0.1, -0.1),
            Some((
                south_polar,
                SpatialReference::epsg_4326(),
                NonNegativeFiniteF64::new(0.5).unwrap(),
            )),
        );

        // the pole lies inside: its edges reach -77° to -81° only, the data reaches -90°
        let around_the_pole = GridBoundingBox2D::new([1000, 1000], [2999, 2999]).unwrap();
        assert_eq!(mapping(&[around_the_pole]), None);

        // a box away from the pole keeps its bounds
        let away = GridBoundingBox2D::new([0, 0], [99, 99]).unwrap();
        assert!(mapping(&[away]).is_some());
    }
}
