use crate::engine::{QueryContext, RasterQueryProcessor};
use crate::util::Result;
use futures::future::{BoxFuture, JoinAll};
use futures::stream::{BoxStream, Fuse, FusedStream, Stream};
use futures::{Future, StreamExt, ready};
use geoengine_datatypes::primitives::{BandSelection, RasterQueryRectangle, TimeInterval};
use geoengine_datatypes::raster::{
    GridBounds, GridIdx2D, Pixel, RasterTile2D, TileIdxBandCrossProductIter, TilingStrategy,
};
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A wrapper around a `QueryProcessor` and a `QueryContext` that allows querying
/// with only a `QueryRectangle`.
pub struct QueryWrapper<'a, P, T>
where
    P: RasterQueryProcessor<RasterType = T>,
    T: Pixel,
{
    pub p: &'a P,
    pub ctx: &'a dyn QueryContext,
}

/// This trait allows hiding the concrete type of the `QueryProcessor` from the
/// `RasterTimeAdapter` and allows querying with only a `QueryRectangle`.
/// Notice, that the `query` function is not async, but return a `Future`.
/// This is necessary because there are no async function traits or async closures yet.
pub trait Queryable<T>
where
    T: Pixel,
{
    /// the type of the stream produced by the `QueryProcessor`
    type Stream;
    /// the type of the future produced by the `QueryProcessor`. We need both types
    /// to correctly specify trait bounds in the `RasterTimeAdapter`
    type Output;

    fn query(&self, rect: RasterQueryRectangle) -> Self::Output;
}

impl<'a, P, T> Queryable<T> for QueryWrapper<'a, P, T>
where
    P: RasterQueryProcessor<RasterType = T>,
    T: Pixel,
{
    type Stream = BoxStream<'a, Result<RasterTile2D<T>>>;
    type Output = BoxFuture<'a, Result<Self::Stream>>;

    fn query(&self, rect: RasterQueryRectangle) -> Self::Output {
        self.p.raster_query(rect, self.ctx)
    }
}

#[pin_project(project = ArrayStateProjection)]
enum State<T, F>
where
    T: Pixel,
    F: Queryable<T>,
    F::Stream: Stream<Item = Result<RasterTile2D<T>>>,
    F::Output: Future<Output = Result<F::Stream>>,
{
    Initial,
    AwaitingQuery {
        #[pin]
        query_futures: JoinAll<F::Output>,
    },
    ConsumingStreams {
        #[pin]
        streams: Vec<Fuse<F::Stream>>,
        stream_state: StreamState<T>,
    },

    Finished,
}

enum StreamState<T> {
    CollectingFirstTiles {
        first_tiles: Vec<Option<Result<RasterTile2D<T>>>>,
    },
    ProducingTimeSlice {
        first_tiles: Vec<RasterTile2D<T>>,
        time_slice: TimeInterval,
        current_stream: usize,
        current_stream_band_pos: usize,
        current_tile: GridIdx2D,
        current_out_band: u32,
    },
}

pub struct RasterStackerSource<Q> {
    pub queryable: Q,
    pub band_idxs: Vec<u32>,
}

impl<Q> From<(Q, Vec<u32>)> for RasterStackerSource<Q> {
    fn from(value: (Q, Vec<u32>)) -> Self {
        debug_assert!(!value.1.is_empty(), "At least one band required");
        Self {
            queryable: value.0,
            band_idxs: value.1,
        }
    }
}

/// Stacks the bands of the input raster streams to create a single raster stream with all the combined bands.
/// The input streams are automatically temporally aligned.
#[pin_project(project = RasterArrayTimeAdapterProjection)]
pub struct RasterStackerAdapter<T, F>
where
    T: Pixel,
    F: Queryable<T>,
    F::Stream: Stream<Item = Result<RasterTile2D<T>>>,
    F::Output: Future<Output = Result<F::Stream>>,
{
    // the sources (wrapped `QueryProcessor`s)
    sources: Vec<RasterStackerSource<F>>,
    #[pin]
    state: State<T, F>,
    // the current query rectangle, which is advanced over time by increasing the start time
    query_rect: RasterQueryRectangle,
    tiling_strategy: TilingStrategy,
    // this iterator helps to keep track of the elements being produced
    tile_band_iter: TileIdxBandCrossProductIter,
}

impl<T, F> RasterStackerAdapter<T, F>
where
    T: Pixel,
    F: Queryable<T>,
    F::Stream: Stream<Item = Result<RasterTile2D<T>>>,
    F::Output: Future<Output = Result<F::Stream>>,
{
    pub fn new(
        queryables: Vec<RasterStackerSource<F>>,
        query_rect: RasterQueryRectangle,
        tiling_strategy: TilingStrategy,
    ) -> Self {
        let tile_band_iter = TileIdxBandCrossProductIter::with_grid_bounds_and_selection(
            tiling_strategy
                .global_pixel_grid_bounds_to_tile_grid_bounds(query_rect.spatial_bounds()),
            query_rect.attributes().clone(),
        );

        Self {
            sources: queryables,
            query_rect,
            state: State::Initial,
            tiling_strategy,
            tile_band_iter,
        }
    }
}

impl<T, F> Stream for RasterStackerAdapter<T, F>
where
    T: Pixel,
    F: Queryable<T>,
    F::Stream: Stream<Item = Result<RasterTile2D<T>>> + Unpin,
    F::Output: Future<Output = Result<F::Stream>>,
{
    type Item = Result<RasterTile2D<T>>;

    #[allow(clippy::too_many_lines)] // TODO: refactor
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let RasterArrayTimeAdapterProjection {
            sources,
            mut state,
            query_rect,
            tiling_strategy: _tiling_strategy,
            tile_band_iter,
        } = self.project();

        loop {
            match state.as_mut().project() {
                ArrayStateProjection::Initial => {
                    let array_of_futures = sources
                        .iter()
                        .map(|source| {
                            let query_rect = query_rect.select_attributes(
                                BandSelection::new_unchecked(source.band_idxs.clone()),
                            );
                            source.queryable.query(query_rect)
                        })
                        .collect::<Vec<_>>();

                    let query_futures = futures::future::join_all(array_of_futures);

                    state.set(State::AwaitingQuery { query_futures });
                }
                ArrayStateProjection::AwaitingQuery { query_futures } => {
                    let queries = ready!(query_futures.poll(cx));

                    let mut ok_queries = Vec::with_capacity(sources.len());
                    for query in queries {
                        match query {
                            Ok(query) => ok_queries.push(query),
                            Err(e) => {
                                // at least one source failed, output error and end the stream
                                state.set(State::Finished);
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }

                    // all sources produced an output, set the stream to be consumed
                    state.set(State::ConsumingStreams {
                        streams: ok_queries.into_iter().map(StreamExt::fuse).collect(),
                        stream_state: StreamState::CollectingFirstTiles {
                            first_tiles: (0..sources.len()).map(|_| None).collect(),
                        },
                    });
                }
                ArrayStateProjection::ConsumingStreams {
                    mut streams,
                    stream_state,
                } => match stream_state {
                    StreamState::CollectingFirstTiles { first_tiles } => {
                        for (stream, value) in streams.iter_mut().zip(first_tiles.iter_mut()) {
                            if value.is_none() {
                                match Pin::new(stream).poll_next(cx) {
                                    Poll::Ready(Some(item)) => *value = Some(item),
                                    Poll::Ready(None) | Poll::Pending => {}
                                }
                            }
                        }

                        let first_tiles = if first_tiles.iter().all(Option::is_some) {
                            let mut values = Vec::new();

                            for option in &mut *first_tiles {
                                if let Some(value) = option.take() {
                                    values.push(value);
                                }
                            }

                            values
                        } else if streams.iter().any(FusedStream::is_terminated) {
                            state.set(State::Finished);
                            return Poll::Ready(None);
                        } else {
                            return Poll::Pending;
                        };

                        let mut ok_tiles = Vec::with_capacity(sources.len());
                        for tile in first_tiles {
                            match tile {
                                Ok(tile) => ok_tiles.push(tile),
                                Err(e) => {
                                    // at least one stream failed, output error and end the stream
                                    state.set(State::Finished);
                                    return Poll::Ready(Some(Err(e)));
                                }
                            }
                        }

                        let mut iter = ok_tiles.iter();
                        let mut time = iter
                            .next()
                            .expect("RasterArrayTimeAdapter must have at least one input")
                            .time;

                        for tile in iter {
                            time = time.intersect(&tile.time).unwrap_or_else(|| {
                                    panic!(
                                        "intervals must overlap: ({}/{}) <-> ({}/{})\nThis is a bug and most likely means an operator or adapter has a faulty implementation.",
                                        time.start().as_datetime_string(),
                                        time.end().as_datetime_string(),
                                        tile.time.start().as_datetime_string(),
                                        tile.time.end().as_datetime_string()
                                    )
                                });
                        }

                        let (first_tile, first_band) = tile_band_iter
                            .next()
                            .expect("There must be at least one tile and band");

                        *stream_state = StreamState::ProducingTimeSlice {
                            first_tiles: ok_tiles,
                            time_slice: time,
                            current_stream: 0,
                            current_stream_band_pos: 0,
                            current_tile: first_tile,
                            current_out_band: first_band,
                        }
                    }
                    StreamState::ProducingTimeSlice {
                        first_tiles,
                        time_slice,
                        current_stream,
                        current_stream_band_pos,
                        current_tile,
                        current_out_band,
                    } => {
                        let tile = if *current_tile == tile_band_iter.grid_bounds().min_index()
                            && *current_stream_band_pos == 0
                        {
                            // consume tiles that were already computed first
                            Some(Ok(first_tiles[*current_stream].clone())) // TODO: avoid clone and instead consume the tile
                        } else {
                            ready!(Pin::new(&mut streams[*current_stream]).poll_next(cx))
                        };

                        let mut tile: geoengine_datatypes::raster::BaseTile<
                            geoengine_datatypes::raster::GridOrEmpty<
                                geoengine_datatypes::raster::GridShape<[usize; 2]>,
                                T,
                            >,
                        > = match tile {
                            Some(Ok(tile)) => tile,
                            Some(Err(e)) => {
                                state.set(State::Finished);
                                return Poll::Ready(Some(Err(e)));
                            }
                            None => {
                                state.set(State::Finished);
                                return Poll::Ready(None);
                            }
                        };

                        #[cfg(debug_assertions)]
                        {
                            debug_assert!(
                                !tile.time.is_instant(),
                                "Tile time must be intervals with length > 0. Got an instant {}",
                                tile.time
                            );

                            let source_bands = sources[*current_stream].band_idxs.as_slice();
                            let current_souce_band = source_bands[*current_stream_band_pos];

                            debug_assert_eq!(
                                tile.band,
                                current_souce_band,
                                "RasterStacker got tile with unexpected band index: expected {}, got {} for source {} selection {:?} pos {}",
                                current_souce_band,
                                tile.band,
                                current_stream,
                                source_bands,
                                current_stream_band_pos
                            );

                            debug_assert!(
                                tile.time.contains(time_slice),
                                "RasterStacker got tile with unexpected time: time slice [{}, {}) not contained in tile time [{}, {}) for source {}",
                                time_slice.start().as_datetime_string(),
                                time_slice.end().as_datetime_string(),
                                tile.time.start().as_datetime_string(),
                                tile.time.end().as_datetime_string(),
                                current_stream
                            );

                            debug_assert_eq!(
                                tile.tile_position, *current_tile,
                                "RasteStacker got tile with unexpected tile_position: expected (state) {:?}, got (tile) {:?} for source {}",
                                current_tile, tile.tile_position, current_stream
                            );
                        }

                        tile.band = *current_out_band;
                        tile.time = *time_slice;

                        let next_step = tile_band_iter.next();

                        match next_step {
                            Some((next_tile, next_out_band)) if next_tile == *current_tile => {
                                // the next tile is in the same spatial tile, just a different band

                                // make progress
                                *current_out_band = next_out_band;
                                *current_stream_band_pos += 1;
                                if *current_stream_band_pos as usize
                                    >= sources[*current_stream].band_idxs.len()
                                {
                                    *current_stream_band_pos = 0;
                                    *current_stream += 1;
                                }
                                debug_assert!(
                                    *current_stream < sources.len(),
                                    "Current stream must not excede the number of streams"
                                );
                            }
                            Some((next_tile, next_band)) => {
                                // the next tile is in a different spatial tile. We need to start with 0 here...
                                *current_stream_band_pos = 0;
                                *current_stream = 0;
                                *current_tile = next_tile;
                                *current_out_band = next_band;
                            }
                            None => {
                                // this is either a new TimeStep OR finish!
                                let new_start = time_slice.end();

                                if new_start >= query_rect.time_interval().end() {
                                    // the query window is exhausted, end the stream
                                    state.set(State::Finished);
                                } else {
                                    // advance the query rectangle and reset the state so that the sources are queried again for the next time step
                                    *query_rect = query_rect.select_time_interval(
                                        TimeInterval::new_unchecked(
                                            new_start,
                                            query_rect.time_interval().end(),
                                        ),
                                    );
                                    tile_band_iter.reset(); // reset iter to start at first tile / band

                                    state.set(State::Initial);
                                }
                            }
                        }

                        return Poll::Ready(Some(Ok(tile)));
                    }
                },

                ArrayStateProjection::Finished => return Poll::Ready(None),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::{CacheHint, Measurement, TimeInterval, TimeStep},
        raster::{
            GeoTransform, Grid, GridBoundingBox2D, GridShape, RasterDataType,
            TilesEqualIgnoringCacheHint,
        },
        spatial_reference::SpatialReference,
        util::test::{TestDefault, assert_eq_two_list_of_tiles},
    };

    use crate::{
        engine::{
            MockExecutionContext, RasterBandDescriptor, RasterBandDescriptors, RasterOperator,
            RasterResultDescriptor, SpatialGridDescriptor, TimeDescriptor, WorkflowOperatorPath,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
    };

    use super::*;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_stacks() {
        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(None, TimeStep::millis(5).unwrap()),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [0, 4]).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let data2: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let qp1 = mrs1
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qp2 = mrs2
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let stacker = RasterStackerAdapter::new(
            vec![
                (
                    QueryWrapper {
                        p: &qp1,
                        ctx: &query_ctx,
                    },
                    vec![0],
                )
                    .into(),
                (
                    QueryWrapper {
                        p: &qp2,
                        ctx: &query_ctx,
                    },
                    vec![0],
                )
                    .into(),
            ],
            RasterQueryRectangle::new(
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
                TimeInterval::new_unchecked(0, 10),
                BandSelection::new_unchecked(vec![0, 1]),
            ),
            result_descriptor
                .tiling_grid_definition(exe_ctx.tiling_specification)
                .generate_data_tiling_strategy(),
        );

        let result = stacker.collect::<Vec<_>>().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let expected: Vec<_> = data
            .into_iter()
            .zip(data2.into_iter().map(|mut tile| {
                tile.band = 1;
                tile
            }))
            .flat_map(|(a, b)| vec![a.clone(), b.clone()])
            .collect();

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_keeps_single_band_input() {
        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(None, TimeStep::millis(5).unwrap()),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 4]).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let qp1 = mrs1
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let stacker = RasterStackerAdapter::new(
            vec![
                (
                    QueryWrapper {
                        p: &qp1,
                        ctx: &query_ctx,
                    },
                    vec![0],
                )
                    .into(),
            ],
            RasterQueryRectangle::new(
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
                TimeInterval::new_unchecked(0, 10),
                BandSelection::new_unchecked(vec![0]),
            ),
            result_descriptor
                .tiling_grid_definition(exe_ctx.tiling_specification)
                .generate_data_tiling_strategy(),
        );

        let result = stacker.collect::<Vec<_>>().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert!(data.tiles_equal_ignoring_cache_hint(&result));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_stacks_stacks() {
        let result_descriptor_1 = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(None, TimeStep::millis(5).unwrap()),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            ),
            bands: vec![
                RasterBandDescriptor::new("mrs1 band1".to_string(), Measurement::Unitless),
                RasterBandDescriptor::new("mrs1 band2".to_string(), Measurement::Unitless),
            ]
            .try_into()
            .unwrap(),
        };

        let result_descriptor_2 = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(None, TimeStep::millis(5).unwrap()),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            ),
            bands: vec![
                RasterBandDescriptor::new("mrs2 band1".to_string(), Measurement::Unitless),
                RasterBandDescriptor::new("mrs2 band2".to_string(), Measurement::Unitless),
            ]
            .try_into()
            .unwrap(),
        };

        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 2, 1, 0]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 6, 5, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![11, 10, 9, 8]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![15, 14, 13, 12])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let data2: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 18, 17, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![32, 22, 21, 20])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![27, 26, 25, 24])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![31, 30, 39, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor_1.clone(),
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor: result_descriptor_2,
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let qp1 = mrs1
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qp2 = mrs2
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let stacker = RasterStackerAdapter::new(
            vec![
                (
                    QueryWrapper {
                        p: &qp1,
                        ctx: &query_ctx,
                    },
                    vec![0, 1],
                )
                    .into(),
                (
                    QueryWrapper {
                        p: &qp2,
                        ctx: &query_ctx,
                    },
                    vec![0, 1],
                )
                    .into(),
            ],
            RasterQueryRectangle::new(
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
                TimeInterval::new_unchecked(0, 10),
                BandSelection::first_n(4),
            ),
            result_descriptor_1
                .tiling_grid_definition(exe_ctx.tiling_specification)
                .generate_data_tiling_strategy(),
        );

        let result = stacker.collect::<Vec<_>>().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let expected: Vec<_> = data
            .chunks(2)
            .zip(
                data2
                    .into_iter()
                    .map(|mut tile| {
                        tile.band += 2;
                        tile
                    })
                    .collect::<Vec<_>>()
                    .chunks(2),
            )
            .flat_map(|(chunk1, chunk2)| chunk1.iter().chain(chunk2.iter()))
            .cloned()
            .collect();

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_aligns_temporally_while_stacking_stacks() {
        let result_descriptor_1 = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(None, TimeStep::millis(5).unwrap()),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            ),

            bands: vec![
                RasterBandDescriptor::new("mrs1 band1".to_string(), Measurement::Unitless),
                RasterBandDescriptor::new("mrs1 band2".to_string(), Measurement::Unitless),
            ]
            .try_into()
            .unwrap(),
        };

        let result_descriptor_2 = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(None),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            ),
            bands: vec![
                RasterBandDescriptor::new("mrs2 band1".to_string(), Measurement::Unitless),
                RasterBandDescriptor::new("mrs2 band2".to_string(), Measurement::Unitless),
            ]
            .try_into()
            .unwrap(),
        };

        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 2, 1, 0]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 6, 5, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![11, 10, 9, 8]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![15, 14, 13, 12])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let data2: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 18, 17, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![32, 22, 21, 20])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![27, 26, 25, 24])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![31, 30, 39, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor_1.clone(),
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor: result_descriptor_2,
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let qp1 = mrs1
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qp2 = mrs2
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let stacker = RasterStackerAdapter::new(
            vec![
                (
                    QueryWrapper {
                        p: &qp1,
                        ctx: &query_ctx,
                    },
                    vec![0, 1],
                )
                    .into(),
                (
                    QueryWrapper {
                        p: &qp2,
                        ctx: &query_ctx,
                    },
                    vec![0, 1],
                )
                    .into(),
            ],
            RasterQueryRectangle::new(
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
                TimeInterval::new_unchecked(0, 10),
                BandSelection::first_n(4),
            ),
            result_descriptor_1
                .tiling_grid_definition(exe_ctx.tiling_specification)
                .generate_data_tiling_strategy(),
        );

        let result = stacker.collect::<Vec<_>>().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let expected: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 2, 1, 0]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 18, 17, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 6, 5, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![32, 22, 21, 20])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 2, 1, 0]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 0].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![27, 26, 25, 24])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 6, 5, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 1].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 1].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![31, 30, 39, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![11, 10, 9, 8]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![27, 26, 25, 24])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![15, 14, 13, 12])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![31, 30, 39, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_stacks_more() {
        let result_descriptor_1 = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(None, TimeStep::millis(5).unwrap()),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            ),
            bands: vec![
                RasterBandDescriptor::new("mrs1 band1".to_string(), Measurement::Unitless),
                RasterBandDescriptor::new("mrs1 band2".to_string(), Measurement::Unitless),
                RasterBandDescriptor::new("mrs1 band3".to_string(), Measurement::Unitless),
            ]
            .try_into()
            .unwrap(),
        };

        let result_descriptor_2 = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(None),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            ),
            bands: vec![RasterBandDescriptor::new(
                "mrs2 band2".to_string(),
                Measurement::Unitless,
            )]
            .try_into()
            .unwrap(),
        };

        let result_descriptor_3 = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(None),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            ),
            bands: vec![
                RasterBandDescriptor::new("mrs3 band1".to_string(), Measurement::Unitless),
                RasterBandDescriptor::new("mrs3 band2".to_string(), Measurement::Unitless),
            ]
            .try_into()
            .unwrap(),
        };

        // input 1: 3 bands
        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 2, 1, 0]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![223, 222, 221, 20])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 6, 5, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![227, 226, 225, 224])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![11, 10, 9, 8]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![211, 210, 29, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![15, 14, 13, 12])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![215, 214, 213, 212])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        // input 2: 1 band
        let data2: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![88, 77, 66, 55])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![87, 76, 65, 54])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![86, 75, 63, 51])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![85, 74, 62, 50])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        // input 3: 2 bands
        let data3: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 18, 17, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![32, 22, 21, 20])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![27, 26, 25, 24])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![31, 30, 39, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor_1.clone(),
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor: result_descriptor_2,
            },
        }
        .boxed();

        let mrs3 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data3.clone(),
                result_descriptor: result_descriptor_3,
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let qp1 = mrs1
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qp2 = mrs2
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qp3 = mrs3
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let stacker = RasterStackerAdapter::new(
            vec![
                (
                    QueryWrapper {
                        p: &qp1,
                        ctx: &query_ctx,
                    },
                    vec![0, 1, 2],
                )
                    .into(),
                (
                    QueryWrapper {
                        p: &qp2,
                        ctx: &query_ctx,
                    },
                    vec![0],
                )
                    .into(),
                (
                    QueryWrapper {
                        p: &qp3,
                        ctx: &query_ctx,
                    },
                    vec![0, 1],
                )
                    .into(),
            ],
            RasterQueryRectangle::new(
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
                TimeInterval::new_unchecked(0, 10),
                BandSelection::first_n(6),
            ),
            result_descriptor_1
                .tiling_grid_definition(exe_ctx.tiling_specification)
                .generate_data_tiling_strategy(),
        );

        let result = stacker.collect::<Vec<_>>().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let expected: Vec<RasterTile2D<u8>> = vec![
            // time slice 1
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 2, 1, 0]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![223, 222, 221, 20])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![88, 77, 66, 55])
                    .unwrap()
                    .into(),
                properties: Default::default(),

                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 4,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 5,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 18, 17, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 6, 5, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![227, 226, 225, 224])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![87, 76, 65, 54])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 4,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 5,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![32, 22, 21, 20])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            // time slice 2
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 2, 1, 0]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![223, 222, 221, 20])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 0].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![86, 75, 63, 51])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 0].into(),
                band: 4,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 0].into(),
                band: 5,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![27, 26, 25, 24])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 6, 5, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 1].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![227, 226, 225, 224])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 1].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![85, 74, 62, 50])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 1].into(),
                band: 4,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 1].into(),
                band: 5,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![31, 30, 39, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            // time slice 3
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![11, 10, 9, 8]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![211, 210, 29, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![86, 75, 63, 51])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 4,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 5,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![27, 26, 25, 24])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![15, 14, 13, 12])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![215, 214, 213, 212])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![85, 74, 62, 50])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 4,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 5,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![31, 30, 39, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        assert_eq_two_list_of_tiles(&expected, &result, false);
    }

    #[tokio::test]
    async fn it_produces_selected_bands() {
        let result_descriptor_1 = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(None, TimeStep::millis(5).unwrap()),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            ),
            bands: vec![
                RasterBandDescriptor::new("mrs1 band1".to_string(), Measurement::Unitless),
                RasterBandDescriptor::new("mrs1 band2".to_string(), Measurement::Unitless),
                RasterBandDescriptor::new("mrs1 band3".to_string(), Measurement::Unitless),
            ]
            .try_into()
            .unwrap(),
        };

        let result_descriptor_2 = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(None),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            ),
            bands: vec![RasterBandDescriptor::new(
                "mrs2 band2".to_string(),
                Measurement::Unitless,
            )]
            .try_into()
            .unwrap(),
        };

        let result_descriptor_3 = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(None),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            ),
            bands: vec![
                RasterBandDescriptor::new("mrs3 band1".to_string(), Measurement::Unitless),
                RasterBandDescriptor::new("mrs3 band2".to_string(), Measurement::Unitless),
            ]
            .try_into()
            .unwrap(),
        };

        // input 1: 3 bands
        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 2, 1, 0]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![223, 222, 221, 20])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 6, 5, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![227, 226, 225, 224])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![11, 10, 9, 8]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![211, 210, 29, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![15, 14, 13, 12])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 2,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![215, 214, 213, 212])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        // input 2: 1 band
        let data2: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![88, 77, 66, 55])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![87, 76, 65, 54])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        // input 3: 2 bands
        let data3: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 18, 17, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![32, 22, 21, 20])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![27, 26, 25, 24])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![31, 30, 39, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor_1.clone(),
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor: result_descriptor_2,
            },
        }
        .boxed();

        let mrs3 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data3.clone(),
                result_descriptor: result_descriptor_3,
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let qp1 = mrs1
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qp2 = mrs2
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qp3 = mrs3
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let stacker = RasterStackerAdapter::new(
            vec![
                (
                    QueryWrapper {
                        p: &qp1,
                        ctx: &query_ctx,
                    },
                    vec![1],
                )
                    .into(),
                (
                    QueryWrapper {
                        p: &qp2,
                        ctx: &query_ctx,
                    },
                    vec![0],
                )
                    .into(),
                (
                    QueryWrapper {
                        p: &qp3,
                        ctx: &query_ctx,
                    },
                    vec![1],
                )
                    .into(),
            ],
            RasterQueryRectangle::new(
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
                TimeInterval::new_unchecked(0, 10),
                BandSelection::new(vec![1, 3, 5]).unwrap(),
            ),
            result_descriptor_1
                .tiling_grid_definition(exe_ctx.tiling_specification)
                .generate_data_tiling_strategy(),
        );

        let result = stacker.collect::<Vec<_>>().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let expected: Vec<RasterTile2D<u8>> = vec![
            // time slice 1
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 2, 1, 0]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![88, 77, 66, 55])
                    .unwrap()
                    .into(),
                properties: Default::default(),

                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 0].into(),
                band: 5,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 18, 17, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 6, 5, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![87, 76, 65, 54])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 2),
                tile_position: [-1, 1].into(),
                band: 5,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![32, 22, 21, 20])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 2, 1, 0]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 0].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![86, 75, 63, 51])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 0].into(),
                band: 5,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![27, 26, 25, 24])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 6, 5, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 1].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![85, 74, 62, 50])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(2, 5),
                tile_position: [-1, 1].into(),
                band: 5,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![31, 30, 39, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            // time slice 3
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![11, 10, 9, 8]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![86, 75, 63, 51])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 5,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![27, 26, 25, 24])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![15, 14, 13, 12])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 3,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![85, 74, 62, 50])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 5,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![31, 30, 39, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        assert_eq_two_list_of_tiles(&expected, &result, false);
    }
}
