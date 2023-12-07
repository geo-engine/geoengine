use crate::error::{AtLeastOneStreamRequired, Error};
use crate::util::stream_zip::StreamArrayZip;
use crate::util::Result;
use futures::future::JoinAll;
use futures::stream::Stream;
use futures::{ready, Future, StreamExt};
use geoengine_datatypes::primitives::{
    RasterQueryRectangle, SpatialPartition2D, TimeInstance, TimeInterval,
};
use geoengine_datatypes::raster::{GridSize, Pixel, RasterTile2D, TileInformation, TilingStrategy};
use pin_project::pin_project;
use snafu::ensure;
use std::cmp::min;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::Queryable;

#[pin_project(project = ArrayStateProjection)]
enum State<T, F, const N: usize>
where
    T: Pixel,
    F: Queryable<T>,
    F::Stream: Stream<Item = Result<RasterTile2D<T>>>,
    F::Output: Future<Output = Result<F::Stream>>,
{
    Initial,
    AwaitingQuery {
        #[pin]
        query_futures: JoinAll<F::Output>, // TODO: use join construct on array if available
    },
    ConsumingStreams {
        #[pin]
        streams: [F::Stream; N],
        stream_state: StreamState<T, N>,
    },

    Finished,
}

// #[pin_project(project = StreamStateProjection)]
enum StreamState<T, const N: usize> {
    CollectingFirstTiles {
        first_tiles: [Option<Result<RasterTile2D<T>>>; N],
    },
    ProducingTimeSlice {
        first_tiles: [RasterTile2D<T>; N],
        time_slice_end: TimeInstance,
        current_stream: usize,
        current_band: usize,
        current_spatial_tile: usize,
    },
}

/// Stacks the bands of the input raster streams to create a single raster stream with all the combined bands.
#[pin_project(project = RasterArrayTimeAdapterProjection)]
pub struct RasterStackerAdapter<T, F, const N: usize>
where
    T: Pixel,
    F: Queryable<T>,
    F::Stream: Stream<Item = Result<RasterTile2D<T>>>,
    F::Output: Future<Output = Result<F::Stream>>,
{
    // the sources (wrapped `QueryProcessor`s)
    sources: [F; N],
    #[pin]
    state: State<T, F, N>,
    // the current query rectangle, which is advanced over time by increasing the start time
    query_rect: RasterQueryRectangle,
    num_spatial_tiles: Option<usize>,
    num_bands: [u32; N],
}

impl<T, F, const N: usize> RasterStackerAdapter<T, F, N>
where
    T: Pixel,
    F: Queryable<T>,
    F::Stream: Stream<Item = Result<RasterTile2D<T>>>,
    F::Output: Future<Output = Result<F::Stream>>,
{
    pub fn new(sources: [F; N], num_bands: [u32; N], query_rect: RasterQueryRectangle) -> Self {
        // TODO: need to ensure all sources are single-band
        Self {
            sources,
            query_rect,
            state: State::Initial,
            num_spatial_tiles: None,
            num_bands,
        }
    }

    fn align_tiles(mut tiles: [RasterTile2D<T>; N]) -> [RasterTile2D<T>; N] {
        let mut iter = tiles.iter();

        // first time as initial time
        let mut time = iter.next().expect("RasterArrayTimeAdapter: N > 0").time;

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

        for tile in &mut tiles {
            tile.time = time;
        }

        tiles
    }

    fn number_of_tiles_in_partition(
        tile_info: &TileInformation,
        partition: SpatialPartition2D,
    ) -> usize {
        // TODO: get tiling strategy from stream or execution context instead of creating it here
        let strat = TilingStrategy {
            tile_size_in_pixels: tile_info.tile_size_in_pixels,
            geo_transform: tile_info.global_geo_transform,
        };

        strat.tile_grid_box(partition).number_of_elements()
    }
}

impl<T, F, const N: usize> Stream for RasterStackerAdapter<T, F, N>
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
            num_spatial_tiles,
            num_bands,
        } = self.project();

        loop {
            match state.as_mut().project() {
                ArrayStateProjection::Initial => {
                    let array_of_futures = sources
                        .iter()
                        .map(|source| source.query(query_rect.clone()))
                        .collect::<Vec<_>>();

                    let query_futures = futures::future::join_all(array_of_futures);

                    state.set(State::AwaitingQuery { query_futures });
                }
                ArrayStateProjection::AwaitingQuery { query_futures } => {
                    let queries = ready!(query_futures.poll(cx));

                    let mut ok_queries = Vec::with_capacity(N);
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
                    let Ok(streams) = ok_queries.try_into() else {
                        unreachable!("RasterArrayTimeAdapter: ok_queries.len() != N");
                    };
                    state.set(State::ConsumingStreams {
                        streams, // TODO: fuse?
                        stream_state: StreamState::CollectingFirstTiles {
                            first_tiles: [(); N].map(|_| None),
                        },
                    });
                }
                ArrayStateProjection::ConsumingStreams {
                    mut streams,
                    stream_state,
                } => match stream_state {
                    StreamState::CollectingFirstTiles { first_tiles } => {
                        for (stream, value) in streams.iter_mut().zip(first_tiles.as_mut()) {
                            if value.is_none() {
                                match Pin::new(stream).poll_next(cx) {
                                    Poll::Ready(Some(item)) => *value = Some(item),
                                    Poll::Ready(None) | Poll::Pending => {}
                                }
                            }
                        }

                        let first_tiles = if first_tiles.iter().all(Option::is_some) {
                            let values: [Option<Result<RasterTile2D<T>>>; N] =
                                std::mem::replace(first_tiles, [(); N].map(|_| None));
                            values.map(Option::unwrap)
                        } else {
                            state.set(State::Finished);
                            return Poll::Ready(None);
                        };

                        let mut ok_tiles = Vec::with_capacity(N);
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

                        let first_tiles: [RasterTile2D<T>; N] = ok_tiles
                            .try_into()
                            .expect("RasterArrayTimeAdapter: ok_tiles.len() != N");

                        let mut iter = first_tiles.iter();
                        let mut time = iter.next().expect("RasterArrayTimeAdapter: N > 0").time;

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

                        *stream_state = StreamState::ProducingTimeSlice {
                            first_tiles,
                            time_slice_end: time.end(),
                            current_stream: 0,
                            current_band: 0,
                            current_spatial_tile: 0,
                        }
                    }
                    StreamState::ProducingTimeSlice {
                        first_tiles,
                        time_slice_end,
                        current_stream,
                        current_band,
                        current_spatial_tile,
                    } => {
                        let tile = if *current_spatial_tile == 0 && *current_band == 0 {
                            // consume tiles that were already computed first
                            Some(Ok(first_tiles[*current_stream].clone()))
                        } else {
                            ready!(Pin::new(&mut streams[*current_stream]).poll_next(cx))
                        };

                        let mut tile = match tile {
                            Some(Ok(tile)) => tile,
                            Some(Err(e)) => {
                                // at least one stream failed, output error and end the stream
                                state.set(State::Finished);
                                return Poll::Ready(Some(Err(e)));
                            }
                            None => {
                                state.set(State::Finished);
                                return Poll::Ready(None);
                            }
                        };

                        tile.time =
                            TimeInterval::new_unchecked(tile.time.start(), time_slice_end.clone());

                        // make progress
                        let inc_stream = if *current_band + 1 < num_bands[*current_stream] as usize
                        {
                            *current_band += 1;
                            false
                        } else {
                            *current_band = 0;
                            true
                        };

                        let inc_space = if inc_stream && *current_stream + 1 < streams.len() {
                            *current_stream += 1;
                            false
                        } else {
                            *current_stream = 0;
                            true
                        };

                        let inc_time = if inc_space
                            && *current_spatial_tile + 1 < num_spatial_tiles.unwrap()
                        // TODO?
                        {
                            *current_spatial_tile += 1;
                            false
                        } else {
                            *current_spatial_tile = 0;
                            true
                        };

                        if inc_time {
                            let mut new_start = *time_slice_end;

                            if new_start == query_rect.time_interval.start() {
                                // in the case that the time interval has no length, i.e. start=end,
                                // we have to advance `new_start` to prevent infinite loops.
                                // Otherwise, the new query rectangle would be equal to the previous one.
                                new_start += 1;
                            }

                            if new_start >= query_rect.time_interval.end() {
                                // the query window is exhausted, end the stream
                                state.set(State::Finished);
                            } else {
                                query_rect.time_interval = TimeInterval::new_unchecked(
                                    new_start,
                                    query_rect.time_interval.end(),
                                );

                                state.set(State::Initial);
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

// #[cfg(test)]
// mod tests {
//     use futures::{stream, StreamExt};
//     use geoengine_datatypes::{
//         primitives::{CacheHint, TimeInterval},
//         raster::{Grid, TilesEqualIgnoringCacheHint},
//         util::test::TestDefault,
//     };

//     use super::*;

//     #[tokio::test]
//     #[allow(clippy::too_many_lines)]
//     async fn it_stacks() {
//         let data: Vec<RasterTile2D<u8>> = vec![
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(0, 5),
//                 tile_position: [-1, 0].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(0, 5),
//                 tile_position: [-1, 1].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(5, 10),
//                 tile_position: [-1, 0].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(5, 10),
//                 tile_position: [-1, 1].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//         ];

//         let data2: Vec<RasterTile2D<u8>> = vec![
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(0, 5),
//                 tile_position: [-1, 0].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(0, 5),
//                 tile_position: [-1, 1].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(5, 10),
//                 tile_position: [-1, 0].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(5, 10),
//                 tile_position: [-1, 1].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//         ];

//         let stream = stream::iter(data.clone().into_iter().map(Result::Ok)).boxed();
//         let stream2 = stream::iter(data2.clone().into_iter().map(Result::Ok)).boxed();

//         let stacker =
//             RasterStackerAdapter::new(vec![(stream, 1).into(), (stream2, 1).into()]).unwrap();

//         let result = stacker.collect::<Vec<_>>().await;
//         let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

//         let expected: Vec<_> = data
//             .into_iter()
//             .zip(data2.into_iter().map(|mut tile| {
//                 tile.band = 1;
//                 tile
//             }))
//             .flat_map(|(a, b)| vec![a.clone(), b.clone()])
//             .collect();

//         assert!(expected.tiles_equal_ignoring_cache_hint(&result));
//     }

//     #[tokio::test]
//     #[allow(clippy::too_many_lines)]
//     async fn it_stacks_stacks() {
//         let data: Vec<RasterTile2D<u8>> = vec![
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(0, 5),
//                 tile_position: [-1, 0].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(0, 5),
//                 tile_position: [-1, 0].into(),
//                 band: 1,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![3, 2, 1, 0]).unwrap().into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(0, 5),
//                 tile_position: [-1, 1].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(0, 5),
//                 tile_position: [-1, 1].into(),
//                 band: 1,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![7, 6, 5, 4]).unwrap().into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(5, 10),
//                 tile_position: [-1, 0].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(5, 10),
//                 tile_position: [-1, 0].into(),
//                 band: 1,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![11, 10, 9, 8]).unwrap().into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(5, 10),
//                 tile_position: [-1, 1].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(5, 10),
//                 tile_position: [-1, 1].into(),
//                 band: 1,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![15, 14, 13, 12])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//         ];

//         let data2: Vec<RasterTile2D<u8>> = vec![
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(0, 5),
//                 tile_position: [-1, 0].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(0, 5),
//                 tile_position: [-1, 0].into(),
//                 band: 1,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![19, 18, 17, 16])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(0, 5),
//                 tile_position: [-1, 1].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(0, 5),
//                 tile_position: [-1, 1].into(),
//                 band: 1,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![32, 22, 21, 20])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(5, 10),
//                 tile_position: [-1, 0].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(5, 10),
//                 tile_position: [-1, 0].into(),
//                 band: 1,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![27, 26, 25, 24])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(5, 10),
//                 tile_position: [-1, 1].into(),
//                 band: 0,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//             RasterTile2D {
//                 time: TimeInterval::new_unchecked(5, 10),
//                 tile_position: [-1, 1].into(),
//                 band: 1,
//                 global_geo_transform: TestDefault::test_default(),
//                 grid_array: Grid::new([2, 2].into(), vec![31, 30, 39, 28])
//                     .unwrap()
//                     .into(),
//                 properties: Default::default(),
//                 cache_hint: CacheHint::default(),
//             },
//         ];

//         let stream = stream::iter(data.clone().into_iter().map(Result::Ok)).boxed();
//         let stream2 = stream::iter(data2.clone().into_iter().map(Result::Ok)).boxed();

//         let stacker =
//             RasterStackerAdapter::new(vec![(stream, 2).into(), (stream2, 2).into()]).unwrap();

//         let result = stacker.collect::<Vec<_>>().await;
//         let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

//         let expected: Vec<_> = data
//             .chunks(2)
//             .zip(
//                 data2
//                     .into_iter()
//                     .map(|mut tile| {
//                         tile.band += 2;
//                         tile
//                     })
//                     .collect::<Vec<_>>()
//                     .chunks(2),
//             )
//             .flat_map(|(chunk1, chunk2)| chunk1.iter().chain(chunk2.iter()))
//             .cloned()
//             .collect();

//         assert!(expected.tiles_equal_ignoring_cache_hint(&result));
//     }

//     #[tokio::test]
//     async fn it_checks_temporal_alignment() {
//         let data: Vec<RasterTile2D<u8>> = vec![RasterTile2D {
//             time: TimeInterval::new_unchecked(0, 5),
//             tile_position: [-1, 0].into(),
//             band: 0,
//             global_geo_transform: TestDefault::test_default(),
//             grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
//             properties: Default::default(),
//             cache_hint: CacheHint::default(),
//         }];

//         let data2: Vec<RasterTile2D<u8>> = vec![RasterTile2D {
//             time: TimeInterval::new_unchecked(1, 6),
//             tile_position: [-1, 0].into(),
//             band: 0,
//             global_geo_transform: TestDefault::test_default(),
//             grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
//                 .unwrap()
//                 .into(),
//             properties: Default::default(),
//             cache_hint: CacheHint::default(),
//         }];

//         let stream = stream::iter(data.clone().into_iter().map(Result::Ok)).boxed();
//         let stream2 = stream::iter(data2.clone().into_iter().map(Result::Ok)).boxed();

//         let stacker =
//             RasterStackerAdapter::new(vec![(stream, 1).into(), (stream2, 1).into()]).unwrap();

//         let result = stacker.collect::<Vec<_>>().await;

//         assert!(result[0].is_ok());
//         assert!(matches!(
//             result[1],
//             Err(Error::InputStreamsMustBeTemporallyAligned { .. })
//         ));
//     }
// }
