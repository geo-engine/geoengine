use crate::util::Result;
use futures::future::JoinAll;
use futures::stream::Stream;
use futures::{ready, Future};
use geoengine_datatypes::primitives::{
    BandSelection, RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval,
};
use geoengine_datatypes::raster::{GridSize, Pixel, RasterTile2D, TileInformation, TilingStrategy};
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::Queryable;

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
        streams: Vec<F::Stream>,
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
        current_band: usize,
        current_spatial_tile: usize,
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

#[derive(Debug)]
pub struct PartialQueryRect {
    pub spatial_bounds: SpatialPartition2D,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
}

impl PartialQueryRect {
    fn raster_query_rectangle(&self, attributes: BandSelection) -> RasterQueryRectangle {
        RasterQueryRectangle {
            spatial_bounds: self.spatial_bounds,
            time_interval: self.time_interval,
            spatial_resolution: self.spatial_resolution,
            attributes,
        }
    }
}

impl From<RasterQueryRectangle> for PartialQueryRect {
    fn from(value: RasterQueryRectangle) -> Self {
        Self {
            spatial_bounds: value.spatial_bounds,
            time_interval: value.time_interval,
            spatial_resolution: value.spatial_resolution,
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
    query_rect: PartialQueryRect,
    num_spatial_tiles: Option<usize>,
}

impl<T, F> RasterStackerAdapter<T, F>
where
    T: Pixel,
    F: Queryable<T>,
    F::Stream: Stream<Item = Result<RasterTile2D<T>>>,
    F::Output: Future<Output = Result<F::Stream>>,
{
    pub fn new(queryables: Vec<RasterStackerSource<F>>, query_rect: PartialQueryRect) -> Self {
        Self {
            sources: queryables,
            query_rect,
            state: State::Initial,
            num_spatial_tiles: None,
        }
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
            num_spatial_tiles,
        } = self.project();

        loop {
            match state.as_mut().project() {
                ArrayStateProjection::Initial => {
                    let array_of_futures = sources
                        .iter()
                        .map(|source| {
                            let query_rect = query_rect.raster_query_rectangle(
                                BandSelection::new_unchecked(source.band_idxs.clone()),
                            );
                            source.queryable.query(query_rect.clone())
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
                        streams: ok_queries, // TODO: fuse?
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
                        } else {
                            state.set(State::Finished);
                            return Poll::Ready(None);
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

                        *num_spatial_tiles = Some(Self::number_of_tiles_in_partition(
                            &ok_tiles[0].tile_information(),
                            query_rect.spatial_bounds,
                        ));

                        *stream_state = StreamState::ProducingTimeSlice {
                            first_tiles: ok_tiles,
                            time_slice: time,
                            current_stream: 0,
                            current_band: 0,
                            current_spatial_tile: 0,
                        }
                    }
                    StreamState::ProducingTimeSlice {
                        first_tiles,
                        time_slice,
                        current_stream,
                        current_band,
                        current_spatial_tile,
                    } => {
                        let tile = if *current_spatial_tile == 0 && *current_band == 0 {
                            // consume tiles that were already computed first
                            Some(Ok(first_tiles[*current_stream].clone())) // TODO: avoid clone and instead consume the tile
                        } else {
                            ready!(Pin::new(&mut streams[*current_stream]).poll_next(cx))
                        };

                        let mut tile = match tile {
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

                        tile.band = sources
                            .iter()
                            .take(*current_stream)
                            .map(|b| b.band_idxs.len() as u32)
                            .sum::<u32>()
                            + *current_band as u32;
                        tile.time = *time_slice;

                        // make progress
                        *current_band += 1;
                        if *current_band >= sources[*current_stream].band_idxs.len() {
                            *current_band = 0;
                            *current_stream += 1;
                        }

                        if *current_stream >= streams.len() {
                            *current_stream = 0;
                            *current_spatial_tile += 1;
                        }

                        if *current_spatial_tile >= num_spatial_tiles.unwrap() {
                            *current_spatial_tile = 0;
                            *current_band = 0;
                            *current_stream = 0;

                            let mut new_start = time_slice.end();

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
                                // advance the query rectangle and reset the state so that the sources are queried again for the next time step
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

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::{CacheHint, Measurement, SpatialResolution, TimeInterval},
        raster::{Grid, GridShape, RasterDataType, TilesEqualIgnoringCacheHint},
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use crate::{
        adapters::QueryWrapper,
        engine::{
            MockExecutionContext, MockQueryContext, RasterBandDescriptor, RasterBandDescriptors,
            RasterOperator, RasterResultDescriptor, WorkflowOperatorPath,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
    };

    use super::*;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_stacks() {
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
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
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

        let query_ctx = MockQueryContext::test_default();

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
            PartialQueryRect {
                spatial_bounds: SpatialPartition2D::new_unchecked([0., 1.].into(), [3., 0.].into()),
                time_interval: TimeInterval::new_unchecked(0, 10),
                spatial_resolution: SpatialResolution::one(),
            },
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
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
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

        let query_ctx = MockQueryContext::test_default();

        let stacker = RasterStackerAdapter::new(
            vec![(
                QueryWrapper {
                    p: &qp1,
                    ctx: &query_ctx,
                },
                vec![0],
            )
                .into()],
            PartialQueryRect {
                spatial_bounds: SpatialPartition2D::new_unchecked([0., 1.].into(), [3., 0.].into()),
                time_interval: TimeInterval::new_unchecked(0, 10),
                spatial_resolution: SpatialResolution::one(),
            },
        );

        let result = stacker.collect::<Vec<_>>().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert!(data.tiles_equal_ignoring_cache_hint(&result));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_stacks_stacks() {
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
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: vec![
                        RasterBandDescriptor::new("mrs1 band1".to_string(), Measurement::Unitless),
                        RasterBandDescriptor::new("mrs1 band2".to_string(), Measurement::Unitless),
                    ]
                    .try_into()
                    .unwrap(),
                },
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: vec![
                        RasterBandDescriptor::new("mrs2 band1".to_string(), Measurement::Unitless),
                        RasterBandDescriptor::new("mrs2 band2".to_string(), Measurement::Unitless),
                    ]
                    .try_into()
                    .unwrap(),
                },
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

        let query_ctx = MockQueryContext::test_default();

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
            PartialQueryRect {
                spatial_bounds: SpatialPartition2D::new_unchecked([0., 1.].into(), [3., 0.].into()),
                time_interval: TimeInterval::new_unchecked(0, 10),
                spatial_resolution: SpatialResolution::one(),
            },
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
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: vec![
                        RasterBandDescriptor::new("mrs1 band1".to_string(), Measurement::Unitless),
                        RasterBandDescriptor::new("mrs1 band2".to_string(), Measurement::Unitless),
                    ]
                    .try_into()
                    .unwrap(),
                },
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: vec![
                        RasterBandDescriptor::new("mrs2 band1".to_string(), Measurement::Unitless),
                        RasterBandDescriptor::new("mrs2 band2".to_string(), Measurement::Unitless),
                    ]
                    .try_into()
                    .unwrap(),
                },
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

        let query_ctx = MockQueryContext::test_default();

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
            PartialQueryRect {
                spatial_bounds: SpatialPartition2D::new_unchecked([0., 1.].into(), [3., 0.].into()),
                time_interval: TimeInterval::new_unchecked(0, 10),
                spatial_resolution: SpatialResolution::one(),
            },
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
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: vec![
                        RasterBandDescriptor::new("mrs1 band1".to_string(), Measurement::Unitless),
                        RasterBandDescriptor::new("mrs1 band2".to_string(), Measurement::Unitless),
                        RasterBandDescriptor::new("mrs1 band3".to_string(), Measurement::Unitless),
                    ]
                    .try_into()
                    .unwrap(),
                },
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: vec![RasterBandDescriptor::new(
                        "mrs2 band1".to_string(),
                        Measurement::Unitless,
                    )]
                    .try_into()
                    .unwrap(),
                },
            },
        }
        .boxed();

        let mrs3 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data3.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: vec![
                        RasterBandDescriptor::new("mrs3 band1".to_string(), Measurement::Unitless),
                        RasterBandDescriptor::new("mrs3 band2".to_string(), Measurement::Unitless),
                    ]
                    .try_into()
                    .unwrap(),
                },
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

        let query_ctx = MockQueryContext::test_default();

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
            PartialQueryRect {
                spatial_bounds: SpatialPartition2D::new_unchecked([0., 1.].into(), [3., 0.].into()),
                time_interval: TimeInterval::new_unchecked(0, 10),
                spatial_resolution: SpatialResolution::one(),
            },
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

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }
}
