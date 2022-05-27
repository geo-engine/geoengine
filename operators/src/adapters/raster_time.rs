use crate::engine::{QueryContext, RasterQueryProcessor};
use crate::util::Result;
use futures::future::{self, BoxFuture, Join};
use futures::stream::{BoxStream, FusedStream, Zip};
use futures::{ready, StreamExt};
use futures::{Future, Stream};
use geoengine_datatypes::primitives::{RasterQueryRectangle, SpatialPartition2D, TimeInterval};
use geoengine_datatypes::raster::{GridSize, Pixel, RasterTile2D, TileInformation, TilingStrategy};
use pin_project::pin_project;
use std::cmp::min;
use std::pin::Pin;
use std::task::{Context, Poll};

#[pin_project(project = StateProjection)]
enum State<T1, T2, F1, F2>
where
    T1: Pixel,
    T2: Pixel,
    F1: Queryable<T1>,
    F2: Queryable<T2>,
    F1::Stream: Stream<Item = Result<RasterTile2D<T1>>>,
    F2::Stream: Stream<Item = Result<RasterTile2D<T2>>>,
    F1::Output: Future<Output = Result<F1::Stream>>,
    F2::Output: Future<Output = Result<F2::Stream>>,
{
    Initial,
    AwaitingQuery {
        #[pin]
        query_a_b_fut: Join<F1::Output, F2::Output>,
    },
    ConsumingStream {
        #[pin]
        stream: Zip<F1::Stream, F2::Stream>,
        current_spatial_tile: usize,
    },
    Finished,
}

/// Merges two raster sources by aligning the temporal validity.
///
/// # Assumptions
/// * Assumes that the raster tiles already align spatially.
/// * Assumes that raster tiles are contiguous temporally, with no-data-tiles filling gaps.
///
/// # Notice
/// Potentially queries the same tiles multiple times from its sources.
#[pin_project(project = RasterTimeAdapterProjection)]
pub struct RasterTimeAdapter<T1, T2, F1, F2>
where
    T1: Pixel,
    T2: Pixel,
    F1: Queryable<T1>,
    F2: Queryable<T2>,
    F1::Stream: Stream<Item = Result<RasterTile2D<T1>>>,
    F2::Stream: Stream<Item = Result<RasterTile2D<T2>>>,
    F1::Output: Future<Output = Result<F1::Stream>>,
    F2::Output: Future<Output = Result<F2::Stream>>,
{
    // the first source (wrapped QueryProcessor)
    source_a: F1,
    // the second source (wrapped QueryProcessor)
    source_b: F2,
    #[pin]
    state: State<T1, T2, F1, F2>,
    // the current query rectangle, which is advanced over time by increasing the start time
    query_rect: RasterQueryRectangle,
    num_spatial_tiles: Option<usize>,
}

impl<T1, T2, F1, F2> RasterTimeAdapter<T1, T2, F1, F2>
where
    T1: Pixel,
    T2: Pixel,
    F1: Queryable<T1>,
    F2: Queryable<T2>,
    F1::Stream: Stream<Item = Result<RasterTile2D<T1>>>,
    F2::Stream: Stream<Item = Result<RasterTile2D<T2>>>,
    F1::Output: Future<Output = Result<F1::Stream>>,
    F2::Output: Future<Output = Result<F2::Stream>>,
{
    pub fn new(source_a: F1, source_b: F2, query_rect: RasterQueryRectangle) -> Self {
        Self {
            source_a,
            source_b,
            query_rect,
            state: State::Initial,
            num_spatial_tiles: None,
        }
    }

    fn align_tiles(
        mut tile_a: RasterTile2D<T1>,
        mut tile_b: RasterTile2D<T2>,
    ) -> (RasterTile2D<T1>, RasterTile2D<T2>) {
        // TODO: scale data if measurement unit requires it?
        let time = tile_a.time.intersect(&tile_b.time).unwrap_or_else(|| {
            panic!(
                "intervals must overlap: ({}/{}) <-> ({},{})",
                tile_a.time.start().as_rfc3339(),
                tile_a.time.end().as_rfc3339(),
                tile_b.time.start().as_rfc3339(),
                tile_b.time.end().as_rfc3339()
            )
        });
        tile_a.time = time;
        tile_b.time = time;
        (tile_a, tile_b)
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

impl<T1, T2, F1, F2> Stream for RasterTimeAdapter<T1, T2, F1, F2>
where
    T1: Pixel,
    T2: Pixel,
    F1: Queryable<T1>,
    F2: Queryable<T2>,
    F1::Stream: Stream<Item = Result<RasterTile2D<T1>>>,
    F2::Stream: Stream<Item = Result<RasterTile2D<T2>>>,
    F1::Output: Future<Output = Result<F1::Stream>>,
    F2::Output: Future<Output = Result<F2::Stream>>,
{
    type Item = Result<(RasterTile2D<T1>, RasterTile2D<T2>)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // The adapter aligns the two input time series by querying both sources simultaneously.
        // All spatial tiles for the current time step are aligned, s.t. they are both valid
        // for the same time (minimum of both). Then both queries are reset to start at the end
        // of the previous time step.

        let RasterTimeAdapterProjection {
            source_a,
            source_b,
            mut state,
            query_rect,
            num_spatial_tiles,
        } = self.project();

        loop {
            match state.as_mut().project() {
                StateProjection::Initial => {
                    let fut1 = source_a.query(*query_rect);
                    let fut2 = source_b.query(*query_rect);
                    let join = future::join(fut1, fut2);

                    state.set(State::AwaitingQuery {
                        query_a_b_fut: join,
                    });
                }
                StateProjection::AwaitingQuery { query_a_b_fut } => {
                    let queries = ready!(query_a_b_fut.poll(cx));

                    match queries {
                        (Ok(stream_a), Ok(stream_b)) => {
                            // both sources produced an output, set the stream to be consumed
                            state.set(State::ConsumingStream {
                                stream: stream_a.zip(stream_b),
                                current_spatial_tile: 0,
                            });
                        }
                        (Err(e), _) | (_, Err(e)) => {
                            // at least one source failed, output error and end the stream
                            state.set(State::Finished);
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                StateProjection::ConsumingStream {
                    stream,
                    current_spatial_tile,
                } => {
                    match ready!(stream.poll_next(cx)) {
                        Some((Ok(tile_a), Ok(tile_b))) => {
                            // TODO: calculate at start when tiling info is available before querying first tile
                            let num_spatial_tiles = *num_spatial_tiles.get_or_insert_with(|| {
                                Self::number_of_tiles_in_partition(
                                    &tile_a.tile_information(),
                                    query_rect.spatial_bounds,
                                )
                            });

                            if *current_spatial_tile + 1 >= num_spatial_tiles {
                                // time slice ended => query next time slice of sources

                                if tile_a.time.end() == tile_b.time.end() {
                                    // the tiles are currently aligned, continue with next tile of the stream
                                    *current_spatial_tile = 0;
                                } else {
                                    // the tiles are not aligned. We need to reset the stream with the next time step
                                    // and replay the spatial tiles of the source that is still valid with the next
                                    // time step of the other one

                                    // advance current query rectangle
                                    let new_start = min(tile_a.time.end(), tile_b.time.end());

                                    if new_start >= query_rect.time_interval.end() {
                                        // the query window is exhausted, end the stream
                                        state.set(State::Finished);
                                        continue;
                                    }

                                    query_rect.time_interval = TimeInterval::new_unchecked(
                                        new_start,
                                        query_rect.time_interval.end(),
                                    );

                                    state.set(State::Initial);
                                }
                            } else {
                                *current_spatial_tile += 1;
                            }
                            return Poll::Ready(Some(Ok(Self::align_tiles(tile_a, tile_b))));
                        }
                        Some((Ok(_), Err(e)) | (Err(e), Ok(_) | Err(_))) => {
                            state.set(State::Finished);
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            state.set(State::Finished);
                            return Poll::Ready(None);
                        }
                    }
                }
                StateProjection::Finished => return Poll::Ready(None),
            }
        }
    }
}

impl<T1, T2, F1, F2> FusedStream for RasterTimeAdapter<T1, T2, F1, F2>
where
    T1: Pixel,
    T2: Pixel,
    F1: Queryable<T1>,
    F2: Queryable<T2>,
    F1::Stream: Stream<Item = Result<RasterTile2D<T1>>>,
    F2::Stream: Stream<Item = Result<RasterTile2D<T2>>>,
    F1::Output: Future<Output = Result<F1::Stream>>,
    F2::Output: Future<Output = Result<F2::Stream>>,
{
    fn is_terminated(&self) -> bool {
        matches!(self.state, State::Finished)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        MockExecutionContext, MockQueryContext, RasterOperator, RasterResultDescriptor,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use futures::StreamExt;
    use geoengine_datatypes::raster::{Grid, RasterDataType, RasterProperties};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_datatypes::{
        primitives::{Measurement, SpatialResolution},
        raster::TilingSpecification,
    };
    use num_traits::AsPrimitive;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn adapter() {
        let no_data_value = Some(0);
        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams::<u8> {
                data: vec![
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 5),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value)
                            .unwrap()
                            .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 5),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![7, 8, 9, 10, 11, 12],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(5, 10),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![13, 14, 15, 16, 17, 18],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(5, 10),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![19, 20, 21, 22, 23, 24],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 3),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![101, 102, 103, 104, 105, 106],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 3),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![107, 108, 109, 110, 111, 112],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(3, 6),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![113, 114, 115, 116, 117, 118],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(3, 6),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![119, 120, 121, 122, 123, 124],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(6, 10),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![125, 126, 127, 128, 129, 130],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(6, 10),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![131, 132, 133, 134, 135, 136],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp1 = mrs1
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qp2 = mrs2
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let source_a = QueryWrapper {
            p: &qp1,
            ctx: &query_ctx,
        };

        let source_b = QueryWrapper {
            p: &qp2,
            ctx: &query_ctx,
        };

        let adapter = RasterTimeAdapter::new(source_a, source_b, query_rect);

        let result = adapter
            .map(Result::unwrap)
            .collect::<Vec<(RasterTile2D<u8>, RasterTile2D<u8>)>>()
            .await;

        let times: Vec<_> = result.iter().map(|(a, b)| (a.time, b.time)).collect();
        assert_eq!(
            &times,
            &[
                (
                    TimeInterval::new_unchecked(0, 3),
                    TimeInterval::new_unchecked(0, 3)
                ),
                (
                    TimeInterval::new_unchecked(0, 3),
                    TimeInterval::new_unchecked(0, 3)
                ),
                (
                    TimeInterval::new_unchecked(3, 5),
                    TimeInterval::new_unchecked(3, 5)
                ),
                (
                    TimeInterval::new_unchecked(3, 5),
                    TimeInterval::new_unchecked(3, 5)
                ),
                (
                    TimeInterval::new_unchecked(5, 6),
                    TimeInterval::new_unchecked(5, 6)
                ),
                (
                    TimeInterval::new_unchecked(5, 6),
                    TimeInterval::new_unchecked(5, 6)
                ),
                (
                    TimeInterval::new_unchecked(6, 10),
                    TimeInterval::new_unchecked(6, 10)
                ),
                (
                    TimeInterval::new_unchecked(6, 10),
                    TimeInterval::new_unchecked(6, 10)
                )
            ]
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn already_aligned() {
        let no_data_value = Some(0);
        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams::<u8> {
                data: vec![
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 5),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value)
                            .unwrap()
                            .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 5),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![7, 8, 9, 10, 11, 12],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(5, 10),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![13, 14, 15, 16, 17, 18],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(5, 10),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![19, 20, 21, 22, 23, 24],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams::<u8> {
                data: vec![
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 5),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value)
                            .unwrap()
                            .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 5),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![7, 8, 9, 10, 11, 12],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(5, 10),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![13, 14, 15, 16, 17, 18],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(5, 10),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![19, 20, 21, 22, 23, 24],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp1 = mrs1
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qp2 = mrs2
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let source_a = QueryWrapper {
            p: &qp1,
            ctx: &query_ctx,
        };

        let source_b = QueryWrapper {
            p: &qp2,
            ctx: &query_ctx,
        };

        let adapter = RasterTimeAdapter::new(source_a, source_b, query_rect);

        let result = adapter
            .map(Result::unwrap)
            .collect::<Vec<(RasterTile2D<u8>, RasterTile2D<u8>)>>()
            .await;

        let times: Vec<_> = result.iter().map(|(a, b)| (a.time, b.time)).collect();
        assert_eq!(
            &times,
            &[
                (
                    TimeInterval::new_unchecked(0, 5),
                    TimeInterval::new_unchecked(0, 5)
                ),
                (
                    TimeInterval::new_unchecked(0, 5),
                    TimeInterval::new_unchecked(0, 5)
                ),
                (
                    TimeInterval::new_unchecked(5, 10),
                    TimeInterval::new_unchecked(5, 10)
                ),
                (
                    TimeInterval::new_unchecked(5, 10),
                    TimeInterval::new_unchecked(5, 10)
                ),
            ]
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn query_contained_in_tile() {
        let no_data_value = Some(0);
        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams::<u8> {
                data: vec![
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 10),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value)
                            .unwrap()
                            .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 10),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![7, 8, 9, 10, 11, 12],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams::<u8> {
                data: vec![
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(2, 4),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value)
                            .unwrap()
                            .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(2, 4),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![7, 8, 9, 10, 11, 12],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(4, 10),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![no_data_value.unwrap(); 6],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(4, 10),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![no_data_value.unwrap(); 6],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(2, 4),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp1 = mrs1
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qp2 = mrs2
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let source_a = QueryWrapper {
            p: &qp1,
            ctx: &query_ctx,
        };

        let source_b = QueryWrapper {
            p: &qp2,
            ctx: &query_ctx,
        };

        let adapter = RasterTimeAdapter::new(source_a, source_b, query_rect);

        let result = adapter
            .map(Result::unwrap)
            .collect::<Vec<(RasterTile2D<u8>, RasterTile2D<u8>)>>()
            .await;

        let times: Vec<_> = result.iter().map(|(a, b)| (a.time, b.time)).collect();
        assert_eq!(
            &times,
            &[(
                TimeInterval::new_unchecked(2, 4),
                TimeInterval::new_unchecked(2, 4)
            )]
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn both_tiles_longer_valid_than_query() {
        let no_data_value = Some(0);
        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams::<u8> {
                data: vec![
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 10),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value)
                            .unwrap()
                            .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 10),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![7, 8, 9, 10, 11, 12],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(10, 20),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![no_data_value.unwrap(); 6],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(10, 20),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![no_data_value.unwrap(); 6],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams::<u8> {
                data: vec![
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(2, 9),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value)
                            .unwrap()
                            .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(2, 9),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![7, 8, 9, 10, 11, 12],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(9, 20),
                        tile_position: [-1, 0].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![no_data_value.unwrap(); 6],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(9, 20),
                        tile_position: [-1, 1].into(),
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![no_data_value.unwrap(); 6],
                            no_data_value,
                        )
                        .unwrap()
                        .into(),
                        properties: RasterProperties::default(),
                    },
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(2, 8),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

        let qp1 = mrs1
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qp2 = mrs2
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let source_a = QueryWrapper {
            p: &qp1,
            ctx: &query_ctx,
        };

        let source_b = QueryWrapper {
            p: &qp2,
            ctx: &query_ctx,
        };

        let adapter = RasterTimeAdapter::new(source_a, source_b, query_rect);

        let result = adapter
            .map(Result::unwrap)
            .collect::<Vec<(RasterTile2D<u8>, RasterTile2D<u8>)>>()
            .await;

        let times: Vec<_> = result.iter().map(|(a, b)| (a.time, b.time)).collect();
        assert_eq!(
            &times,
            &[(
                TimeInterval::new_unchecked(2, 9),
                TimeInterval::new_unchecked(2, 9)
            )]
        );
    }
}
