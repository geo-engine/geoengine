use crate::engine::QueryRectangle;
use crate::util::Result;
use futures::stream::{FusedStream, Zip};
use futures::Stream;
use futures::{ready, StreamExt};
use geoengine_datatypes::primitives::{BoundingBox2D, TimeInstance, TimeInterval};
use geoengine_datatypes::raster::{GridSize, Pixel, RasterTile2D, TileInformation, TilingStrategy};
use pin_project::pin_project;
use std::cmp::min;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Merges two raster sources by aligning the temporal validity.
/// Assumes that the raster tiles already align spatially.
/// Assumes that raster tiles are contiguous temporally, with no-data-tiles filling gaps.
/// Potentially queries the same tiles multiple times from its sources.
#[pin_project(project = RasterTimeAdapterProjection)]
pub struct RasterTimeAdapter<T1, T2, St1, St2, F1, F2>
where
    T1: Pixel,
    T2: Pixel,
    St1: Stream<Item = Result<RasterTile2D<T1>>>,
    St2: Stream<Item = Result<RasterTile2D<T2>>>,
    F1: Fn(QueryRectangle) -> St1,
    F2: Fn(QueryRectangle) -> St2,
{
    source_a: F1,
    source_b: F2,
    query_rect: QueryRectangle,
    time_end: Option<(TimeInstance, TimeInstance)>,
    // TODO: calculate at start when tiling info is available before querying first tile
    num_spatial_tiles: Option<usize>,
    current_spatial_tile: usize,
    #[pin]
    stream: Zip<St1, St2>,
    ended: bool,
}

impl<T1, T2, St1, St2, F1, F2> RasterTimeAdapter<T1, T2, St1, St2, F1, F2>
where
    T1: Pixel,
    T2: Pixel,
    St1: Stream<Item = Result<RasterTile2D<T1>>>,
    St2: Stream<Item = Result<RasterTile2D<T2>>>,
    F1: Fn(QueryRectangle) -> St1,
    F2: Fn(QueryRectangle) -> St2,
{
    pub fn new(source_a: F1, source_b: F2, query_rect: QueryRectangle) -> Self {
        Self {
            stream: source_a(query_rect).zip(source_b(query_rect)),
            source_a,
            source_b,
            query_rect,
            num_spatial_tiles: None,
            current_spatial_tile: 0,
            time_end: None,
            ended: false,
        }
    }

    fn align_tiles(
        mut tile_a: RasterTile2D<T1>,
        mut tile_b: RasterTile2D<T2>,
    ) -> (RasterTile2D<T1>, RasterTile2D<T2>) {
        // TODO: scale data if measurement unit requires it?
        let time = tile_a
            .time
            .intersect(&tile_b.time)
            .expect("intervals must overlap");
        tile_a.time = time;
        tile_b.time = time;
        (tile_a, tile_b)
    }

    fn number_of_tiles_in_bbox(tile_info: &TileInformation, bbox: BoundingBox2D) -> usize {
        // TODO: get tiling strategy from stream or execution context instead of creating it here
        let strat = TilingStrategy {
            tile_pixel_size: tile_info.tile_size_in_pixels,
            geo_transform: tile_info.global_geo_transform,
        };

        strat.tile_grid_box(bbox).number_of_elements()
    }
}

impl<T1, T2, St1, St2, F1, F2> Stream for RasterTimeAdapter<T1, T2, St1, St2, F1, F2>
where
    T1: Pixel,
    T2: Pixel,
    St1: Stream<Item = Result<RasterTile2D<T1>>>,
    St2: Stream<Item = Result<RasterTile2D<T2>>>,
    F1: Fn(QueryRectangle) -> St1,
    F2: Fn(QueryRectangle) -> St2,
{
    type Item = Result<(RasterTile2D<T1>, RasterTile2D<T2>)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_terminated() {
            return Poll::Ready(None);
        }

        let RasterTimeAdapterProjection {
            source_a,
            source_b,
            query_rect,
            time_end,
            num_spatial_tiles,
            current_spatial_tile,
            mut stream,
            ended,
        } = self.project();

        let next = ready!(stream.as_mut().poll_next(cx));

        match next {
            Some((Ok(tile_a), Ok(tile_b))) => {
                // TODO: calculate at start when tiling info is available before querying first tile
                if num_spatial_tiles.is_none() {
                    *num_spatial_tiles = Some(Self::number_of_tiles_in_bbox(
                        &tile_a.tile_information(),
                        query_rect.bbox,
                    ));
                }

                if *current_spatial_tile >= num_spatial_tiles.expect("checked") {
                    // time slice ended => query next time slice of sources
                    let mut next_qrect = *query_rect;
                    next_qrect.time_interval = TimeInterval::new_unchecked(
                        min(tile_a.time.end(), tile_b.time.end()),
                        query_rect.time_interval.end(),
                    );
                    *time_end = None;

                    stream.set(source_a(next_qrect).zip(source_b(next_qrect)));
                    *current_spatial_tile = 0;
                } else {
                    *current_spatial_tile += 1;
                }

                Poll::Ready(Some(Ok(Self::align_tiles(tile_a, tile_b))))
            }
            Some((Ok(_), Err(e))) | Some((Err(e), Ok(_))) | Some((Err(e), Err(_))) => {
                *ended = true;
                Poll::Ready(Some(Err(e)))
            }
            None => {
                *ended = true;
                Poll::Ready(None)
            }
        }
    }
}

impl<T1, T2, St1, St2, F1, F2> FusedStream for RasterTimeAdapter<T1, T2, St1, St2, F1, F2>
where
    T1: Pixel,
    T2: Pixel,
    St1: Stream<Item = Result<RasterTile2D<T1>>>,
    St2: Stream<Item = Result<RasterTile2D<T2>>>,
    F1: Fn(QueryRectangle) -> St1,
    F2: Fn(QueryRectangle) -> St2,
{
    fn is_terminated(&self) -> bool {
        self.ended
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrency::ThreadPool;
    use crate::engine::{
        ExecutionContext, QueryContext, QueryProcessor, QueryRectangle, RasterOperator,
        RasterResultDescriptor,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use futures::StreamExt;
    use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::{
        primitives::Coordinate2D,
        raster::{Grid, GridShape2D, RasterDataType, TilingSpecification},
    };

    #[tokio::test]
    async fn adapter() {
        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 5),
                        tile_position: [0, 0].into(),
                        global_geo_transform: Default::default(),
                        grid_array: Grid::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], Some(0))
                            .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 5),
                        tile_position: [0, 1].into(),
                        global_geo_transform: Default::default(),
                        grid_array: Grid::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12], Some(0))
                            .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(5, 10),
                        tile_position: [0, 0].into(),
                        global_geo_transform: Default::default(),
                        grid_array: Grid::new([3, 2].into(), vec![13, 14, 15, 16, 17, 18], Some(0))
                            .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(5, 10),
                        tile_position: [0, 1].into(),
                        global_geo_transform: Default::default(),
                        grid_array: Grid::new([3, 2].into(), vec![19, 20, 21, 22, 23, 24], Some(0))
                            .unwrap(),
                    },
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::wgs84().into(),
                },
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 3),
                        tile_position: [0, 0].into(),
                        global_geo_transform: Default::default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![101, 102, 103, 104, 105, 106],
                            Some(0),
                        )
                        .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 3),
                        tile_position: [0, 1].into(),
                        global_geo_transform: Default::default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![107, 108, 109, 110, 111, 112],
                            Some(0),
                        )
                        .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(3, 6),
                        tile_position: [0, 0].into(),
                        global_geo_transform: Default::default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![113, 114, 115, 116, 117, 118],
                            Some(0),
                        )
                        .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(3, 6),
                        tile_position: [0, 1].into(),
                        global_geo_transform: Default::default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![119, 120, 121, 122, 123, 124],
                            Some(0),
                        )
                        .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(6, 10),
                        tile_position: [0, 0].into(),
                        global_geo_transform: Default::default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![125, 126, 127, 128, 129, 130],
                            Some(0),
                        )
                        .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(6, 10),
                        tile_position: [0, 1].into(),
                        global_geo_transform: Default::default(),
                        grid_array: Grid::new(
                            [3, 2].into(),
                            vec![131, 132, 133, 134, 135, 136],
                            Some(0),
                        )
                        .unwrap(),
                    },
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::wgs84().into(),
                },
            },
        }
        .boxed();

        let thread_pool = ThreadPool::new(2);
        let exe_ctx = ExecutionContext {
            raster_data_root: Default::default(),
            thread_pool: thread_pool.create_context(),
            tiling_specification: TilingSpecification {
                origin_coordinate: Coordinate2D::default(),
                tile_size: GridShape2D::from([600, 600]),
            },
        };
        let query_rect = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (1., 1.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = QueryContext {
            chunk_byte_size: 1024 * 1024,
        };

        let qp1 = mrs1
            .initialize(&exe_ctx)
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qp2 = mrs2
            .initialize(&exe_ctx)
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let source_a = |query_rect| qp1.query(query_rect, query_ctx);

        let source_b = |query_rect| qp2.query(query_rect, query_ctx);

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
}
