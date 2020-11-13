use crate::engine::QueryRectangle;
use crate::util::Result;
use futures::stream::Zip;
use futures::Stream;
use futures::{ready, StreamExt};
use geoengine_datatypes::primitives::{TimeInstance, TimeInterval};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
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
    #[pin]
    stream: Zip<St1, St2>,
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
            time_end: None,
        }
    }

    // TODO: move to TimeInterval
    fn intersect_time_intervals(t1: TimeInterval, t2: TimeInterval) -> Option<TimeInterval> {
        if t1.intersects(&t2) {
            let start = std::cmp::max(t1.start, t2.start);
            let end = std::cmp::min(t1.end, t2.end);
            Some(TimeInterval::new_unchecked(start, end))
        } else {
            None
        }
    }

    fn align_tiles(
        mut tile_a: RasterTile2D<T1>,
        mut tile_b: RasterTile2D<T2>,
    ) -> (RasterTile2D<T1>, RasterTile2D<T2>) {
        // TODO: handle error
        let time = Self::intersect_time_intervals(tile_a.time, tile_b.time).unwrap();
        tile_a.time = time;
        tile_b.time = time;
        (tile_a, tile_b)
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

    #[allow(clippy::needless_return)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let RasterTimeAdapterProjection {
            source_a,
            source_b,
            query_rect,
            time_end,
            mut stream,
        } = self.project();

        // TODO: handle error
        let next = ready!(stream.as_mut().poll_next(cx)).map(|(a, b)| (a.unwrap(), b.unwrap()));

        if let Some((tile_a, tile_b)) = next {
            if let Some(end) = time_end {
                if tile_a.time.start >= end.0 || tile_b.time.start >= end.1 {
                    // a tile belongs to next slice => query next time slice of sources instead
                    // TODO: determine end of time slice in stream without computing an additional tile
                    let mut next_qrect = *query_rect;
                    next_qrect.time_interval.start = min(end.0, end.1);
                    *time_end = None;

                    stream.set(source_a(next_qrect).zip(source_b(next_qrect)));

                    // TODO: handle error
                    let next = ready!(stream.as_mut().poll_next(cx))
                        .map(|(a, b)| (a.unwrap(), b.unwrap()));
                    if let Some((tile_a, tile_b)) = next {
                        return Poll::Ready(Some(Ok(Self::align_tiles(tile_a, tile_b))));
                    } else {
                        // source streams ended
                        return Poll::Ready(None);
                    }
                }
            } else {
                // first tile in time slice
                *time_end = Some((tile_a.time.end, tile_b.time.end));
                return Poll::Ready(Some(Ok(Self::align_tiles(tile_a, tile_b))));
            }

            return Poll::Ready(Some(Ok(Self::align_tiles(tile_a, tile_b))));
        } else if let Some(end) = time_end {
            // at least one of the input streams ended
            if end.0 != query_rect.time_interval.end || end.1 != query_rect.time_interval.end {
                // the other input stream still has more data => query next time slice of sources
                let mut next_qrect = *query_rect;
                next_qrect.time_interval.start = min(end.0, end.1);
                *time_end = None;

                stream.set(source_a(next_qrect).zip(source_b(next_qrect)));

                // TODO: handle error
                let next =
                    ready!(stream.as_mut().poll_next(cx)).map(|(a, b)| (a.unwrap(), b.unwrap()));
                if let Some((tile_a, tile_b)) = next {
                    return Poll::Ready(Some(Ok(Self::align_tiles(tile_a, tile_b))));
                } else {
                    unreachable!("stream must return result");
                }
            } else {
                // both streams ended
                return Poll::Ready(None);
            }
        } else {
            // both input streams were empty
            return Poll::Ready(None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        ExecutionContext, QueryContext, QueryProcessor, QueryRectangle, RasterOperator,
        RasterResultDescriptor,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use futures::StreamExt;
    use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution};
    use geoengine_datatypes::raster::{Raster2D, RasterDataType, TileInformation};
    use geoengine_datatypes::spatial_reference::SpatialReference;

    #[tokio::test]
    async fn adapter() {
        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 5),
                        tile: TileInformation {
                            global_geo_transform: Default::default(),
                            global_pixel_position: [0, 0].into(),
                            global_size_in_tiles: [1, 2].into(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        data: Raster2D::new(
                            [3, 2].into(),
                            vec![1, 2, 3, 4, 5, 6],
                            Some(0),
                            TimeInterval::new_unchecked(0, 5),
                            Default::default(),
                        )
                        .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 5),
                        tile: TileInformation {
                            global_geo_transform: Default::default(),
                            global_pixel_position: [0, 2].into(),
                            global_size_in_tiles: [1, 2].into(),
                            global_tile_position: [0, 1].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        data: Raster2D::new(
                            [3, 2].into(),
                            vec![7, 8, 9, 10, 11, 12],
                            Some(0),
                            TimeInterval::new_unchecked(0, 5),
                            Default::default(),
                        )
                        .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(5, 10),
                        tile: TileInformation {
                            global_geo_transform: Default::default(),
                            global_pixel_position: [0, 0].into(),
                            global_size_in_tiles: [1, 2].into(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        data: Raster2D::new(
                            [3, 2].into(),
                            vec![13, 14, 15, 16, 17, 18],
                            Some(0),
                            TimeInterval::new_unchecked(5, 10),
                            Default::default(),
                        )
                        .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(5, 10),
                        tile: TileInformation {
                            global_geo_transform: Default::default(),
                            global_pixel_position: [0, 2].into(),
                            global_size_in_tiles: [1, 2].into(),
                            global_tile_position: [0, 1].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        data: Raster2D::new(
                            [3, 2].into(),
                            vec![19, 20, 21, 22, 23, 24],
                            Some(0),
                            TimeInterval::new_unchecked(5, 10),
                            Default::default(),
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

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 3),
                        tile: TileInformation {
                            global_geo_transform: Default::default(),
                            global_pixel_position: [0, 0].into(),
                            global_size_in_tiles: [1, 2].into(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        data: Raster2D::new(
                            [3, 2].into(),
                            vec![101, 102, 103, 104, 105, 106],
                            Some(0),
                            TimeInterval::new_unchecked(0, 3),
                            Default::default(),
                        )
                        .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(0, 3),
                        tile: TileInformation {
                            global_geo_transform: Default::default(),
                            global_pixel_position: [0, 2].into(),
                            global_size_in_tiles: [1, 2].into(),
                            global_tile_position: [0, 1].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        data: Raster2D::new(
                            [3, 2].into(),
                            vec![107, 108, 109, 110, 111, 112],
                            Some(0),
                            TimeInterval::new_unchecked(0, 3),
                            Default::default(),
                        )
                        .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(3, 6),
                        tile: TileInformation {
                            global_geo_transform: Default::default(),
                            global_pixel_position: [0, 0].into(),
                            global_size_in_tiles: [1, 2].into(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        data: Raster2D::new(
                            [3, 2].into(),
                            vec![113, 114, 115, 116, 117, 118],
                            Some(0),
                            TimeInterval::new_unchecked(3, 6),
                            Default::default(),
                        )
                        .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(3, 6),
                        tile: TileInformation {
                            global_geo_transform: Default::default(),
                            global_pixel_position: [0, 2].into(),
                            global_size_in_tiles: [1, 2].into(),
                            global_tile_position: [0, 1].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        data: Raster2D::new(
                            [3, 2].into(),
                            vec![119, 120, 121, 122, 123, 124],
                            Some(0),
                            TimeInterval::new_unchecked(3, 6),
                            Default::default(),
                        )
                        .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(6, 10),
                        tile: TileInformation {
                            global_geo_transform: Default::default(),
                            global_pixel_position: [0, 0].into(),
                            global_size_in_tiles: [1, 2].into(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        data: Raster2D::new(
                            [3, 2].into(),
                            vec![125, 126, 127, 128, 129, 130],
                            Some(0),
                            TimeInterval::new_unchecked(6, 10),
                            Default::default(),
                        )
                        .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(6, 10),
                        tile: TileInformation {
                            global_geo_transform: Default::default(),
                            global_pixel_position: [0, 2].into(),
                            global_size_in_tiles: [1, 2].into(),
                            global_tile_position: [0, 1].into(),
                            tile_size_in_pixels: [3, 2].into(),
                        },
                        data: Raster2D::new(
                            [3, 2].into(),
                            vec![131, 132, 133, 134, 135, 136],
                            Some(0),
                            TimeInterval::new_unchecked(6, 10),
                            Default::default(),
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

        let exe_ctx = ExecutionContext {
            raster_data_root: Default::default(),
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
        // let s1 = qp1.query(query_rect, query_ctx).fuse();

        let qp2 = mrs2
            .initialize(&exe_ctx)
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();
        // let s2 = qp2.query(query_rect, query_ctx).fuse();

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
