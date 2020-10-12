use crate::util::Result;
use failure::_core::cmp::Ordering;
use futures::stream::FusedStream;
use futures::Stream;
use geoengine_datatypes::primitives::TimeInterval;
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use pin_project::pin_project;
use std::cmp::min;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Merges two raster streams by aligning the temporal validity.
/// Assumes that the raster tiles already align spatially.
#[pin_project(project = RasterTimeAdapterProjection)]
pub struct RasterTimeAdapter<S1, S2, T1, T2>
where
    S1: Stream<Item = Result<RasterTile2D<T1>>> + FusedStream,
    S2: Stream<Item = Result<RasterTile2D<T2>>> + FusedStream,
    T1: Pixel,
    T2: Pixel,
{
    #[pin]
    stream1: S1,
    #[pin]
    stream2: S2,

    queued1: Option<Result<RasterTile2D<T1>>>,
    queued2: Option<Result<RasterTile2D<T2>>>,
}

impl<S1, S2, T1, T2> Stream for RasterTimeAdapter<S1, S2, T1, T2>
where
    S1: Stream<Item = Result<RasterTile2D<T1>>> + FusedStream,
    S2: Stream<Item = Result<RasterTile2D<T2>>> + FusedStream,
    T1: Pixel,
    T2: Pixel,
{
    type Item = Result<(RasterTile2D<T1>, RasterTile2D<T2>)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let RasterTimeAdapterProjection {
            mut stream1,
            mut stream2,
            queued1,
            queued2,
        } = self.as_mut().project();

        if queued1.is_none() {
            match stream1.as_mut().poll_next(cx) {
                Poll::Ready(Some(item1)) => *queued1 = Some(item1),
                Poll::Ready(None) | Poll::Pending => {}
            }
        }
        if queued2.is_none() {
            match stream2.as_mut().poll_next(cx) {
                Poll::Ready(Some(item2)) => *queued2 = Some(item2),
                Poll::Ready(None) | Poll::Pending => {}
            }
        }

        if queued1.is_some() && queued2.is_some() {
            let r1 = queued1.as_mut().unwrap().as_mut().unwrap(); // TODO: handle error
            let r2 = queued2.as_mut().unwrap().as_mut().unwrap();

            let time_slice = time_slice(r1.time, r2.time);

            let out1 = slice_raster(r1, time_slice);
            let out2 = slice_raster(r2, time_slice);

            if r1.time.end == time_slice.end {
                queued1.take(); // TODO: return this instead of copy?
            } else {
                r1.time.start = time_slice.end;
            }

            if r2.time.end == time_slice.end {
                queued2.take(); // TODO: return this instead of copy?
            } else {
                r2.time.start = time_slice.end;
            }

            Poll::Ready(Some(Ok((out1, out2))))
        } else if stream1.is_terminated() || stream2.is_terminated() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<S1, S2, T1, T2> FusedStream for RasterTimeAdapter<S1, S2, T1, T2>
where
    S1: Stream<Item = Result<RasterTile2D<T1>>> + FusedStream,
    S2: Stream<Item = Result<RasterTile2D<T2>>> + FusedStream,
    T1: Pixel,
    T2: Pixel,
{
    fn is_terminated(&self) -> bool {
        self.stream1.is_terminated() && self.stream2.is_terminated()
    }
}

fn slice_raster<T: Pixel>(raster: &RasterTile2D<T>, t: TimeInterval) -> RasterTile2D<T> {
    if t.intersects(&raster.time) {
        // TODO: avoid copying data
        let mut out = raster.clone();
        out.time = t;

        out
    } else {
        // TODO: is this case even necessary if raster stream contiguous and filled with no data where there is no raster?

        // TODO: create no data raster directly without copying raster
        let mut out = raster.clone();
        // TODO: handle no nodata
        out.data.data_container =
            vec![out.data.no_data_value.unwrap(); out.data.data_container.len()];
        out.time = t;

        out
    }
}

/// produce a new time slice
fn time_slice(t1: TimeInterval, t2: TimeInterval) -> TimeInterval {
    match t1.start.cmp(&t2.start) {
        Ordering::Less => TimeInterval::new_unchecked(t1.start, t2.start),
        Ordering::Equal => TimeInterval::new_unchecked(t1.start, min(t1.end, t2.end)),
        Ordering::Greater => TimeInterval::new_unchecked(t2.start, t1.start),
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

    #[test]
    fn time_slice_test() {
        assert_eq!(
            time_slice(
                TimeInterval::new_unchecked(5, 10),
                TimeInterval::new_unchecked(7, 10)
            ),
            TimeInterval::new_unchecked(5, 7)
        );

        assert_eq!(
            time_slice(
                TimeInterval::new_unchecked(5, 7),
                TimeInterval::new_unchecked(5, 10)
            ),
            TimeInterval::new_unchecked(5, 7)
        );
    }

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
                            vec![1, 2, 3, 4, 5, 6],
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
                        time: TimeInterval::new_unchecked(2, 3),
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
                            TimeInterval::new_unchecked(2, 3), // TODO: fill in no data raster for time (0, 2)?
                            Default::default(),
                        )
                        .unwrap(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(3, 10),
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
                            TimeInterval::new_unchecked(3, 6),
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
        let q_ctx = QueryContext {
            chunk_byte_size: 1024 * 1024,
        };

        let qp1 = mrs1
            .initialize(&exe_ctx)
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();
        let s1 = qp1.query(query_rect, q_ctx).fuse();

        let qp2 = mrs2
            .initialize(&exe_ctx)
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();
        let s2 = qp2.query(query_rect, q_ctx).fuse();

        let adapter = RasterTimeAdapter {
            stream1: s1,
            stream2: s2,
            queued1: None,
            queued2: None,
        }
        .boxed();

        let result = adapter
            .map(Result::unwrap)
            .collect::<Vec<(RasterTile2D<u8>, RasterTile2D<u8>)>>()
            .await;

        assert_eq!(result.len(), 4);

        assert_eq!(result[0].0.time, TimeInterval::new_unchecked(0, 2));
        assert_eq!(result[0].0.time, result[0].1.time);
        assert_eq!(result[0].1.data.data_container, vec![0; 6]);

        assert_eq!(result[1].0.time, TimeInterval::new_unchecked(2, 3));
        assert_eq!(result[1].0.time, result[1].1.time);

        assert_eq!(result[2].0.time, TimeInterval::new_unchecked(3, 5));
        assert_eq!(result[2].0.time, result[2].1.time);

        assert_eq!(result[3].0.time, TimeInterval::new_unchecked(5, 10));
        assert_eq!(result[3].0.time, result[3].1.time);
    }
}
