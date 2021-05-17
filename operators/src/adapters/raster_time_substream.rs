use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::util::Result;
use futures::{
    ready,
    stream::{FusedStream, Stream},
    Future,
};
use geoengine_datatypes::{
    primitives::TimeInterval,
    raster::{Pixel, RasterTile2D},
};
use pin_project::pin_project;

#[must_use = "streams do nothing unless polled"]
#[pin_project(project = RasterTimeSubStreamAdapterProjection)]
pub struct RasterTimeMultiFold<St, Accum, AccumInitFn, FoldFn, Fut> {
    #[pin]
    stream: St,

    accum_init_fn: AccumInitFn,
    fold_fn: FoldFn,

    accum: Option<Accum>,
    time: Option<TimeInterval>,

    #[pin]
    future: Option<Fut>,
}

impl<St, P, Accum, AccumInitFn, FoldFn, Fut>
    RasterTimeMultiFold<St, Accum, AccumInitFn, FoldFn, Fut>
where
    St: Stream<Item = Result<RasterTile2D<P>>>,
    P: Pixel,
    AccumInitFn: FnMut() -> Accum,
    FoldFn: FnMut(Accum, St::Item) -> Fut,
    Fut: Future<Output = Accum>,
{
    pub fn new(stream: St, mut accum_init_fn: AccumInitFn, fold_fn: FoldFn) -> Self {
        let accum = accum_init_fn();

        Self {
            stream,
            accum_init_fn,
            fold_fn,
            accum: Some(accum),
            future: None,
            time: None,
        }
    }
}

impl<St, P, Accum, AccumInitFn, FoldFn, Fut> Stream
    for RasterTimeMultiFold<St, Accum, AccumInitFn, FoldFn, Fut>
where
    St: Stream<Item = Result<RasterTile2D<P>>>,
    P: Pixel,
    AccumInitFn: FnMut() -> Accum,
    FoldFn: FnMut(Accum, St::Item) -> Fut,
    Fut: Future<Output = Accum>,
{
    type Item = Accum;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_terminated() {
            return Poll::Ready(None);
        }

        let mut this = self.project();

        let value: Accum = loop {
            // Retrieve either results from the future or more data from the stream
            // Stops when there is something to output or when the stream is empty.

            if let Some(fut) = this.future.as_mut().as_pin_mut() {
                // we're currently processing a future to produce a new accum value
                *this.accum = Some(ready!(fut.poll(cx)));
                this.future.set(None);

                continue;
            }

            // we're waiting on a new item from the stream
            let stream_item = ready!(this.stream.as_mut().poll_next(cx));

            let raster_tile: Result<RasterTile2D<P>> = match stream_item {
                Some(raster_tile) => raster_tile,
                None => {
                    // output last accumulator if stream has ended
                    return Poll::Ready(this.accum.take());
                }
            };

            let accum = this.accum.take().expect("checked via `is_terminated`");

            let time_does_not_match = match (&raster_tile, &mut this.time) {
                (Ok(raster_tile), Some(time)) => raster_tile.time != *time,
                (Ok(raster_tile), time_option) => {
                    // if there is no time, set it
                    **time_option = Some(raster_tile.time);
                    false
                }
                (Err(_), _) => false,
            };

            if time_does_not_match {
                // set new time and process tile
                *this.time = raster_tile.as_ref().map(|tile| tile.time).ok();
                this.future
                    .set(Some((this.fold_fn)((this.accum_init_fn)(), raster_tile)));

                // return the result from the previous time step
                break accum;
            }

            this.future.set(Some((this.fold_fn)(accum, raster_tile)));
        };

        Poll::Ready(Some(value))
    }
}

impl<St, P, Accum, AccumInitFn, FoldFn, Fut> FusedStream
    for RasterTimeMultiFold<St, Accum, AccumInitFn, FoldFn, Fut>
where
    St: Stream<Item = Result<RasterTile2D<P>>>,
    P: Pixel,
    AccumInitFn: FnMut() -> Accum,
    FoldFn: FnMut(Accum, St::Item) -> Fut,
    Fut: Future<Output = Accum>,
{
    fn is_terminated(&self) -> bool {
        self.future.is_none() && self.accum.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::{self, StreamExt};
    use geoengine_datatypes::primitives::TimeInterval;
    use geoengine_datatypes::raster::{Grid2D, TileInformation};
    use tokio::pin;

    #[tokio::test]
    async fn simple() {
        let tile_information = TileInformation {
            global_geo_transform: Default::default(),
            global_tile_position: [0, 0].into(),
            tile_size_in_pixels: [3, 2].into(),
        };

        let raster_tiles: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D::new_with_tile_info(
                TimeInterval::new(0, 1).unwrap(),
                tile_information,
                Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None)
                    .unwrap()
                    .into(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new(1, 2).unwrap(),
                tile_information,
                Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None)
                    .unwrap()
                    .into(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new(2, 3).unwrap(),
                tile_information,
                Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None)
                    .unwrap()
                    .into(),
            ),
        ];
        let stream = stream::iter(
            raster_tiles
                .iter()
                .cloned()
                .map(Result::Ok)
                .collect::<Vec<Result<RasterTile2D<u8>>>>(),
        );

        let first_tile_stream = RasterTimeMultiFold::new(
            stream,
            || None,
            |accum, tile| async move { accum.or(Some(tile)) },
        );
        pin!(first_tile_stream);

        assert!(!first_tile_stream.is_terminated());

        let next: RasterTile2D<u8> = (first_tile_stream.next().await).flatten().unwrap().unwrap();
        assert_eq!(next, raster_tiles[0]);

        let next: RasterTile2D<u8> = (first_tile_stream.next().await).flatten().unwrap().unwrap();
        assert_eq!(next, raster_tiles[1]);

        let next: RasterTile2D<u8> = (first_tile_stream.next().await).flatten().unwrap().unwrap();
        assert_eq!(next, raster_tiles[2]);

        assert!(first_tile_stream.next().await.is_none());
        assert!(first_tile_stream.is_terminated());
    }

    #[tokio::test]
    async fn first_value() {
        let tile_information = TileInformation {
            global_geo_transform: Default::default(),
            global_tile_position: [0, 0].into(),
            tile_size_in_pixels: [3, 2].into(),
        };

        let raster_tiles: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D::new_with_tile_info(
                TimeInterval::new(0, 1).unwrap(),
                tile_information,
                Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None)
                    .unwrap()
                    .into(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new(1, 2).unwrap(),
                tile_information,
                Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None)
                    .unwrap()
                    .into(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new(1, 2).unwrap(),
                tile_information,
                Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1], None)
                    .unwrap()
                    .into(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new(2, 3).unwrap(),
                tile_information,
                Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None)
                    .unwrap()
                    .into(),
            ),
        ];
        let stream = stream::iter(
            raster_tiles
                .iter()
                .cloned()
                .map(Result::Ok)
                .collect::<Vec<Result<RasterTile2D<u8>>>>(),
        );

        let first_tile_stream = RasterTimeMultiFold::new(
            stream,
            || None,
            |accum, tile| async move { accum.or(Some(tile)) },
        );
        pin!(first_tile_stream);

        assert!(!first_tile_stream.is_terminated());

        let next: RasterTile2D<u8> = (first_tile_stream.next().await).flatten().unwrap().unwrap();
        assert_eq!(next, raster_tiles[0]);

        let next: RasterTile2D<u8> = (first_tile_stream.next().await).flatten().unwrap().unwrap();
        assert_eq!(next, raster_tiles[1]);

        let next: RasterTile2D<u8> = (first_tile_stream.next().await).flatten().unwrap().unwrap();
        assert_eq!(next, raster_tiles[3]);

        assert!(first_tile_stream.next().await.is_none());
        assert!(first_tile_stream.is_terminated());
    }
}
