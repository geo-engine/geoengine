use std::{
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll},
};

use crate::util::Result;
use futures::{
    future::BoxFuture,
    ready,
    stream::{FusedStream, Stream},
    Future, FutureExt,
};
use geoengine_datatypes::{
    primitives::TimeInterval,
    raster::{Pixel, RasterTile2D},
};
use pin_project::pin_project;
use std::ops::DerefMut;

#[pin_project(project = RasterTimeSubStreamAdapterProjection)]
pub struct RasterTimeSubStreamAdapter<St, P, F, Fut>
where
    St: Stream<Item = Result<RasterTile2D<P>>> + Send,
    P: Pixel,
    F: Fn(&mut RasterTimeSubStream<St, P>) -> Fut + Send,
    Fut: Future,
{
    stream: Arc<Mutex<RasterTimeSubStream<St, P>>>,

    f: F,

    #[pin]
    pending_fut: Option<BoxFuture<'static, Fut::Output>>,
}

impl<St, P, F, Fut> RasterTimeSubStreamAdapter<St, P, F, Fut>
where
    St: Stream<Item = Result<RasterTile2D<P>>> + Send,
    P: Pixel,
    F: Fn(&mut RasterTimeSubStream<St, P>) -> Fut + Send,
    Fut: Future,
{
    fn locked_stream(&self) -> MutexGuard<RasterTimeSubStream<St, P>> {
        // TODO: handle poisoning
        self.stream.lock().unwrap()
    }

    fn wrapped_future(&self) -> BoxFuture<'static, Fut::Output> {
        // self.fut().boxed()
        let stream = self.stream.clone();
        tokio::task::spawn_blocking(move || {
            let stream_lock = stream.lock().unwrap();
            (self.f)(stream_lock.deref_mut()).await
        })
        .boxed()
    }

    // async fn fut(&mut self) -> Fut::Output {
    //     todo!()
    // }
}

impl<St, P, F, Fut> Stream for RasterTimeSubStreamAdapter<St, P, F, Fut>
where
    St: Stream<Item = Result<RasterTile2D<P>>> + Send,
    P: Pixel,
    F: Fn(&mut RasterTimeSubStream<St, P>) -> Fut + Send,
    Fut: Future,
{
    type Item = Fut::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.pending_fut.is_none() && self.locked_stream().is_terminated() {
            return Poll::Ready(None);
        }

        if self.pending_fut.is_none() {
            // let future = (self.f)(RasterTimeSubStream { stream: &mut self });
            let future = self.as_mut().wrapped_future();
            self.pending_fut = Some(future);
        };

        let mut this = self.project();

        let future_result: Fut::Output = match this.pending_fut.as_mut().as_pin_mut() {
            Some(fut) => ready!(fut.poll(cx)),
            None => return Poll::Ready(None),
        };

        this.pending_fut.set(None);

        Poll::Ready(Some(future_result))
    }
}

#[pin_project(project = RasterTimeSubStreamProjection)]
pub struct RasterTimeSubStream<St, P>
where
    St: Stream<Item = Result<RasterTile2D<P>>> + Send,
    P: Pixel,
{
    #[pin]
    stream: St,

    pending_item: Option<St::Item>,

    time: TimeInterval,

    is_finished: bool,
}

impl<St, P> RasterTimeSubStream<St, P>
where
    St: Stream<Item = Result<RasterTile2D<P>>> + Send,
    P: Pixel,
{
    pub fn new(stream: St) -> Self {
        Self {
            stream,
            pending_item: None,
            time: TimeInterval::default(),
            is_finished: false,
        }
    }

    pub fn reset_time(&mut self, time: TimeInterval) {
        self.time = time;
    }
}

impl<St, P> Stream for RasterTimeSubStream<St, P>
where
    St: Stream<Item = Result<RasterTile2D<P>>> + Send,
    P: Pixel,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_finished {
            return Poll::Ready(None);
        }

        let this = self.project();

        let raster_tile: Option<Self::Item> = match this.pending_item.take() {
            Some(tile) => Some(tile),
            None => ready!(this.stream.poll_next(cx)),
        };

        let raster_tile: Self::Item = if let Some(raster_tile) = raster_tile {
            raster_tile
        } else {
            // unable to get further tiles
            *this.is_finished = true;
            return Poll::Ready(None);
        };

        let time_does_not_match = if let Ok(ref raster_tile) = raster_tile {
            raster_tile.time != *this.time
        } else {
            false
        };

        if time_does_not_match {
            // store item, because we must not loose it for the next time step
            *this.pending_item = Some(raster_tile);

            return Poll::Ready(None);
        }

        Poll::Ready(Some(raster_tile))
    }
}

impl<St, P> FusedStream for RasterTimeSubStream<St, P>
where
    St: Stream<Item = Result<RasterTile2D<P>>> + Send,
    P: Pixel,
{
    fn is_terminated(&self) -> bool {
        self.is_finished
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::{self, StreamExt};
    use geoengine_datatypes::primitives::TimeInterval;
    use geoengine_datatypes::raster::{Grid2D, TileInformation};

    #[tokio::test]
    async fn substream() {
        let tile_information = TileInformation {
            global_geo_transform: Default::default(),
            global_tile_position: [0, 0].into(),
            tile_size_in_pixels: [3, 2].into(),
        };

        let raster_tiles: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D::new_with_tile_info(
                TimeInterval::new(0, 1).unwrap(),
                tile_information,
                Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None).unwrap(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new(1, 2).unwrap(),
                tile_information,
                Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None).unwrap(),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new(2, 3).unwrap(),
                tile_information,
                Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], None).unwrap(),
            ),
        ];

        let mut substream = RasterTimeSubStream::new(stream::iter(
            raster_tiles
                .iter()
                .cloned()
                .map(Result::Ok)
                .collect::<Vec<Result<RasterTile2D<u8>>>>(),
        ));

        assert!(substream.next().await.is_none());
        assert!(!substream.is_terminated());

        substream.reset_time(TimeInterval::new(0, 1).unwrap());

        assert_eq!(substream.next().await.unwrap().unwrap(), raster_tiles[0]);
        assert!(substream.next().await.is_none());

        substream.reset_time(TimeInterval::new(1, 2).unwrap());

        assert_eq!(substream.next().await.unwrap().unwrap(), raster_tiles[1]);
        assert!(substream.next().await.is_none());

        assert!(!substream.is_terminated());

        substream.reset_time(TimeInterval::new(2, 3).unwrap());

        assert_eq!(substream.next().await.unwrap().unwrap(), raster_tiles[2]);

        assert!(substream.next().await.is_none());
        assert!(substream.is_terminated());
    }
}
