use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::engine::{
    CanonicOperatorName, InitializedRasterOperator, QueryContext, QueryProcessor,
    RasterResultDescriptor, TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{ready, Stream, StreamExt, TryStreamExt};
use geoengine_datatypes::primitives::{QueryRectangle, SpatialPartition2D};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use pin_project::{pin_project, pinned_drop};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

use super::tile_cache::{Cachable, TileCache};

/// A cache operator that caches the results of its source operator
pub struct InitializedCacheOperator<S> {
    source: S,
}

impl<S> InitializedCacheOperator<S> {
    pub fn new(source: S) -> Self {
        Self { source }
    }
}

impl InitializedRasterOperator for InitializedCacheOperator<Box<dyn InitializedRasterOperator>> {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        self.source.result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let processor_result = self.source.query_processor();
        match processor_result {
            Ok(p) => {
                let res_processor = match p {
                    TypedRasterQueryProcessor::U8(p) => TypedRasterQueryProcessor::U8(Box::new(
                        CacheQueryProcessor::new(p, self.source.canonic_name()),
                    )),
                    TypedRasterQueryProcessor::U16(p) => TypedRasterQueryProcessor::U16(Box::new(
                        CacheQueryProcessor::new(p, self.source.canonic_name()),
                    )),
                    TypedRasterQueryProcessor::U32(p) => TypedRasterQueryProcessor::U32(Box::new(
                        CacheQueryProcessor::new(p, self.source.canonic_name()),
                    )),
                    TypedRasterQueryProcessor::U64(p) => TypedRasterQueryProcessor::U64(Box::new(
                        CacheQueryProcessor::new(p, self.source.canonic_name()),
                    )),
                    TypedRasterQueryProcessor::I8(p) => TypedRasterQueryProcessor::I8(Box::new(
                        CacheQueryProcessor::new(p, self.source.canonic_name()),
                    )),
                    TypedRasterQueryProcessor::I16(p) => TypedRasterQueryProcessor::I16(Box::new(
                        CacheQueryProcessor::new(p, self.source.canonic_name()),
                    )),
                    TypedRasterQueryProcessor::I32(p) => TypedRasterQueryProcessor::I32(Box::new(
                        CacheQueryProcessor::new(p, self.source.canonic_name()),
                    )),
                    TypedRasterQueryProcessor::I64(p) => TypedRasterQueryProcessor::I64(Box::new(
                        CacheQueryProcessor::new(p, self.source.canonic_name()),
                    )),
                    TypedRasterQueryProcessor::F32(p) => TypedRasterQueryProcessor::F32(Box::new(
                        CacheQueryProcessor::new(p, self.source.canonic_name()),
                    )),
                    TypedRasterQueryProcessor::F64(p) => TypedRasterQueryProcessor::F64(Box::new(
                        CacheQueryProcessor::new(p, self.source.canonic_name()),
                    )),
                };
                tracing::debug!(event = "query processor created");
                Ok(res_processor)
            }
            Err(err) => {
                tracing::debug!(event = "query processor failed");
                Err(err)
            }
        }
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.source.canonic_name()
    }
}

/// A cache operator that caches the results of its source operator
struct CacheQueryProcessor<Q, T>
where
    Q: QueryProcessor<Output = RasterTile2D<T>, SpatialBounds = SpatialPartition2D>,
    T: Pixel,
{
    processor: Q,
    cache_key: CanonicOperatorName,
}

impl<Q, T> CacheQueryProcessor<Q, T>
where
    Q: QueryProcessor<Output = RasterTile2D<T>, SpatialBounds = SpatialPartition2D> + Sized,
    T: Pixel,
{
    pub fn new(processor: Q, cache_key: CanonicOperatorName) -> Self {
        CacheQueryProcessor {
            processor,
            cache_key,
        }
    }
}

#[async_trait]
impl<Q, T> QueryProcessor for CacheQueryProcessor<Q, T>
where
    Q: QueryProcessor<Output = RasterTile2D<T>, SpatialBounds = SpatialPartition2D> + Sized,
    T: Pixel + Cachable,
{
    type Output = RasterTile2D<T>;
    type SpatialBounds = SpatialPartition2D;

    async fn _query<'a>(
        &'a self,
        query: QueryRectangle<Self::SpatialBounds>,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let tile_cache = ctx
            .extensions()
            .get::<Arc<TileCache>>()
            .expect("`TileCache` extension should be set during `ProContext` creation");

        let cache_result = tile_cache
            .query_cache::<T>(self.cache_key.clone(), &query)
            .await;

        if let Some(cache_result) = cache_result {
            // cache hit
            return Ok(Box::pin(cache_result));
        }

        // cache miss
        let source_stream = self.processor.query(query, ctx).await?;

        let query_id = tile_cache
            .insert_query::<T>(self.cache_key.clone(), &query)
            .await;

        // lazily insert tiles into the cache as they are produced
        let (stream_event_sender, mut stream_event_receiver) = unbounded_channel();

        let cache_key = self.cache_key.clone();
        let tile_cache = tile_cache.clone();
        crate::util::spawn(async move {
            while let Some(event) = stream_event_receiver.recv().await {
                match event {
                    SourceStreamEvent::Tile(tile) => {
                        tile_cache
                            .insert_tile(cache_key.clone(), query_id, tile)
                            .await; // TODO: handle error
                    }
                    SourceStreamEvent::Abort => {
                        tile_cache.abort_query(cache_key.clone(), query_id).await;
                    }
                    SourceStreamEvent::Finished => {
                        tile_cache.finish_query(cache_key.clone(), query_id).await;
                    }
                }
            }
        });

        let output_stream = CacheOutputStream {
            source: source_stream,
            stream_event_sender,
            finished: false,
        };

        Ok(Box::pin(output_stream))
    }
}

#[allow(clippy::large_enum_variant)] // TODO: Box instead?
enum SourceStreamEvent<T> {
    Tile(RasterTile2D<T>),
    Abort,
    Finished,
}

/// Custom stream that lazily puts the produced tile in the cache and finishes the cache entry when the source stream completes
#[pin_project(PinnedDrop, project = CacheOutputStreamProjection)]
struct CacheOutputStream<S, T>
where
    S: Stream<Item = Result<RasterTile2D<T>>>,
{
    #[pin]
    source: S,
    // cache: Arc<TileCache>,
    // cache_key: CanonicOperatorName,
    stream_event_sender: UnboundedSender<SourceStreamEvent<T>>,
    finished: bool,
}

impl<S, T> Stream for CacheOutputStream<S, T>
where
    S: Stream<Item = Result<RasterTile2D<T>>>,
    T: Pixel,
{
    type Item = Result<RasterTile2D<T>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let next = ready!(this.source.poll_next(cx));

        if let Some(tile) = &next {
            if let Ok(tile) = tile {
                let _ = this
                    .stream_event_sender
                    .send(SourceStreamEvent::Tile(tile.clone()));
            } else {
                let _ = this.stream_event_sender.send(SourceStreamEvent::Abort);
            }
        } else {
            let _ = this.stream_event_sender.send(SourceStreamEvent::Finished);
            *this.finished = true;
        }

        Poll::Ready(next)
    }
}

#[pinned_drop]
impl<S, T> PinnedDrop for CacheOutputStream<S, T>
where
    S: Stream<Item = Result<RasterTile2D<T>>>,
{
    fn drop(self: Pin<&mut Self>) {
        if !self.finished {
            let _ = self.stream_event_sender.send(SourceStreamEvent::Abort);
        }
    }
}
