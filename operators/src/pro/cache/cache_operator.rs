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
use futures::{ready, Stream};
use geoengine_datatypes::primitives::{QueryRectangle, SpatialGridQueryRectangle};
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
    Q: QueryProcessor<Output = RasterTile2D<T>, SpatialQuery = SpatialGridQueryRectangle>,
    T: Pixel,
{
    processor: Q,
    cache_key: CanonicOperatorName,
}

impl<Q, T> CacheQueryProcessor<Q, T>
where
    Q: QueryProcessor<Output = RasterTile2D<T>, SpatialQuery = SpatialGridQueryRectangle> + Sized,
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
    Q: QueryProcessor<Output = RasterTile2D<T>, SpatialQuery = SpatialGridQueryRectangle> + Sized,
    T: Pixel + Cachable,
{
    type Output = RasterTile2D<T>;
    type SpatialQuery = SpatialGridQueryRectangle;

    async fn _query<'a>(
        &'a self,
        query: QueryRectangle<Self::SpatialQuery>,
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
            log::debug!("cache hit for operator {:?}", self.cache_key);
            return Ok(Box::pin(cache_result));
        }

        // cache miss
        log::debug!("cache miss for operator {:?}", self.cache_key);
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
                        let result = tile_cache
                            .insert_tile(cache_key.clone(), query_id, tile)
                            .await;
                        log::debug!(
                            "inserted tile into cache for cache key {:?} and query id {}. result: {:?}",
                            cache_key,
                            query_id,
                            result
                        );
                    }
                    SourceStreamEvent::Abort => {
                        tile_cache.abort_query(cache_key.clone(), query_id).await;
                        log::debug!(
                            "aborted cache insertion for cache key {:?} and query id {}",
                            cache_key,
                            query_id
                        );
                    }
                    SourceStreamEvent::Finished => {
                        let result = tile_cache.finish_query(cache_key.clone(), query_id).await;
                        log::debug!(
                            "finished cache insertion for cache key {:?} and query id {}, result: {:?}",
                            cache_key,query_id,
                            result
                        );
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
                // ignore the result. The receiver shold never drop prematurely, but if it does we don't want to crash
                let _ = this
                    .stream_event_sender
                    .send(SourceStreamEvent::Tile(tile.clone()));
            } else {
                // ignore the result. The receiver shold never drop prematurely, but if it does we don't want to crash
                let _ = this.stream_event_sender.send(SourceStreamEvent::Abort);
            }
        } else {
            // ignore the result. The receiver shold never drop prematurely, but if it does we don't want to crash
            let _ = this.stream_event_sender.send(SourceStreamEvent::Finished);
            *this.finished = true;
        }

        Poll::Ready(next)
    }
}

/// On drop, trigger the removal of the cache entry if it hasn't been finished yet
#[pinned_drop]
impl<S, T> PinnedDrop for CacheOutputStream<S, T>
where
    S: Stream<Item = Result<RasterTile2D<T>>>,
{
    fn drop(self: Pin<&mut Self>) {
        if !self.finished {
            // ignore the result. The receiver shold never drop prematurely, but if it does we don't want to crash
            let _ = self.stream_event_sender.send(SourceStreamEvent::Abort);
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::{RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval},
        util::test::TestDefault,
    };

    use crate::{
        engine::{
            ChunkByteSize, MockExecutionContext, MockQueryContext, QueryContextExtensions,
            RasterOperator, WorkflowOperatorPath,
        },
        source::{GdalSource, GdalSourceParameters},
        util::gdal::add_ndvi_dataset,
    };

    use super::*;

    #[tokio::test]
    async fn it_caches() {
        let mut exe_ctx = MockExecutionContext::test_default();

        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);

        let operator = GdalSource {
            params: GdalSourceParameters { data: ndvi_id },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap();

        let cached_op = InitializedCacheOperator::new(operator);

        let processor = cached_op.query_processor().unwrap().get_u8().unwrap();

        let tile_cache = Arc::new(TileCache::default());

        let mut extensions = QueryContextExtensions::default();

        extensions.insert(tile_cache);

        let query_ctx =
            MockQueryContext::new_with_query_extensions(ChunkByteSize::test_default(), extensions);

        let stream = processor
            .query(
                RasterQueryRectangle::with_partition_and_resolution_and_origin(
                    SpatialPartition2D::new_unchecked([-180., -90.].into(), [180., 90.].into()),
                    SpatialResolution::zero_point_one(),
                    exe_ctx.tiling_specification.origin_coordinate,
                    TimeInterval::default(),
                ),
                &query_ctx,
            )
            .await
            .unwrap();

        let tiles = stream.collect::<Vec<_>>().await;
        let tiles = tiles.into_iter().collect::<Result<Vec<_>>>().unwrap();

        // wait for the cache to be filled, which happens asynchronously
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

        let stream_from_cache = processor
            .query(
                RasterQueryRectangle::with_partition_and_resolution_and_origin(
                    SpatialPartition2D::new_unchecked([-180., -90.].into(), [180., 90.].into()),
                    SpatialResolution::zero_point_one(),
                    exe_ctx.tiling_specification.origin_coordinate,
                    TimeInterval::default(),
                ),
                &query_ctx,
            )
            .await
            .unwrap();

        let tiles_from_cache = stream_from_cache.collect::<Vec<_>>().await;
        let tiles_from_cache = tiles_from_cache
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        // TODO: how to ensure the tiles are actually from the cache?

        assert_eq!(tiles, tiles_from_cache);
    }
}
