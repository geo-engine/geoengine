use super::error::CacheError;
use super::shared_cache::CacheElement;
use crate::adapters::FeatureCollectionChunkMerger;
use crate::engine::{
    CanonicOperatorName, ChunkByteSize, InitializedRasterOperator, InitializedVectorOperator,
    QueryContext, QueryProcessor, RasterResultDescriptor, TypedRasterQueryProcessor,
};
use crate::error::Error;
use crate::pro::cache::shared_cache::{AsyncCache, SharedCache};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::{BoxStream, FusedStream};
use futures::{ready, Stream, TryStreamExt};
use geoengine_datatypes::collections::FeatureCollection;
use geoengine_datatypes::primitives::{AxisAlignedRectangle, Geometry, QueryRectangle};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use geoengine_datatypes::util::arrow::ArrowTyped;
use pin_project::{pin_project, pinned_drop};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

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

impl InitializedVectorOperator for InitializedCacheOperator<Box<dyn InitializedVectorOperator>> {
    fn result_descriptor(&self) -> &crate::engine::VectorResultDescriptor {
        self.source.result_descriptor()
    }

    fn query_processor(&self) -> Result<crate::engine::TypedVectorQueryProcessor> {
        let processor_result = self.source.query_processor();
        match processor_result {
            Ok(p) => {
                let res_processor = match p {
                    crate::engine::TypedVectorQueryProcessor::Data(p) => {
                        crate::engine::TypedVectorQueryProcessor::Data(Box::new(
                            CacheQueryProcessor::new(p, self.source.canonic_name()),
                        ))
                    }
                    crate::engine::TypedVectorQueryProcessor::MultiPoint(p) => {
                        crate::engine::TypedVectorQueryProcessor::MultiPoint(Box::new(
                            CacheQueryProcessor::new(p, self.source.canonic_name()),
                        ))
                    }
                    crate::engine::TypedVectorQueryProcessor::MultiLineString(p) => {
                        crate::engine::TypedVectorQueryProcessor::MultiLineString(Box::new(
                            CacheQueryProcessor::new(p, self.source.canonic_name()),
                        ))
                    }
                    crate::engine::TypedVectorQueryProcessor::MultiPolygon(p) => {
                        crate::engine::TypedVectorQueryProcessor::MultiPolygon(Box::new(
                            CacheQueryProcessor::new(p, self.source.canonic_name()),
                        ))
                    }
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
struct CacheQueryProcessor<P, E, Q>
where
    E: CacheElement + Send + Sync + 'static,
    P: QueryProcessor<Output = E, SpatialBounds = Q>,
{
    processor: P,
    cache_key: CanonicOperatorName,
}

impl<P, E, Q> CacheQueryProcessor<P, E, Q>
where
    E: CacheElement + Send + Sync + 'static,
    P: QueryProcessor<Output = E, SpatialBounds = Q> + Sized,
{
    pub fn new(processor: P, cache_key: CanonicOperatorName) -> Self {
        CacheQueryProcessor {
            processor,
            cache_key,
        }
    }
}

#[async_trait]
impl<P, E, S> QueryProcessor for CacheQueryProcessor<P, E, S>
where
    P: QueryProcessor<Output = E, SpatialBounds = S> + Sized,
    S: AxisAlignedRectangle + Send + Sync + 'static,
    E: CacheElement<Query = QueryRectangle<S>>
        + Send
        + Sync
        + 'static
        + ResultStreamWrapper
        + Clone,
    E::ResultStream: Stream<Item = Result<E, CacheError>> + Send + Sync + 'static,
    SharedCache: AsyncCache<E>,
{
    type Output = E;
    type SpatialBounds = S;

    async fn _query<'a>(
        &'a self,
        query: QueryRectangle<Self::SpatialBounds>,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let shared_cache = ctx
            .extensions()
            .get::<Arc<SharedCache>>()
            .expect("`SharedCache` extension should be set during `ProContext` creation");

        let cache_result = shared_cache.query_cache(&self.cache_key, &query).await;

        if let Ok(Some(cache_result)) = cache_result {
            // cache hit
            log::debug!("cache hit for operator {}", self.cache_key);

            let wrapped_result_steam = E::wrap_result_stream(cache_result, ctx.chunk_byte_size());

            return Ok(wrapped_result_steam);
        }

        // cache miss
        log::debug!("cache miss for operator {}", self.cache_key);
        let source_stream = self.processor.query(query, ctx).await?;

        let query_id = shared_cache.insert_query(&self.cache_key, &query).await;

        if let Err(e) = query_id {
            log::debug!("could not insert query into cache: {}", e);
            return Ok(source_stream);
        }

        let query_id = query_id.expect("query_id should be set because of the previous check");

        // lazily insert tiles into the cache as they are produced
        let (stream_event_sender, mut stream_event_receiver) = unbounded_channel();

        let cache_key = self.cache_key.clone();
        let tile_cache = shared_cache.clone();
        crate::util::spawn(async move {
            while let Some(event) = stream_event_receiver.recv().await {
                match event {
                    SourceStreamEvent::Element(tile) => {
                        let result = tile_cache
                            .insert_query_element(&cache_key, &query_id, tile)
                            .await;
                        log::trace!(
                            "inserted tile into cache for cache key {} and query id {}. result: {:?}",
                            cache_key,
                            query_id,
                            result
                        );
                    }
                    SourceStreamEvent::Abort => {
                        tile_cache.abort_query(&cache_key, &query_id).await;
                        log::debug!(
                            "aborted cache insertion for cache key {} and query id {}",
                            cache_key,
                            query_id
                        );
                    }
                    SourceStreamEvent::Finished => {
                        let result = tile_cache.finish_query(&cache_key, &query_id).await;
                        log::debug!(
                            "finished cache insertion for cache key {} and query id {}, result: {:?}",
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
            pristine: true,
        };

        Ok(Box::pin(output_stream))
    }
}

#[allow(clippy::large_enum_variant)] // TODO: Box instead?
enum SourceStreamEvent<E: CacheElement> {
    Element(E),
    Abort,
    Finished,
}

/// Custom stream that lazily puts the produced tile in the cache and finishes the cache entry when the source stream completes
#[pin_project(PinnedDrop, project = CacheOutputStreamProjection)]
struct CacheOutputStream<S, E>
where
    S: Stream<Item = Result<E>>,
    E: CacheElement + Clone,
{
    #[pin]
    source: S,
    stream_event_sender: UnboundedSender<SourceStreamEvent<E>>,
    finished: bool,
    pristine: bool,
}

impl<S, E> Stream for CacheOutputStream<S, E>
where
    S: Stream<Item = Result<E>>,
    E: CacheElement + Clone,
{
    type Item = Result<E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let next = ready!(this.source.poll_next(cx));

        if let Some(element) = &next {
            *this.pristine = false;
            if let Ok(element) = element {
                // ignore the result. The receiver shold never drop prematurely, but if it does we don't want to crash
                let r = this
                    .stream_event_sender
                    .send(SourceStreamEvent::Element(element.clone()));
                if let Err(e) = r {
                    log::warn!("could not send tile to cache: {}", e.to_string());
                }
            } else {
                // ignore the result. The receiver shold never drop prematurely, but if it does we don't want to crash
                let r = this.stream_event_sender.send(SourceStreamEvent::Abort);
                if let Err(e) = r {
                    log::warn!("could not send abort to cache: {}", e.to_string());
                }
            }
        } else {
            if *this.pristine {
                // ignore the result. The receiver shold never drop prematurely, but if it does we don't want to crash
                let r = this.stream_event_sender.send(SourceStreamEvent::Abort);
                if let Err(e) = r {
                    log::warn!("could not send abort to cache: {}", e.to_string());
                }
            } else {
                // ignore the result. The receiver shold never drop prematurely, but if it does we don't want to crash
                let r = this.stream_event_sender.send(SourceStreamEvent::Finished);
                if let Err(e) = r {
                    log::warn!("could not send finished to cache: {}", e.to_string());
                }
                log::debug!("stream finished, mark cache entry as finished.");
            }
            *this.finished = true;
        }

        Poll::Ready(next)
    }
}

/// On drop, trigger the removal of the cache entry if it hasn't been finished yet
#[pinned_drop]
impl<S, E> PinnedDrop for CacheOutputStream<S, E>
where
    S: Stream<Item = Result<E>>,
    E: CacheElement + Clone,
{
    fn drop(self: Pin<&mut Self>) {
        if !self.finished {
            // ignore the result. The receiver shold never drop prematurely, but if it does we don't want to crash
            let r = self.stream_event_sender.send(SourceStreamEvent::Abort);
            if let Err(e) = r {
                log::debug!("could not send abort to cache: {}", e.to_string());
            }
        }
    }
}

trait ResultStreamWrapper: CacheElement {
    fn wrap_result_stream<'a>(
        stream: Self::ResultStream,
        chunk_byte_size: ChunkByteSize,
    ) -> BoxStream<'a, Result<Self>>;
}

impl<G> ResultStreamWrapper for FeatureCollection<G>
where
    G: Geometry + ArrowTyped + Send + Sync + 'static,
    FeatureCollection<G>: CacheElement + Send + Sync,
    Self::ResultStream: FusedStream + Send + Sync,
{
    fn wrap_result_stream<'a>(
        stream: Self::ResultStream,
        chunk_byte_size: ChunkByteSize,
    ) -> BoxStream<'a, Result<Self>> {
        Box::pin(FeatureCollectionChunkMerger::new(
            stream.map_err(|ce| Error::CacheCantProduceResult { source: ce.into() }),
            chunk_byte_size.into(),
        ))
    }
}

impl<P> ResultStreamWrapper for RasterTile2D<P>
where
    P: 'static + Pixel,
    RasterTile2D<P>: CacheElement,
    Self::ResultStream: Send + Sync,
{
    fn wrap_result_stream<'a>(
        stream: Self::ResultStream,
        _chunk_byte_size: ChunkByteSize,
    ) -> BoxStream<'a, Result<Self>> {
        Box::pin(stream.map_err(|ce| Error::CacheCantProduceResult { source: ce.into() }))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::{SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::TilesEqualIgnoringCacheHint,
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

        let tile_cache = Arc::new(SharedCache::test_default());

        let mut extensions = QueryContextExtensions::default();

        extensions.insert(tile_cache);

        let query_ctx =
            MockQueryContext::new_with_query_extensions(ChunkByteSize::test_default(), extensions);

        let stream = processor
            .query(
                QueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        [-180., -90.].into(),
                        [180., 90.].into(),
                    ),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::zero_point_one(),
                },
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
                QueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        [-180., -90.].into(),
                        [180., 90.].into(),
                    ),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::zero_point_one(),
                },
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

        assert!(tiles.tiles_equal_ignoring_cache_hint(&tiles_from_cache));
    }
}
