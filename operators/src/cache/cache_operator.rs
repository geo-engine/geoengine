use super::cache_chunks::CacheElementSpatialBounds;
use super::error::CacheError;
use super::shared_cache::CacheElement;
use crate::adapters::FeatureCollectionChunkMerger;
use crate::cache::shared_cache::{AsyncCache, SharedCache};
use crate::engine::{
    CanonicOperatorName, ChunkByteSize, InitializedRasterOperator, InitializedVectorOperator,
    QueryContext, QueryProcessor, RasterResultDescriptor, ResultDescriptor,
    TypedRasterQueryProcessor, WorkflowOperatorPath,
};
use crate::error::Error;
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::{BoxStream, FusedStream};
use futures::{ready, Stream, StreamExt, TryStreamExt};
use geoengine_datatypes::collections::{FeatureCollection, FeatureCollectionInfos};
use geoengine_datatypes::primitives::{
    Geometry, QueryAttributeSelection, QueryRectangle, VectorQueryRectangle,
};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use geoengine_datatypes::util::arrow::ArrowTyped;
use geoengine_datatypes::util::helpers::ge_report;
use pin_project::{pin_project, pinned_drop};
use std::pin::Pin;
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

    fn name(&self) -> &'static str {
        self.source.name()
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.source.path()
    }

    fn optimize(
        &self,
        resolution: geoengine_datatypes::primitives::SpatialResolution,
    ) -> Result<Box<dyn crate::engine::RasterOperator>, crate::optimization::OptimizationError>
    {
        self.source.optimize(resolution)
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

    fn name(&self) -> &'static str {
        self.source.name()
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.source.path()
    }

    fn optimize(
        &self,
        resolution: geoengine_datatypes::primitives::SpatialResolution,
    ) -> Result<Box<dyn crate::engine::VectorOperator>, crate::optimization::OptimizationError>
    {
        self.source.optimize(resolution)
    }
}

/// A cache operator that caches the results of its source operator
struct CacheQueryProcessor<P, E, Q, U, R>
where
    E: CacheElement + Send + Sync + 'static,
    P: QueryProcessor<Output = E, SpatialQuery = Q, Selection = U, ResultDescription = R>,
{
    processor: P,
    cache_key: CanonicOperatorName,
}

impl<P, E, Q, U, R> CacheQueryProcessor<P, E, Q, U, R>
where
    E: CacheElement + Send + Sync + 'static,
    P: QueryProcessor<Output = E, SpatialQuery = Q, Selection = U, ResultDescription = R> + Sized,
{
    pub fn new(processor: P, cache_key: CanonicOperatorName) -> Self {
        CacheQueryProcessor {
            processor,
            cache_key,
        }
    }
}

#[async_trait]
impl<P, E, S, U, R> QueryProcessor for CacheQueryProcessor<P, E, S, U, R>
where
    P: QueryProcessor<Output = E, SpatialQuery = S, Selection = U, ResultDescription = R> + Sized,
    S: Clone + Send + Sync + 'static,
    U: QueryAttributeSelection,
    E: CacheElement<Query = QueryRectangle<S, U>>
        + Send
        + Sync
        + 'static
        + ResultStreamWrapper
        + Clone,
    E::ResultStream: Stream<Item = Result<E, CacheError>> + Send + Sync + 'static,
    SharedCache: AsyncCache<E>,
    R: ResultDescriptor<QueryRectangleSpatialBounds = S, QueryRectangleAttributeSelection = U>,
{
    type Output = E;
    type SpatialQuery = S;
    type Selection = U;
    type ResultDescription = R;

    async fn _query<'a>(
        &'a self,
        query: QueryRectangle<Self::SpatialQuery, Self::Selection>,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let shared_cache = ctx
            .cache()
            .expect("`SharedCache` extension should be set during `ProContext` creation");

        let cache_result = shared_cache.query_cache(&self.cache_key, &query).await;

        if let Ok(Some(cache_result)) = cache_result {
            // cache hit
            log::debug!("cache hit for operator {}", self.cache_key);

            let wrapped_result_steam =
                E::wrap_result_stream(cache_result, ctx.chunk_byte_size(), query.clone());

            return Ok(wrapped_result_steam);
        }

        // cache miss
        log::debug!("cache miss for operator {}", self.cache_key);
        let source_stream = self.processor.query(query.clone(), ctx).await?;

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

    fn result_descriptor(&self) -> &Self::ResultDescription {
        self.processor.result_descriptor()
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
                    log::warn!("could not send tile to cache: {}", ge_report(e));
                }
            } else {
                // ignore the result. The receiver shold never drop prematurely, but if it does we don't want to crash
                let r = this.stream_event_sender.send(SourceStreamEvent::Abort);
                if let Err(e) = r {
                    log::warn!("could not send abort to cache: {}", ge_report(e));
                }
            }
        } else {
            if *this.pristine {
                // ignore the result. The receiver shold never drop prematurely, but if it does we don't want to crash
                let r = this.stream_event_sender.send(SourceStreamEvent::Abort);
                if let Err(e) = r {
                    log::warn!("could not send abort to cache: {}", ge_report(e));
                }
            } else {
                // ignore the result. The receiver shold never drop prematurely, but if it does we don't want to crash
                let r = this.stream_event_sender.send(SourceStreamEvent::Finished);
                if let Err(e) = r {
                    log::warn!("could not send finished to cache: {}", ge_report(e));
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
        query: Self::Query,
    ) -> BoxStream<'a, Result<Self>>;
}

impl<G> ResultStreamWrapper for FeatureCollection<G>
where
    G: Geometry + ArrowTyped + Send + Sync + 'static,
    FeatureCollection<G>:
        CacheElement<Query = VectorQueryRectangle> + Send + Sync + CacheElementSpatialBounds,
    Self::ResultStream: FusedStream + Send + Sync,
{
    fn wrap_result_stream<'a>(
        stream: Self::ResultStream,
        chunk_byte_size: ChunkByteSize,
        query: Self::Query,
    ) -> BoxStream<'a, Result<Self>> {
        let filter_stream = stream.filter_map(move |result| {
            let query = query.clone();
            async move {
                result
                    .and_then(|collection| collection.filter_cache_element_entries(&query))
                    .map_err(|source| Error::CacheCantProduceResult {
                        source: source.into(),
                    })
                    .map(|fc| if fc.is_empty() { None } else { Some(fc) })
                    .transpose()
            }
        });

        let merger_stream =
            FeatureCollectionChunkMerger::new(filter_stream, chunk_byte_size.into());
        Box::pin(merger_stream)
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
        _query: Self::Query,
    ) -> BoxStream<'a, Result<Self>> {
        Box::pin(stream.map_err(|ce| Error::CacheCantProduceResult { source: ce.into() }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        engine::{
            ChunkByteSize, MockExecutionContext, MultipleRasterSources, RasterOperator,
            SingleRasterSource, WorkflowOperatorPath,
        },
        processing::{Expression, ExpressionParams, RasterStacker, RasterStackerParams},
        source::{GdalSource, GdalSourceParameters},
        util::gdal::add_ndvi_dataset,
    };
    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::{BandSelection, RasterQueryRectangle, TimeInterval},
        raster::{GridBoundingBox2D, RasterDataType, RenameBands, TilesEqualIgnoringCacheHint},
        util::test::TestDefault,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn it_caches() {
        let mut exe_ctx = MockExecutionContext::test_default();

        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);

        let operator = GdalSource {
            params: GdalSourceParameters::new(ndvi_id.clone()),
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap();

        let cached_op = InitializedCacheOperator::new(operator);

        let processor = cached_op.query_processor().unwrap().get_u8().unwrap();

        let tile_cache = Arc::new(SharedCache::test_default());

        let query_ctx = exe_ctx.mock_query_context_with_query_extensions(
            ChunkByteSize::test_default(),
            Some(tile_cache),
            None,
            None,
        );

        let stream = processor
            .query(
                RasterQueryRectangle::new_with_grid_bounds(
                    GridBoundingBox2D::new([-90, -180], [89, 179]).unwrap(),
                    TimeInterval::default(),
                    BandSelection::first(),
                ),
                &query_ctx,
            )
            .await
            .unwrap();

        let tiles = stream.collect::<Vec<_>>().await;
        let tiles = tiles.into_iter().collect::<Result<Vec<_>>>().unwrap();

        // wait for the cache to be filled, which happens asynchronously
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

        // delete the dataset to make sure the result is served from the cache
        exe_ctx.delete_meta_data(&ndvi_id);

        let stream_from_cache = processor
            .query(
                RasterQueryRectangle::new_with_grid_bounds(
                    GridBoundingBox2D::new([-90, -180], [89, 179]).unwrap(),
                    TimeInterval::default(),
                    BandSelection::first(),
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

        assert!(tiles.tiles_equal_ignoring_cache_hint(&tiles_from_cache));
    }

    #[tokio::test]
    async fn it_reuses_bands_from_cache_entries() {
        let mut exe_ctx = MockExecutionContext::test_default();

        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);

        let operator = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![
                    GdalSource {
                        params: GdalSourceParameters {
                            data: ndvi_id.clone(),
                            overview_level: None,
                        },
                    }
                    .boxed(),
                    Expression {
                        params: ExpressionParams {
                            expression: "2 * A".to_string(),
                            output_type: RasterDataType::U8,
                            output_band: None,
                            map_no_data: false,
                        },
                        sources: SingleRasterSource {
                            raster: GdalSource {
                                params: GdalSourceParameters {
                                    data: ndvi_id.clone(),
                                    overview_level: None,
                                },
                            }
                            .boxed(),
                        },
                    }
                    .boxed(),
                ],
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap();

        let cached_op = InitializedCacheOperator::new(operator);

        let processor = cached_op.query_processor().unwrap().get_u8().unwrap();

        let tile_cache = Arc::new(SharedCache::test_default());

        let query_ctx = exe_ctx.mock_query_context_with_query_extensions(
            ChunkByteSize::test_default(),
            Some(tile_cache),
            None,
            None,
        );

        // query the first two bands
        let stream = processor
            .query(
                RasterQueryRectangle::new_with_grid_bounds(
                    GridBoundingBox2D::new([-90, -180], [89, 179]).unwrap(),
                    TimeInterval::default(),
                    BandSelection::new(vec![0, 1]).unwrap(),
                ),
                &query_ctx,
            )
            .await
            .unwrap();

        let tiles = stream.collect::<Vec<_>>().await;
        let tiles = tiles.into_iter().collect::<Result<Vec<_>>>().unwrap();
        // only keep the second band for comparison
        let tiles = tiles
            .into_iter()
            .filter_map(|mut tile| {
                if tile.band == 1 {
                    tile.band = 0;
                    Some(tile)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // wait for the cache to be filled, which happens asynchronously
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

        // delete the dataset to make sure the result is served from the cache
        exe_ctx.delete_meta_data(&ndvi_id);

        // now query only the second band
        let stream_from_cache = processor
            .query(
                RasterQueryRectangle::new_with_grid_bounds(
                    GridBoundingBox2D::new([-90, -180], [89, 179]).unwrap(),
                    TimeInterval::default(),
                    BandSelection::new_single(1),
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

        assert!(tiles.tiles_equal_ignoring_cache_hint(&tiles_from_cache));
    }
}
