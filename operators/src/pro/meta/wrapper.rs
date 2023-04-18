use std::sync::atomic::{AtomicUsize, Ordering};

use crate::engine::{
    CreateSpan, InitializedRasterOperator, InitializedVectorOperator, QueryContext, QueryProcessor,
    RasterResultDescriptor, TypedRasterQueryProcessor, TypedVectorQueryProcessor,
    VectorResultDescriptor, WorkflowOperatorPath,
};
use crate::pro::adapters::stream_statistics_adapter::StreamStatisticsAdapter;
use crate::pro::meta::quota::{QuotaChecker, QuotaTracking};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::primitives::{AxisAlignedRectangle, QueryRectangle};

// A wrapper around an initialized operator that adds statistics and quota tracking
pub struct InitializedOperatorWrapper<S> {
    source: S,
    span: CreateSpan,
    path: WorkflowOperatorPath,
}

impl<S> InitializedOperatorWrapper<S> {
    pub fn new(source: S, span: CreateSpan, path: WorkflowOperatorPath) -> Self {
        Self { source, span, path }
    }
}

impl InitializedRasterOperator for InitializedOperatorWrapper<Box<dyn InitializedRasterOperator>> {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        tracing::debug!(event = "raster result descriptor", path = ?self.path);
        self.source.result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        tracing::debug!(event = "query processor", path = ?self.path);
        let processor_result = self.source.query_processor();
        match processor_result {
            Ok(p) => {
                let path_clone = self.path.clone();
                let res_processor = match p {
                    TypedRasterQueryProcessor::U8(p) => TypedRasterQueryProcessor::U8(Box::new(
                        QueryProcessorWrapper::new(p, self.span, path_clone),
                    )),
                    TypedRasterQueryProcessor::U16(p) => TypedRasterQueryProcessor::U16(Box::new(
                        QueryProcessorWrapper::new(p, self.span, path_clone),
                    )),
                    TypedRasterQueryProcessor::U32(p) => TypedRasterQueryProcessor::U32(Box::new(
                        QueryProcessorWrapper::new(p, self.span, path_clone),
                    )),
                    TypedRasterQueryProcessor::U64(p) => TypedRasterQueryProcessor::U64(Box::new(
                        QueryProcessorWrapper::new(p, self.span, path_clone),
                    )),
                    TypedRasterQueryProcessor::I8(p) => TypedRasterQueryProcessor::I8(Box::new(
                        QueryProcessorWrapper::new(p, self.span, path_clone),
                    )),
                    TypedRasterQueryProcessor::I16(p) => TypedRasterQueryProcessor::I16(Box::new(
                        QueryProcessorWrapper::new(p, self.span, path_clone),
                    )),
                    TypedRasterQueryProcessor::I32(p) => TypedRasterQueryProcessor::I32(Box::new(
                        QueryProcessorWrapper::new(p, self.span, path_clone),
                    )),
                    TypedRasterQueryProcessor::I64(p) => TypedRasterQueryProcessor::I64(Box::new(
                        QueryProcessorWrapper::new(p, self.span, path_clone),
                    )),
                    TypedRasterQueryProcessor::F32(p) => TypedRasterQueryProcessor::F32(Box::new(
                        QueryProcessorWrapper::new(p, self.span, path_clone),
                    )),
                    TypedRasterQueryProcessor::F64(p) => TypedRasterQueryProcessor::F64(Box::new(
                        QueryProcessorWrapper::new(p, self.span, path_clone),
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
}

impl InitializedVectorOperator for InitializedOperatorWrapper<Box<dyn InitializedVectorOperator>> {
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        tracing::debug!(event = "vector result descriptor");
        self.source.result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        tracing::debug!(event = "query processor");
        let processor_result = self.source.query_processor();
        match processor_result {
            Ok(p) => {
                let result = map_typed_query_processor!(
                    p,
                    p => Box::new(QueryProcessorWrapper::new(p,
                    self.span, self.path.clone()))
                );
                tracing::debug!(event = "query processor created");
                Ok(result)
            }
            Err(err) => {
                tracing::debug!(event = "query processor failed");
                Err(err)
            }
        }
    }
}

// A wrapper around a query processor that adds statistics and quota tracking
struct QueryProcessorWrapper<Q, T>
where
    Q: QueryProcessor<Output = T>,
{
    processor: Q,
    span: CreateSpan,
    path: WorkflowOperatorPath,
    query_count: AtomicUsize,
}

impl<Q, T> QueryProcessorWrapper<Q, T>
where
    Q: QueryProcessor<Output = T> + Sized,
{
    pub fn new(processor: Q, span: CreateSpan, path: WorkflowOperatorPath) -> Self {
        QueryProcessorWrapper {
            processor,
            span,
            path,
            query_count: AtomicUsize::new(0),
        }
    }

    pub fn inc_query_count(&self) -> usize {
        self.query_count.fetch_add(1, Ordering::SeqCst)
    }
}

#[async_trait]
impl<Q, T, S> QueryProcessor for QueryProcessorWrapper<Q, T>
where
    Q: QueryProcessor<Output = T, SpatialBounds = S>,
    S: AxisAlignedRectangle + Send + Sync + 'static,
    T: Send,
{
    type Output = T;
    type SpatialBounds = S;

    async fn _query<'a>(
        &'a self,
        query: QueryRectangle<Self::SpatialBounds>,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let qc = self.inc_query_count() + 1;

        tracing::trace!(
            event = "query",
            query_count = qc,
            path = self.path.to_string()
        );

        let quota_checker = ctx
            .extensions()
            .get::<QuotaChecker>()
            .expect("`QuotaChecker` extension should be set during `ProContext` creation");

        // TODO: check the quota only once per query and not for every operator
        quota_checker.ensure_quota_available().await?;

        let quota_tracker = ctx
            .extensions()
            .get::<QuotaTracking>()
            .expect("`QuotaTracking` extension should be set during `ProContext` creation")
            .clone();

        let span = (self.span)();
        let _enter = span.enter();

        let stream_result = self.processor.query(query, ctx).await;
        tracing::debug!(event = "query ready");

        match stream_result {
            Ok(stream) => {
                tracing::debug!(event = "query ok", path = ?self.path);
                Ok(StreamStatisticsAdapter::new(
                    stream,
                    span.clone(),
                    quota_tracker,
                    self.path.clone(),
                )
                .boxed())
            }
            Err(err) => {
                tracing::debug!(event = "query error", path = ?self.path);
                Err(err)
            }
        }
    }
}
