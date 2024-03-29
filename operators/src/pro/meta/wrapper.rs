use std::sync::atomic::{AtomicUsize, Ordering};

use crate::engine::{
    CanonicOperatorName, CreateSpan, InitializedRasterOperator, InitializedVectorOperator,
    QueryContext, QueryProcessor, RasterResultDescriptor, ResultDescriptor,
    TypedRasterQueryProcessor, TypedVectorQueryProcessor, VectorResultDescriptor,
    WorkflowOperatorPath,
};
use crate::pro::adapters::stream_statistics_adapter::StreamStatisticsAdapter;
use crate::pro::meta::quota::{QuotaChecker, QuotaTracking};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::primitives::{QueryAttributeSelection, QueryRectangle};
use tracing::{span, Level};

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
        tracing::debug!(
            event = "raster result descriptor",
            path = self.path.to_string()
        );
        self.source.result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        tracing::debug!(event = "query processor", path = self.path.to_string());
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

    fn canonic_name(&self) -> CanonicOperatorName {
        self.source.canonic_name()
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

    fn canonic_name(&self) -> CanonicOperatorName {
        self.source.canonic_name()
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

    pub fn next_query_count(&self) -> usize {
        self.query_count.fetch_add(1, Ordering::SeqCst)
    }
}

#[async_trait]
impl<Q, T, S, A, R> QueryProcessor for QueryProcessorWrapper<Q, T>
where
    Q: QueryProcessor<Output = T, SpatialQuery = S, Selection = A, ResultDescription = R>,
    S: std::fmt::Debug + Send + Sync + 'static + Clone + Copy,
    A: QueryAttributeSelection + 'static,
    R: ResultDescriptor<QueryRectangleSpatialBounds = S, QueryRectangleAttributeSelection = A>
        + 'static,
    T: Send,
{
    type Output = T;
    type SpatialQuery = S;
    type Selection = A;
    type ResultDescription = R;

    async fn _query<'a>(
        &'a self,
        query: QueryRectangle<Self::SpatialQuery, Self::Selection>,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let qc = self.next_query_count();

        // the top level operator creates a new query span for identifying individual queries
        let query_span = if self.path.is_root() {
            let span = span!(
                Level::TRACE,
                "Query",
                query_id = %uuid::Uuid::new_v4()
            );
            Some(span)
        } else {
            None
        };

        let _query_span_enter = query_span.as_ref().map(tracing::Span::enter);

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

        let span = (self.span)(&self.path, qc);

        let _enter = span.enter();

        let spbox = query.spatial_query;
        let time = query.time_interval;
        tracing::trace!(
            event = %"query_start",
            path = %self.path,
            bbox = %format!("{:?}", spbox), // FIXME: better format then debug here
            time = %format!("[{},{}]",
                time.start().inner(),
                time.end().inner()
            )
        );

        let stream_result = self.processor.query(query, ctx).await;
        tracing::trace!(event = %"query_ready");

        match stream_result {
            Ok(stream) => {
                tracing::trace!(event = %"query_ok");
                Ok(StreamStatisticsAdapter::new(
                    stream,
                    span.clone(),
                    quota_tracker,
                    self.path.clone(),
                )
                .boxed())
            }
            Err(err) => {
                tracing::trace!(event = %"query_error");
                Err(err)
            }
        }
    }

    fn result_descriptor(&self) -> &Self::ResultDescription {
        self.processor.result_descriptor()
    }
}
