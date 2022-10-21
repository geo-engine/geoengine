use crate::engine::{
    CreateSpan, InitializedRasterOperator, InitializedVectorOperator, QueryContext, QueryProcessor,
    RasterResultDescriptor, TypedRasterQueryProcessor, TypedVectorQueryProcessor,
    VectorResultDescriptor,
};
use crate::pro::adapters::stream_statistics_adapter::StreamStatisticsAdapter;
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::primitives::{AxisAlignedRectangle, QueryRectangle};

pub struct InitializedProcessorStatistics<S> {
    source: S,
    span: CreateSpan,
}

impl<S> InitializedProcessorStatistics<S> {
    pub fn new(source: S, span: CreateSpan) -> Self {
        Self { source, span }
    }
}

impl InitializedRasterOperator
    for InitializedProcessorStatistics<Box<dyn InitializedRasterOperator>>
{
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        tracing::debug!(event = "raster result descriptor");
        self.source.result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        tracing::debug!(event = "query processor");
        let processor_result = self.source.query_processor();
        match processor_result {
            Ok(p) => {
                let res_processor = match p {
                    TypedRasterQueryProcessor::U8(p) => TypedRasterQueryProcessor::U8(Box::new(
                        ProcessorStatisticsProcessor::new(p, self.span),
                    )),
                    TypedRasterQueryProcessor::U16(p) => TypedRasterQueryProcessor::U16(Box::new(
                        ProcessorStatisticsProcessor::new(p, self.span),
                    )),
                    TypedRasterQueryProcessor::U32(p) => TypedRasterQueryProcessor::U32(Box::new(
                        ProcessorStatisticsProcessor::new(p, self.span),
                    )),
                    TypedRasterQueryProcessor::U64(p) => TypedRasterQueryProcessor::U64(Box::new(
                        ProcessorStatisticsProcessor::new(p, self.span),
                    )),
                    TypedRasterQueryProcessor::I8(p) => TypedRasterQueryProcessor::I8(Box::new(
                        ProcessorStatisticsProcessor::new(p, self.span),
                    )),
                    TypedRasterQueryProcessor::I16(p) => TypedRasterQueryProcessor::I16(Box::new(
                        ProcessorStatisticsProcessor::new(p, self.span),
                    )),
                    TypedRasterQueryProcessor::I32(p) => TypedRasterQueryProcessor::I32(Box::new(
                        ProcessorStatisticsProcessor::new(p, self.span),
                    )),
                    TypedRasterQueryProcessor::I64(p) => TypedRasterQueryProcessor::I64(Box::new(
                        ProcessorStatisticsProcessor::new(p, self.span),
                    )),
                    TypedRasterQueryProcessor::F32(p) => TypedRasterQueryProcessor::F32(Box::new(
                        ProcessorStatisticsProcessor::new(p, self.span),
                    )),
                    TypedRasterQueryProcessor::F64(p) => TypedRasterQueryProcessor::F64(Box::new(
                        ProcessorStatisticsProcessor::new(p, self.span),
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

impl InitializedVectorOperator
    for InitializedProcessorStatistics<Box<dyn InitializedVectorOperator>>
{
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
                    p => Box::new(ProcessorStatisticsProcessor::new(p,
                    self.span))
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

struct ProcessorStatisticsProcessor<Q, T>
where
    Q: QueryProcessor<Output = T>,
{
    processor: Q,
    span: CreateSpan,
}

impl<Q, T> ProcessorStatisticsProcessor<Q, T>
where
    Q: QueryProcessor<Output = T> + Sized,
{
    pub fn new(processor: Q, span: CreateSpan) -> Self {
        ProcessorStatisticsProcessor { processor, span }
    }
}

#[async_trait]
impl<Q, T, S> QueryProcessor for ProcessorStatisticsProcessor<Q, T>
where
    Q: QueryProcessor<Output = T, SpatialBounds = S>,
    S: AxisAlignedRectangle + Send + Sync + 'static,
    T: Send,
{
    type Output = T;
    type SpatialBounds = S;

    async fn query<'a>(
        &'a self,
        query: QueryRectangle<Self::SpatialBounds>,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        tracing::trace!(event = "query");
        let stream_result = self.processor.query(query, ctx).await;
        tracing::debug!(event = "query ready");
        match stream_result {
            Ok(stream) => {
                tracing::debug!(event = "query ok");
                Ok(StreamStatisticsAdapter::new(stream, (self.span)()).boxed())
            }
            Err(err) => {
                tracing::debug!(event = "query error");
                Err(err)
            }
        }
    }
}
