use crate::engine::{
    ChunkByteSize, CreateSpan, ExecutionContext, ExecutionContextExtensions,
    InitializedPlotOperator, InitializedRasterOperator, InitializedVectorOperator, MetaData,
    MetaDataProvider, MockExecutionContext, MockQueryContext, QueryContextExtensions,
    ResultDescriptor, WorkflowOperatorPath,
};
use crate::pro::meta::wrapper::InitializedOperatorWrapper;
use crate::util::Result;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataId, NamedData};
use geoengine_datatypes::machine_learning::{MlModelMetadata, MlModelName};
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::util::test::TestDefault;
use rayon::ThreadPool;
use std::sync::Arc;

/// A mock execution context that wraps all operators with a statistics operator.
pub struct StatisticsWrappingMockExecutionContext {
    pub inner: MockExecutionContext,
}

impl TestDefault for StatisticsWrappingMockExecutionContext {
    fn test_default() -> Self {
        Self {
            inner: MockExecutionContext::test_default(),
        }
    }
}

impl StatisticsWrappingMockExecutionContext {
    pub fn mock_query_context_with_query_extensions(
        &self,
        chunk_byte_size: ChunkByteSize,
        extensions: QueryContextExtensions,
    ) -> MockQueryContext {
        MockQueryContext::new_with_query_extensions(
            chunk_byte_size,
            self.tiling_specification(),
            extensions,
        )
    }
}

#[async_trait::async_trait]
impl ExecutionContext for StatisticsWrappingMockExecutionContext {
    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.inner.thread_pool
    }

    fn tiling_specification(&self) -> TilingSpecification {
        self.inner.tiling_specification
    }

    fn wrap_initialized_raster_operator(
        &self,
        op: Box<dyn InitializedRasterOperator>,
        span: CreateSpan,
        path: WorkflowOperatorPath,
    ) -> Box<dyn InitializedRasterOperator> {
        InitializedOperatorWrapper::new(op, span, path).boxed()
    }

    fn wrap_initialized_vector_operator(
        &self,
        op: Box<dyn InitializedVectorOperator>,
        span: CreateSpan,
        path: WorkflowOperatorPath,
    ) -> Box<dyn InitializedVectorOperator> {
        InitializedOperatorWrapper::new(op, span, path).boxed()
    }

    fn wrap_initialized_plot_operator(
        &self,
        op: Box<dyn InitializedPlotOperator>,
        _span: CreateSpan,
        _path: WorkflowOperatorPath,
    ) -> Box<dyn InitializedPlotOperator> {
        op
    }

    async fn resolve_named_data(&self, data: &NamedData) -> Result<DataId> {
        self.inner.resolve_named_data(data).await
    }

    async fn ml_model_metadata(&self, name: &MlModelName) -> Result<MlModelMetadata> {
        self.inner.ml_model_metadata(name).await
    }

    fn extensions(&self) -> &ExecutionContextExtensions {
        self.inner.extensions()
    }
}

#[async_trait]
impl<L, R, Q> MetaDataProvider<L, R, Q> for StatisticsWrappingMockExecutionContext
where
    L: 'static,
    R: 'static + ResultDescriptor,
    Q: 'static,
{
    async fn meta_data(&self, id: &DataId) -> Result<Box<dyn MetaData<L, R, Q>>> {
        self.inner.meta_data(id).await
    }
}
