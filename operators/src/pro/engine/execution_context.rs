use crate::engine::{
    CreateSpan, ExecutionContext, InitializedPlotOperator, InitializedRasterOperator,
    InitializedVectorOperator, MetaData, MetaDataProvider, MockExecutionContext, ResultDescriptor,
    WorkflowOperatorPath,
};
use crate::pro::meta::wrapper::InitializedOperatorWrapper;
use crate::util::Result;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataId, NamedData};
use geoengine_datatypes::ml_model::MlModelId;
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

    async fn load_ml_model(&self, model_id: MlModelId) -> Result<String> {
        self.inner.load_ml_model(model_id).await
    }

    async fn store_ml_model_in_db(
        &mut self,
        model_id: MlModelId,
        ml_model_str: String,
    ) -> Result<()> {
        self.inner
            .store_ml_model_in_db(model_id, ml_model_str)
            .await
    }

    async fn resolve_named_data(&self, data: &NamedData) -> Result<DataId> {
        self.inner.resolve_named_data(data).await
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
