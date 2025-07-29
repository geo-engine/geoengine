use super::{
    CreateSpan, InitializedPlotOperator, InitializedRasterOperator, InitializedVectorOperator,
    MockQueryContext,
};
use crate::cache::shared_cache::SharedCache;
use crate::engine::{
    ChunkByteSize, RasterResultDescriptor, ResultDescriptor, VectorResultDescriptor,
};
use crate::error::Error;
use crate::machine_learning::MlModelLoadingInfo;
use crate::meta::quota::{QuotaChecker, QuotaTracking};
use crate::meta::wrapper::InitializedOperatorWrapper;
use crate::mock::MockDatasetDataSourceLoadingInfo;
use crate::source::{GdalLoadingInfo, OgrSourceDataset};
use crate::util::{Result, create_rayon_thread_pool};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataId, NamedData};
use geoengine_datatypes::machine_learning::MlModelName;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::util::test::TestDefault;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

/// A context that provides certain utility access during operator initialization
#[async_trait::async_trait]
pub trait ExecutionContext: Send
    + Sync
    + MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    + MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
{
    fn thread_pool(&self) -> &Arc<ThreadPool>;
    fn tiling_specification(&self) -> TilingSpecification;

    fn wrap_initialized_raster_operator(
        &self,
        op: Box<dyn InitializedRasterOperator>,
        span: CreateSpan,
    ) -> Box<dyn InitializedRasterOperator>;

    fn wrap_initialized_vector_operator(
        &self,
        op: Box<dyn InitializedVectorOperator>,
        span: CreateSpan,
    ) -> Box<dyn InitializedVectorOperator>;

    fn wrap_initialized_plot_operator(
        &self,
        op: Box<dyn InitializedPlotOperator>,
        span: CreateSpan,
    ) -> Box<dyn InitializedPlotOperator>;

    async fn resolve_named_data(&self, data: &NamedData) -> Result<DataId>;

    async fn ml_model_loading_info(&self, name: &MlModelName) -> Result<MlModelLoadingInfo>;
}

#[async_trait]
pub trait MetaDataProvider<L, R, Q>
where
    R: ResultDescriptor,
{
    async fn meta_data(&self, id: &DataId) -> Result<Box<dyn MetaData<L, R, Q>>>;
}

#[async_trait]
pub trait MetaData<L, R, Q>: Debug + Send + Sync
where
    R: ResultDescriptor,
{
    async fn loading_info(&self, query: Q) -> Result<L>;
    async fn result_descriptor(&self) -> Result<R>;

    fn box_clone(&self) -> Box<dyn MetaData<L, R, Q>>;
}

impl<L, R, Q> Clone for Box<dyn MetaData<L, R, Q>>
where
    R: ResultDescriptor,
{
    fn clone(&self) -> Box<dyn MetaData<L, R, Q>> {
        self.box_clone()
    }
}

pub struct MockExecutionContext {
    pub thread_pool: Arc<ThreadPool>,
    pub meta_data: HashMap<DataId, Box<dyn Any + Send + Sync>>,
    pub named_data: HashMap<NamedData, DataId>,
    pub ml_models: HashMap<MlModelName, MlModelLoadingInfo>,
    pub tiling_specification: TilingSpecification,
}

impl TestDefault for MockExecutionContext {
    fn test_default() -> Self {
        Self {
            thread_pool: create_rayon_thread_pool(0),
            meta_data: HashMap::default(),
            named_data: HashMap::default(),
            ml_models: HashMap::default(),
            tiling_specification: TilingSpecification::test_default(),
        }
    }
}

impl MockExecutionContext {
    pub fn new_with_tiling_spec(tiling_specification: TilingSpecification) -> Self {
        Self {
            thread_pool: create_rayon_thread_pool(0),
            meta_data: HashMap::default(),
            named_data: HashMap::default(),
            ml_models: HashMap::default(),
            tiling_specification,
        }
    }

    pub fn new_with_tiling_spec_and_thread_count(
        tiling_specification: TilingSpecification,
        num_threads: usize,
    ) -> Self {
        Self {
            thread_pool: create_rayon_thread_pool(num_threads),
            meta_data: HashMap::default(),
            named_data: HashMap::default(),
            ml_models: HashMap::default(),
            tiling_specification,
        }
    }

    pub fn add_meta_data<L, R, Q>(
        &mut self,
        data: DataId,
        named_data: NamedData,
        meta_data: Box<dyn MetaData<L, R, Q>>,
    ) where
        L: Send + Sync + 'static,
        R: Send + Sync + 'static + ResultDescriptor,
        Q: Send + Sync + 'static,
    {
        self.meta_data.insert(
            data.clone(),
            Box::new(meta_data) as Box<dyn Any + Send + Sync>,
        );

        self.named_data.insert(named_data, data);
    }

    pub fn delete_meta_data(&mut self, named_data: &NamedData) {
        let data = self.named_data.remove(named_data);
        if let Some(data) = data {
            self.meta_data.remove(&data);
        }
    }

    pub fn mock_query_context_test_default(&self) -> MockQueryContext {
        MockQueryContext::new(ChunkByteSize::test_default(), self.tiling_specification)
    }

    pub fn mock_query_context(&self, chunk_byte_size: ChunkByteSize) -> MockQueryContext {
        MockQueryContext::new(chunk_byte_size, self.tiling_specification)
    }

    pub fn mock_query_context_with_query_extensions(
        &self,
        chunk_byte_size: ChunkByteSize,
        cache: Option<Arc<SharedCache>>,
        quota_tracking: Option<QuotaTracking>,
        quota_checker: Option<QuotaChecker>,
    ) -> MockQueryContext {
        MockQueryContext::new_with_query_extensions(
            chunk_byte_size,
            self.tiling_specification,
            cache,
            quota_tracking,
            quota_checker,
        )
    }

    pub fn mock_query_context_with_chunk_size_and_thread_count(
        &self,
        chunk_byte_size: ChunkByteSize,
        num_threads: usize,
    ) -> MockQueryContext {
        MockQueryContext::with_chunk_size_and_thread_count(
            chunk_byte_size,
            self.tiling_specification,
            num_threads,
        )
    }
}

#[async_trait::async_trait]
impl ExecutionContext for MockExecutionContext {
    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.thread_pool
    }

    fn tiling_specification(&self) -> TilingSpecification {
        self.tiling_specification
    }

    fn wrap_initialized_raster_operator(
        &self,
        op: Box<dyn InitializedRasterOperator>,
        _span: CreateSpan,
    ) -> Box<dyn InitializedRasterOperator> {
        op
    }

    fn wrap_initialized_vector_operator(
        &self,
        op: Box<dyn InitializedVectorOperator>,
        _span: CreateSpan,
    ) -> Box<dyn InitializedVectorOperator> {
        op
    }

    fn wrap_initialized_plot_operator(
        &self,
        op: Box<dyn InitializedPlotOperator>,
        _span: CreateSpan,
    ) -> Box<dyn InitializedPlotOperator> {
        op
    }

    async fn resolve_named_data(&self, data: &NamedData) -> Result<DataId> {
        self.named_data
            .get(data)
            .cloned()
            .ok_or_else(|| Error::UnknownDatasetName { name: data.clone() })
    }

    async fn ml_model_loading_info(&self, name: &MlModelName) -> Result<MlModelLoadingInfo> {
        self.ml_models
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnknownMlModelName { name: name.clone() })
    }
}

#[async_trait]
impl<L, R, Q> MetaDataProvider<L, R, Q> for MockExecutionContext
where
    L: 'static,
    R: 'static + ResultDescriptor,
    Q: 'static,
{
    async fn meta_data(&self, id: &DataId) -> Result<Box<dyn MetaData<L, R, Q>>> {
        let meta_data = self
            .meta_data
            .get(id)
            .ok_or(Error::UnknownDataId)?
            .downcast_ref::<Box<dyn MetaData<L, R, Q>>>()
            .ok_or(Error::InvalidMetaDataType)?;

        Ok(meta_data.clone())
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticMetaData<L, R, Q>
where
    L: Debug + Clone + Send + Sync + 'static,
    R: Debug + Send + Sync + 'static + ResultDescriptor,
    Q: Debug + Clone + Send + Sync + 'static,
{
    pub loading_info: L,
    pub result_descriptor: R,
    #[serde(skip)]
    pub phantom: PhantomData<Q>,
}

#[async_trait]
impl<L, R, Q> MetaData<L, R, Q> for StaticMetaData<L, R, Q>
where
    L: Debug + Clone + Send + Sync + 'static,
    R: Debug + Send + Sync + 'static + ResultDescriptor,
    Q: Debug + Clone + Send + Sync + 'static,
{
    async fn loading_info(&self, _query: Q) -> Result<L> {
        Ok(self.loading_info.clone())
    }

    async fn result_descriptor(&self) -> Result<R> {
        Ok(self.result_descriptor.clone())
    }

    fn box_clone(&self) -> Box<dyn MetaData<L, R, Q>> {
        Box::new(self.clone())
    }
}

mod db_types {
    use geoengine_datatypes::delegate_from_to_sql;
    use postgres_types::{FromSql, ToSql};

    use super::*;

    pub type MockMetaData = StaticMetaData<
        MockDatasetDataSourceLoadingInfo,
        VectorResultDescriptor,
        VectorQueryRectangle,
    >;

    #[derive(Debug, ToSql, FromSql)]
    #[postgres(name = "MockMetaData")]
    pub struct MockMetaDataDbType {
        pub loading_info: MockDatasetDataSourceLoadingInfo,
        pub result_descriptor: VectorResultDescriptor,
    }

    impl From<&MockMetaData> for MockMetaDataDbType {
        fn from(other: &MockMetaData) -> Self {
            Self {
                loading_info: other.loading_info.clone(),
                result_descriptor: other.result_descriptor.clone(),
            }
        }
    }

    impl TryFrom<MockMetaDataDbType> for MockMetaData {
        type Error = Error;

        fn try_from(other: MockMetaDataDbType) -> Result<Self, Self::Error> {
            Ok(Self {
                loading_info: other.loading_info,
                result_descriptor: other.result_descriptor,
                phantom: PhantomData,
            })
        }
    }

    pub type OgrMetaData =
        StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>;

    #[derive(Debug, ToSql, FromSql)]
    #[postgres(name = "OgrMetaData")]
    pub struct OgrMetaDataDbType {
        pub loading_info: OgrSourceDataset,
        pub result_descriptor: VectorResultDescriptor,
    }

    impl From<&StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>
        for OgrMetaDataDbType
    {
        fn from(other: &OgrMetaData) -> Self {
            Self {
                loading_info: other.loading_info.clone(),
                result_descriptor: other.result_descriptor.clone(),
            }
        }
    }

    impl TryFrom<OgrMetaDataDbType> for OgrMetaData {
        type Error = Error;

        fn try_from(other: OgrMetaDataDbType) -> Result<Self, Self::Error> {
            Ok(Self {
                loading_info: other.loading_info,
                result_descriptor: other.result_descriptor,
                phantom: PhantomData,
            })
        }
    }

    delegate_from_to_sql!(MockMetaData, MockMetaDataDbType);
    delegate_from_to_sql!(OgrMetaData, OgrMetaDataDbType);
}

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
        cache: Option<Arc<SharedCache>>,
        quota_tracking: Option<QuotaTracking>,
        quota_checker: Option<QuotaChecker>,
    ) -> MockQueryContext {
        self.inner.mock_query_context_with_query_extensions(
            chunk_byte_size,
            cache,
            quota_tracking,
            quota_checker,
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
    ) -> Box<dyn InitializedRasterOperator> {
        InitializedOperatorWrapper::new(op, span).boxed()
    }

    fn wrap_initialized_vector_operator(
        &self,
        op: Box<dyn InitializedVectorOperator>,
        span: CreateSpan,
    ) -> Box<dyn InitializedVectorOperator> {
        InitializedOperatorWrapper::new(op, span).boxed()
    }

    fn wrap_initialized_plot_operator(
        &self,
        op: Box<dyn InitializedPlotOperator>,
        _span: CreateSpan,
    ) -> Box<dyn InitializedPlotOperator> {
        op
    }

    async fn resolve_named_data(&self, data: &NamedData) -> Result<DataId> {
        self.inner.resolve_named_data(data).await
    }

    async fn ml_model_loading_info(&self, name: &MlModelName) -> Result<MlModelLoadingInfo> {
        self.inner.ml_model_loading_info(name).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;

    #[tokio::test]
    async fn test() {
        let info = StaticMetaData {
            loading_info: 1_i32,
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::Data,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: Default::default(),
                time: None,
                bbox: None,
            },
            phantom: Default::default(),
        };

        let info: Box<dyn MetaData<i32, VectorResultDescriptor, VectorQueryRectangle>> =
            Box::new(info);

        let info2: Box<dyn Any + Send + Sync> = Box::new(info);

        let info3 = info2
            .downcast_ref::<Box<dyn MetaData<i32, VectorResultDescriptor, VectorQueryRectangle>>>()
            .unwrap();

        assert_eq!(
            info3.result_descriptor().await.unwrap(),
            VectorResultDescriptor {
                data_type: VectorDataType::Data,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: Default::default(),
                time: None,
                bbox: None,
            }
        );
    }
}
