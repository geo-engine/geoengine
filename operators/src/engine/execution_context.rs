use super::query::QueryAbortRegistration;
use super::{
    CreateSpan, InitializedPlotOperator, InitializedRasterOperator, InitializedVectorOperator,
    MockQueryContext,
};
use crate::engine::{
    ChunkByteSize, RasterResultDescriptor, ResultDescriptor, VectorResultDescriptor,
};
use crate::error::Error;
use crate::mock::MockDatasetDataSourceLoadingInfo;
use crate::source::{GdalLoadingInfo, OgrSourceDataset};
use crate::util::{create_rayon_thread_pool, Result};
use async_trait::async_trait;
use geoengine_datatypes::dataset::DataId;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::util::test::TestDefault;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::PathBuf;
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

    async fn read_ml_model(&self, path: PathBuf) -> Result<String>;

    async fn write_ml_model(&mut self, path: PathBuf, ml_model_str: String) -> Result<()>;
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
    pub tiling_specification: TilingSpecification,
    pub ml_models: HashMap<PathBuf, String>,
}

impl TestDefault for MockExecutionContext {
    fn test_default() -> Self {
        Self {
            thread_pool: create_rayon_thread_pool(0),
            meta_data: HashMap::default(),
            tiling_specification: TilingSpecification::test_default(),
            ml_models: HashMap::default(),
        }
    }
}

impl MockExecutionContext {
    pub fn new_with_tiling_spec(tiling_specification: TilingSpecification) -> Self {
        Self {
            thread_pool: create_rayon_thread_pool(0),
            meta_data: HashMap::default(),
            tiling_specification,
            ml_models: HashMap::default(),
        }
    }

    pub fn new_with_tiling_spec_and_thread_count(
        tiling_specification: TilingSpecification,
        num_threads: usize,
    ) -> Self {
        Self {
            thread_pool: create_rayon_thread_pool(num_threads),
            meta_data: HashMap::default(),
            tiling_specification,
            ml_models: HashMap::default(),
        }
    }

    pub fn add_meta_data<L, R, Q>(&mut self, data: DataId, meta_data: Box<dyn MetaData<L, R, Q>>)
    where
        L: Send + Sync + 'static,
        R: Send + Sync + 'static + ResultDescriptor,
        Q: Send + Sync + 'static,
    {
        self.meta_data
            .insert(data, Box::new(meta_data) as Box<dyn Any + Send + Sync>);
    }

    pub fn mock_query_context(&self, chunk_byte_size: ChunkByteSize) -> MockQueryContext {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        MockQueryContext {
            chunk_byte_size,
            thread_pool: self.thread_pool.clone(),
            extensions: Default::default(),
            abort_registration,
            abort_trigger: Some(abort_trigger),
        }
    }

    pub fn initialize_ml_model(&mut self, model_path: PathBuf) -> Result<()> {
        let model = std::fs::read_to_string(&model_path)?;

        self.ml_models.insert(model_path, model);

        Ok(())
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

    async fn read_ml_model(&self, path: PathBuf) -> Result<String> {
        let res = self
            .ml_models
            .get(&path)
            .ok_or(Error::MachineLearningModelNotFound)?
            .clone();

        Ok(res)
    }

    async fn write_ml_model(&mut self, path: PathBuf, ml_model_str: String) -> Result<()> {
        self.ml_models.insert(path, ml_model_str);

        Ok(())
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
