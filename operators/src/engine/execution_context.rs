use super::MockQueryContext;
use crate::engine::{
    ChunkByteSize, RasterResultDescriptor, ResultDescriptor, VectorResultDescriptor,
};
use crate::error::Error;
use crate::mock::MockDatasetDataSourceLoadingInfo;
use crate::source::{GdalLoadingInfo, GdalMetaDataStatic, OgrSourceDataset};
use crate::util::{create_rayon_thread_pool, Result};
use async_trait::async_trait;
use geoengine_datatypes::dataset::DatasetId;
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
#[async_trait]
pub trait ExecutionContext: Send + Sync {
    fn thread_pool(&self) -> &Arc<ThreadPool>;
    fn tiling_specification(&self) -> TilingSpecification;
    async fn meta_data(&self, dataset: &DatasetId) -> Result<MetaDataLookupResult>; // TODO: separate trait?
}

#[derive(Clone)]
pub enum MetaDataLookupResult {
    Mock(
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
    ),
    Ogr(Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>),
    Gdal(Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>),
}

impl MetaDataLookupResult {
    pub fn mock_meta_data(
        &self,
    ) -> Result<
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
    > {
        match self {
            MetaDataLookupResult::Mock(meta_data) => Ok(meta_data.clone()),
            _ => Err(Error::MetaDataMissmatch),
        }
    }

    pub fn ogr_meta_data(
        &self,
    ) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>>
    {
        match self {
            MetaDataLookupResult::Ogr(meta_data) => Ok(meta_data.clone()),
            _ => Err(Error::MetaDataMissmatch),
        }
    }

    pub fn gdal_meta_data(
        &self,
    ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
    {
        match self {
            MetaDataLookupResult::Gdal(meta_data) => Ok(meta_data.clone()),
            _ => Err(Error::MetaDataMissmatch),
        }
    }
}

impl From<GdalMetaDataStatic> for MetaDataLookupResult {
    fn from(meta_data: GdalMetaDataStatic) -> Self {
        MetaDataLookupResult::Gdal(Box::new(meta_data))
    }
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
    pub meta_data: HashMap<DatasetId, MetaDataLookupResult>,
    pub tiling_specification: TilingSpecification,
}

impl TestDefault for MockExecutionContext {
    fn test_default() -> Self {
        Self {
            thread_pool: create_rayon_thread_pool(0),
            meta_data: HashMap::default(),
            tiling_specification: TilingSpecification::test_default(),
        }
    }
}

impl MockExecutionContext {
    pub fn new_with_tiling_spec(tiling_specification: TilingSpecification) -> Self {
        Self {
            thread_pool: create_rayon_thread_pool(0),
            meta_data: HashMap::default(),
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
            tiling_specification,
        }
    }

    pub fn add_meta_data(&mut self, dataset: DatasetId, meta_data: MetaDataLookupResult) {
        self.meta_data.insert(dataset, meta_data);
    }

    pub fn mock_query_context(&self, chunk_byte_size: ChunkByteSize) -> MockQueryContext {
        MockQueryContext {
            chunk_byte_size,
            thread_pool: self.thread_pool.clone(),
        }
    }
}

#[async_trait]
impl ExecutionContext for MockExecutionContext {
    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.thread_pool
    }

    fn tiling_specification(&self) -> TilingSpecification {
        self.tiling_specification
    }

    async fn meta_data(&self, dataset: &DatasetId) -> Result<MetaDataLookupResult> {
        self.meta_data
            .get(dataset)
            .cloned()
            .ok_or(Error::UnknownDatasetId)
    }
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
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
            }
        );
    }
}
