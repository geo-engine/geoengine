use crate::concurrency::{ThreadPool, ThreadPoolContext, ThreadPoolContextCreator};
use crate::engine::{
    QueryRectangle, RasterResultDescriptor, ResultDescriptor, VectorResultDescriptor,
};
use crate::error::Error;
use crate::mock::MockDatasetDataSourceLoadingInfo;
use crate::source::{GdalLoadingInfo, OgrSourceDataset};
use crate::util::Result;
use async_trait::async_trait;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::raster::GridShape;
use geoengine_datatypes::raster::TilingSpecification;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// A context that provides certain utility access during operator initialization
pub trait ExecutionContext:
    Send
    + Sync
    + MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>
    + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor>
    + MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor>
{
    fn thread_pool(&self) -> ThreadPoolContext;
    fn tiling_specification(&self) -> TilingSpecification;
}

#[async_trait]
pub trait MetaDataProvider<L, R>
where
    R: ResultDescriptor,
{
    async fn meta_data(&self, dataset: &DatasetId) -> Result<Box<dyn MetaData<L, R>>>;
}

#[async_trait]
pub trait MetaData<L, R>: Debug + Send + Sync
where
    R: ResultDescriptor,
{
    async fn loading_info(&self, query: QueryRectangle) -> Result<L>;
    async fn result_descriptor(&self) -> Result<R>;

    fn box_clone(&self) -> Box<dyn MetaData<L, R>>;
}

impl<L, R> Clone for Box<dyn MetaData<L, R>>
where
    R: ResultDescriptor,
{
    fn clone(&self) -> Box<dyn MetaData<L, R>> {
        self.box_clone()
    }
}

pub struct MockExecutionContext {
    pub thread_pool: ThreadPoolContext,
    pub meta_data: HashMap<DatasetId, Box<dyn Any + Send + Sync>>,
    pub tiling_specification: TilingSpecification,
}

impl Default for MockExecutionContext {
    fn default() -> Self {
        Self {
            thread_pool: Arc::new(ThreadPool::default()).create_context(),
            meta_data: HashMap::default(),
            tiling_specification: TilingSpecification {
                origin_coordinate: Default::default(),
                tile_size_in_pixels: GridShape {
                    shape_array: [600, 600],
                },
            },
        }
    }
}

impl MockExecutionContext {
    pub fn add_meta_data<L, R>(&mut self, dataset: DatasetId, meta_data: Box<dyn MetaData<L, R>>)
    where
        L: Send + Sync + 'static,
        R: Send + Sync + 'static + ResultDescriptor,
    {
        self.meta_data
            .insert(dataset, Box::new(meta_data) as Box<dyn Any + Send + Sync>);
    }
}

impl ExecutionContext for MockExecutionContext {
    fn thread_pool(&self) -> ThreadPoolContext {
        self.thread_pool.clone()
    }

    fn tiling_specification(&self) -> TilingSpecification {
        self.tiling_specification
    }
}

#[async_trait]
impl<L, R> MetaDataProvider<L, R> for MockExecutionContext
where
    L: 'static,
    R: 'static + ResultDescriptor,
{
    async fn meta_data(&self, dataset: &DatasetId) -> Result<Box<dyn MetaData<L, R>>> {
        let meta_data = self
            .meta_data
            .get(dataset)
            .ok_or(Error::UnknownDatasetId)?
            .downcast_ref::<Box<dyn MetaData<L, R>>>()
            .ok_or(Error::DatasetLoadingInfoProviderMismatch)?;

        Ok(meta_data.clone())
    }
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticMetaData<L, R>
where
    L: Debug + Clone + Send + Sync + 'static,
    R: Debug + Send + Sync + 'static + ResultDescriptor,
{
    pub loading_info: L,
    pub result_descriptor: R,
}

#[async_trait]
impl<L, R> MetaData<L, R> for StaticMetaData<L, R>
where
    L: Debug + Clone + Send + Sync + 'static,
    R: Debug + Send + Sync + 'static + ResultDescriptor,
{
    async fn loading_info(&self, _query: QueryRectangle) -> Result<L> {
        Ok(self.loading_info.clone())
    }

    async fn result_descriptor(&self) -> Result<R> {
        Ok(self.result_descriptor.clone())
    }

    fn box_clone(&self) -> Box<dyn MetaData<L, R>> {
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
        };

        let info: Box<dyn MetaData<i32, VectorResultDescriptor>> = Box::new(info);

        let info2: Box<dyn Any + Send + Sync> = Box::new(info);

        let info3 = info2
            .downcast_ref::<Box<dyn MetaData<i32, VectorResultDescriptor>>>()
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
