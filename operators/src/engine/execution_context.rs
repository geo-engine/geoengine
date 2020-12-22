use crate::concurrency::ThreadPool;
use crate::engine::{QueryRectangle, VectorResultDescriptor};
use crate::error::Error;
use crate::mock::MockDataSetDataSourceLoadingInfo;
use crate::util::Result;
use geoengine_datatypes::dataset::DataSetId;
use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;

/// A context that provides certain utility access during operator initialization
pub trait ExecutionContext:
    Send + Sync + LoadingInfoProvider<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>
{
    fn thread_pool(&self) -> &ThreadPool;
    fn raster_data_root(&self) -> Result<PathBuf>; // TODO: remove once GdalSource uses LoadingInfo
}

pub trait LoadingInfoProvider<T, U> {
    fn loading_info(&self, data_set: &DataSetId) -> Result<Box<dyn LoadingInfo<T, U>>>;
}

pub trait LoadingInfo<T, U>: Send + Sync {
    fn get(&self, query: QueryRectangle) -> Result<T>;
    fn meta(&self) -> Result<U>;

    fn box_clone(&self) -> Box<dyn LoadingInfo<T, U>>;
}

impl<T, U> Clone for Box<dyn LoadingInfo<T, U>> {
    fn clone(&self) -> Box<dyn LoadingInfo<T, U>> {
        self.box_clone()
    }
}

#[derive(Default)]
pub struct MockExecutionContext {
    pub thread_pool: ThreadPool,
    pub loading_info: HashMap<DataSetId, Box<dyn MockLoadingInfo>>,
}

impl MockExecutionContext {
    pub(crate) fn add_loading_info<T: MockLoadingInfo>(
        &mut self,
        data_set: DataSetId,
        loading_info: T,
    ) {
        self.loading_info
            .insert(data_set, Box::new(loading_info) as Box<dyn MockLoadingInfo>);
    }
}

impl ExecutionContext for MockExecutionContext {
    fn thread_pool(&self) -> &ThreadPool {
        &self.thread_pool
    }

    fn raster_data_root(&self) -> Result<PathBuf> {
        todo!()
    }
}

impl<T, U> LoadingInfoProvider<T, U> for MockExecutionContext {
    fn loading_info(&self, data_set: &DataSetId) -> Result<Box<dyn LoadingInfo<T, U>>> {
        let loading_info = self
            .loading_info
            .get(data_set)
            .ok_or(Error::UnknownDataSetId)?
            .downcast_ref::<Box<dyn LoadingInfo<T, U>>>()
            .ok_or(Error::DataSetLoadingInfoProviderMismatch)?;

        Ok(loading_info.clone())
    }
}
