use crate::concurrency::ThreadPool;
use crate::engine::{QueryRectangle, VectorResultDescriptor};
use crate::error::Error;
use crate::mock::MockDataSetDataSourceLoadingInfo;
use crate::source::OgrSourceDataset;
use crate::util::Result;
use geoengine_datatypes::dataset::DataSetId;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;

/// A context that provides certain utility access during operator initialization
pub trait ExecutionContext:
    Send
    + Sync
    + LoadingInfoProvider<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>
    + LoadingInfoProvider<OgrSourceDataset, VectorResultDescriptor>
{
    fn thread_pool(&self) -> &ThreadPool;
    fn raster_data_root(&self) -> Result<PathBuf>; // TODO: remove once GdalSource uses LoadingInfo
}

pub trait LoadingInfoProvider<T, U> {
    fn loading_info(&self, data_set: &DataSetId) -> Result<Box<dyn LoadingInfo<T, U>>>;
}

pub trait LoadingInfo<T, U>: Debug + Send + Sync {
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
    pub loading_info: HashMap<DataSetId, Box<dyn Any + Send + Sync>>,
}

impl MockExecutionContext {
    pub fn add_loading_info<T, U>(
        &mut self,
        data_set: DataSetId,
        loading_info: Box<dyn LoadingInfo<T, U>>,
    ) where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        self.loading_info.insert(
            data_set,
            Box::new(loading_info) as Box<dyn Any + Send + Sync>,
        );
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

impl<T, U> LoadingInfoProvider<T, U> for MockExecutionContext
where
    U: 'static,
    T: 'static,
{
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

#[derive(Debug, Clone)]
pub struct MockLoadingInfo<T, U>
where
    T: Debug + Clone + Send + Sync + 'static,
    U: Debug + Clone + Send + Sync + 'static,
{
    pub(crate) info: T,
    pub(crate) meta: U,
}

impl<T, U> LoadingInfo<T, U> for MockLoadingInfo<T, U>
where
    T: Debug + Clone + Send + Sync + 'static,
    U: Debug + Clone + Send + Sync + 'static,
{
    fn get(&self, _query: QueryRectangle) -> Result<T, Error> {
        Ok(self.info.clone())
    }

    fn meta(&self) -> Result<U, Error> {
        Ok(self.meta.clone())
    }

    fn box_clone(&self) -> Box<dyn LoadingInfo<T, U>> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let info = MockLoadingInfo {
            info: 1_i32,
            meta: 2_i32,
        };

        let info: Box<dyn LoadingInfo<i32, i32>> = Box::new(info);

        let info2: Box<dyn Any + Send + Sync> = Box::new(info);

        let info3 = info2
            .downcast_ref::<Box<dyn LoadingInfo<i32, i32>>>()
            .unwrap();

        assert_eq!(info3.meta().unwrap(), 2_i32);
    }
}
