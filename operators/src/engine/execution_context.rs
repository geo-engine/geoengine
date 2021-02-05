use crate::concurrency::{ThreadPool, ThreadPoolContext};
use crate::engine::{QueryRectangle, ResultDescriptor, VectorResultDescriptor};
use crate::error::Error;
use crate::mock::MockDataSetDataSourceLoadingInfo;
use crate::source::OgrSourceDataset;
use crate::util::Result;
use geoengine_datatypes::dataset::DataSetId;
use geoengine_datatypes::raster::GridShape;
use geoengine_datatypes::raster::TilingSpecification;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;

/// A context that provides certain utility access during operator initialization
pub trait ExecutionContext:
    Send
    + Sync
    + MetaDataProvider<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>
    + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor>
{
    fn thread_pool(&self) -> ThreadPoolContext;
    fn raster_data_root(&self) -> Result<PathBuf>; // TODO: remove once GdalSource uses MetaData
    fn tiling_specification(&self) -> TilingSpecification;
}

pub trait MetaDataProvider<L, R>
where
    R: ResultDescriptor,
{
    fn meta_data(&self, data_set: &DataSetId) -> Result<Box<dyn MetaData<L, R>>>;
}

pub trait MetaData<L, R>: Debug + Send + Sync
where
    R: ResultDescriptor,
{
    fn loading_info(&self, query: QueryRectangle) -> Result<L>;
    fn result_descriptor(&self) -> Result<R>;

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
    pub raster_data_root: PathBuf,
    pub thread_pool: ThreadPool,
    pub meta_data: HashMap<DataSetId, Box<dyn Any + Send + Sync>>,
    pub tiling_specification: TilingSpecification,
}

impl Default for MockExecutionContext {
    fn default() -> Self {
        Self {
            raster_data_root: "../operators/test-data/raster".into(),
            thread_pool: ThreadPool::default(),
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
    pub fn add_meta_data<L, R>(&mut self, data_set: DataSetId, meta_data: Box<dyn MetaData<L, R>>)
    where
        L: Send + Sync + 'static,
        R: Send + Sync + 'static + ResultDescriptor,
    {
        self.meta_data
            .insert(data_set, Box::new(meta_data) as Box<dyn Any + Send + Sync>);
    }
}

impl ExecutionContext for MockExecutionContext {
    fn thread_pool(&self) -> ThreadPoolContext {
        self.thread_pool.create_context()
    }

    fn raster_data_root(&self) -> Result<PathBuf> {
        Ok(self.raster_data_root.clone())
    }

    fn tiling_specification(&self) -> TilingSpecification {
        self.tiling_specification
    }
}

impl<L, R> MetaDataProvider<L, R> for MockExecutionContext
where
    L: 'static,
    R: 'static + ResultDescriptor,
{
    fn meta_data(&self, data_set: &DataSetId) -> Result<Box<dyn MetaData<L, R>>> {
        let meta_data = self
            .meta_data
            .get(data_set)
            .ok_or(Error::UnknownDataSetId)?
            .downcast_ref::<Box<dyn MetaData<L, R>>>()
            .ok_or(Error::DataSetLoadingInfoProviderMismatch)?;

        Ok(meta_data.clone())
    }
}

#[derive(Debug, Clone)]
pub struct StaticMetaData<L, R>
where
    L: Debug + Clone + Send + Sync + 'static,
    R: Debug + Send + Sync + 'static + ResultDescriptor,
{
    pub loading_info: L,
    pub result_descriptor: R,
}

impl<L, R> MetaData<L, R> for StaticMetaData<L, R>
where
    L: Debug + Clone + Send + Sync + 'static,
    R: Debug + Send + Sync + 'static + ResultDescriptor,
{
    fn loading_info(&self, _query: QueryRectangle) -> Result<L> {
        Ok(self.loading_info.clone())
    }

    fn result_descriptor(&self) -> Result<R> {
        Ok(self.result_descriptor)
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

    #[test]
    fn test() {
        let info = StaticMetaData {
            loading_info: 1_i32,
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::Data,
                spatial_reference: SpatialReferenceOption::Unreferenced,
            },
        };

        let info: Box<dyn MetaData<i32, VectorResultDescriptor>> = Box::new(info);

        let info2: Box<dyn Any + Send + Sync> = Box::new(info);

        let info3 = info2
            .downcast_ref::<Box<dyn MetaData<i32, VectorResultDescriptor>>>()
            .unwrap();

        assert_eq!(
            info3.result_descriptor().unwrap(),
            VectorResultDescriptor {
                data_type: VectorDataType::Data,
                spatial_reference: SpatialReferenceOption::Unreferenced,
            }
        );
    }
}
