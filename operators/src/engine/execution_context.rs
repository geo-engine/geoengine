use geoengine_datatypes::{
    primitives::Coordinate2D,
    raster::{GridShape2D, TilingSpecification},
};

use crate::concurrency::{ThreadPool, ThreadPoolContext};
use std::path::PathBuf;

/// A context that provides certain utility access during operator execution
/// TODO: maybe this is rather a trait
#[derive(Debug, Clone)]
pub struct ExecutionContext<'pool> {
    pub raster_data_root: PathBuf,
    pub thread_pool: ThreadPoolContext<'pool>,
    pub tiling_specification: TilingSpecification,
}

/// Create a provider for execution contexts in test environments
pub struct MockExecutionContextCreator {
    raster_data_root: PathBuf,
    thread_pool: ThreadPool,
    tiling_specification: TilingSpecification,
}

impl MockExecutionContextCreator {
    pub fn context(&self) -> ExecutionContext {
        ExecutionContext {
            raster_data_root: self.raster_data_root.clone(),
            thread_pool: self.thread_pool.create_context(),
            tiling_specification: self.tiling_specification,
        }
    }
}

impl Default for MockExecutionContextCreator {
    fn default() -> Self {
        Self {
            raster_data_root: PathBuf::default(),
            thread_pool: ThreadPool::new(1),
            tiling_specification: TilingSpecification {
                origin_coordinate: Coordinate2D::default(),
                tile_size: GridShape2D::from([600, 600]),
            },
        }
    }
}
