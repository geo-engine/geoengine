use geoengine_datatypes::raster::{GeoTransform, GridShape2D, TilingStrategy};

use crate::concurrency::{ThreadPool, ThreadPoolContext};
use std::path::PathBuf;

/// A context that provides certain utility access during operator execution
/// TODO: maybe this is rather a trait
#[derive(Debug, Clone)]
pub struct ExecutionContext<'pool> {
    pub raster_data_root: PathBuf,
    pub thread_pool: ThreadPoolContext<'pool>,
    pub tiling_strategy: TilingStrategy,
}

/// Create a provider for execution contexts in test environments
pub struct MockExecutionContextCreator {
    raster_data_root: PathBuf,
    thread_pool: ThreadPool,
    tiling_strategy: TilingStrategy,
}

impl MockExecutionContextCreator {
    pub fn context(&self) -> ExecutionContext {
        ExecutionContext {
            raster_data_root: self.raster_data_root.clone(),
            thread_pool: self.thread_pool.create_context(),
            tiling_strategy: self.tiling_strategy,
        }
    }
}

impl Default for MockExecutionContextCreator {
    fn default() -> Self {
        Self {
            raster_data_root: PathBuf::default(),
            thread_pool: ThreadPool::new(1),
            tiling_strategy: TilingStrategy::new(
                GridShape2D::new([600, 600]),
                GeoTransform::default(),
            ),
        }
    }
}
