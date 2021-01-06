use crate::concurrency::{ThreadPool, ThreadPoolContext};
use std::path::PathBuf;

/// A context that provides certain utility access during operator execution
/// TODO: maybe this is rather a trait
#[derive(Debug, Clone)]
pub struct ExecutionContext<'pool> {
    pub raster_data_root: PathBuf,
    pub thread_pool: ThreadPoolContext<'pool>,
}

/// Create a provider for execution contexts in test environments
pub struct MockExecutionContextCreator {
    raster_data_root: PathBuf,
    thread_pool: ThreadPool,
}

impl MockExecutionContextCreator {
    pub fn context(&self) -> ExecutionContext {
        ExecutionContext {
            raster_data_root: self.raster_data_root.clone(),
            thread_pool: self.thread_pool.create_context(),
        }
    }
}

impl Default for MockExecutionContextCreator {
    fn default() -> Self {
        Self {
            raster_data_root: PathBuf::default(),
            thread_pool: ThreadPool::new(1),
        }
    }
}
