mod error;
mod in_memory;

use crate::error::Result;
pub use error::TaskError;
use geoengine_datatypes::{error::ErrorSource, identifier};
pub use in_memory::{InMemoryTaskDb, InMemoryTaskDbContext};
use serde::{Deserialize, Serialize, Serializer};
use std::{fmt, sync::Arc};

/// A database that allows registering and retrieving tasks.
#[async_trait::async_trait]
pub trait TaskDb<C: TaskContext>: Send + Sync {
    async fn register(&self, task: Box<dyn Task<C>>) -> Result<TaskId, TaskError>;

    async fn status(&self, task_id: TaskId) -> Result<TaskStatus, TaskError>;

    /// TODO: pagination
    async fn list(&self, filter: Option<TaskFilter>) -> Result<Vec<TaskStatus>, TaskError>;
}

identifier!(TaskId);

/// A task that can run asynchronously and reports its status.
#[async_trait::async_trait]
pub trait Task<C: TaskContext>: Send + Sync {
    async fn run(self: Box<Self>, ctx: C) -> Result<Box<dyn TaskStatusInfo>, Box<dyn ErrorSource>>;

    fn boxed(self) -> Box<dyn Task<C>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

/// A way to supply status updates from a `Task` to a `TaskDb`.
#[async_trait::async_trait]
pub trait TaskContext: Send + Sync {
    /// Set the completion percentage (%) of the task.
    /// This is a number between 0 and 100.
    ///
    /// Moreover, set a status message.
    ///
    async fn set_completion(&self, pct_complete: u8, status: Box<dyn TaskStatusInfo>);
}

/// One of the statuses a `Task` can be in.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum TaskStatus {
    // Pending, // TODO: at some point, don't just run things
    Running(Arc<RunningTaskStatusInfo>),
    Completed {
        info: Arc<Box<dyn TaskStatusInfo>>,
    },
    #[serde(serialize_with = "serialize_failed_task_status")]
    Failed {
        error: Arc<Box<dyn ErrorSource>>,
    },
}

impl TaskStatus {
    pub fn completed(info: Arc<Box<dyn TaskStatusInfo>>) -> Self {
        TaskStatus::Completed { info }
    }

    pub fn failed(error: Arc<Box<dyn ErrorSource>>) -> Self {
        TaskStatus::Failed { error }
    }
}

#[derive(Debug, Serialize)]
pub struct RunningTaskStatusInfo {
    pct_complete: u8,
    info: Box<dyn TaskStatusInfo>,
}

impl RunningTaskStatusInfo {
    pub fn new(pct_complete: u8, info: Box<dyn TaskStatusInfo>) -> Arc<Self> {
        Arc::new(RunningTaskStatusInfo { pct_complete, info })
    }
}

#[allow(clippy::borrowed_box)]
fn serialize_failed_task_status<S>(
    error: &Box<dyn ErrorSource>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(error.to_string().as_str())
}

/// Trait for information about the status of a task.
pub trait TaskStatusInfo: erased_serde::Serialize + Send + Sync + fmt::Debug {
    fn boxed(self) -> Box<dyn TaskStatusInfo>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

erased_serde::serialize_trait_object!(TaskStatusInfo);

impl TaskStatusInfo for () {}
impl TaskStatusInfo for String {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskListOptions {
    #[serde(default)]
    pub filter: Option<TaskFilter>,
    pub offset: u32,
    pub limit: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TaskFilter {
    Running,
    Failed,
    Completed,
}
