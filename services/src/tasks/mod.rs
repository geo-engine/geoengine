mod error;
mod in_memory;
mod time_estimation;
pub mod util;

use self::time_estimation::TimeEstimation;
use crate::identifier;
use crate::{
    error::Result,
    util::{
        config::get_config_element,
        user_input::{UserInput, Validated},
    },
};
pub use error::TaskError;
use futures::channel::oneshot;
use geoengine_datatypes::primitives::DateTime;
use geoengine_datatypes::{error::ErrorSource, util::AsAnyArc};
pub use in_memory::{SimpleTaskManager, SimpleTaskManagerBackend, SimpleTaskManagerContext};
use serde::{Deserialize, Serialize, Serializer};
use snafu::ensure;
use std::{fmt, sync::Arc};
use utoipa::{IntoParams, ToSchema};

/// A database that allows scheduling and retrieving tasks.
#[async_trait::async_trait]
pub trait TaskManager<C: TaskContext>: Send + Sync {
    #[must_use]
    async fn schedule_task(
        &self,
        task: Box<dyn Task<C>>,
        notify: Option<oneshot::Sender<TaskStatus>>,
    ) -> Result<TaskId, TaskError>;

    #[must_use]
    async fn get_task_status(&self, task_id: TaskId) -> Result<TaskStatus, TaskError>;

    #[must_use]
    async fn list_tasks(
        &self,
        options: Validated<TaskListOptions>,
    ) -> Result<Vec<TaskStatusWithId>, TaskError>;

    /// Abort a running task.
    ///
    /// # Parameters
    ///  - `force`: If `true`, the task will be aborted without calling clean-up functions.
    ///
    async fn abort_tasks(&self, task_id: TaskId, force: bool) -> Result<(), TaskError>;
}

identifier!(TaskId);

/// A task that can run asynchronously and reports its status.
#[async_trait::async_trait]
pub trait Task<C: TaskContext>: Send + Sync {
    async fn run(&self, ctx: C) -> Result<Box<dyn TaskStatusInfo>, Box<dyn ErrorSource>>;

    /// Clean-up the task on error or abortion
    async fn cleanup_on_error(&self, ctx: C) -> Result<(), Box<dyn ErrorSource>>;

    fn boxed(self) -> Box<dyn Task<C>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }

    fn task_type(&self) -> &'static str;

    fn task_unique_id(&self) -> Option<String> {
        None
    }

    /// Return subtasks of this tasks.
    ///
    /// For instance, they will get aborted when this tasks gets aborted.
    ///
    async fn subtasks(&self) -> Vec<TaskId> {
        Default::default()
    }
}

/// A way to supply status updates from a [`Task`] to a [`TaskManager`].
#[async_trait::async_trait]
pub trait TaskContext: Send + Sync {
    /// Set the completion percentage (%) of the task.
    /// This is a number between `0.0` and `1.0`.
    ///
    /// Moreover, set a status message.
    ///
    async fn set_completion(&self, pct_complete: f64, status: Box<dyn TaskStatusInfo>);
}

/// One of the statuses a `Task` can be in.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum TaskStatus {
    // Pending, // TODO: at some point, don't just run things
    Running(Arc<RunningTaskStatusInfo>),
    #[serde(rename_all = "camelCase")]
    Completed {
        info: Arc<dyn TaskStatusInfo>,
        time_total: String,
        time_started: DateTime,
    },
    #[serde(rename_all = "camelCase")]
    Aborted {
        clean_up: TaskCleanUpStatus,
    },
    #[serde(rename_all = "camelCase")]
    Failed {
        #[serde(serialize_with = "serialize_failed_task_status")]
        error: Arc<dyn ErrorSource>,
        clean_up: TaskCleanUpStatus,
    },
}

// TODO: replace TaskStatus with a more API friendly type
impl<'a> ToSchema<'a> for TaskStatus {
    fn schema() -> (&'a str, utoipa::openapi::RefOr<utoipa::openapi::Schema>) {
        use utoipa::openapi::*;
        (
            "TaskStatus",
            OneOfBuilder::new()
                .item(
                    ObjectBuilder::new()
                        .property(
                            "status",
                            ObjectBuilder::new()
                                .schema_type(SchemaType::String)
                                .enum_values::<[&str; 1], &str>(Some(["running"])),
                        )
                        .property("info", Object::new())
                        .property("pctComplete", Object::with_type(SchemaType::String))
                        .property(
                            "estimatedTimeRemaining",
                            Object::with_type(SchemaType::String),
                        )
                        .property("timeStarted", Object::with_type(SchemaType::String)),
                )
                .item(
                    ObjectBuilder::new()
                        .property(
                            "status",
                            ObjectBuilder::new()
                                .schema_type(SchemaType::String)
                                .enum_values::<[&str; 1], &str>(Some(["completed"])),
                        )
                        .property("info", Object::new())
                        .property("timeTotal", Object::with_type(SchemaType::String))
                        .property("timeStarted", Object::with_type(SchemaType::String)),
                )
                .item(
                    ObjectBuilder::new()
                        .property(
                            "status",
                            ObjectBuilder::new()
                                .schema_type(SchemaType::String)
                                .enum_values::<[&str; 1], &str>(Some(["aborted"])),
                        )
                        .property("cleanUp", Object::new()),
                )
                .item(
                    ObjectBuilder::new()
                        .property(
                            "status",
                            ObjectBuilder::new()
                                .schema_type(SchemaType::String)
                                .enum_values::<[&str; 1], &str>(Some(["failed"])),
                        )
                        .property("error", Object::new())
                        .property("cleanUp", Object::new()),
                )
                .into(),
        )
    }
}

/// One of the statuses a `Task` clean-up can be in.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum TaskCleanUpStatus {
    NoCleanUp,
    Running(Arc<RunningTaskStatusInfo>),
    #[serde(rename_all = "camelCase")]
    Completed {
        info: Arc<Box<dyn TaskStatusInfo>>,
    },
    #[serde(rename_all = "camelCase")]
    Aborted {
        info: Arc<Box<dyn TaskStatusInfo>>,
    },
    #[serde(rename_all = "camelCase")]
    Failed {
        #[serde(serialize_with = "serialize_failed_task_status")]
        error: Arc<dyn ErrorSource>,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStatusWithId {
    pub task_id: TaskId,
    #[serde(flatten)]
    pub status: TaskStatus,
}

impl TaskStatus {
    #[must_use]
    pub fn completed(&self, info: Arc<dyn TaskStatusInfo>) -> Self {
        Self::Completed {
            info,
            time_total: self.time_total(),
            time_started: self
                .time_started()
                .expect("completed() is only called on tasks in the running state"),
        }
    }

    pub fn aborted(clean_up: TaskCleanUpStatus) -> Self {
        Self::Aborted { clean_up }
    }

    pub fn failed(error: Arc<dyn ErrorSource>, clean_up: TaskCleanUpStatus) -> Self {
        Self::Failed { error, clean_up }
    }

    pub fn is_running(&self) -> bool {
        matches!(self, TaskStatus::Running(_))
    }

    pub fn has_aborted(&self) -> bool {
        matches!(self, TaskStatus::Aborted { .. })
    }

    pub fn has_failed(&self) -> bool {
        matches!(self, TaskStatus::Failed { .. })
    }

    pub fn is_finished(&self) -> bool {
        matches!(self, TaskStatus::Completed { .. })
            || matches!(
                self,
                TaskStatus::Aborted {
                    clean_up: TaskCleanUpStatus::NoCleanUp
                        | TaskCleanUpStatus::Completed { .. }
                        | TaskCleanUpStatus::Failed { .. }
                        | TaskCleanUpStatus::Aborted { .. }
                }
            )
            || matches!(
                self,
                TaskStatus::Failed {
                    error: _,
                    clean_up: TaskCleanUpStatus::NoCleanUp
                        | TaskCleanUpStatus::Completed { .. }
                        | TaskCleanUpStatus::Failed { .. }
                        | TaskCleanUpStatus::Aborted { .. }
                }
            )
    }

    fn time_total(&self) -> String {
        match self {
            TaskStatus::Completed { time_total, .. } => time_total.clone(),
            TaskStatus::Running(info) => info.estimated_time_remaining.time_total(),
            _ => String::new(),
        }
    }

    fn time_started(&self) -> Option<DateTime> {
        match self {
            TaskStatus::Completed { time_started, .. } => Some(*time_started),
            TaskStatus::Running(info) => Some(info.estimated_time_remaining.time_started()),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunningTaskStatusInfo {
    #[serde(serialize_with = "serialize_as_pct")]
    pct_complete: f64,
    time_started: DateTime,
    estimated_time_remaining: TimeEstimation,
    info: Box<dyn TaskStatusInfo>,
}

impl RunningTaskStatusInfo {
    pub fn new(pct_complete: f64, info: Box<dyn TaskStatusInfo>) -> Arc<Self> {
        let time_estimate = TimeEstimation::new();
        Arc::new(RunningTaskStatusInfo {
            pct_complete: pct_complete.clamp(0., 1.),
            time_started: time_estimate.time_started(),
            estimated_time_remaining: time_estimate,
            info,
        })
    }

    pub fn update(&self, pct_complete: f64, info: Box<dyn TaskStatusInfo>) -> Arc<Self> {
        let pct_complete = pct_complete.clamp(0., 1.);

        let mut time_estimate = self.estimated_time_remaining;
        time_estimate.update_now(pct_complete);

        Arc::new(RunningTaskStatusInfo {
            pct_complete,
            time_started: self.time_started,
            estimated_time_remaining: time_estimate,
            info,
        })
    }
}

#[allow(clippy::borrowed_box)]
fn serialize_failed_task_status<S>(
    error: &dyn ErrorSource,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let error_string = error.to_string();
    serializer.serialize_str(error_string.as_str())
}

#[allow(clippy::trivially_copy_pass_by_ref)] // must adhere to serde's signature
fn serialize_as_pct<S>(pct: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("{:.2}%", pct * 100.))
}

/// Trait for information about the status of a task.
pub trait TaskStatusInfo: erased_serde::Serialize + Send + Sync + fmt::Debug + AsAnyArc {
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

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, IntoParams)]
pub struct TaskListOptions {
    #[serde(default)]
    pub filter: Option<TaskFilter>,
    #[serde(default)]
    #[param(example = 0)]
    pub offset: u32,
    #[serde(default = "task_list_limit_default")]
    #[param(example = 20)]
    pub limit: u32,
}

impl UserInput for TaskListOptions {
    fn validate(&self) -> Result<()> {
        let limit = get_config_element::<crate::util::config::TaskManager>()?.list_limit;
        ensure!(
            self.limit <= limit,
            crate::error::InvalidListLimit {
                limit: limit as usize
            }
        );

        Ok(())
    }
}

fn task_list_limit_default() -> u32 {
    get_config_element::<crate::util::config::TaskManager>()
        .map(|config| config.list_default_limit)
        .unwrap_or(1)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum TaskFilter {
    Running,
    Aborted,
    Failed,
    Completed,
}
