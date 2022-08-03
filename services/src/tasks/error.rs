use snafu::Snafu;

use super::TaskId;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum TaskError {
    #[snafu(display("Task not found with id: {task_id}"))]
    TaskNotFound { task_id: TaskId },
    #[snafu(display("Task is duplicate. Type: {task_type}, Unique ID: {task_unique_id}"))]
    DuplicateTask {
        task_type: &'static str,
        task_unique_id: String,
    },
}
