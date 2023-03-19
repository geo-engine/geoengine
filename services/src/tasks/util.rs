use super::{TaskContext, TaskId, TaskManager, TaskStatusInfo};

pub mod test {
    use super::{TaskContext, TaskId, TaskManager};
    use std::sync::Arc;

    /// Test helper for waiting for a task to finish
    ///
    /// # Panics
    /// Panics if task does not finish within some time.
    ///
    pub async fn wait_for_task_to_finish<C: TaskContext + 'static>(
        task_manager: Arc<impl TaskManager<C>>,
        task_id: TaskId,
    ) {
        geoengine_operators::util::retry::retry(5, 100, 2., None, move || {
            let task_manager = task_manager.clone();
            async move {
                let status = task_manager.get_task_status(task_id).await.unwrap();
                status.is_finished().then_some(()).ok_or(())
            }
        })
        .await
        .unwrap();
    }
}

/// A task context for testing that does nothing
pub struct NopTaskContext;

#[async_trait::async_trait]
impl TaskContext for NopTaskContext {
    async fn set_completion(&self, _pct_complete: f64, _status: Box<dyn TaskStatusInfo>) {}
}
