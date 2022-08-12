use super::{TaskContext, TaskId, TaskManager};

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
        crate::util::retry::retry(5, 100, 2., move || {
            let task_manager = task_manager.clone();
            async move {
                let status = task_manager.status(task_id).await.unwrap();
                status.is_finished().then_some(()).ok_or(())
            }
        })
        .await
        .unwrap();
    }
}
