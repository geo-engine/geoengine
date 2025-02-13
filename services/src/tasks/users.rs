use crate::users::UserSession;
use crate::{
    error,
    tasks::{
        SimpleTaskManagerBackend, SimpleTaskManagerContext, Task, TaskError, TaskId,
        TaskListOptions, TaskManager, TaskStatus, TaskStatusWithId,
    },
};
use futures::channel::oneshot;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

// TODO: implement real permissions on task types
const ADMIN_ONLY_TASKS: [&str; 3] = [
    crate::api::handlers::ebv::EBV_OVERVIEW_TASK_TYPE,
    crate::api::handlers::ebv::EBV_MULTI_OVERVIEW_TASK_TYPE,
    crate::api::handlers::ebv::EBV_REMOVE_OVERVIEW_TASK_TYPE,
];

#[derive(Default)]
pub struct TypedTaskManagerBackend {
    simple_task_manager: SimpleTaskManagerBackend,
    task_type_by_id: RwLock<HashMap<TaskId, &'static str>>,
}

pub struct UserTaskManager {
    backend: Arc<TypedTaskManagerBackend>,
    session: UserSession,
}

impl UserTaskManager {
    pub fn new(backend: Arc<TypedTaskManagerBackend>, session: UserSession) -> Self {
        Self { backend, session }
    }
}

fn check_task_type_is_allowed(
    session: &UserSession,
    task_type: &'static str,
) -> Result<(), TaskError> {
    if ADMIN_ONLY_TASKS.contains(&task_type) && !session.is_admin() {
        return Err(crate::tasks::TaskError::TaskManagerOperationFailed {
            source: Box::new(error::Error::PermissionDenied),
        });
    }

    Ok(())
}

#[async_trait::async_trait]
impl TaskManager<SimpleTaskManagerContext> for UserTaskManager {
    async fn schedule_task(
        &self,
        task: Box<dyn Task<SimpleTaskManagerContext>>,
        notify: Option<oneshot::Sender<TaskStatus>>,
    ) -> Result<TaskId, TaskError> {
        let task_type = task.task_type();
        check_task_type_is_allowed(&self.session, task_type)?;

        // TODO: check permissions for user tasks

        let task_id = self
            .backend
            .simple_task_manager
            .schedule_task(task, notify)
            .await?;

        self.backend
            .task_type_by_id
            .write()
            .await
            .insert(task_id, task_type);

        Ok(task_id)
    }

    async fn get_task_status(&self, task_id: TaskId) -> Result<TaskStatus, TaskError> {
        check_task_type_is_allowed(
            &self.session,
            self.backend
                .task_type_by_id
                .read()
                .await
                .get(&task_id)
                .ok_or(TaskError::TaskNotFound { task_id })?,
        )?;

        // TODO: check permissions for user tasks

        self.backend
            .simple_task_manager
            .get_task_status(task_id)
            .await
    }

    async fn list_tasks(
        &self,
        options: TaskListOptions,
    ) -> Result<Vec<TaskStatusWithId>, TaskError> {
        // TODO: check permissions for user tasks

        let tasks = self
            .backend
            .simple_task_manager
            .list_tasks(TaskListOptions {
                filter: options.filter,
                offset: 0,
                limit: u32::MAX,
            })
            .await?;

        let task_types = self.backend.task_type_by_id.read().await;

        Ok(tasks
            .iter()
            .filter(|t| {
                if let Some(task_type) = task_types.get(&t.task_id) {
                    return check_task_type_is_allowed(&self.session, task_type).is_ok();
                }
                false
            })
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .cloned()
            .collect::<Vec<_>>())
    }

    async fn abort_tasks(&self, task_id: TaskId, force: bool) -> Result<(), TaskError> {
        check_task_type_is_allowed(
            &self.session,
            self.backend
                .task_type_by_id
                .read()
                .await
                .get(&task_id)
                .ok_or(TaskError::TaskNotFound { task_id })?,
        )?;

        // TODO: check permissions for user tasks

        self.backend
            .simple_task_manager
            .abort_tasks(task_id, force)
            .await
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        contexts::PostgresContext,
        contexts::{ApplicationContext, SessionContext},
        ge_context,
        users::UserAuth,
    };
    use tokio_postgres::NoTls;

    #[ge_context::test]
    async fn it_lists(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let ctx = app_ctx.session_context(session);

        let tasks = ctx.tasks();

        tasks.list_tasks(TaskListOptions::default()).await.unwrap();
    }
}
