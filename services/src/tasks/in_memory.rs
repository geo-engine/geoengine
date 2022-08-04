use super::{
    RunningTaskStatusInfo, Task, TaskContext, TaskError, TaskFilter, TaskId, TaskListOptions,
    TaskManager, TaskStatus, TaskStatusInfo, TaskStatusWithId,
};
use crate::{contexts::Db, error::Result, util::user_input::Validated};
use futures::StreamExt;
use geoengine_datatypes::util::Identifier;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use tokio::{
    sync::{RwLock, RwLockWriteGuard},
    task::JoinHandle,
};

type SharedTask = Arc<Box<dyn Task<SimpleTaskManagerContext>>>;

/// An in-memory implementation of the [`TaskManager`] trait.
#[derive(Default, Clone)]
pub struct SimpleTaskManager {
    handles: Db<HashMap<TaskId, JoinHandle<()>>>,
    tasks: Db<HashMap<TaskId, SharedTask>>,
    tasks_by_id: Db<HashMap<TaskId, Db<TaskStatus>>>,
    task_list: Db<VecDeque<TaskUpdateStatusWithTaskId>>,
}

impl SimpleTaskManager {
    async fn write_lock_all(&self) -> WriteLockAll {
        let (handles, tasks, tasks_by_id, task_list) = tokio::join!(
            self.handles.write(),
            self.tasks.write(),
            self.tasks_by_id.write(),
            self.task_list.write()
        );
        WriteLockAll {
            handles,
            tasks,
            tasks_by_id,
            task_list,
        }
    }
}

struct TaskUpdateStatusWithTaskId {
    pub task_id: TaskId,
    pub status: Db<TaskStatus>,
}

struct WriteLockAll<'a> {
    pub handles: RwLockWriteGuard<'a, HashMap<TaskId, JoinHandle<()>>>,
    pub tasks: RwLockWriteGuard<'a, HashMap<TaskId, SharedTask>>,
    pub tasks_by_id: RwLockWriteGuard<'a, HashMap<TaskId, Db<TaskStatus>>>,
    pub task_list: RwLockWriteGuard<'a, VecDeque<TaskUpdateStatusWithTaskId>>,
}

#[async_trait::async_trait]
impl TaskManager<SimpleTaskManagerContext> for SimpleTaskManager {
    async fn schedule(
        &self,
        task: Box<dyn Task<SimpleTaskManagerContext>>,
    ) -> Result<TaskId, TaskError> {
        let task_id = TaskId::new();

        // we can clone here, since all interior stuff is wrapped into `Arc`s
        let task_manager = self.clone();

        // get lock before starting the task to prevent a race condition of initial status setting
        let mut lock = self.write_lock_all().await;

        let status = Arc::new(RwLock::new(TaskStatus::Running(
            RunningTaskStatusInfo::new(0, ().boxed()),
        )));

        let task_ctx = SimpleTaskManagerContext {
            status: status.clone(),
        };

        let task = Arc::new(task);

        lock.tasks.insert(task_id, task.clone());

        let handle = crate::util::spawn(async move {
            let result = task.run(task_ctx.clone()).await;

            let (mut handles_lock, mut task_status_lock) =
                tokio::join!(task_manager.handles.write(), task_ctx.status.write());

            handles_lock.remove(&task_id);

            *task_status_lock = match result {
                Ok(status) => TaskStatus::completed(Arc::new(status)),
                Err(err) => TaskStatus::failed(Arc::new(err)),
            };
        });

        lock.handles.insert(task_id, handle);
        lock.tasks_by_id.insert(task_id, status.clone());
        lock.task_list
            .push_front(TaskUpdateStatusWithTaskId { task_id, status });

        Ok(task_id)
    }

    async fn status(&self, task_id: TaskId) -> Result<TaskStatus, TaskError> {
        if let Some(task_result) = self.tasks_by_id.read().await.get(&task_id) {
            Ok(task_result.read().await.clone())
        } else {
            Err(TaskError::TaskNotFound { task_id })
        }
    }

    async fn list(
        &self,
        options: Validated<TaskListOptions>,
    ) -> Result<Vec<TaskStatusWithId>, TaskError> {
        let lock = self.task_list.read().await;

        let iter = lock.range(options.offset as usize..);
        let stream = futures::stream::iter(iter);

        let result = stream
            .filter_map(|task_status_with_id| async {
                let task_status = task_status_with_id.status.read().await;

                match (options.filter, &*task_status) {
                    (None, _)
                    | (Some(TaskFilter::Running), &TaskStatus::Running(_))
                    | (Some(TaskFilter::Completed), &TaskStatus::Completed { info: _ })
                    | (Some(TaskFilter::Failed), &TaskStatus::Failed { error: _ }) => {
                        Some(TaskStatusWithId {
                            task_id: task_status_with_id.task_id,
                            status: task_status.clone(),
                        })
                    }
                    _ => None,
                }
            })
            .take(options.limit as usize)
            .collect()
            .await;

        Ok(result)
    }

    async fn abort(&self, task_id: TaskId) -> Result<(), TaskError> {
        let mut write_lock = self.write_lock_all().await;

        let task_handle = write_lock
            .handles
            .remove(&task_id)
            .ok_or(TaskError::TaskNotFound { task_id })?;

        task_handle.abort();

        let result = task_handle.await;
        let task_finished_before_being_aborted = result.is_ok();

        if task_finished_before_being_aborted {
            return Ok(());
        }

        // reflect abortion in status
        let task_status = write_lock
            .tasks_by_id
            .get(&task_id)
            .ok_or(TaskError::TaskNotFound { task_id })?;
        let mut task_status_lock = task_status.write().await;
        *task_status_lock =
            TaskStatus::failed(Arc::new(Box::new(TaskError::TaskAborted { task_id })));

        // clean up task
        let task = write_lock
            .tasks
            .get(&task_id)
            .ok_or(TaskError::TaskNotFound { task_id })?
            .clone();
        let task_ctx = SimpleTaskManagerContext {
            status: task_status.clone(),
        };

        // store task handle

        // we can clone here, since all interior stuff is wrapped into `Arc`s
        let task_manager = self.clone();

        let handle = crate::util::spawn(async move {
            let result = task.run(task_ctx.clone()).await;

            let (mut handles_lock, mut task_status_lock) =
                tokio::join!(task_manager.handles.write(), task_ctx.status.write());

            handles_lock.remove(&task_id);

            *task_status_lock = match result {
                Ok(status) => TaskStatus::completed(Arc::new(status)),
                Err(err) => TaskStatus::failed(Arc::new(err)),
            };
        });

        // TODO: store clean-up status

        write_lock.handles.insert(task_id, handle);

        Ok(())
    }
}

#[derive(Clone)]
pub struct SimpleTaskManagerContext {
    status: Db<TaskStatus>,
}

#[async_trait::async_trait]
impl TaskContext for SimpleTaskManagerContext {
    async fn set_completion(&self, pct_complete: u8, status: Box<dyn TaskStatusInfo>) {
        let mut task_status = self.status.write().await;

        if task_status.is_running() {
            *task_status = TaskStatus::Running(RunningTaskStatusInfo::new(pct_complete, status));
        } else {
            // already completed, so we ignore the status update
        }
    }
}
