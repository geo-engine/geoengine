use super::{
    RunningTaskStatusInfo, Task, TaskContext, TaskError, TaskFilter, TaskId, TaskListOptions,
    TaskManager, TaskStatus, TaskStatusInfo, TaskStatusWithId,
};
use crate::{contexts::Db, error::Result, util::user_input::Validated};
use futures::channel::oneshot;
use futures::StreamExt;
use geoengine_datatypes::util::Identifier;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};
use tokio::{
    sync::{RwLock, RwLockWriteGuard},
    task::JoinHandle,
};

/// An in-memory implementation of the [`TaskManager`] trait.
#[derive(Default, Clone)]
pub struct SimpleTaskManager {
    handles: Db<HashMap<TaskId, JoinHandle<()>>>,
    tasks_by_id: Db<HashMap<TaskId, Db<TaskStatus>>>,
    task_list: Db<VecDeque<TaskUpdateStatusWithTaskId>>,
    unique_tasks: Db<HashSet<(&'static str, String)>>,
}

impl SimpleTaskManager {
    async fn write_lock_all(&self) -> WriteLockAll {
        let (handles, tasks_by_id, task_list, unique_tasks) = tokio::join!(
            self.handles.write(),
            self.tasks_by_id.write(),
            self.task_list.write(),
            self.unique_tasks.write(),
        );
        WriteLockAll {
            handles,
            tasks_by_id,
            task_list,
            unique_tasks,
        }
    }
}

struct TaskUpdateStatusWithTaskId {
    pub task_id: TaskId,
    pub status: Db<TaskStatus>,
}

struct WriteLockAll<'a> {
    pub handles: RwLockWriteGuard<'a, HashMap<TaskId, JoinHandle<()>>>,
    pub tasks_by_id: RwLockWriteGuard<'a, HashMap<TaskId, Db<TaskStatus>>>,
    pub task_list: RwLockWriteGuard<'a, VecDeque<TaskUpdateStatusWithTaskId>>,
    pub unique_tasks: RwLockWriteGuard<'a, HashSet<(&'static str, String)>>,
}

#[async_trait::async_trait]
impl TaskManager<SimpleTaskManagerContext> for SimpleTaskManager {
    async fn schedule(
        &self,
        task: Box<dyn Task<SimpleTaskManagerContext>>,
        notify: Option<oneshot::Sender<TaskStatus>>,
    ) -> Result<TaskId, TaskError> {
        let task_id = TaskId::new();

        // we can clone here, since all interior stuff is wrapped into `Arc`s
        let task_manager = self.clone();

        // get lock before starting the task to prevent a race condition of initial status setting
        let mut lock = self.write_lock_all().await;

        // check if task is duplicate
        let task_unique_key = task
            .task_unique_id()
            .map(|task_unique_id| (task.task_type(), task_unique_id));

        if let Some(task_unique_id) = &task_unique_key {
            if !lock.unique_tasks.insert(task_unique_id.clone()) {
                return Err(TaskError::DuplicateTask {
                    task_type: task_unique_id.0,
                    task_unique_id: task_unique_id.1.clone(),
                });
            }
        }

        let status = Arc::new(RwLock::new(TaskStatus::Running(
            RunningTaskStatusInfo::new(0, ().boxed()),
        )));

        let task_ctx = SimpleTaskManagerContext {
            status: status.clone(),
        };

        let handle = crate::util::spawn(async move {
            let result = task.run(task_ctx.clone()).await;

            let (mut handles_lock, mut unique_tasks, mut task_status_lock) = tokio::join!(
                task_manager.handles.write(),
                task_manager.unique_tasks.write(),
                task_ctx.status.write()
            );

            handles_lock.remove(&task_id);

            *task_status_lock = match result {
                Ok(status) => TaskStatus::completed(Arc::new(status)),
                Err(err) => TaskStatus::failed(Arc::new(err)),
            };

            if let Some(notify) = notify {
                // we can ignore the returned error because this means
                // that the receiver has already been dropped
                notify.send(task_status_lock.clone()).unwrap_or_default();
            }

            if let Some(task_unique_id) = task_unique_key {
                unique_tasks.remove(&task_unique_id);
            }
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
