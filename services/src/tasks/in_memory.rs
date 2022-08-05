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

type SharedTask = Arc<Box<dyn Task<SimpleTaskManagerContext>>>;

/// An in-memory implementation of the [`TaskManager`] trait.
#[derive(Default, Clone)]
pub struct SimpleTaskManager {
    tasks_by_id: Db<HashMap<TaskId, TaskHandle>>,
    unique_tasks: Db<HashSet<(&'static str, String)>>,
    // these two lists won't be cleaned-up
    status_by_id: Db<HashMap<TaskId, Db<TaskStatus>>>,
    status_list: Db<VecDeque<TaskUpdateStatusWithTaskId>>,
}

struct TaskHandle {
    task: SharedTask,
    handle: Option<JoinHandle<()>>,
    status: Db<TaskStatus>,
    unique_key: Option<(&'static str, String)>,
}

impl SimpleTaskManager {
    async fn write_lock_all(&self) -> WriteLockAll {
        let (tasks_by_id, status_by_id, task_list, unique_tasks) = tokio::join!(
            self.tasks_by_id.write(),
            self.status_by_id.write(),
            self.status_list.write(),
            self.unique_tasks.write(),
        );
        WriteLockAll {
            tasks_by_id,
            status_by_id,
            status_list: task_list,
            unique_tasks,
        }
    }

    async fn write_lock_for_update(&self) -> WriteLockForUpdate {
        let (tasks_by_id, unique_tasks) =
            tokio::join!(self.tasks_by_id.write(), self.unique_tasks.write());
        WriteLockForUpdate {
            tasks_by_id,
            unique_tasks,
        }
    }
}

struct TaskUpdateStatusWithTaskId {
    pub task_id: TaskId,
    pub status: Db<TaskStatus>,
}

struct WriteLockAll<'a> {
    pub tasks_by_id: RwLockWriteGuard<'a, HashMap<TaskId, TaskHandle>>,
    pub status_by_id: RwLockWriteGuard<'a, HashMap<TaskId, Db<TaskStatus>>>,
    pub status_list: RwLockWriteGuard<'a, VecDeque<TaskUpdateStatusWithTaskId>>,
    pub unique_tasks: RwLockWriteGuard<'a, HashSet<(&'static str, String)>>,
}

struct WriteLockForUpdate<'a> {
    pub tasks_by_id: RwLockWriteGuard<'a, HashMap<TaskId, TaskHandle>>,
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

        let mut task_handle = TaskHandle {
            task: Arc::new(task),
            handle: None,
            status: Arc::new(RwLock::new(TaskStatus::Running(
                RunningTaskStatusInfo::new(0, ().boxed()),
            ))),
            unique_key: task_unique_key,
        };

        let task = task_handle.task.clone();
        let task_ctx = SimpleTaskManagerContext {
            status: task_handle.status.clone(),
        };

        let handle = crate::util::spawn(async move {
            let result = task.run(task_ctx.clone()).await;

            let mut update_lock = task_manager.write_lock_for_update().await;

            let task_handle = match update_lock.tasks_by_id.remove(&task_id) {
                Some(task_handle) => task_handle,
                None => return, // never happens
            };

            let mut task_status_lock = task_handle.status.write().await;

            *task_status_lock = match result {
                Ok(status) => TaskStatus::completed(Arc::new(status)),
                // TODO: add clean-up phase in this case (?)
                Err(err) => TaskStatus::failed(Arc::new(err)),
            };

            if let Some(notify) = notify {
                // we can ignore the returned error because this means
                // that the receiver has already been dropped
                notify.send(task_status_lock.clone()).unwrap_or_default();
            }

            remove_unique_key(&task_handle, &mut update_lock.unique_tasks);
        });

        task_handle.handle = Some(handle);

        lock.status_by_id
            .insert(task_id, task_handle.status.clone());
        lock.status_list.push_front(TaskUpdateStatusWithTaskId {
            task_id,
            status: task_handle.status.clone(),
        });

        if let Some(task_unique_id) = &task_handle.unique_key {
            lock.unique_tasks.insert(task_unique_id.clone());
        }

        lock.tasks_by_id.insert(task_id, task_handle);

        Ok(task_id)
    }

    async fn status(&self, task_id: TaskId) -> Result<TaskStatus, TaskError> {
        let task_status_map = self.status_by_id.read().await;
        let task_status = task_status_map
            .get(&task_id)
            .ok_or(TaskError::TaskNotFound { task_id })?
            .read()
            .await
            .clone();

        Ok(task_status)
    }

    async fn list(
        &self,
        options: Validated<TaskListOptions>,
    ) -> Result<Vec<TaskStatusWithId>, TaskError> {
        let lock = self.status_list.read().await;

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

    async fn abort(&self, task_id: TaskId, force: bool) -> Result<(), TaskError> {
        let mut write_lock = self.write_lock_all().await;

        let mut task_handle = write_lock
            .tasks_by_id
            .remove(&task_id)
            .ok_or(TaskError::TaskNotFound { task_id })?;

        let task_status_lock = task_handle.status.read().await;

        if task_status_lock.is_finished() {
            return Err(TaskError::TaskAlreadyFinished { task_id });
        } else if !force && task_status_lock.is_aborting() {
            drop(task_status_lock);

            // put clean-up handle back
            write_lock.tasks_by_id.insert(task_id, task_handle);

            return Err(TaskError::TaskAlreadyAborted { task_id });
        }

        drop(task_status_lock); // prevent deadlocks on the status lock

        let task_finished_before_being_aborted = if let Some(handle) = task_handle.handle.take() {
            handle.abort();
            handle.await.is_ok()
        } else {
            // this case should not happen, so we just assume that the task finished before being aborted
            true
        };

        if force || task_finished_before_being_aborted {
            set_status_to_aborted(&task_handle.status).await?;

            remove_unique_key(&task_handle, &mut write_lock.unique_tasks);

            // no clean-up in this case
            return Ok(());
        }

        clean_up_phase(self.clone(), task_handle, write_lock, task_id).await
    }
}

fn remove_unique_key(
    task_handle: &TaskHandle,
    unique_lock: &mut RwLockWriteGuard<'_, HashSet<(&'static str, String)>>,
) {
    if let Some(task_unique_id) = &task_handle.unique_key {
        unique_lock.remove(task_unique_id);
    }
}

async fn set_status_to_aborting(task_status: &Db<TaskStatus>) -> Result<(), TaskError> {
    let mut task_status_lock = task_status.write().await;
    *task_status_lock = TaskStatus::Aborting(RunningTaskStatusInfo::new(0, ().boxed()));
    Ok(())
}

async fn set_status_to_aborted(task_status: &Db<TaskStatus>) -> Result<(), TaskError> {
    let mut task_status_lock = task_status.write().await;
    *task_status_lock = TaskStatus::aborted(Arc::new(().boxed()));
    Ok(())
}

async fn clean_up_phase(
    task_manager: SimpleTaskManager,
    mut task_handle: TaskHandle,
    mut write_lock: WriteLockAll<'_>,
    task_id: TaskId,
) -> Result<(), TaskError> {
    set_status_to_aborting(&task_handle.status).await?;

    let task = task_handle.task.clone();
    let task_ctx = SimpleTaskManagerContext {
        status: task_handle.status.clone(),
    };

    let handle = crate::util::spawn(async move {
        let result = task.cleanup_on_error(task_ctx.clone()).await;

        let mut update_lock = task_manager.write_lock_for_update().await;

        let task_handle = match update_lock.tasks_by_id.remove(&task_id) {
            Some(task_handle) => task_handle,
            None => return, // never happens
        };

        let mut task_status_lock = task_handle.status.write().await;

        *task_status_lock = match result {
            Ok(status) => TaskStatus::aborted(Arc::new(status.boxed())),
            Err(err) => TaskStatus::failed(Arc::new(err)),
        };

        remove_unique_key(&task_handle, &mut update_lock.unique_tasks);
    });

    task_handle.handle = Some(handle);

    write_lock.tasks_by_id.insert(task_id, task_handle);

    Ok(())
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
        } else if task_status.is_aborting() {
            *task_status = TaskStatus::Aborting(RunningTaskStatusInfo::new(pct_complete, status));
        } else {
            // already completed, aborted or failed, so we ignore the status update
        }
    }
}
