use super::{
    RunningTaskStatusInfo, Task, TaskCleanUpStatus, TaskContext, TaskError, TaskFilter, TaskId,
    TaskListOptions, TaskManager, TaskStatus, TaskStatusInfo, TaskStatusWithId,
};
use crate::{contexts::Db, error::Result, util::user_input::Validated};
use futures::channel::oneshot;
use futures::StreamExt;
use geoengine_datatypes::{error::ErrorSource, util::Identifier};
use log::warn;
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

#[derive(Debug)]
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

        let handle = run_task(
            self.clone(), // we can clone here, since all interior stuff is wrapped into `Arc`s
            task_id,
            task,
            task_ctx,
            notify,
        );

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

        let stream = futures::stream::iter(lock.iter());

        let result: Vec<TaskStatusWithId> = stream
            .filter_map(|task_status_with_id| async {
                let task_status = task_status_with_id.status.read().await;

                match (options.filter, &*task_status) {
                    (None, _)
                    | (Some(TaskFilter::Running), &TaskStatus::Running(_))
                    | (Some(TaskFilter::Completed), &TaskStatus::Completed { .. })
                    | (Some(TaskFilter::Aborted), &TaskStatus::Aborted { .. })
                    | (Some(TaskFilter::Failed), &TaskStatus::Failed { .. }) => {
                        Some(TaskStatusWithId {
                            task_id: task_status_with_id.task_id,
                            status: task_status.clone(),
                        })
                    }
                    _ => None,
                }
            })
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .collect()
            .await;

        Ok(result)
    }

    async fn abort(&self, task_id: TaskId, force: bool) -> Result<(), TaskError> {
        let mut write_lock = self.write_lock_for_update().await;

        let mut task_handle = write_lock
            .tasks_by_id
            .remove(&task_id)
            .ok_or(TaskError::TaskNotFound { task_id })?;

        let task_status_lock = task_handle.status.read().await;

        if task_status_lock.is_finished() {
            return Err(TaskError::TaskAlreadyFinished { task_id });
        } else if !force && task_status_lock.has_aborted() {
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

        let subtask_ids = task_handle.task.subtasks().await;

        if force || task_finished_before_being_aborted {
            set_status_to_no_clean_up(&task_handle.status).await;

            remove_unique_key(&task_handle, &mut write_lock.unique_tasks);

            // propagate abort to subtasks
            drop(write_lock); // prevent deadlocks because the subtask abort tries to fetch the lock
            abort_subtasks(self.clone(), subtask_ids, force, task_id).await;

            // no clean-up in this case
            return Ok(());
        }

        set_status_to_aborting(&task_handle.status).await;
        let result = clean_up_phase(self.clone(), task_handle, &mut write_lock, task_id).await;

        // propagate abort to subtasks
        drop(write_lock); // prevent deadlocks because the subtask abort tries to fetch the lock
        abort_subtasks(self.clone(), subtask_ids, force, task_id).await;

        result
    }
}

async fn abort_subtasks(
    task_manager: SimpleTaskManager,
    subtask_ids: Vec<TaskId>,
    force: bool,
    supertask_id: TaskId,
) {
    for subtask_id in subtask_ids {
        // don't fail if subtask failed to abort
        let subtask_abort_result = task_manager.abort(subtask_id, force).await;

        if let Err(subtask_abort_result) = subtask_abort_result {
            warn!(
                "failed to abort subtask {subtask_id} of task {supertask_id}: {subtask_abort_result:?}"
            );
        }
    }
}

fn run_task(
    task_manager: SimpleTaskManager,
    task_id: TaskId,
    task: SharedTask,
    task_ctx: SimpleTaskManagerContext,
    notify: Option<oneshot::Sender<TaskStatus>>,
) -> JoinHandle<()> {
    crate::util::spawn(async move {
        let result = task.run(task_ctx.clone()).await;

        let mut update_lock = task_manager.write_lock_for_update().await;

        let task_handle = match update_lock.tasks_by_id.remove(&task_id) {
            Some(task_handle) => task_handle,
            None => return, // never happens
        };

        let task_status = task_handle.status.clone();

        match result {
            Ok(status) => {
                *task_handle.status.write().await = TaskStatus::completed(Arc::from(status));

                remove_unique_key(&task_handle, &mut update_lock.unique_tasks);
            }
            Err(err) => {
                let err = Arc::from(err);

                *task_handle.status.write().await = TaskStatus::failed(
                    Arc::clone(&err),
                    TaskCleanUpStatus::Running(RunningTaskStatusInfo::new(0, ().boxed())),
                );

                if let Err(clean_up_err) =
                    clean_up_phase(task_manager.clone(), task_handle, &mut update_lock, task_id)
                        .await
                {
                    *task_status.write().await = TaskStatus::failed(
                        err,
                        TaskCleanUpStatus::Failed {
                            error: Arc::new(Box::new(clean_up_err)),
                        },
                    );

                    let task_handle = update_lock.tasks_by_id.remove(&task_id);

                    if let Some(task_handle) = task_handle {
                        remove_unique_key(&task_handle, &mut update_lock.unique_tasks);
                    }
                }
            }
        };

        // TODO: move this into clean-up?
        if let Some(notify) = notify {
            // we can ignore the returned error because this means
            // that the receiver has already been dropped
            notify
                .send(task_status.read().await.clone())
                .unwrap_or_default();
        }
    })
}

fn remove_unique_key(
    task_handle: &TaskHandle,
    unique_lock: &mut RwLockWriteGuard<'_, HashSet<(&'static str, String)>>,
) {
    if let Some(task_unique_id) = &task_handle.unique_key {
        unique_lock.remove(task_unique_id);
    }
}

async fn set_status_to_aborting(task_status: &Db<TaskStatus>) {
    let mut task_status_lock = task_status.write().await;
    *task_status_lock = TaskStatus::aborted(TaskCleanUpStatus::Running(
        RunningTaskStatusInfo::new(0, ().boxed()),
    ));
}

async fn set_status_to_clean_up_completed(task_status: &Db<TaskStatus>) {
    let mut task_status_lock = task_status.write().await;

    let task_clean_up_status = TaskCleanUpStatus::Completed {
        info: Arc::new(().boxed()),
    };

    *task_status_lock = match &*task_status_lock {
        TaskStatus::Running(_) | TaskStatus::Completed { .. } => return, // must not happen, ignore
        TaskStatus::Aborted { .. } => TaskStatus::aborted(task_clean_up_status),
        TaskStatus::Failed { error, .. } => TaskStatus::failed(error.clone(), task_clean_up_status),
    };
}

async fn set_status_to_no_clean_up(task_status: &Db<TaskStatus>) {
    let mut task_status_lock = task_status.write().await;

    let task_clean_up_status = TaskCleanUpStatus::NoCleanUp;

    *task_status_lock = match &*task_status_lock {
        TaskStatus::Completed { .. } => return, // must not happen, ignore
        TaskStatus::Running(_) | TaskStatus::Aborted { .. } => {
            TaskStatus::aborted(task_clean_up_status)
        }
        TaskStatus::Failed { error, .. } => TaskStatus::failed(error.clone(), task_clean_up_status),
    }
}

async fn set_status_to_clean_up_failed(task_status: &Db<TaskStatus>, error: Box<dyn ErrorSource>) {
    let mut task_status_lock = task_status.write().await;

    let task_clean_up_status = TaskCleanUpStatus::Failed {
        error: Arc::from(error),
    };

    *task_status_lock = match &*task_status_lock {
        TaskStatus::Running(_) | TaskStatus::Completed { .. } => return, // must not happen, ignore
        TaskStatus::Aborted { .. } => TaskStatus::aborted(task_clean_up_status),
        TaskStatus::Failed { error, .. } => TaskStatus::failed(error.clone(), task_clean_up_status),
    }
}

async fn clean_up_phase(
    task_manager: SimpleTaskManager,
    mut task_handle: TaskHandle,
    write_lock: &mut WriteLockForUpdate<'_>,
    task_id: TaskId,
) -> Result<(), TaskError> {
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

        match result {
            Ok(_) => set_status_to_clean_up_completed(&task_handle.status).await,
            Err(err) => set_status_to_clean_up_failed(&task_handle.status, err).await,
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

        let status_info = RunningTaskStatusInfo::new(pct_complete, status);

        *task_status = match &*task_status {
            TaskStatus::Running(_) => TaskStatus::Running(status_info),
            TaskStatus::Aborted {
                clean_up: TaskCleanUpStatus::Running(_),
            } => TaskStatus::aborted(TaskCleanUpStatus::Running(status_info)),
            TaskStatus::Failed {
                error,
                clean_up: TaskCleanUpStatus::Running(_),
            } => TaskStatus::failed(error.clone(), TaskCleanUpStatus::Running(status_info)),
            _ => return, // already completed, aborted or failed, so we ignore the status update
        };
    }
}
