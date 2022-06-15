use super::{
    RunningTaskStatusInfo, Task, TaskContext, TaskDb, TaskError, TaskFilter, TaskId, TaskStatus,
    TaskStatusInfo,
};
use crate::{contexts::Db, error::Result};
use geoengine_datatypes::{error::ErrorSource, util::Identifier};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::{RwLockReadGuard, RwLockWriteGuard},
    task::JoinHandle,
};

/// An in-memory implementation of the `TaskDb` trait.
#[derive(Default, Clone)]
pub struct InMemoryTaskDb {
    handles: Db<HashMap<TaskId, JoinHandle<()>>>,
    running: Db<TaskStatusMap>,
    completed: Db<TaskResultMap>,
}

type TaskStatusMap = HashMap<TaskId, Arc<RunningTaskStatusInfo>>;
type TaskResultMap =
    HashMap<TaskId, Result<Arc<Box<dyn TaskStatusInfo>>, Arc<Box<dyn ErrorSource>>>>;

impl InMemoryTaskDb {
    async fn write_lock_all(&self) -> WriteLockAll {
        let (handles, running, completed) = tokio::join!(
            self.handles.write(),
            self.running.write(),
            self.completed.write()
        );
        WriteLockAll {
            handles,
            running,
            completed,
        }
    }

    async fn read_lock_status(&self) -> ReadLockStatus {
        let (running, completed) = tokio::join!(self.running.read(), self.completed.read());
        ReadLockStatus { running, completed }
    }
}

struct WriteLockAll<'a> {
    pub handles: RwLockWriteGuard<'a, HashMap<TaskId, JoinHandle<()>>>,
    pub running: RwLockWriteGuard<'a, TaskStatusMap>,
    pub completed: RwLockWriteGuard<'a, TaskResultMap>,
}

struct ReadLockStatus<'a> {
    pub running: RwLockReadGuard<'a, TaskStatusMap>,
    pub completed: RwLockReadGuard<'a, TaskResultMap>,
}

#[async_trait::async_trait]
impl TaskDb<InMemoryTaskDbContext> for InMemoryTaskDb {
    async fn register(
        &self,
        task: Box<dyn Task<InMemoryTaskDbContext>>,
    ) -> Result<TaskId, TaskError> {
        let task_id = TaskId::new();

        let task_ctx = InMemoryTaskDbContext {
            task_id,
            running: self.running.clone(),
        };

        // we can clone here, since all interior stuff is wrapped into `Arc`s
        let task_db = self.clone();

        // get lock before starting the task to prevent a race condition of initial status setting
        let mut lock = self.write_lock_all().await;

        let handle = crate::util::spawn(async move {
            let result = task.run(task_ctx).await;

            let mut lock = task_db.write_lock_all().await;

            lock.handles.remove(&task_id);
            lock.running.remove(&task_id);

            lock.completed
                .insert(task_id, result.map(Arc::new).map_err(Arc::new));
        });

        lock.handles.insert(task_id, handle);
        lock.running
            .insert(task_id, RunningTaskStatusInfo::new(0, ().boxed()));

        Ok(task_id)
    }

    async fn status(&self, task_id: TaskId) -> Result<TaskStatus, TaskError> {
        // we can lock the two maps after each other, because we lock all simultaneously during writing

        if let Some(task_status) = self.running.read().await.get(&task_id) {
            return Ok(TaskStatus::Running(task_status.clone()));
        }

        if let Some(task_result) = self.completed.read().await.get(&task_id) {
            match task_result {
                Ok(task_status) => Ok(TaskStatus::completed(task_status.clone())),
                Err(task_status) => Ok(TaskStatus::failed(task_status.clone())),
            }
        } else {
            Err(TaskError::TaskNotFound { task_id })
        }
    }

    async fn list(&self, filter: Option<TaskFilter>) -> Result<Vec<TaskStatus>, TaskError> {
        // TODO: order by timestamp of creation

        match filter {
            None => {
                let lock = self.read_lock_status().await;

                let running = lock
                    .running
                    .values()
                    .map(|running| TaskStatus::Running(running.clone()));

                let completed = lock.completed.values().map(|completed| match completed {
                    Ok(task_status) => TaskStatus::completed(task_status.clone()),
                    Err(task_status) => TaskStatus::failed(task_status.clone()),
                });

                Ok(running.chain(completed).collect())
            }
            Some(TaskFilter::Running) => {
                let lock = self.running.read().await;

                let running = lock
                    .values()
                    .map(|running| TaskStatus::Running(running.clone()));

                Ok(running.collect())
            }
            Some(TaskFilter::Completed) => {
                let lock = self.completed.read().await;

                let completed = lock.values().filter_map(|status| match status {
                    Ok(task_status) => Some(TaskStatus::completed(task_status.clone())),
                    Err(_) => None,
                });

                Ok(completed.collect())
            }
            Some(TaskFilter::Failed) => {
                let lock = self.completed.read().await;

                let failed = lock.values().filter_map(|status| match status {
                    Ok(_) => None,
                    Err(task_status) => Some(TaskStatus::failed(task_status.clone())),
                });

                Ok(failed.collect())
            }
        }
    }
}

pub struct InMemoryTaskDbContext {
    task_id: TaskId,
    running: Db<TaskStatusMap>,
}

#[async_trait::async_trait]
impl TaskContext for InMemoryTaskDbContext {
    async fn set_completion(&self, pct_complete: u8, status: Box<dyn TaskStatusInfo>) {
        let mut running = self.running.write().await;

        if let Some(task_status) = running.get_mut(&self.task_id) {
            *task_status = RunningTaskStatusInfo::new(pct_complete, status);
        } else {
            // this case should not happen, so we ignore it
        }
    }
}
