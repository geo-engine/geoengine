mod in_memory;

#[cfg(feature = "postgres")]
mod postgres;

pub use in_memory::ProInMemoryContext;
#[cfg(feature = "postgres")]
pub use postgres::PostgresContext;
use std::sync::Arc;

use crate::contexts::{Context, Db};
use crate::pro::executor::{
    MultiLinestringDescription, MultiPointDescription, MultiPolygonDescription, PlotDescription,
};
use crate::pro::users::{UserDb, UserSession};
use crate::util::config::get_config_element;
use async_trait::async_trait;
use geoengine_operators::pro::executor::Executor;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

/// A pro contexts that extends the default context.
// TODO: avoid locking the individual DBs here IF they are already thread safe (e.g. guaranteed by postgres)
#[async_trait]
pub trait ProContext: Context<Session = UserSession> {
    type UserDB: UserDb;

    fn user_db(&self) -> Db<Self::UserDB>;
    async fn user_db_ref(&self) -> RwLockReadGuard<Self::UserDB>;
    async fn user_db_ref_mut(&self) -> RwLockWriteGuard<Self::UserDB>;
    fn task_manager(&self) -> TaskManager;
}

#[derive(Clone)]
pub struct TaskManager {
    executors: Arc<Executors>,
}

struct Executors {
    plot_executor: Executor<PlotDescription>,
    point_executor: Executor<MultiPointDescription>,
    line_executor: Executor<MultiLinestringDescription>,
    polygon_executor: Executor<MultiPolygonDescription>,
}

impl TaskManager {
    pub fn plot_executor(&self) -> &Executor<PlotDescription> {
        &self.executors.plot_executor
    }
    pub fn point_executor(&self) -> &Executor<MultiPointDescription> {
        &self.executors.point_executor
    }
    pub fn line_executor(&self) -> &Executor<MultiLinestringDescription> {
        &self.executors.line_executor
    }
    pub fn polygon_executor(&self) -> &Executor<MultiPolygonDescription> {
        &self.executors.polygon_executor
    }
}

impl Default for TaskManager {
    fn default() -> Self {
        let queue_size =
            get_config_element::<crate::util::config::Executor>().map_or(5, |it| it.queue_size);

        TaskManager {
            executors: Arc::new(Executors {
                plot_executor: Executor::new(queue_size),
                point_executor: Executor::new(queue_size),
                line_executor: Executor::new(queue_size),
                polygon_executor: Executor::new(queue_size),
            }),
        }
    }
}
