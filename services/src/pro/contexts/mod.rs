mod in_memory;

#[cfg(feature = "postgres")]
mod postgres;

pub use in_memory::ProInMemoryContext;
#[cfg(feature = "postgres")]
pub use postgres::PostgresContext;
use std::sync::Arc;

use crate::contexts::{Context, Db};
use crate::pro::users::{UserDb, UserSession};
use crate::util::config::get_config_element;
use crate::workflows::workflow::WorkflowId;
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
    plot_executor:
        Arc<Executor<WorkflowId, crate::error::Result<crate::handlers::plots::WrappedPlotOutput>>>,
}

impl TaskManager {
    pub fn plot_executor(
        &self,
    ) -> &Executor<WorkflowId, crate::error::Result<crate::handlers::plots::WrappedPlotOutput>>
    {
        self.plot_executor.as_ref()
    }
}

impl Default for TaskManager {
    fn default() -> Self {
        let queue_size =
            get_config_element::<crate::util::config::Executor>().map_or(5, |it| it.queue_size);

        TaskManager {
            plot_executor: Arc::new(Executor::new(queue_size)),
        }
    }
}
