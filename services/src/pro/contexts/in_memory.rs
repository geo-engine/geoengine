use crate::contexts::{ExecutionContextImpl, QueryContextImpl};
use crate::error;
use crate::pro::contexts::{Context, Db, ProContext};
use crate::pro::datasets::ProHashMapDatasetDb;
use crate::pro::projects::ProHashMapProjectDb;
use crate::pro::users::{HashMapUserDb, UserDb, UserSession};
use crate::util::config;
use crate::workflows::registry::HashMapRegistry;
use crate::{
    datasets::add_from_directory::{add_datasets_from_directory, add_providers_from_directory},
    error::Result,
};
use async_trait::async_trait;
use geoengine_operators::util::create_rayon_thread_pool;
use rayon::ThreadPool;
use snafu::ResultExt;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// A context with references to in-memory versions of the individual databases.
#[derive(Clone)]
pub struct ProInMemoryContext {
    user_db: Db<HashMapUserDb>,
    project_db: Db<ProHashMapProjectDb>,
    workflow_registry: Db<HashMapRegistry>,
    dataset_db: Db<ProHashMapDatasetDb>,
    thread_pool: Arc<ThreadPool>,
}

impl Default for ProInMemoryContext {
    fn default() -> Self {
        Self {
            user_db: Default::default(),
            project_db: Default::default(),
            workflow_registry: Default::default(),
            dataset_db: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
        }
    }
}

impl ProInMemoryContext {
    #[allow(clippy::too_many_lines)]
    pub async fn new_with_data(dataset_defs_path: PathBuf, provider_defs_path: PathBuf) -> Self {
        let mut db = ProHashMapDatasetDb::default();
        add_datasets_from_directory(&mut db, dataset_defs_path).await;
        add_providers_from_directory(&mut db, provider_defs_path.clone()).await;
        add_providers_from_directory(&mut db, provider_defs_path.join("pro")).await;

        Self {
            dataset_db: Arc::new(RwLock::new(db)),
            ..Default::default()
        }
    }
}

#[async_trait]
impl ProContext for ProInMemoryContext {
    type UserDB = HashMapUserDb;

    fn user_db(&self) -> Db<Self::UserDB> {
        self.user_db.clone()
    }
    async fn user_db_ref(&self) -> RwLockReadGuard<'_, Self::UserDB> {
        self.user_db.read().await
    }
    async fn user_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::UserDB> {
        self.user_db.write().await
    }
}

#[async_trait]
impl Context for ProInMemoryContext {
    type Session = UserSession;
    type ProjectDB = ProHashMapProjectDb;
    type WorkflowRegistry = HashMapRegistry;
    type DatasetDB = ProHashMapDatasetDb;
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<UserSession, ProHashMapDatasetDb>;

    fn project_db(&self) -> Db<Self::ProjectDB> {
        self.project_db.clone()
    }
    async fn project_db_ref(&self) -> RwLockReadGuard<'_, Self::ProjectDB> {
        self.project_db.read().await
    }
    async fn project_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::ProjectDB> {
        self.project_db.write().await
    }

    fn workflow_registry(&self) -> Db<Self::WorkflowRegistry> {
        self.workflow_registry.clone()
    }
    async fn workflow_registry_ref(&self) -> RwLockReadGuard<'_, Self::WorkflowRegistry> {
        self.workflow_registry.read().await
    }
    async fn workflow_registry_ref_mut(&self) -> RwLockWriteGuard<'_, Self::WorkflowRegistry> {
        self.workflow_registry.write().await
    }

    fn dataset_db(&self) -> Db<Self::DatasetDB> {
        self.dataset_db.clone()
    }
    async fn dataset_db_ref(&self) -> RwLockReadGuard<'_, Self::DatasetDB> {
        self.dataset_db.read().await
    }
    async fn dataset_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::DatasetDB> {
        self.dataset_db.write().await
    }

    fn query_context(&self) -> Result<Self::QueryContext> {
        // TODO: load config only once
        Ok(QueryContextImpl {
            chunk_byte_size: config::get_config_element::<config::QueryContext>()?.chunk_byte_size,
            thread_pool: self.thread_pool.clone(),
        })
    }

    fn execution_context(&self, session: UserSession) -> Result<Self::ExecutionContext> {
        Ok(
            ExecutionContextImpl::<UserSession, ProHashMapDatasetDb>::new(
                self.dataset_db.clone(),
                self.thread_pool.clone(),
                session,
            ),
        )
    }

    async fn session_by_id(&self, session_id: crate::contexts::SessionId) -> Result<Self::Session> {
        self.user_db_ref()
            .await
            .session(session_id)
            .await
            .map_err(Box::new)
            .context(error::Authorization)
    }
}
