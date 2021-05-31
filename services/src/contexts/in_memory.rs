use crate::{
    datasets::add_from_directory::{add_datasets_from_directory, add_providers_from_directory},
    error::Result,
    util::{dataset_defs_dir, provider_defs_dir},
};
use crate::{
    projects::hashmap_projectdb::HashMapProjectDb, users::hashmap_userdb::HashMapUserDb,
    users::session::Session, workflows::registry::HashMapRegistry,
};
use async_trait::async_trait;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::{Context, Db};
use crate::contexts::{ExecutionContextImpl, QueryContextImpl};
use crate::datasets::in_memory::HashMapDatasetDb;
use crate::util::config;
use geoengine_operators::concurrency::ThreadPool;
use std::sync::Arc;

/// A context with references to in-memory versions of the individual databases.
#[derive(Clone, Default)]
pub struct InMemoryContext {
    user_db: Db<HashMapUserDb>,
    project_db: Db<HashMapProjectDb>,
    workflow_registry: Db<HashMapRegistry>,
    dataset_db: Db<HashMapDatasetDb>,
    session: Option<Session>,
    thread_pool: Arc<ThreadPool>,
}

impl InMemoryContext {
    #[allow(clippy::too_many_lines)]
    pub async fn new_with_data() -> Self {
        let mut db = HashMapDatasetDb::default();
        add_datasets_from_directory(&mut db, dataset_defs_dir()).await;
        add_providers_from_directory(&mut db, provider_defs_dir()).await;

        InMemoryContext {
            dataset_db: Arc::new(RwLock::new(db)),
            ..Default::default()
        }
    }
}

#[async_trait]
impl Context for InMemoryContext {
    type UserDB = HashMapUserDb;
    type ProjectDB = HashMapProjectDb;
    type WorkflowRegistry = HashMapRegistry;
    type DatasetDB = HashMapDatasetDb;
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<HashMapDatasetDb>;

    fn user_db(&self) -> Db<Self::UserDB> {
        self.user_db.clone()
    }
    async fn user_db_ref(&self) -> RwLockReadGuard<'_, Self::UserDB> {
        self.user_db.read().await
    }
    async fn user_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::UserDB> {
        self.user_db.write().await
    }

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
        Ok(QueryContextImpl {
            // TODO: load config only once
            chunk_byte_size: config::get_config_element::<config::QueryContext>()?.chunk_byte_size,
        })
    }

    fn execution_context(&self, session: &Session) -> Result<Self::ExecutionContext> {
        Ok(ExecutionContextImpl::<HashMapDatasetDb> {
            dataset_db: self.dataset_db.clone(),
            thread_pool: self.thread_pool.clone(),
            user: session.user.id,
        })
    }
}
