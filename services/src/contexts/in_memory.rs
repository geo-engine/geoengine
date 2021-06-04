use std::sync::Arc;

use crate::error::Error;
use crate::{
    datasets::add_from_directory::{add_datasets_from_directory, add_providers_from_directory},
    error::Result,
    util::{dataset_defs_dir, provider_defs_dir},
};
use crate::{projects::hashmap_projectdb::HashMapProjectDb, workflows::registry::HashMapRegistry};
use async_trait::async_trait;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::{Context, Db, SimpleSession};
use super::{Session, SimpleContext};
use crate::contexts::{ExecutionContextImpl, QueryContextImpl, SessionId};
use crate::datasets::in_memory::HashMapDatasetDb;
use crate::util::config;
use geoengine_operators::concurrency::ThreadPool;

/// A context with references to in-memory versions of the individual databases.
#[derive(Clone, Default)]
pub struct InMemoryContext {
    project_db: Db<HashMapProjectDb>,
    workflow_registry: Db<HashMapRegistry>,
    dataset_db: Db<HashMapDatasetDb>,
    default_session: Db<SimpleSession>,
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
    type Session = SimpleSession;
    type ProjectDB = HashMapProjectDb;
    type WorkflowRegistry = HashMapRegistry;
    type DatasetDB = HashMapDatasetDb;
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<SimpleSession, HashMapDatasetDb>;

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
        Ok(QueryContextImpl::new(
            config::get_config_element::<config::QueryContext>()?.chunk_byte_size,
        ))
    }

    fn execution_context(&self, session: SimpleSession) -> Result<Self::ExecutionContext> {
        Ok(ExecutionContextImpl::<SimpleSession, HashMapDatasetDb> {
            dataset_db: self.dataset_db.clone(),
            thread_pool: self.thread_pool.clone(),
            session,
        })
    }

    async fn session_id_to_session(&self, session_id: SessionId) -> Result<Self::Session> {
        let default_session = self.default_session_ref().await;

        if default_session.id() != session_id {
            return Err(Error::Authorization {
                source: Box::new(Error::InvalidSession),
            });
        }

        Ok(default_session.clone())
    }
}

#[async_trait]
impl SimpleContext for InMemoryContext {
    fn default_session(&self) -> Db<SimpleSession> {
        self.default_session.clone()
    }

    async fn default_session_ref(&self) -> RwLockReadGuard<SimpleSession> {
        self.default_session.read().await
    }

    async fn default_session_ref_mut(&self) -> RwLockWriteGuard<SimpleSession> {
        self.default_session.write().await
    }

    // async fn default_session(&self) -> &SimpleSession {
    //     self.default_session.lock().await.borrow()
    // }

    // fn set_default_session(&mut self, session: SimpleSession) {
    //     *self.default_session.get_mut() = session;
    // }
}
