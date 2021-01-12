use crate::error;
use crate::error::Result;
use crate::{
    projects::hashmap_projectdb::HashMapProjectDB, users::hashmap_userdb::HashMapUserDB,
    users::session::Session, workflows::registry::HashMapRegistry,
};
use async_trait::async_trait;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use super::{Context, DB};
use crate::contexts::{ExecutionContextImpl, QueryContextImpl};
use crate::datasets::in_memory::HashmapDataSetDB;
use geoengine_operators::concurrency::ThreadPool;
use std::sync::Arc;

/// A context with references to in-memory versions of the individual databases.
#[derive(Clone, Default)]
pub struct InMemoryContext {
    user_db: DB<HashMapUserDB>,
    project_db: DB<HashMapProjectDB>,
    workflow_registry: DB<HashMapRegistry>,
    data_set_db: DB<HashmapDataSetDB>,
    session: Option<Session>,
    thread_pool: Arc<ThreadPool>,
}

#[async_trait]
impl Context for InMemoryContext {
    type UserDB = HashMapUserDB;
    type ProjectDB = HashMapProjectDB;
    type WorkflowRegistry = HashMapRegistry;
    type DataSetDB = HashmapDataSetDB;
    type QueryContext = QueryContextImpl<HashmapDataSetDB>;
    type ExecutionContext = ExecutionContextImpl<HashmapDataSetDB>;

    fn user_db(&self) -> DB<Self::UserDB> {
        self.user_db.clone()
    }
    async fn user_db_ref(&self) -> RwLockReadGuard<'_, Self::UserDB> {
        self.user_db.read().await
    }
    async fn user_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::UserDB> {
        self.user_db.write().await
    }

    fn project_db(&self) -> DB<Self::ProjectDB> {
        self.project_db.clone()
    }
    async fn project_db_ref(&self) -> RwLockReadGuard<'_, Self::ProjectDB> {
        self.project_db.read().await
    }
    async fn project_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::ProjectDB> {
        self.project_db.write().await
    }

    fn workflow_registry(&self) -> DB<Self::WorkflowRegistry> {
        self.workflow_registry.clone()
    }
    async fn workflow_registry_ref(&self) -> RwLockReadGuard<'_, Self::WorkflowRegistry> {
        self.workflow_registry.read().await
    }
    async fn workflow_registry_ref_mut(&self) -> RwLockWriteGuard<'_, Self::WorkflowRegistry> {
        self.workflow_registry.write().await
    }

    fn data_set_db(&self) -> DB<Self::DataSetDB> {
        self.data_set_db.clone()
    }
    async fn data_set_db_ref(&self) -> RwLockReadGuard<'_, Self::DataSetDB> {
        self.data_set_db.read().await
    }
    async fn data_set_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::DataSetDB> {
        self.data_set_db.write().await
    }

    fn session(&self) -> Result<&Session> {
        self.session
            .as_ref()
            .ok_or(error::Error::SessionNotInitialized)
    }

    fn set_session(&mut self, session: Session) {
        self.session = Some(session)
    }

    fn query_context(&self) -> Self::QueryContext {
        QueryContextImpl::<HashmapDataSetDB> {
            // TODO: use production config and test config sizes here
            chunk_byte_size: 1024,
            data_set_db: self.data_set_db.clone(),
            user: self.session().unwrap().user.id, // TODO: error handling? or need initialized session...
        }
    }

    fn execution_context(&self) -> Self::ExecutionContext {
        ExecutionContextImpl::<HashmapDataSetDB> {
            data_set_db: self.data_set_db.clone(),
            thread_pool: self.thread_pool.clone(),
        }
    }
}
