use crate::error;
use crate::error::Result;
use crate::{
    projects::hashmap_projectdb::HashMapProjectDB, users::hashmap_userdb::HashMapUserDB,
    users::session::Session, workflows::registry::HashMapRegistry,
};
use async_trait::async_trait;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use super::{Context, DB};

/// A context with references to in-memory versions of the individual databases.
#[derive(Clone, Default)]
pub struct InMemoryContext {
    user_db: DB<HashMapUserDB>,
    project_db: DB<HashMapProjectDB>,
    workflow_registry: DB<HashMapRegistry>,
    session: Option<Session>,
}

#[async_trait]
impl Context for InMemoryContext {
    type UserDB = HashMapUserDB;
    type ProjectDB = HashMapProjectDB;
    type WorkflowRegistry = HashMapRegistry;

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

    fn session(&self) -> Result<&Session> {
        self.session
            .as_ref()
            .ok_or(error::Error::SessionNotInitialized)
    }

    fn set_session(&mut self, session: Session) {
        self.session = Some(session)
    }
}
