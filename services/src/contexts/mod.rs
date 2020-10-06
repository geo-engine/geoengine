use crate::error::Result;
use crate::{
    projects::projectdb::ProjectDB, users::session::Session, users::userdb::UserDB,
    workflows::registry::WorkflowRegistry,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

mod in_memory;

pub use in_memory::InMemoryContext;

type DB<T> = Arc<RwLock<T>>;

/// A context bundles access to shared resources like databases and session specific information
/// about the user to pass to the services handlers.
#[async_trait]
pub trait Context: 'static + Send + Sync + Clone {
    type UserDB: UserDB;
    type ProjectDB: ProjectDB;
    type WorkflowRegistry: WorkflowRegistry;

    fn user_db(&self) -> DB<Self::UserDB>;
    async fn user_db_ref(&self) -> RwLockReadGuard<Self::UserDB>;
    async fn user_db_ref_mut(&self) -> RwLockWriteGuard<Self::UserDB>;

    fn project_db(&self) -> DB<Self::ProjectDB>;
    async fn project_db_ref(&self) -> RwLockReadGuard<Self::ProjectDB>;
    async fn project_db_ref_mut(&self) -> RwLockWriteGuard<Self::ProjectDB>;

    fn workflow_registry(&self) -> DB<Self::WorkflowRegistry>;
    async fn workflow_registry_ref(&self) -> RwLockReadGuard<Self::WorkflowRegistry>;
    async fn workflow_registry_ref_mut(&self) -> RwLockWriteGuard<Self::WorkflowRegistry>;

    fn session(&self) -> Result<&Session>;

    fn set_session(&mut self, session: Session);
}
