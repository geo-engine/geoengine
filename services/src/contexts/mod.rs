use crate::error::Result;
use crate::{
    projects::projectdb::ProjectDB, users::session::Session, users::userdb::UserDB,
    workflows::registry::WorkflowRegistry,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
mod in_memory;
mod postgres;
use crate::datasets::storage::DataSetDB;

use crate::users::user::UserId;
use geoengine_datatypes::dataset::DataSetId;
use geoengine_operators::engine::{LoadingInfo, QueryContext};
pub use in_memory::InMemoryContext;
pub use postgres::PostgresContext;

type DB<T> = Arc<RwLock<T>>;

/// A context bundles access to shared resources like databases and session specific information
/// about the user to pass to the services handlers.
// TODO: avoid locking the individual DBs here IF they are already thread safe (e.g. guaranteed by postgres)
#[async_trait]
pub trait Context: 'static + Send + Sync + Clone {
    type UserDB: UserDB;
    type ProjectDB: ProjectDB;
    type WorkflowRegistry: WorkflowRegistry;
    type DataSetDB: DataSetDB;
    type QueryContext: QueryContext;

    fn user_db(&self) -> DB<Self::UserDB>;
    async fn user_db_ref(&self) -> RwLockReadGuard<Self::UserDB>;
    async fn user_db_ref_mut(&self) -> RwLockWriteGuard<Self::UserDB>;

    fn project_db(&self) -> DB<Self::ProjectDB>;
    async fn project_db_ref(&self) -> RwLockReadGuard<Self::ProjectDB>;
    async fn project_db_ref_mut(&self) -> RwLockWriteGuard<Self::ProjectDB>;

    fn workflow_registry(&self) -> DB<Self::WorkflowRegistry>;
    async fn workflow_registry_ref(&self) -> RwLockReadGuard<Self::WorkflowRegistry>;
    async fn workflow_registry_ref_mut(&self) -> RwLockWriteGuard<Self::WorkflowRegistry>;

    fn data_set_db(&self) -> DB<Self::DataSetDB>;
    async fn data_set_db_ref(&self) -> RwLockReadGuard<Self::DataSetDB>;
    async fn data_set_db_ref_mut(&self) -> RwLockWriteGuard<Self::DataSetDB>;

    fn session(&self) -> Result<&Session>;

    fn set_session(&mut self, session: Session);

    fn query_context(&self) -> Self::QueryContext;
}

pub struct QueryContextImpl<D>
where
    D: DataSetDB,
{
    chunk_byte_size: usize,
    data_set_db: DB<D>,
    user: UserId,
}

impl<D> QueryContext for QueryContextImpl<D>
where
    D: DataSetDB,
{
    fn chunk_byte_size(&self) -> usize {
        self.chunk_byte_size
    }

    // TODO: make async
    fn loading_info(
        &self,
        data_set: DataSetId,
    ) -> Result<LoadingInfo, geoengine_operators::error::Error> {
        futures::executor::block_on(async {
            match data_set {
                DataSetId::Internal(_id) => todo!(),
                DataSetId::External(id) => self
                    .data_set_db
                    .read()
                    .await
                    .data_set_provider(self.user, id.provider)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                        reason: e.to_string(),
                    })?
                    .loading_info(self.user, DataSetId::External(id))
                    .await
                    .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                        reason: e.to_string(),
                    }),
            }
        })
    }
}
