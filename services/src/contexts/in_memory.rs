use std::path::PathBuf;
use std::sync::Arc;

use super::{Context, Db, SimpleSession};
use super::{Session, SimpleContext};
use crate::contexts::{ExecutionContextImpl, QueryContextImpl, SessionId};
use crate::error::Error;
use crate::projects::hashmap_projectdb::HashMapProjectDb;
use crate::storage::in_memory::InMemoryStore;
use crate::storage::{GeoEngineStore, Storable, Store};
use crate::{
    datasets::add_from_directory::{add_datasets_from_directory, add_providers_from_directory},
    error::Result,
};
use async_trait::async_trait;
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::util::test::TestDefault;
use geoengine_operators::engine::ChunkByteSize;
use geoengine_operators::util::create_rayon_thread_pool;
use rayon::ThreadPool;
use tokio::sync::{RwLock, RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard};

/// A context with references to in-memory versions of the individual databases.
#[derive(Clone)]
pub struct InMemoryContext {
    store: Db<InMemoryStore>,
    project_db: Db<HashMapProjectDb>,
    session: Db<SimpleSession>,
    thread_pool: Arc<ThreadPool>,
    exe_ctx_tiling_spec: TilingSpecification,
    query_ctx_chunk_size: ChunkByteSize,
}

impl TestDefault for InMemoryContext {
    fn test_default() -> Self {
        Self {
            store: Default::default(),
            project_db: Default::default(),
            session: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec: TestDefault::test_default(),
            query_ctx_chunk_size: TestDefault::test_default(),
        }
    }
}

impl InMemoryContext {
    pub async fn new_with_data(
        dataset_defs_path: PathBuf,
        provider_defs_path: PathBuf,
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
    ) -> Self {
        let mut store = InMemoryStore::default();
        add_datasets_from_directory(&mut store, dataset_defs_path).await;
        add_providers_from_directory(&mut store, provider_defs_path).await;

        Self {
            store: Arc::new(RwLock::new(store)),
            project_db: Default::default(),
            session: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
        }
    }

    pub fn new_with_context_spec(
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
    ) -> Self {
        Self {
            store: Default::default(),
            project_db: Default::default(),
            session: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
        }
    }
}

#[async_trait]
impl Context for InMemoryContext {
    type Session = SimpleSession;
    type Store = InMemoryStore;
    type ProjectDB = HashMapProjectDb;
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<InMemoryStore>;

    fn project_db(&self) -> Db<Self::ProjectDB> {
        self.project_db.clone()
    }

    async fn project_db_ref(&self) -> RwLockReadGuard<'_, Self::ProjectDB> {
        self.project_db.read().await
    }
    async fn project_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::ProjectDB> {
        self.project_db.write().await
    }

    fn store(&self) -> Db<Self::Store> {
        self.store.clone()
    }

    async fn store_ref<S>(&self) -> RwLockReadGuard<'_, dyn Store<S>>
    where
        InMemoryStore: Store<S>,
        S: Storable,
    {
        let guard = self.store.read().await;
        RwLockReadGuard::map(guard, |s| s as &dyn Store<S>)
    }

    async fn store_ref_mut<S>(&self) -> RwLockMappedWriteGuard<'_, dyn Store<S>>
    where
        InMemoryStore: Store<S>,
        S: Storable,
    {
        let guard = self.store.write().await;
        RwLockWriteGuard::map(guard, |s| s as &mut dyn Store<S>)
    }

    fn query_context(&self) -> Result<Self::QueryContext> {
        Ok(QueryContextImpl {
            chunk_byte_size: self.query_ctx_chunk_size,
            thread_pool: self.thread_pool.clone(),
        })
    }

    fn execution_context(&self, session: SimpleSession) -> Result<Self::ExecutionContext> {
        Ok(ExecutionContextImpl::<InMemoryStore>::new(
            self.store.clone(),
            self.thread_pool.clone(),
            self.exe_ctx_tiling_spec,
        ))
    }

    async fn session_by_id(&self, session_id: SessionId) -> Result<Self::Session> {
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
        self.session.clone()
    }

    async fn default_session_ref(&self) -> RwLockReadGuard<SimpleSession> {
        self.session.read().await
    }

    async fn default_session_ref_mut(&self) -> RwLockWriteGuard<SimpleSession> {
        self.session.write().await
    }
}
