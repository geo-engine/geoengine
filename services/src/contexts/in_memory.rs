use std::path::PathBuf;
use std::sync::Arc;

use super::{Context, Db, SimpleSession};
use super::{Session, SimpleContext};
use crate::contexts::{ExecutionContextImpl, QueryContextImpl, SessionId};
use crate::datasets::in_memory::HashMapDatasetDb;
use crate::error::Error;
use crate::projects::hashmap_projectdb::HashMapProjectDb;
use crate::storage::InMemoryStore;
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
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// A context with references to in-memory versions of the individual databases.
#[derive(Clone)]
pub struct InMemoryContext {
    store: Db<InMemoryStore>,
    project_db: Db<HashMapProjectDb>,
    dataset_db: Db<HashMapDatasetDb>,
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
            dataset_db: Default::default(),
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
        let mut db = HashMapDatasetDb::default();
        add_datasets_from_directory(&mut db, dataset_defs_path).await;
        add_providers_from_directory(&mut db, provider_defs_path).await;

        Self {
            store: Default::default(),
            project_db: Default::default(),
            session: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            dataset_db: Arc::new(RwLock::new(db)),
        }
    }

    pub fn new_with_context_spec(
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
    ) -> Self {
        Self {
            store: Default::default(),
            project_db: Default::default(),
            dataset_db: Default::default(),
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

    fn store(&self) -> Db<Self::Store> {
        self.store.clone()
    }
    async fn store_ref(&self) -> RwLockReadGuard<'_, Self::Store> {
        self.store.read().await
    }
    async fn store_ref_mut(&self) -> RwLockWriteGuard<'_, Self::Store> {
        self.store.write().await
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
            chunk_byte_size: self.query_ctx_chunk_size,
            thread_pool: self.thread_pool.clone(),
        })
    }

    fn execution_context(&self, session: SimpleSession) -> Result<Self::ExecutionContext> {
        Ok(
            ExecutionContextImpl::<SimpleSession, HashMapDatasetDb>::new(
                self.dataset_db.clone(),
                self.thread_pool.clone(),
                session,
                self.exe_ctx_tiling_spec,
            ),
        )
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
