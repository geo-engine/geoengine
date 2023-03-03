use std::path::PathBuf;
use std::sync::Arc;

use super::{Context, Db, GeoEngineDb, SimpleSession};
use super::{Session, SimpleContext};
use crate::contexts::{ExecutionContextImpl, QueryContextImpl, SessionId};
use crate::datasets::in_memory::HashMapDatasetDbBackend;
use crate::error::Error;
use crate::layers::add_from_directory::{
    add_layer_collections_from_directory, add_layers_from_directory,
};
use crate::layers::storage::{
    HashMapLayerDb, HashMapLayerDbBackend, HashMapLayerProviderDbBackend,
};
use crate::projects::hashmap_projectdb::HashMapProjectDbBackend;
use crate::tasks::{SimpleTaskManager, SimpleTaskManagerContext};
use crate::workflows::registry::HashMapRegistryBackend;
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
    db: Arc<InMemoryDbBackend>,
    task_manager: Arc<SimpleTaskManager>,
    session: Db<SimpleSession>,
    thread_pool: Arc<ThreadPool>,
    exe_ctx_tiling_spec: TilingSpecification,
    query_ctx_chunk_size: ChunkByteSize,
}

impl TestDefault for InMemoryContext {
    fn test_default() -> Self {
        Self {
            db: Default::default(),
            task_manager: Default::default(),
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
        layer_defs_path: PathBuf,
        layer_collection_defs_path: PathBuf,
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
    ) -> Self {
        let ctx = Self {
            db: Default::default(),
            task_manager: Default::default(),
            session: Default::default(),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            thread_pool: create_rayon_thread_pool(0),
        };

        let mut db = ctx.db(ctx.default_session().read().await.clone());
        add_layers_from_directory(&mut db, layer_defs_path).await;
        add_layer_collections_from_directory(&mut db, layer_collection_defs_path).await;

        add_datasets_from_directory(&mut db, dataset_defs_path).await;

        add_providers_from_directory(&mut db, provider_defs_path, &[]).await;

        ctx
    }

    pub fn new_with_context_spec(
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
    ) -> Self {
        Self {
            db: Default::default(),
            task_manager: Default::default(),
            session: Default::default(),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            thread_pool: create_rayon_thread_pool(0),
        }
    }
}

#[async_trait]
impl Context for InMemoryContext {
    type Session = SimpleSession;
    type GeoEngineDB = InMemoryDb;
    type TaskContext = SimpleTaskManagerContext;
    type TaskManager = SimpleTaskManager;
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<Self::GeoEngineDB>;

    fn db(&self, _session: Self::Session) -> Self::GeoEngineDB {
        InMemoryDb::new(self.db.clone())
    }

    fn tasks(&self) -> Arc<Self::TaskManager> {
        self.task_manager.clone()
    }
    fn tasks_ref(&self) -> &Self::TaskManager {
        &self.task_manager
    }

    fn query_context(&self, _session: SimpleSession) -> Result<Self::QueryContext> {
        Ok(QueryContextImpl::new(
            self.query_ctx_chunk_size,
            self.thread_pool.clone(),
        ))
    }

    fn execution_context(&self, session: SimpleSession) -> Result<Self::ExecutionContext> {
        Ok(ExecutionContextImpl::<InMemoryDb>::new(
            self.db(session),
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

#[derive(Default)]
pub struct InMemoryDbBackend {
    pub(crate) workflow_registry: RwLock<HashMapRegistryBackend>,
    pub(crate) dataset_db: RwLock<HashMapDatasetDbBackend>,
    pub(crate) project_db: RwLock<HashMapProjectDbBackend>,
    pub(crate) layer_db: HashMapLayerDb,
    pub(crate) layer_provider_db: RwLock<HashMapLayerProviderDbBackend>,
}

#[derive(Default, Clone)]
pub struct InMemoryDb {
    pub(crate) backend: Arc<InMemoryDbBackend>,
}

impl InMemoryDb {
    pub fn new(backend: Arc<InMemoryDbBackend>) -> Self {
        Self { backend }
    }
}

impl GeoEngineDb for InMemoryDb {}
