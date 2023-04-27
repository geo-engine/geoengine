use std::path::PathBuf;
use std::sync::Arc;

use super::{ApplicationContext, Db, GeoEngineDb, SessionContext, SimpleSession};
use super::{Session, SimpleApplicationContext};
use crate::contexts::{ExecutionContextImpl, QueryContextImpl, SessionId};
use crate::datasets::in_memory::HashMapDatasetDbBackend;
use crate::datasets::upload::{Volume, Volumes};
use crate::error::Error;
use crate::layers::add_from_directory::{
    add_layer_collections_from_directory, add_layers_from_directory,
};
use crate::layers::storage::{HashMapLayerDb, HashMapLayerProviderDbBackend};
use crate::projects::hashmap_projectdb::HashMapProjectDbBackend;
use crate::tasks::{SimpleTaskManager, SimpleTaskManagerBackend, SimpleTaskManagerContext};
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
    pub(crate) task_manager: Arc<SimpleTaskManagerBackend>,
    session: Db<SimpleSession>,
    thread_pool: Arc<ThreadPool>,
    exe_ctx_tiling_spec: TilingSpecification,
    query_ctx_chunk_size: ChunkByteSize,
    volumes: Volumes,
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
            volumes: Default::default(),
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
        let session = SimpleSession::default();

        let app_ctx = Self {
            db: Default::default(),
            task_manager: Default::default(),
            session: Arc::new(RwLock::new(session.clone())),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            thread_pool: create_rayon_thread_pool(0),
            volumes: Default::default(),
        };

        let ctx = app_ctx.session_context(session);

        let mut db = ctx.db();
        add_layers_from_directory(&mut db, layer_defs_path).await;
        add_layer_collections_from_directory(&mut db, layer_collection_defs_path).await;

        add_datasets_from_directory(&mut db, dataset_defs_path).await;

        add_providers_from_directory(&mut db, provider_defs_path, &[]).await;

        app_ctx
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
            volumes: Default::default(),
        }
    }
}

#[async_trait]
impl ApplicationContext for InMemoryContext {
    type SessionContext = InMemorySessionContext;
    type Session = SimpleSession;

    fn session_context(&self, session: Self::Session) -> Self::SessionContext {
        InMemorySessionContext {
            session,
            context: self.clone(),
        }
    }

    async fn session_by_id(&self, session_id: SessionId) -> Result<Self::Session> {
        let default_session = self.default_session_ref().await;

        if default_session.id() != session_id {
            return Err(Error::Unauthorized {
                source: Box::new(Error::InvalidSession),
            });
        }

        Ok(default_session.clone())
    }
}

#[derive(Clone)]
pub struct InMemorySessionContext {
    session: SimpleSession,
    context: InMemoryContext,
}

#[async_trait]
impl SessionContext for InMemorySessionContext {
    type Session = SimpleSession;
    type GeoEngineDB = InMemoryDb;
    type TaskContext = SimpleTaskManagerContext;
    type TaskManager = SimpleTaskManager;
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<Self::GeoEngineDB>;

    fn db(&self) -> Self::GeoEngineDB {
        InMemoryDb::new(self.context.db.clone())
    }

    fn tasks(&self) -> Self::TaskManager {
        SimpleTaskManager::new(self.context.task_manager.clone())
    }

    fn query_context(&self) -> Result<Self::QueryContext> {
        Ok(QueryContextImpl::new(
            self.context.query_ctx_chunk_size,
            self.context.thread_pool.clone(),
        ))
    }

    fn execution_context(&self) -> Result<Self::ExecutionContext> {
        Ok(ExecutionContextImpl::<InMemoryDb>::new(
            self.db(),
            self.context.thread_pool.clone(),
            self.context.exe_ctx_tiling_spec,
        ))
    }

    fn volumes(&self) -> Result<Vec<Volume>> {
        Ok(self.context.volumes.volumes.clone())
    }

    fn session(&self) -> &Self::Session {
        &self.session
    }
}

#[async_trait]
impl SimpleApplicationContext for InMemoryContext {
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
