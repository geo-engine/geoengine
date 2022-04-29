use std::path::PathBuf;
use std::sync::Arc;

use super::{Context, Db, SimpleSession};
use super::{Session, SimpleContext};
use crate::contexts::{ExecutionContextImpl, QueryContextImpl, SessionId};
use crate::datasets::in_memory::HashMapDatasetDb;
use crate::error::Error;
use crate::layers::add_from_directory::{
    add_layer_collections_from_directory, add_layers_from_directory,
};
use crate::layers::storage::HashMapLayerDb;
use crate::{
    datasets::add_from_directory::{add_datasets_from_directory, add_providers_from_directory},
    error::Result,
};
use crate::{projects::hashmap_projectdb::HashMapProjectDb, workflows::registry::HashMapRegistry};
use async_trait::async_trait;
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::util::test::TestDefault;
use geoengine_operators::engine::ChunkByteSize;
use geoengine_operators::util::create_rayon_thread_pool;
use rayon::ThreadPool;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

/// A context with references to in-memory versions of the individual databases.
#[derive(Clone)]
pub struct InMemoryContext {
    project_db: Arc<HashMapProjectDb>,
    workflow_registry: Arc<HashMapRegistry>,
    dataset_db: Arc<HashMapDatasetDb>,
    layer_db: Arc<HashMapLayerDb>,
    session: Db<SimpleSession>,
    thread_pool: Arc<ThreadPool>,
    exe_ctx_tiling_spec: TilingSpecification,
    query_ctx_chunk_size: ChunkByteSize,
}

impl TestDefault for InMemoryContext {
    fn test_default() -> Self {
        Self {
            project_db: Default::default(),
            workflow_registry: Default::default(),
            dataset_db: Default::default(),
            layer_db: Default::default(),
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
        let mut dataset_db = HashMapDatasetDb::default();
        add_datasets_from_directory(&mut dataset_db, dataset_defs_path).await;
        add_providers_from_directory(&mut dataset_db, provider_defs_path).await;

        let mut workflow_registry = HashMapRegistry::default();
        let mut layer_db = HashMapLayerDb::default();
        add_layers_from_directory(&mut layer_db, &mut workflow_registry, layer_defs_path).await;
        add_layer_collections_from_directory(&mut layer_db, layer_collection_defs_path).await;

        Self {
            project_db: Default::default(),
            workflow_registry: Arc::new(workflow_registry),
            layer_db: Arc::new(layer_db),
            session: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            dataset_db: Arc::new(dataset_db),
        }
    }

    pub fn new_with_context_spec(
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
    ) -> Self {
        Self {
            project_db: Default::default(),
            workflow_registry: Default::default(),
            dataset_db: Default::default(),
            layer_db: Default::default(),
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
    type ProjectDB = HashMapProjectDb;
    type WorkflowRegistry = HashMapRegistry;
    type DatasetDB = HashMapDatasetDb;
    type LayerDB = HashMapLayerDb;
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<SimpleSession, HashMapDatasetDb>;

    fn project_db(&self) -> Arc<Self::ProjectDB> {
        self.project_db.clone()
    }
    fn project_db_ref(&self) -> &Self::ProjectDB {
        &self.project_db
    }

    fn workflow_registry(&self) -> Arc<Self::WorkflowRegistry> {
        self.workflow_registry.clone()
    }
    fn workflow_registry_ref(&self) -> &Self::WorkflowRegistry {
        &self.workflow_registry
    }

    fn dataset_db(&self) -> Arc<Self::DatasetDB> {
        self.dataset_db.clone()
    }
    fn dataset_db_ref(&self) -> &Self::DatasetDB {
        &self.dataset_db
    }

    fn layer_db(&self) -> Arc<Self::LayerDB> {
        self.layer_db.clone()
    }
    fn layer_db_ref(&self) -> &Self::LayerDB {
        &self.layer_db
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
