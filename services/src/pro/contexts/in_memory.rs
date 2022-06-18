use crate::contexts::{ExecutionContextImpl, QueryContextImpl};
use crate::error;
use crate::layers::add_from_directory::{
    add_layer_collections_from_directory, add_layers_from_directory,
};
use crate::layers::storage::HashMapLayerDb;
use crate::pro::contexts::{Context, ProContext};
use crate::pro::datasets::{add_datasets_from_directory, ProHashMapDatasetDb};
use crate::pro::projects::ProHashMapProjectDb;
use crate::pro::users::{HashMapUserDb, UserDb, UserSession};
use crate::tasks::{SimpleTaskManager, SimpleTaskManagerContext};
use crate::workflows::registry::HashMapRegistry;
use crate::{datasets::add_from_directory::add_providers_from_directory, error::Result};
use async_trait::async_trait;
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::util::test::TestDefault;
use geoengine_operators::engine::ChunkByteSize;
use geoengine_operators::util::create_rayon_thread_pool;
use rayon::ThreadPool;
use snafu::ResultExt;
use std::path::PathBuf;
use std::sync::Arc;

/// A context with references to in-memory versions of the individual databases.
#[derive(Clone)]
pub struct ProInMemoryContext {
    user_db: Arc<HashMapUserDb>,
    project_db: Arc<ProHashMapProjectDb>,
    workflow_registry: Arc<HashMapRegistry>,
    dataset_db: Arc<ProHashMapDatasetDb>,
    layer_db: Arc<HashMapLayerDb>,
    thread_pool: Arc<ThreadPool>,
    exe_ctx_tiling_spec: TilingSpecification,
    query_ctx_chunk_size: ChunkByteSize,
    task_manager: Arc<SimpleTaskManager>,
}

impl TestDefault for ProInMemoryContext {
    fn test_default() -> Self {
        Self {
            user_db: Default::default(),
            project_db: Default::default(),
            workflow_registry: Default::default(),
            dataset_db: Default::default(),
            layer_db: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec: TestDefault::test_default(),
            query_ctx_chunk_size: TestDefault::test_default(),
            task_manager: Default::default(),
        }
    }
}
impl ProInMemoryContext {
    #[allow(clippy::too_many_lines)]
    pub async fn new_with_data(
        dataset_defs_path: PathBuf,
        provider_defs_path: PathBuf,
        layer_defs_path: PathBuf,
        layer_collection_defs_path: PathBuf,
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
    ) -> Self {
        let mut workflow_db = HashMapRegistry::default();
        let mut layer_db = HashMapLayerDb::default();

        add_layers_from_directory(&mut layer_db, &mut workflow_db, layer_defs_path).await;
        add_layer_collections_from_directory(&mut layer_db, layer_collection_defs_path).await;

        let mut dataset_db = ProHashMapDatasetDb::default();
        add_datasets_from_directory(
            &mut dataset_db,
            &mut layer_db,
            &mut workflow_db,
            dataset_defs_path,
        )
        .await;
        add_providers_from_directory(&mut dataset_db, provider_defs_path.clone()).await;
        add_providers_from_directory(&mut dataset_db, provider_defs_path.join("pro")).await;

        Self {
            user_db: Default::default(),
            project_db: Default::default(),
            workflow_registry: Arc::new(workflow_db),
            dataset_db: Arc::new(dataset_db),
            layer_db: Arc::new(layer_db),
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
        }
    }

    pub fn new_with_context_spec(
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
    ) -> Self {
        ProInMemoryContext {
            user_db: Default::default(),
            project_db: Default::default(),
            workflow_registry: Default::default(),
            dataset_db: Default::default(),
            layer_db: Default::default(),
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
        }
    }
}

#[async_trait]
impl ProContext for ProInMemoryContext {
    type UserDB = HashMapUserDb;

    fn user_db(&self) -> Arc<Self::UserDB> {
        self.user_db.clone()
    }
    fn user_db_ref(&self) -> &Self::UserDB {
        &self.user_db
    }
}

#[async_trait]
impl Context for ProInMemoryContext {
    type Session = UserSession;
    type ProjectDB = ProHashMapProjectDb;
    type WorkflowRegistry = HashMapRegistry;
    type DatasetDB = ProHashMapDatasetDb;
    type LayerDB = HashMapLayerDb;
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<UserSession, ProHashMapDatasetDb>;
    type TaskContext = SimpleTaskManagerContext;
    type TaskManager = SimpleTaskManager;

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

    fn tasks(&self) -> Arc<Self::TaskManager> {
        self.task_manager.clone()
    }
    fn tasks_ref(&self) -> &Self::TaskManager {
        &self.task_manager
    }

    fn query_context(&self) -> Result<Self::QueryContext> {
        Ok(QueryContextImpl::new(
            self.query_ctx_chunk_size,
            self.thread_pool.clone(),
        ))
    }

    fn execution_context(&self, session: UserSession) -> Result<Self::ExecutionContext> {
        Ok(
            ExecutionContextImpl::<UserSession, ProHashMapDatasetDb>::new(
                self.dataset_db.clone(),
                self.thread_pool.clone(),
                session,
                self.exe_ctx_tiling_spec,
            ),
        )
    }

    async fn session_by_id(&self, session_id: crate::contexts::SessionId) -> Result<Self::Session> {
        self.user_db_ref()
            .session(session_id)
            .await
            .map_err(Box::new)
            .context(error::Authorization)
    }
}
