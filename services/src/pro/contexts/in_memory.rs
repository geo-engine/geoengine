use crate::contexts::{ExecutionContextImpl, QueryContextImpl};
use crate::error;
use crate::pro::contexts::{Context, ProContext};
use crate::pro::datasets::{add_datasets_from_directory, ProHashMapDatasetDb};
use crate::pro::projects::ProHashMapProjectDb;
use crate::pro::users::{HashMapUserDb, UserDb, UserSession};
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
    thread_pool: Arc<ThreadPool>,
    exe_ctx_tiling_spec: TilingSpecification,
    query_ctx_chunk_size: ChunkByteSize,
}

impl TestDefault for ProInMemoryContext {
    fn test_default() -> Self {
        Self {
            user_db: Default::default(),
            project_db: Default::default(),
            workflow_registry: Default::default(),
            dataset_db: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec: TestDefault::test_default(),
            query_ctx_chunk_size: TestDefault::test_default(),
        }
    }
}
impl ProInMemoryContext {
    #[allow(clippy::too_many_lines)]
    pub async fn new_with_data(
        dataset_defs_path: PathBuf,
        provider_defs_path: PathBuf,
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
    ) -> Self {
        let mut db = ProHashMapDatasetDb::default();
        add_datasets_from_directory(&mut db, dataset_defs_path).await;
        add_providers_from_directory(&mut db, provider_defs_path.clone()).await;
        add_providers_from_directory(&mut db, provider_defs_path.join("pro")).await;

        Self {
            user_db: Default::default(),
            project_db: Default::default(),
            workflow_registry: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            dataset_db: Arc::new(db),
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
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<UserSession, ProHashMapDatasetDb>;

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
