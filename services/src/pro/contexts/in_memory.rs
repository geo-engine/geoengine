use crate::contexts::QueryContextImpl;
use crate::error;
use crate::layers::add_from_directory::{
    add_layer_collections_from_directory, add_layers_from_directory,
};
use crate::layers::storage::{HashMapLayerDb, HashMapLayerProviderDb};
use crate::pro::contexts::{Context, ProContext};
use crate::pro::datasets::{add_datasets_from_directory, ProHashMapDatasetDb};
use crate::pro::projects::ProHashMapProjectDb;
use crate::pro::quota::{initialize_quota_tracking, QuotaTrackingFactory};
use crate::pro::users::{HashMapUserDb, OidcRequestDb, UserDb, UserSession};
use crate::pro::util::config::Oidc;
use crate::tasks::{SimpleTaskManager, SimpleTaskManagerContext};
use crate::workflows::registry::HashMapRegistry;
use crate::{datasets::add_from_directory::add_providers_from_directory, error::Result};
use async_trait::async_trait;
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::util::test::TestDefault;
use geoengine_datatypes::util::Identifier;
use geoengine_operators::engine::{ChunkByteSize, QueryContextExtensions};
use geoengine_operators::pro::meta::quota::{ComputationContext, QuotaChecker};
use geoengine_operators::util::create_rayon_thread_pool;
use rayon::ThreadPool;
use snafu::ResultExt;
use std::path::PathBuf;
use std::sync::Arc;

use super::{ExecutionContextImpl, QuotaCheckerImpl};

/// A context with references to in-memory versions of the individual databases.
#[derive(Clone)]
pub struct ProInMemoryContext {
    user_db: Arc<HashMapUserDb>,
    project_db: Arc<ProHashMapProjectDb>,
    workflow_registry: Arc<HashMapRegistry>,
    dataset_db: Arc<ProHashMapDatasetDb>,
    layer_db: Arc<HashMapLayerDb>,
    layer_provider_db: Arc<HashMapLayerProviderDb>,
    thread_pool: Arc<ThreadPool>,
    exe_ctx_tiling_spec: TilingSpecification,
    query_ctx_chunk_size: ChunkByteSize,
    task_manager: Arc<SimpleTaskManager>,
    oidc_request_db: Arc<Option<OidcRequestDb>>,
    quota: QuotaTrackingFactory,
}

impl TestDefault for ProInMemoryContext {
    fn test_default() -> Self {
        Self {
            user_db: Default::default(),
            project_db: Default::default(),
            workflow_registry: Default::default(),
            dataset_db: Default::default(),
            layer_db: Default::default(),
            layer_provider_db: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec: TestDefault::test_default(),
            query_ctx_chunk_size: TestDefault::test_default(),
            task_manager: Default::default(),
            oidc_request_db: Arc::new(None),
            quota: TestDefault::test_default(),
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
        oidc_config: Oidc,
    ) -> Self {
        let mut layer_db = HashMapLayerDb::default();

        add_layers_from_directory(&mut layer_db, layer_defs_path).await;
        add_layer_collections_from_directory(&mut layer_db, layer_collection_defs_path).await;

        let mut dataset_db = ProHashMapDatasetDb::default();
        add_datasets_from_directory(&mut dataset_db, dataset_defs_path).await;

        let mut layer_provider_db = HashMapLayerProviderDb::default();
        add_providers_from_directory(
            &mut layer_provider_db,
            provider_defs_path.clone(),
            &[provider_defs_path.join("pro")],
        )
        .await;

        let user_db = Arc::new(HashMapUserDb::default());
        let quota = initialize_quota_tracking(user_db.clone());

        Self {
            user_db,
            project_db: Default::default(),
            workflow_registry: Default::default(),
            dataset_db: Arc::new(dataset_db),
            layer_db: Arc::new(layer_db),
            layer_provider_db: Arc::new(layer_provider_db),
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            oidc_request_db: Arc::new(OidcRequestDb::try_from(oidc_config).ok()),
            quota,
        }
    }

    pub fn new_with_context_spec(
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
    ) -> Self {
        let user_db = Arc::new(HashMapUserDb::default());
        let quota = initialize_quota_tracking(user_db.clone());

        ProInMemoryContext {
            user_db,
            project_db: Default::default(),
            workflow_registry: Default::default(),
            dataset_db: Default::default(),
            layer_db: Default::default(),
            layer_provider_db: Default::default(),
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            oidc_request_db: Arc::new(None),
            quota,
        }
    }

    pub fn new_with_oidc(oidc_db: OidcRequestDb) -> Self {
        let user_db = Arc::new(HashMapUserDb::default());
        let quota = initialize_quota_tracking(user_db.clone());

        Self {
            user_db,
            project_db: Default::default(),
            workflow_registry: Default::default(),
            dataset_db: Default::default(),
            layer_db: Default::default(),
            layer_provider_db: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec: TestDefault::test_default(),
            query_ctx_chunk_size: TestDefault::test_default(),
            task_manager: Default::default(),
            oidc_request_db: Arc::new(Some(oidc_db)),
            quota,
        }
    }
}

#[async_trait]
impl ProContext for ProInMemoryContext {
    type UserDB = HashMapUserDb;
    type ProDatasetDB = ProHashMapDatasetDb;
    type ProProjectDB = ProHashMapProjectDb;

    fn user_db(&self) -> Arc<Self::UserDB> {
        self.user_db.clone()
    }
    fn user_db_ref(&self) -> &Self::UserDB {
        &self.user_db
    }
    fn oidc_request_db(&self) -> Option<&OidcRequestDb> {
        self.oidc_request_db.as_ref().as_ref()
    }

    fn pro_dataset_db(&self) -> Arc<Self::ProDatasetDB> {
        self.dataset_db.clone()
    }
    fn pro_dataset_db_ref(&self) -> &Self::ProDatasetDB {
        &self.dataset_db
    }

    fn pro_project_db(&self) -> Arc<Self::ProProjectDB> {
        self.project_db.clone()
    }
    fn pro_project_db_ref(&self) -> &Self::ProProjectDB {
        &self.project_db
    }
}

#[async_trait]
impl Context for ProInMemoryContext {
    type Session = UserSession;
    type ProjectDB = ProHashMapProjectDb;
    type WorkflowRegistry = HashMapRegistry;
    type DatasetDB = ProHashMapDatasetDb;
    type LayerDB = HashMapLayerDb;
    type LayerProviderDB = HashMapLayerProviderDb;
    type QueryContext = QueryContextImpl;
    type ExecutionContext =
        ExecutionContextImpl<ProHashMapDatasetDb, HashMapUserDb, HashMapLayerProviderDb>;
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

    fn layer_provider_db(&self) -> Arc<Self::LayerProviderDB> {
        self.layer_provider_db.clone()
    }
    fn layer_provider_db_ref(&self) -> &Self::LayerProviderDB {
        &self.layer_provider_db
    }

    fn tasks(&self) -> Arc<Self::TaskManager> {
        self.task_manager.clone()
    }
    fn tasks_ref(&self) -> &Self::TaskManager {
        &self.task_manager
    }

    fn query_context(&self, session: UserSession) -> Result<Self::QueryContext> {
        let mut extensions = QueryContextExtensions::default();
        extensions.insert(
            self.quota
                .create_quota_tracking(&session, ComputationContext::new()),
        );
        extensions.insert(Box::new(QuotaCheckerImpl {
            user_db: self.user_db.clone(),
            session,
        }) as QuotaChecker);

        Ok(QueryContextImpl::new_with_extensions(
            self.query_ctx_chunk_size,
            self.thread_pool.clone(),
            extensions,
        ))
    }

    fn execution_context(&self, session: UserSession) -> Result<Self::ExecutionContext> {
        Ok(ExecutionContextImpl::<
            ProHashMapDatasetDb,
            HashMapUserDb,
            HashMapLayerProviderDb,
        >::new(
            self.dataset_db.clone(),
            self.layer_provider_db.clone(),
            self.user_db.clone(),
            self.thread_pool.clone(),
            session,
            self.exe_ctx_tiling_spec,
        ))
    }

    async fn session_by_id(&self, session_id: crate::contexts::SessionId) -> Result<Self::Session> {
        self.user_db_ref()
            .session(session_id)
            .await
            .map_err(Box::new)
            .context(error::Authorization)
    }
}
