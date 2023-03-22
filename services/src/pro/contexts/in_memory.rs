use crate::contexts::{ApplicationContext, GeoEngineDb, QueryContextImpl, SessionId};
use crate::datasets::upload::{Volume, Volumes};
use crate::error;

use crate::layers::storage::{HashMapLayerDb, HashMapLayerProviderDbBackend};
use crate::pro::contexts::SessionContext;
use crate::pro::datasets::{add_datasets_from_directory, ProHashMapDatasetDbBackend};
use crate::pro::layers::add_from_directory::{
    add_layer_collections_from_directory, add_layers_from_directory,
};
use crate::pro::permissions::in_memory_permissiondb::InMemoryPermissionDbBackend;

use crate::pro::projects::ProHashMapProjectDbBackend;
use crate::pro::quota::{initialize_quota_tracking, QuotaTrackingFactory};
use crate::pro::tasks::{ProTaskManager, ProTaskManagerBackend};
use crate::pro::users::{HashMapUserDbBackend, OidcRequestDb, UserAuth, UserSession};
use crate::pro::util::config::Oidc;
use crate::tasks::SimpleTaskManagerContext;
use crate::workflows::registry::HashMapRegistryBackend;
use crate::{datasets::add_from_directory::add_providers_from_directory, error::Result};
use async_trait::async_trait;
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::util::test::TestDefault;
use geoengine_datatypes::util::Identifier;
use geoengine_operators::engine::{ChunkByteSize, QueryContextExtensions};
use geoengine_operators::pro::meta::quota::{ComputationContext, QuotaChecker};
use geoengine_operators::util::create_rayon_thread_pool;
use rayon::ThreadPool;
use snafu::{ensure, ResultExt};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::{ExecutionContextImpl, OidcRequestDbProvider, ProGeoEngineDb, QuotaCheckerImpl};

/// A context with references to in-memory versions of the individual databases.
#[derive(Clone)]
pub struct ProInMemoryContext {
    pub(crate) db: Arc<ProInMemoryDbBackend>,
    oidc_request_db: Arc<Option<OidcRequestDb>>,
    thread_pool: Arc<ThreadPool>,
    exe_ctx_tiling_spec: TilingSpecification,
    query_ctx_chunk_size: ChunkByteSize,
    task_manager: Arc<ProTaskManagerBackend>,
    quota: QuotaTrackingFactory,
    volumes: Volumes,
}

impl TestDefault for ProInMemoryContext {
    fn test_default() -> Self {
        Self {
            db: Default::default(),

            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec: TestDefault::test_default(),
            query_ctx_chunk_size: TestDefault::test_default(),
            task_manager: Default::default(),
            oidc_request_db: Arc::new(None),
            quota: TestDefault::test_default(),
            volumes: Default::default(),
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
        let db_backend = Arc::new(ProInMemoryDbBackend::default());

        let session = UserSession::admin_session();
        let db = ProInMemoryDb::new(db_backend.clone(), session.clone());
        let quota = initialize_quota_tracking(db);

        let app_ctx = Self {
            db: db_backend,
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            oidc_request_db: Arc::new(OidcRequestDb::try_from(oidc_config).ok()),
            quota,
            volumes: Default::default(),
        };

        let mut db = app_ctx.session_context(session).db();

        add_layers_from_directory(&mut db, layer_defs_path).await;
        add_layer_collections_from_directory(&mut db, layer_collection_defs_path).await;

        add_datasets_from_directory(&mut db, dataset_defs_path).await;

        add_providers_from_directory(
            &mut db,
            provider_defs_path.clone(),
            &[provider_defs_path.join("pro")],
        )
        .await;

        app_ctx
    }

    pub fn new_with_context_spec(
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
    ) -> Self {
        let db_backend = Arc::new(ProInMemoryDbBackend::default());

        let db = ProInMemoryDb::new(db_backend.clone(), UserSession::admin_session());
        let quota = initialize_quota_tracking(db);

        Self {
            db: db_backend,
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            oidc_request_db: Arc::new(None),
            quota,
            volumes: Default::default(),
        }
    }

    pub fn new_with_oidc(oidc_db: OidcRequestDb) -> Self {
        let db_backend = Arc::new(ProInMemoryDbBackend::default());

        let db = ProInMemoryDb::new(db_backend.clone(), UserSession::admin_session());
        let quota = initialize_quota_tracking(db);

        Self {
            db: db_backend,
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec: TestDefault::test_default(),
            query_ctx_chunk_size: TestDefault::test_default(),
            oidc_request_db: Arc::new(Some(oidc_db)),
            quota,
            volumes: Default::default(),
        }
    }
}

#[async_trait]
impl ApplicationContext for ProInMemoryContext {
    type SessionContext = ProInMemorySessionContext;
    type Session = UserSession;

    fn session_context(&self, session: Self::Session) -> Self::SessionContext {
        ProInMemorySessionContext {
            session,
            context: self.clone(),
        }
    }

    async fn session_by_id(&self, session_id: SessionId) -> Result<Self::Session> {
        self.user_session_by_id(session_id)
            .await
            .map_err(Box::new)
            .context(error::Authorization)
    }
}

#[async_trait]
impl OidcRequestDbProvider for ProInMemoryContext {
    fn oidc_request_db(&self) -> Option<&OidcRequestDb> {
        self.oidc_request_db.as_ref().as_ref()
    }
}

// TODO: use one single SessionContext struct for both free and pro? would have to be generic though
#[derive(Clone)]
pub struct ProInMemorySessionContext {
    session: UserSession,
    context: ProInMemoryContext,
}

#[async_trait]
impl SessionContext for ProInMemorySessionContext {
    type Session = UserSession;
    type GeoEngineDB = ProInMemoryDb;

    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<Self::GeoEngineDB>;
    type TaskContext = SimpleTaskManagerContext;
    type TaskManager = ProTaskManager;

    fn db(&self) -> Self::GeoEngineDB {
        ProInMemoryDb::new(self.context.db.clone(), self.session.clone())
    }

    fn tasks(&self) -> Self::TaskManager {
        ProTaskManager::new(self.context.task_manager.clone(), self.session.clone())
    }

    fn query_context(&self) -> Result<Self::QueryContext> {
        let mut extensions = QueryContextExtensions::default();
        extensions.insert(
            self.context
                .quota
                .create_quota_tracking(&self.session, ComputationContext::new()),
        );
        extensions.insert(Box::new(QuotaCheckerImpl { user_db: self.db() }) as QuotaChecker);

        Ok(QueryContextImpl::new_with_extensions(
            self.context.query_ctx_chunk_size,
            self.context.thread_pool.clone(),
            extensions,
        ))
    }

    fn execution_context(&self) -> Result<Self::ExecutionContext> {
        Ok(ExecutionContextImpl::<Self::GeoEngineDB>::new(
            self.db(),
            self.context.thread_pool.clone(),
            self.context.exe_ctx_tiling_spec,
        ))
    }

    fn volumes(&self) -> Result<Vec<Volume>> {
        ensure!(self.session.is_admin(), error::PermissionDenied);

        Ok(self.context.volumes.volumes.clone())
    }

    fn session(&self) -> &Self::Session {
        &self.session
    }
}

#[derive(Default)]
pub struct ProInMemoryDbBackend {
    pub user_db: RwLock<HashMapUserDbBackend>,
    pub permission_db: RwLock<InMemoryPermissionDbBackend>,
    pub project_db: RwLock<ProHashMapProjectDbBackend>,
    pub workflow_registry: RwLock<HashMapRegistryBackend>,
    pub dataset_db: RwLock<ProHashMapDatasetDbBackend>,
    pub layer_db: HashMapLayerDb,
    pub layer_provider_db: RwLock<HashMapLayerProviderDbBackend>,
}

pub struct ProInMemoryDb {
    // TODO: limit visibility
    pub(crate) backend: Arc<ProInMemoryDbBackend>,
    // TODO: limit visibility
    pub(crate) session: UserSession,
}

impl ProInMemoryDb {
    fn new(backend: Arc<ProInMemoryDbBackend>, session: UserSession) -> Self {
        Self { backend, session }
    }
}

impl GeoEngineDb for ProInMemoryDb {}

impl ProGeoEngineDb for ProInMemoryDb {}
