use crate::api::model::services::Volume;
use crate::config::{Cache, QuotaTrackingMode, get_config_element};
use crate::datasets::external::WildliveDbCache;
use crate::datasets::external::netcdfcf::NetCdfCfProviderDb;
use crate::datasets::storage::DatasetDb;
use crate::error::Result;
use crate::layers::listing::LayerCollectionProvider;
use crate::layers::storage::{LayerDb, LayerProviderDb};
use crate::machine_learning::MlModelDb;
use crate::permissions::PermissionDb;
use crate::tasks::{TaskContext, TaskManager};
use crate::users::{OidcManager, RoleDb, UserAuth, UserDb};
use crate::{projects::ProjectDb, workflows::registry::WorkflowRegistry};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataId, DataProviderId, ExternalDataId, LayerId};
use geoengine_datatypes::machine_learning::{MlModelMetadata, MlModelName};
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_operators::cache::cache_operator::InitializedCacheOperator;
use geoengine_operators::cache::shared_cache::SharedCache;
use geoengine_operators::engine::{
    ChunkByteSize, CreateSpan, ExecutionContext, InitializedPlotOperator,
    InitializedVectorOperator, MetaData, MetaDataProvider, QueryAbortRegistration,
    QueryAbortTrigger, QueryContext, RasterResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::meta::quota::{QuotaCheck, QuotaChecker, QuotaTracking};
use geoengine_operators::meta::wrapper::InitializedOperatorWrapper;
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
use rayon::ThreadPool;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

pub use migrations::{
    CurrentSchemaMigration, DatabaseVersion, Migration, MigrationResult, initialize_database,
    migrate_database,
};
pub use postgres::PostgresDb;
pub use postgres::{PostgresContext, PostgresSessionContext};
pub use session::{Session, SessionId};

mod db_types;
pub(crate) mod migrations;
mod postgres;
mod session;

pub type Db<T> = Arc<RwLock<T>>;

/// The application context bundles shared resources.
/// It is passed to API handlers and allows creating a session context that provides access to resources.
#[async_trait]
pub trait ApplicationContext: 'static + Send + Sync + Clone + UserAuth {
    type SessionContext: SessionContext;
    type Session: Session + Clone;

    /// Create a new session context for the given session.
    fn session_context(&self, session: Self::Session) -> Self::SessionContext;

    /// Load a session by its id
    async fn session_by_id(&self, session_id: SessionId) -> Result<Self::Session>;

    fn oidc_manager(&self) -> &OidcManager;
}

/// The session context bundles resources that are specific to a session.
#[async_trait]
pub trait SessionContext: 'static + Send + Sync + Clone {
    type Session: Session + Clone;
    type GeoEngineDB: GeoEngineDb;
    type QueryContext: QueryContext;
    type ExecutionContext: ExecutionContext;
    type TaskContext: TaskContext;
    type TaskManager: TaskManager<Self::TaskContext>;

    /// Get the db for accessing resources
    fn db(&self) -> Self::GeoEngineDB;

    /// Get the task manager for accessing tasks
    fn tasks(&self) -> Self::TaskManager;

    /// Create a new query context for executing queries on processors
    // TODO: assign computation id inside SessionContext or let it be provided from outside?
    fn query_context(&self, workflow: Uuid, computation: Uuid) -> Result<Self::QueryContext>;

    /// Create a new execution context initializing operators
    fn execution_context(&self) -> Result<Self::ExecutionContext>;

    /// Get the list of available data volumes
    fn volumes(&self) -> Result<Vec<Volume>>;

    /// Get the current session
    fn session(&self) -> &Self::Session;
}

/// The trait for accessing all resources
pub trait GeoEngineDb:
    std::fmt::Debug
    + DatasetDb
    + LayerCollectionProvider
    + LayerDb
    + LayerProviderDb
    + MlModelDb
    + NetCdfCfProviderDb
    + WildliveDbCache
    + PermissionDb
    + ProjectDb
    + RoleDb
    + UserDb
    + WorkflowRegistry
{
}

pub struct QueryContextImpl {
    chunk_byte_size: ChunkByteSize,
    tiling_specification: TilingSpecification,
    thread_pool: Arc<ThreadPool>,
    cache: Option<Arc<SharedCache>>,
    quota_tracking: Option<QuotaTracking>,
    quota_checker: Option<QuotaChecker>,
    abort_registration: QueryAbortRegistration,
    abort_trigger: Option<QueryAbortTrigger>,
}

impl QueryContextImpl {
    pub fn new(
        chunk_byte_size: ChunkByteSize,
        tiling_specification: TilingSpecification,
        thread_pool: Arc<ThreadPool>,
    ) -> Self {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        QueryContextImpl {
            chunk_byte_size,
            tiling_specification,
            thread_pool,
            cache: None,
            quota_tracking: None,
            quota_checker: None,
            abort_registration,
            abort_trigger: Some(abort_trigger),
        }
    }

    pub fn new_with_extensions(
        chunk_byte_size: ChunkByteSize,
        tiling_specification: TilingSpecification,
        thread_pool: Arc<ThreadPool>,
        cache: Option<Arc<SharedCache>>,
        quota_tracking: Option<QuotaTracking>,
        quota_checker: Option<QuotaChecker>,
    ) -> Self {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        QueryContextImpl {
            chunk_byte_size,
            tiling_specification,
            thread_pool,
            cache,
            quota_checker,
            quota_tracking,
            abort_registration,
            abort_trigger: Some(abort_trigger),
        }
    }
}

impl QueryContext for QueryContextImpl {
    fn chunk_byte_size(&self) -> ChunkByteSize {
        self.chunk_byte_size
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.thread_pool
    }

    fn abort_registration(&self) -> &QueryAbortRegistration {
        &self.abort_registration
    }

    fn abort_trigger(&mut self) -> geoengine_operators::util::Result<QueryAbortTrigger> {
        self.abort_trigger
            .take()
            .ok_or(geoengine_operators::error::Error::AbortTriggerAlreadyUsed)
    }

    fn tiling_specification(&self) -> TilingSpecification {
        self.tiling_specification
    }

    fn quota_tracking(&self) -> Option<&geoengine_operators::meta::quota::QuotaTracking> {
        self.quota_tracking.as_ref()
    }

    fn quota_checker(&self) -> Option<&geoengine_operators::meta::quota::QuotaChecker> {
        self.quota_checker.as_ref()
    }

    fn cache(&self) -> Option<Arc<geoengine_operators::cache::shared_cache::SharedCache>> {
        self.cache.clone()
    }
}

pub struct ExecutionContextImpl<D>
where
    D: DatasetDb + LayerProviderDb,
{
    db: D,
    thread_pool: Arc<ThreadPool>,
    tiling_specification: TilingSpecification,
}

impl<D> ExecutionContextImpl<D>
where
    D: DatasetDb + LayerProviderDb,
{
    pub fn new(
        db: D,
        thread_pool: Arc<ThreadPool>,
        tiling_specification: TilingSpecification,
    ) -> Self {
        Self {
            db,
            thread_pool,
            tiling_specification,
        }
    }
}

#[async_trait::async_trait]
impl<D> ExecutionContext for ExecutionContextImpl<D>
where
    D: DatasetDb
        + MetaDataProvider<
            MockDatasetDataSourceLoadingInfo,
            VectorResultDescriptor,
            VectorQueryRectangle,
        > + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
        + MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
        + LayerProviderDb
        + MlModelDb,
{
    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.thread_pool
    }

    fn tiling_specification(&self) -> TilingSpecification {
        self.tiling_specification
    }

    fn wrap_initialized_raster_operator(
        &self,
        op: Box<dyn geoengine_operators::engine::InitializedRasterOperator>,
        span: CreateSpan,
    ) -> Box<dyn geoengine_operators::engine::InitializedRasterOperator> {
        let wrapped = Box::new(InitializedOperatorWrapper::new(op, span))
            as Box<dyn geoengine_operators::engine::InitializedRasterOperator>;

        if get_config_element::<Cache>()
            .expect(
                "Cache config should be present because it is part of the Settings-default.toml",
            )
            .enabled
        {
            return Box::new(InitializedCacheOperator::new(wrapped));
        }

        wrapped
    }

    fn wrap_initialized_vector_operator(
        &self,
        op: Box<dyn InitializedVectorOperator>,
        span: CreateSpan,
    ) -> Box<dyn InitializedVectorOperator> {
        let wrapped = Box::new(InitializedOperatorWrapper::new(op, span))
            as Box<dyn InitializedVectorOperator>;

        if get_config_element::<Cache>()
            .expect(
                "Cache config should be present because it is part of the Settings-default.toml",
            )
            .enabled
        {
            return Box::new(InitializedCacheOperator::new(wrapped));
        }

        wrapped
    }

    fn wrap_initialized_plot_operator(
        &self,
        op: Box<dyn InitializedPlotOperator>,
        _span: CreateSpan,
    ) -> Box<dyn InitializedPlotOperator> {
        // as plots do not produce a stream of results, we have nothing to count for now
        op
    }

    async fn resolve_named_data(
        &self,
        data: &geoengine_datatypes::dataset::NamedData,
    ) -> Result<geoengine_datatypes::dataset::DataId, geoengine_operators::error::Error> {
        if let Some(provider) = &data.provider {
            // TODO: resolve provider name to provider id
            let provider_id = DataProviderId::from_str(provider)?;

            let data_id = ExternalDataId {
                provider_id,
                layer_id: LayerId(data.name.clone()),
            };

            return Ok(data_id.into());
        }

        let dataset_id = self
            .db
            .resolve_dataset_name_to_id(&data.into())
            .await
            .map_err(
                |source| geoengine_operators::error::Error::CannotResolveDatasetName {
                    name: data.clone(),
                    source: Box::new(source),
                },
            )?;

        // handle the case where the dataset name is not known
        let dataset_id = dataset_id
            .ok_or(geoengine_operators::error::Error::UnknownDatasetName { name: data.clone() })?;

        Ok(dataset_id.into())
    }

    async fn ml_model_metadata(
        &self,
        name: &MlModelName,
    ) -> Result<MlModelMetadata, geoengine_operators::error::Error> {
        self.db
            .load_model(&(name.clone().into()))
            .await
            .map_err(
                |source| geoengine_operators::error::Error::CannotResolveMlModelName {
                    name: name.clone(),
                    source: Box::new(source),
                },
            )?
            .metadata_for_operator()
            .map_err(
                |source| geoengine_operators::error::Error::LoadingMlMetadataFailed {
                    source: Box::new(source),
                },
            )
    }
}

// TODO: use macro(?) for delegating meta_data function to DatasetDB to avoid redundant code
#[async_trait]
impl<D>
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for ExecutionContextImpl<D>
where
    D: DatasetDb
        + MetaDataProvider<
            MockDatasetDataSourceLoadingInfo,
            VectorResultDescriptor,
            VectorQueryRectangle,
        > + LayerProviderDb,
{
    async fn meta_data(
        &self,
        data_id: &DataId,
    ) -> Result<
        Box<
            dyn MetaData<
                    MockDatasetDataSourceLoadingInfo,
                    VectorResultDescriptor,
                    VectorQueryRectangle,
                >,
        >,
        geoengine_operators::error::Error,
    > {
        match data_id {
            DataId::Internal { dataset_id: _ } => {
                self.db.meta_data(&data_id.clone()).await.map_err(|e| {
                    geoengine_operators::error::Error::LoadingInfo {
                        source: Box::new(e),
                    }
                })
            }
            DataId::External(external) => {
                self.db
                    .load_layer_provider(external.provider_id)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?
                    .meta_data(data_id)
                    .await
            }
        }
    }
}

// TODO: use macro(?) for delegating meta_data function to DatasetDB to avoid redundant code
#[async_trait]
impl<D> MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for ExecutionContextImpl<D>
where
    D: DatasetDb
        + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
        + LayerProviderDb,
{
    async fn meta_data(
        &self,
        data_id: &DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        match data_id {
            DataId::Internal { dataset_id: _ } => {
                self.db.meta_data(&data_id.clone()).await.map_err(|e| {
                    geoengine_operators::error::Error::LoadingInfo {
                        source: Box::new(e),
                    }
                })
            }
            DataId::External(external) => {
                self.db
                    .load_layer_provider(external.provider_id)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?
                    .meta_data(data_id)
                    .await
            }
        }
    }
}

// TODO: use macro(?) for delegating meta_data function to DatasetDB to avoid redundant code
#[async_trait]
impl<D> MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for ExecutionContextImpl<D>
where
    D: DatasetDb
        + MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
        + LayerProviderDb,
{
    async fn meta_data(
        &self,
        data_id: &DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        match data_id {
            DataId::Internal { dataset_id: _ } => {
                self.db.meta_data(&data_id.clone()).await.map_err(|e| {
                    geoengine_operators::error::Error::LoadingInfo {
                        source: Box::new(e),
                    }
                })
            }
            DataId::External(external) => {
                self.db
                    .load_layer_provider(external.provider_id)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?
                    .meta_data(data_id)
                    .await
            }
        }
    }
}

pub struct QuotaCheckerImpl<U: UserDb> {
    pub(crate) user_db: U,
}

#[async_trait]
impl<U: UserDb> QuotaCheck for QuotaCheckerImpl<U> {
    async fn ensure_quota_available(&self) -> geoengine_operators::util::Result<()> {
        // TODO: cache the result, s.th. other operators in the same workflow can re-use it
        let quota_check_enabled = crate::config::get_config_element::<crate::config::Quota>()
            .map_err(
                |e| geoengine_operators::error::Error::CreatingProcessorFailed {
                    source: Box::new(e),
                },
            )?
            .mode
            == QuotaTrackingMode::Check;

        if !quota_check_enabled {
            return Ok(());
        }

        let quota_available = self.user_db.quota_available().await.map_err(|e| {
            geoengine_operators::error::Error::CreatingProcessorFailed {
                source: Box::new(e),
            }
        })?;

        if quota_available <= 0 {
            return Err(geoengine_operators::error::Error::CreatingProcessorFailed {
                source: Box::new(crate::quota::QuotaError::QuotaExhausted),
            });
        }

        Ok(())
    }
}
