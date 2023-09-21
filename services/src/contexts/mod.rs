use crate::datasets::upload::Volume;
use crate::error::Result;
use crate::layers::listing::{DatasetLayerCollectionProvider, LayerCollectionProvider};
use crate::layers::storage::{LayerDb, LayerProviderDb};
use crate::tasks::{TaskContext, TaskManager};
use crate::{projects::ProjectDb, workflows::registry::WorkflowRegistry};
use async_trait::async_trait;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use rayon::ThreadPool;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

mod db_types;
mod postgres;
mod session;
mod simple_context;

use crate::datasets::storage::DatasetDb;

use geoengine_datatypes::dataset::{DataId, DataProviderId, ExternalDataId, LayerId, NamedData};

use geoengine_datatypes::raster::TilingSpecification;
use geoengine_operators::engine::{
    ChunkByteSize, CreateSpan, ExecutionContext, ExecutionContextExtensions,
    InitializedPlotOperator, InitializedVectorOperator, MetaData, MetaDataProvider,
    QueryAbortRegistration, QueryAbortTrigger, QueryContext, QueryContextExtensions,
    RasterResultDescriptor, VectorResultDescriptor, WorkflowOperatorPath,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};

pub use postgres::{PostgresContext, PostgresDb, PostgresSessionContext};
pub use session::{MockableSession, Session, SessionId, SimpleSession};
pub use simple_context::SimpleApplicationContext;

pub type Db<T> = Arc<RwLock<T>>;

/// The application context bundles shared resources.
/// It is passed to API handlers and allows creating a session context that provides access to resources.
#[async_trait]
pub trait ApplicationContext: 'static + Send + Sync + Clone {
    type SessionContext: SessionContext;
    type Session: Session + Clone;

    /// Create a new session context for the given session.
    fn session_context(&self, session: Self::Session) -> Self::SessionContext;

    /// Load a session by its id
    async fn session_by_id(&self, session_id: SessionId) -> Result<Self::Session>;
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
    fn query_context(&self) -> Result<Self::QueryContext>;

    /// Create a new execution context initializing operators
    fn execution_context(&self) -> Result<Self::ExecutionContext>;

    /// Get the list of available data volumes
    fn volumes(&self) -> Result<Vec<Volume>>;

    /// Get the current session
    fn session(&self) -> &Self::Session;
}

/// The trait for accessing all resources
pub trait GeoEngineDb:
    DatasetDb
    + LayerDb
    + LayerProviderDb
    + LayerCollectionProvider
    + DatasetLayerCollectionProvider
    + ProjectDb
    + WorkflowRegistry
{
}

pub struct QueryContextImpl {
    chunk_byte_size: ChunkByteSize,
    thread_pool: Arc<ThreadPool>,
    extensions: QueryContextExtensions,
    abort_registration: QueryAbortRegistration,
    abort_trigger: Option<QueryAbortTrigger>,
}

impl QueryContextImpl {
    pub fn new(chunk_byte_size: ChunkByteSize, thread_pool: Arc<ThreadPool>) -> Self {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        QueryContextImpl {
            chunk_byte_size,
            thread_pool,
            extensions: Default::default(),
            abort_registration,
            abort_trigger: Some(abort_trigger),
        }
    }

    pub fn new_with_extensions(
        chunk_byte_size: ChunkByteSize,
        thread_pool: Arc<ThreadPool>,
        extensions: QueryContextExtensions,
    ) -> Self {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        QueryContextImpl {
            chunk_byte_size,
            thread_pool,
            extensions,
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

    fn extensions(&self) -> &QueryContextExtensions {
        &self.extensions
    }

    fn abort_registration(&self) -> &QueryAbortRegistration {
        &self.abort_registration
    }

    fn abort_trigger(&mut self) -> geoengine_operators::util::Result<QueryAbortTrigger> {
        self.abort_trigger
            .take()
            .ok_or(geoengine_operators::error::Error::AbortTriggerAlreadyUsed)
    }
}

pub struct ExecutionContextImpl<D>
where
    D: DatasetDb + LayerProviderDb,
{
    db: D,
    thread_pool: Arc<ThreadPool>,
    tiling_specification: TilingSpecification,
    extensions: ExecutionContextExtensions,
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
            extensions: Default::default(),
        }
    }

    pub fn new_with_extensions(
        db: D,
        thread_pool: Arc<ThreadPool>,
        tiling_specification: TilingSpecification,
        extensions: ExecutionContextExtensions,
    ) -> Self {
        Self {
            db,
            thread_pool,
            tiling_specification,
            extensions,
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
        + LayerProviderDb,
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
        _span: CreateSpan,
        _path: WorkflowOperatorPath,
    ) -> Box<dyn geoengine_operators::engine::InitializedRasterOperator> {
        op
    }

    fn wrap_initialized_vector_operator(
        &self,
        op: Box<dyn InitializedVectorOperator>,
        _span: CreateSpan,
        _path: WorkflowOperatorPath,
    ) -> Box<dyn InitializedVectorOperator> {
        op
    }

    fn wrap_initialized_plot_operator(
        &self,
        op: Box<dyn InitializedPlotOperator>,
        _span: CreateSpan,
        _path: WorkflowOperatorPath,
    ) -> Box<dyn InitializedPlotOperator> {
        op
    }

    async fn resolve_named_data(
        &self,
        data: &NamedData,
    ) -> Result<DataId, geoengine_operators::error::Error> {
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

        Ok(dataset_id.into())
    }

    fn extensions(&self) -> &ExecutionContextExtensions {
        &self.extensions
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
