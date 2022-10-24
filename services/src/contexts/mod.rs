use crate::error::Result;
use crate::layers::storage::{LayerDb, LayerProviderDb};
use crate::tasks::{TaskContext, TaskManager};
use crate::{projects::ProjectDb, workflows::registry::WorkflowRegistry};
use async_trait::async_trait;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use rayon::ThreadPool;
use std::sync::Arc;
use tokio::sync::RwLock;

mod in_memory;
mod session;
mod simple_context;

use crate::datasets::storage::DatasetDb;

use geoengine_datatypes::dataset::DataId;

use geoengine_datatypes::raster::TilingSpecification;
use geoengine_operators::engine::{
    ChunkByteSize, ExecutionContext, MetaData, MetaDataProvider, QueryAbortRegistration,
    QueryAbortTrigger, QueryContext, RasterResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};

use crate::datasets::listing::SessionMetaDataProvider;
pub use in_memory::InMemoryContext;
pub use session::{AdminSession, MockableSession, Session, SessionId, SimpleSession};
pub use simple_context::SimpleContext;

pub type Db<T> = Arc<RwLock<T>>;

/// A context bundles access to shared resources like databases and session specific information
/// about the user to pass to the services handlers.
#[async_trait]
pub trait Context: 'static + Send + Sync + Clone {
    type Session: MockableSession + Clone + From<AdminSession>; // TODO: change to `[Session]` when workarounds are gone
    type ProjectDB: ProjectDb<Self::Session>;
    type WorkflowRegistry: WorkflowRegistry;
    type DatasetDB: DatasetDb<Self::Session>;
    type LayerDB: LayerDb;
    type LayerProviderDB: LayerProviderDb;
    type QueryContext: QueryContext;
    type ExecutionContext: ExecutionContext;
    type TaskContext: TaskContext;
    type TaskManager: TaskManager<Self::TaskContext>;

    fn project_db(&self) -> Arc<Self::ProjectDB>;
    fn project_db_ref(&self) -> &Self::ProjectDB;

    fn workflow_registry(&self) -> Arc<Self::WorkflowRegistry>;
    fn workflow_registry_ref(&self) -> &Self::WorkflowRegistry;

    fn dataset_db(&self) -> Arc<Self::DatasetDB>;
    fn dataset_db_ref(&self) -> &Self::DatasetDB;

    fn layer_db(&self) -> Arc<Self::LayerDB>;
    fn layer_db_ref(&self) -> &Self::LayerDB;

    fn layer_provider_db(&self) -> Arc<Self::LayerProviderDB>;
    fn layer_provider_db_ref(&self) -> &Self::LayerProviderDB;

    fn tasks(&self) -> Arc<Self::TaskManager>;
    fn tasks_ref(&self) -> &Self::TaskManager;

    fn query_context(&self) -> Result<Self::QueryContext>;

    fn execution_context(&self, session: Self::Session) -> Result<Self::ExecutionContext>;

    async fn session_by_id(&self, session_id: SessionId) -> Result<Self::Session>;
}

pub struct QueryContextImpl {
    chunk_byte_size: ChunkByteSize,
    thread_pool: Arc<ThreadPool>,
    abort_registration: QueryAbortRegistration,
    abort_trigger: Option<QueryAbortTrigger>,
}

impl QueryContextImpl {
    pub fn new(chunk_byte_size: ChunkByteSize, thread_pool: Arc<ThreadPool>) -> Self {
        let (abort_registration, abort_trigger) = QueryAbortRegistration::new();
        QueryContextImpl {
            chunk_byte_size,
            thread_pool,
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

    fn abort_trigger(&mut self) -> Option<QueryAbortTrigger> {
        self.abort_trigger.take()
    }
}

pub struct ExecutionContextImpl<S, D, L>
where
    D: DatasetDb<S>,
    L: LayerProviderDb,
    S: Session,
{
    dataset_db: Arc<D>,
    layer_provider_db: Arc<L>,
    thread_pool: Arc<ThreadPool>,
    session: S,
    tiling_specification: TilingSpecification,
}

impl<S, D, L> ExecutionContextImpl<S, D, L>
where
    D: DatasetDb<S>,
    L: LayerProviderDb,
    S: Session,
{
    pub fn new(
        dataset_db: Arc<D>,
        layer_provider_db: Arc<L>,
        thread_pool: Arc<ThreadPool>,
        session: S,
        tiling_specification: TilingSpecification,
    ) -> Self {
        Self {
            dataset_db,
            layer_provider_db,
            thread_pool,
            session,
            tiling_specification,
        }
    }
}

impl<S, D, L> ExecutionContext for ExecutionContextImpl<S, D, L>
where
    D: DatasetDb<S>
        + SessionMetaDataProvider<
            S,
            MockDatasetDataSourceLoadingInfo,
            VectorResultDescriptor,
            VectorQueryRectangle,
        > + SessionMetaDataProvider<S, OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
        + SessionMetaDataProvider<S, GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>,
    L: LayerProviderDb,
    S: Session,
{
    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.thread_pool
    }

    fn tiling_specification(&self) -> TilingSpecification {
        self.tiling_specification
    }
}

// TODO: use macro(?) for delegating meta_data function to DatasetDB to avoid redundant code
#[async_trait]
impl<S, D, L>
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for ExecutionContextImpl<S, D, L>
where
    D: DatasetDb<S>
        + SessionMetaDataProvider<
            S,
            MockDatasetDataSourceLoadingInfo,
            VectorResultDescriptor,
            VectorQueryRectangle,
        >,
    L: LayerProviderDb,
    S: Session,
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
            DataId::Internal { dataset_id: _ } => self
                .dataset_db
                .session_meta_data(&self.session, &data_id.clone().into())
                .await
                .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                }),
            DataId::External(external) => {
                self.layer_provider_db
                    .layer_provider(external.provider_id.into())
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
impl<S, D, L> MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for ExecutionContextImpl<S, D, L>
where
    D: DatasetDb<S>
        + SessionMetaDataProvider<S, OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
    L: LayerProviderDb,
    S: Session,
{
    async fn meta_data(
        &self,
        data_id: &DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        match data_id {
            DataId::Internal { dataset_id: _ } => self
                .dataset_db
                .session_meta_data(&self.session, &data_id.clone().into())
                .await
                .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                }),
            DataId::External(external) => {
                self.layer_provider_db
                    .layer_provider(external.provider_id.into())
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
impl<S, D, L> MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for ExecutionContextImpl<S, D, L>
where
    D: DatasetDb<S>
        + SessionMetaDataProvider<S, GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>,
    L: LayerProviderDb,
    S: Session,
{
    async fn meta_data(
        &self,
        data_id: &DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        match data_id {
            DataId::Internal { dataset_id: _ } => self
                .dataset_db
                .session_meta_data(&self.session, &data_id.clone().into())
                .await
                .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                }),
            DataId::External(external) => {
                self.layer_provider_db
                    .layer_provider(external.provider_id.into())
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
