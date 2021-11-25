use crate::error::Result;
use crate::{projects::ProjectDb, workflows::registry::WorkflowRegistry};
use async_trait::async_trait;
use rayon::ThreadPool;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

mod in_memory;
mod session;
mod simple_context;

use crate::datasets::storage::DatasetDb;

use geoengine_datatypes::dataset::DatasetId;

use geoengine_datatypes::raster::TilingSpecification;
use geoengine_operators::engine::{
    ChunkByteSize, ExecutionContext, MetaData, MetaDataProvider, QueryContext,
    RasterQueryRectangle, RasterResultDescriptor, VectorQueryRectangle, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};

use crate::datasets::listing::SessionMetaDataProvider;
pub use in_memory::InMemoryContext;
pub use session::{MockableSession, Session, SessionId, SimpleSession};
pub use simple_context::SimpleContext;

pub type Db<T> = Arc<RwLock<T>>;

/// A context bundles access to shared resources like databases and session specific information
/// about the user to pass to the services handlers.
// TODO: avoid locking the individual DBs here IF they are already thread safe (e.g. guaranteed by postgres)
#[async_trait]
pub trait Context: 'static + Send + Sync + Clone {
    type Session: MockableSession + Clone; // TODO: change to `[Session]` when workarounds are gone
    type ProjectDB: ProjectDb<Self::Session>;
    type WorkflowRegistry: WorkflowRegistry;
    type DatasetDB: DatasetDb<Self::Session>;
    type QueryContext: QueryContext;
    type ExecutionContext: ExecutionContext;

    fn project_db(&self) -> Db<Self::ProjectDB>;
    async fn project_db_ref(&self) -> RwLockReadGuard<Self::ProjectDB>;
    async fn project_db_ref_mut(&self) -> RwLockWriteGuard<Self::ProjectDB>;

    fn workflow_registry(&self) -> Db<Self::WorkflowRegistry>;
    async fn workflow_registry_ref(&self) -> RwLockReadGuard<Self::WorkflowRegistry>;
    async fn workflow_registry_ref_mut(&self) -> RwLockWriteGuard<Self::WorkflowRegistry>;

    fn dataset_db(&self) -> Db<Self::DatasetDB>;
    async fn dataset_db_ref(&self) -> RwLockReadGuard<Self::DatasetDB>;
    async fn dataset_db_ref_mut(&self) -> RwLockWriteGuard<Self::DatasetDB>;

    fn query_context(&self) -> Result<Self::QueryContext>;

    fn execution_context(&self, session: Self::Session) -> Result<Self::ExecutionContext>;

    async fn session_by_id(&self, session_id: SessionId) -> Result<Self::Session>;
}

pub struct QueryContextImpl {
    chunk_byte_size: ChunkByteSize,
    pub thread_pool: Arc<ThreadPool>,
}

impl QueryContextImpl {
    pub fn new(chunk_byte_size: ChunkByteSize, thread_pool: Arc<ThreadPool>) -> Self {
        QueryContextImpl {
            chunk_byte_size,
            thread_pool,
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
}

pub struct ExecutionContextImpl<S, D>
where
    D: DatasetDb<S>,
    S: Session,
{
    dataset_db: Db<D>,
    thread_pool: Arc<ThreadPool>,
    session: S,
    tiling_specification: TilingSpecification,
}

impl<S, D> ExecutionContextImpl<S, D>
where
    D: DatasetDb<S>,
    S: Session,
{
    pub fn new(
        dataset_db: Db<D>,
        thread_pool: Arc<ThreadPool>,
        session: S,
        tiling_specification: TilingSpecification,
    ) -> Self {
        Self {
            dataset_db,
            thread_pool,
            session,
            tiling_specification,
        }
    }
}

impl<S, D> ExecutionContext for ExecutionContextImpl<S, D>
where
    D: DatasetDb<S>
        + SessionMetaDataProvider<
            S,
            MockDatasetDataSourceLoadingInfo,
            VectorResultDescriptor,
            VectorQueryRectangle,
        > + SessionMetaDataProvider<S, OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
        + SessionMetaDataProvider<S, GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>,
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
impl<S, D>
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for ExecutionContextImpl<S, D>
where
    D: DatasetDb<S>
        + SessionMetaDataProvider<
            S,
            MockDatasetDataSourceLoadingInfo,
            VectorResultDescriptor,
            VectorQueryRectangle,
        >,
    S: Session,
{
    async fn meta_data(
        &self,
        dataset_id: &DatasetId,
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
        match dataset_id {
            DatasetId::Internal { dataset_id: _ } => self
                .dataset_db
                .read()
                .await
                .session_meta_data(&self.session, dataset_id)
                .await
                .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                }),
            DatasetId::External(external) => {
                self.dataset_db
                    .read()
                    .await
                    .dataset_provider(&self.session, external.provider_id)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?
                    .meta_data(dataset_id)
                    .await
            }
        }
    }
}

// TODO: use macro(?) for delegating meta_data function to DatasetDB to avoid redundant code
#[async_trait]
impl<S, D> MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for ExecutionContextImpl<S, D>
where
    D: DatasetDb<S>
        + SessionMetaDataProvider<S, OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
    S: Session,
{
    async fn meta_data(
        &self,
        dataset_id: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        match dataset_id {
            DatasetId::Internal { dataset_id: _ } => self
                .dataset_db
                .read()
                .await
                .session_meta_data(&self.session, dataset_id)
                .await
                .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                }),
            DatasetId::External(external) => {
                self.dataset_db
                    .read()
                    .await
                    .dataset_provider(&self.session, external.provider_id)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?
                    .meta_data(dataset_id)
                    .await
            }
        }
    }
}

// TODO: use macro(?) for delegating meta_data function to DatasetDB to avoid redundant code
#[async_trait]
impl<S, D> MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for ExecutionContextImpl<S, D>
where
    D: DatasetDb<S>
        + SessionMetaDataProvider<S, GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>,
    S: Session,
{
    async fn meta_data(
        &self,
        dataset_id: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        match dataset_id {
            DatasetId::Internal { dataset_id: _ } => self
                .dataset_db
                .read()
                .await
                .session_meta_data(&self.session, dataset_id)
                .await
                .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                }),
            DatasetId::External(external) => {
                self.dataset_db
                    .read()
                    .await
                    .dataset_provider(&self.session, external.provider_id)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?
                    .meta_data(dataset_id)
                    .await
            }
        }
    }
}
