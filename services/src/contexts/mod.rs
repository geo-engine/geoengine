use crate::error::Result;
use crate::projects::ProjectDb;
use crate::storage::{GeoEngineStore, Storable, Store};
use async_trait::async_trait;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use rayon::ThreadPool;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard};

mod in_memory;
mod session;
mod simple_context;

use crate::datasets::storage::{DatasetDb, MetaDataDefinition};

use geoengine_datatypes::dataset::DatasetId;

use geoengine_datatypes::raster::TilingSpecification;
use geoengine_operators::engine::{
    ChunkByteSize, ExecutionContext, MetaData, MetaDataLookupResult, QueryContext,
    RasterResultDescriptor, VectorResultDescriptor,
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
    type Store: GeoEngineStore;
    type ProjectDB: ProjectDb<Self::Session>;
    type QueryContext: QueryContext;
    type ExecutionContext: ExecutionContext;

    fn project_db(&self) -> Db<Self::ProjectDB>;
    async fn project_db_ref(&self) -> RwLockReadGuard<Self::ProjectDB>;
    async fn project_db_ref_mut(&self) -> RwLockWriteGuard<Self::ProjectDB>;

    fn store(&self) -> Db<Self::Store>;
    async fn store_ref<S>(&self) -> RwLockReadGuard<'_, dyn Store<S>>
    where
        Self::Store: Store<S>,
        S: Storable;
    async fn store_ref_mut<S>(&self) -> RwLockMappedWriteGuard<'_, dyn Store<S>>
    where
        Self::Store: Store<S>,
        S: Storable;

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

pub struct ExecutionContextImpl<S>
where
    S: Store<MetaDataDefinition>,
{
    store: Db<S>,
    thread_pool: Arc<ThreadPool>,
    tiling_specification: TilingSpecification,
}

impl<S> ExecutionContextImpl<S>
where
    S: Store<MetaDataDefinition>,
{
    pub fn new(
        store: Db<S>,
        thread_pool: Arc<ThreadPool>,
        tiling_specification: TilingSpecification,
    ) -> Self {
        Self {
            store,
            thread_pool,
            tiling_specification,
        }
    }
}

#[async_trait]
impl<S> ExecutionContext for ExecutionContextImpl<S>
where
    S: Store<MetaDataDefinition>,
{
    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.thread_pool
    }

    fn tiling_specification(&self) -> TilingSpecification {
        self.tiling_specification
    }

    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> geoengine_operators::util::Result<MetaDataLookupResult> {
        match dataset {
            DatasetId::Internal { dataset_id } => {
                let definition: MetaDataDefinition = self
                    .store
                    .read()
                    .await
                    .read(dataset_id)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                        source: Box::new(e),
                    })?;
                Ok(definition.into())
            }
            DatasetId::External(_external) => {
                todo!()
            }
        }
    }
}
