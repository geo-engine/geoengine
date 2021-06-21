use crate::error::Result;
use crate::{projects::ProjectDb, workflows::registry::WorkflowRegistry};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

mod in_memory;
mod session;
mod simple_context;

use crate::datasets::storage::DatasetDb;

use crate::util::config;
use crate::util::config::get_config_element;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::primitives::Coordinate2D;
use geoengine_datatypes::raster::GridShape2D;
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_operators::concurrency::ThreadPoolContext;
use geoengine_operators::engine::{
    ExecutionContext, MetaData, MetaDataProvider, QueryContext, RasterResultDescriptor,
    VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};

pub use in_memory::InMemoryContext;
pub use session::{MockableSession, Session, SessionId, SimpleSession};
pub use simple_context::SimpleContext;

pub type Db<T> = Arc<RwLock<T>>;

/// A context bundles access to shared resources like databases and session specific information
/// about the user to pass to the services handlers.
// TODO: avoid locking the individual DBs here IF they are already thread safe (e.g. guaranteed by postgres)
#[async_trait]
pub trait Context: 'static + Send + Sync + Clone {
    type Session: MockableSession; // TODO: change to `[Session]` when workarounds are gone
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
    pub chunk_byte_size: usize,
    pub thread_pool: ThreadPoolContext,
}

impl QueryContext for QueryContextImpl {
    fn chunk_byte_size(&self) -> usize {
        self.chunk_byte_size
    }

    fn thread_pool(&self) -> &ThreadPoolContext {
        &self.thread_pool
    }
}

pub struct ExecutionContextImpl<S, D>
where
    D: DatasetDb<S>,
    S: Session,
{
    dataset_db: Db<D>,
    thread_pool: ThreadPoolContext,
    session: S,
}

impl<S, D> ExecutionContextImpl<S, D>
where
    D: DatasetDb<S>,
    S: Session,
{
    pub fn new(dataset_db: Db<D>, thread_pool: ThreadPoolContext, session: S) -> Self {
        Self {
            dataset_db,
            thread_pool,
            session,
        }
    }
}

impl<S, D> ExecutionContext for ExecutionContextImpl<S, D>
where
    D: DatasetDb<S>
        + MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>
        + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor>
        + MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor>,
    S: Session,
{
    fn thread_pool(&self) -> ThreadPoolContext {
        self.thread_pool.clone()
    }

    fn tiling_specification(&self) -> TilingSpecification {
        // TODO: load only once and handle error
        let config_tiling_spec = get_config_element::<config::TilingSpecification>().unwrap();

        TilingSpecification {
            origin_coordinate: Coordinate2D::new(
                config_tiling_spec.origin_coordinate_x,
                config_tiling_spec.origin_coordinate_y,
            ),
            tile_size_in_pixels: GridShape2D::from([
                config_tiling_spec.tile_shape_pixels_y,
                config_tiling_spec.tile_shape_pixels_x,
            ]),
        }
    }
}

// TODO: use macro(?) for delegating meta_data function to DatasetDB to avoid redundant code
#[async_trait]
impl<S, D> MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>
    for ExecutionContextImpl<S, D>
where
    D: DatasetDb<S> + MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>,
    S: Session,
{
    // TODO: make async
    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        match dataset {
            DatasetId::Internal(_) => self.dataset_db.read().await.meta_data(dataset).await,
            DatasetId::Staging(_) => todo!(),
            DatasetId::External(external) => {
                self.dataset_db
                    .read()
                    .await
                    .dataset_provider(&self.session, external.provider)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?
                    .meta_data(dataset)
                    .await
            }
        }
    }
}

// TODO: use macro(?) for delegating meta_data function to DatasetDB to avoid redundant code
#[async_trait]
impl<S, D> MetaDataProvider<OgrSourceDataset, VectorResultDescriptor> for ExecutionContextImpl<S, D>
where
    D: DatasetDb<S> + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor>,
    S: Session,
{
    // TODO: make async
    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        match dataset {
            DatasetId::Internal(_) => self.dataset_db.read().await.meta_data(dataset).await,
            DatasetId::Staging(_) => todo!(),
            DatasetId::External(external) => {
                self.dataset_db
                    .read()
                    .await
                    .dataset_provider(&self.session, external.provider)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?
                    .meta_data(dataset)
                    .await
            }
        }
    }
}

// TODO: use macro(?) for delegating meta_data function to DatasetDB to avoid redundant code
#[async_trait]
impl<S, D> MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor> for ExecutionContextImpl<S, D>
where
    D: DatasetDb<S> + MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor>,
    S: Session,
{
    // TODO: make async
    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        match dataset {
            DatasetId::Internal(_) => self.dataset_db.read().await.meta_data(dataset).await,
            DatasetId::Staging(_) => todo!(),
            DatasetId::External(external) => {
                self.dataset_db
                    .read()
                    .await
                    .dataset_provider(&self.session, external.provider)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?
                    .meta_data(dataset)
                    .await
            }
        }
    }
}
