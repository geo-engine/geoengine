use crate::error::Result;
use crate::{
    projects::projectdb::ProjectDb, users::session::Session, users::userdb::UserDb,
    workflows::registry::WorkflowRegistry,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

mod in_memory;
mod session;

use crate::datasets::storage::DatasetDb;

use crate::users::user::UserId;
use crate::util::config;
use crate::util::config::get_config_element;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::primitives::Coordinate2D;
use geoengine_datatypes::raster::GridShape2D;
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_operators::concurrency::{ThreadPool, ThreadPoolContext};
use geoengine_operators::engine::{
    ExecutionContext, MetaData, MetaDataProvider, QueryContext, RasterResultDescriptor,
    VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};

pub use in_memory::InMemoryContext;
pub use session::{Session, SessionId, SimpleSession};

pub type Db<T> = Arc<RwLock<T>>;

/// A context bundles access to shared resources like databases and session specific information
/// about the user to pass to the services handlers.
// TODO: avoid locking the individual DBs here IF they are already thread safe (e.g. guaranteed by postgres)
#[async_trait]
pub trait Context: 'static + Send + Sync + Clone {
    type UserDB: UserDb;
    type ProjectDB: ProjectDb;
    type WorkflowRegistry: WorkflowRegistry;
    type DatasetDB: DatasetDb;
    type QueryContext: QueryContext;
    type ExecutionContext: ExecutionContext;

    fn user_db(&self) -> Db<Self::UserDB>;
    async fn user_db_ref(&self) -> RwLockReadGuard<Self::UserDB>;
    async fn user_db_ref_mut(&self) -> RwLockWriteGuard<Self::UserDB>;

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

    fn execution_context(&self, session: &Session) -> Result<Self::ExecutionContext>;
}

pub struct QueryContextImpl {
    chunk_byte_size: usize,
}

impl QueryContext for QueryContextImpl {
    fn chunk_byte_size(&self) -> usize {
        self.chunk_byte_size
    }
}

pub struct ExecutionContextImpl<D>
where
    D: DatasetDb,
{
    dataset_db: Db<D>,
    thread_pool: Arc<ThreadPool>,
    user: UserId,
}

impl<D> ExecutionContext for ExecutionContextImpl<D>
where
    D: DatasetDb
        + MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>
        + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor>
        + MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor>,
{
    fn thread_pool(&self) -> ThreadPoolContext {
        self.thread_pool.create_context()
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
impl<D> MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>
    for ExecutionContextImpl<D>
where
    D: DatasetDb + MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>,
{
    // TODO: make async
    fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        futures::executor::block_on(async {
            match dataset {
                DatasetId::Internal(_) => self.dataset_db.read().await.meta_data(dataset),
                DatasetId::Staging(_) => todo!(),
                DatasetId::External(external) => self
                    .dataset_db
                    .read()
                    .await
                    .dataset_provider(self.user, external.provider)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?
                    .meta_data(dataset),
            }
        })
    }
}

// TODO: use macro(?) for delegating meta_data function to DatasetDB to avoid redundant code
impl<D> MetaDataProvider<OgrSourceDataset, VectorResultDescriptor> for ExecutionContextImpl<D>
where
    D: DatasetDb + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor>,
{
    // TODO: make async
    fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        futures::executor::block_on(async {
            match dataset {
                DatasetId::Internal(_) => self.dataset_db.read().await.meta_data(dataset),
                DatasetId::Staging(_) => todo!(),
                DatasetId::External(external) => self
                    .dataset_db
                    .read()
                    .await
                    .dataset_provider(self.user, external.provider)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?
                    .meta_data(dataset),
            }
        })
    }
}

// TODO: use macro(?) for delegating meta_data function to DatasetDB to avoid redundant code
impl<D> MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor> for ExecutionContextImpl<D>
where
    D: DatasetDb + MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor>,
{
    // TODO: make async
    fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        futures::executor::block_on(async {
            match dataset {
                DatasetId::Internal(_) => self.dataset_db.read().await.meta_data(dataset),
                DatasetId::Staging(_) => todo!(),
                DatasetId::External(external) => self
                    .dataset_db
                    .read()
                    .await
                    .dataset_provider(self.user, external.provider)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?
                    .meta_data(dataset),
            }
        })
    }
}
