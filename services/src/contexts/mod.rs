use crate::error::Result;
use crate::{
    projects::projectdb::ProjectDB, users::session::Session, users::userdb::UserDB,
    workflows::registry::WorkflowRegistry,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

mod in_memory;
#[cfg(feature = "postgres")]
mod postgres;

use crate::datasets::storage::DataSetDB;

use crate::users::user::UserId;
use crate::util::config;
use crate::util::config::{get_config_element, GdalSource};
use geoengine_datatypes::dataset::DataSetId;
use geoengine_datatypes::primitives::Coordinate2D;
use geoengine_datatypes::raster::GridShape2D;
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_operators::concurrency::{ThreadPool, ThreadPoolContext};
use geoengine_operators::engine::{
    ExecutionContext, MetaData, MetaDataProvider, QueryContext, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDataSetDataSourceLoadingInfo;
use geoengine_operators::source::OgrSourceDataset;
pub use in_memory::InMemoryContext;
#[cfg(feature = "postgres")]
pub use postgres::PostgresContext;
use std::path::PathBuf;

type DB<T> = Arc<RwLock<T>>;

/// A context bundles access to shared resources like databases and session specific information
/// about the user to pass to the services handlers.
// TODO: avoid locking the individual DBs here IF they are already thread safe (e.g. guaranteed by postgres)
#[async_trait]
pub trait Context: 'static + Send + Sync + Clone {
    type UserDB: UserDB;
    type ProjectDB: ProjectDB;
    type WorkflowRegistry: WorkflowRegistry;
    type DataSetDB: DataSetDB;
    type QueryContext: QueryContext;
    type ExecutionContext: ExecutionContext;

    fn user_db(&self) -> DB<Self::UserDB>;
    async fn user_db_ref(&self) -> RwLockReadGuard<Self::UserDB>;
    async fn user_db_ref_mut(&self) -> RwLockWriteGuard<Self::UserDB>;

    fn project_db(&self) -> DB<Self::ProjectDB>;
    async fn project_db_ref(&self) -> RwLockReadGuard<Self::ProjectDB>;
    async fn project_db_ref_mut(&self) -> RwLockWriteGuard<Self::ProjectDB>;

    fn workflow_registry(&self) -> DB<Self::WorkflowRegistry>;
    async fn workflow_registry_ref(&self) -> RwLockReadGuard<Self::WorkflowRegistry>;
    async fn workflow_registry_ref_mut(&self) -> RwLockWriteGuard<Self::WorkflowRegistry>;

    fn data_set_db(&self) -> DB<Self::DataSetDB>;
    async fn data_set_db_ref(&self) -> RwLockReadGuard<Self::DataSetDB>;
    async fn data_set_db_ref_mut(&self) -> RwLockWriteGuard<Self::DataSetDB>;

    fn session(&self) -> Result<&Session>;

    fn set_session(&mut self, session: Session);

    fn query_context(&self) -> Result<Self::QueryContext>;

    fn execution_context(&self) -> Result<Self::ExecutionContext>;
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
    D: DataSetDB,
{
    data_set_db: DB<D>,
    thread_pool: Arc<ThreadPool>,
    user: UserId,
}

impl<D> ExecutionContext for ExecutionContextImpl<D>
where
    D: DataSetDB
        + MetaDataProvider<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>
        + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor>,
{
    fn thread_pool(&self) -> ThreadPoolContext {
        self.thread_pool.create_context()
    }

    fn raster_data_root(&self) -> std::result::Result<PathBuf, geoengine_operators::error::Error> {
        Ok(get_config_element::<GdalSource>()
            .map_err(|_| geoengine_operators::error::Error::RasterRootPathNotConfigured)?
            .raster_data_root_path)
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

// TODO: use macro(?) for delegating meta_data function to DataSetDB to avoid redundant code
impl<D> MetaDataProvider<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>
    for ExecutionContextImpl<D>
where
    D: DataSetDB + MetaDataProvider<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>,
{
    // TODO: make async
    fn meta_data(
        &self,
        data_set: &DataSetId,
    ) -> Result<
        Box<dyn MetaData<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        futures::executor::block_on(async {
            match data_set {
                DataSetId::Internal(_) => self.data_set_db.read().await.meta_data(data_set),
                DataSetId::Staging(_) => todo!(),
                DataSetId::External(external) => self
                    .data_set_db
                    .read()
                    .await
                    .data_set_provider(self.user, external.provider)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DataSetMetaData {
                        source: Box::new(e),
                    })?
                    .meta_data(data_set),
            }
        })
    }
}

// TODO: use macro(?) for delegating meta_data function to DataSetDB to avoid redundant code
impl<D> MetaDataProvider<OgrSourceDataset, VectorResultDescriptor> for ExecutionContextImpl<D>
where
    D: DataSetDB + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor>,
{
    // TODO: make async
    fn meta_data(
        &self,
        data_set: &DataSetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        futures::executor::block_on(async {
            match data_set {
                DataSetId::Internal(_) => self.data_set_db.read().await.meta_data(data_set),
                DataSetId::Staging(_) => todo!(),
                DataSetId::External(external) => self
                    .data_set_db
                    .read()
                    .await
                    .data_set_provider(self.user, external.provider)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DataSetMetaData {
                        source: Box::new(e),
                    })?
                    .meta_data(data_set),
            }
        })
    }
}
