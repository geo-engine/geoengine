mod in_memory;

#[cfg(feature = "postgres")]
mod postgres;

use std::sync::Arc;

use geoengine_datatypes::dataset::DataId;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_operators::engine::{
    CreateSpan, ExecutionContext, InitializedPlotOperator, InitializedVectorOperator, MetaData,
    MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::pro::meta::statistics::InitializedProcessorStatistics;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
pub use in_memory::ProInMemoryContext;
#[cfg(feature = "postgres")]
pub use postgres::PostgresContext;
use rayon::ThreadPool;

use crate::contexts::{Context, Session};
use crate::datasets::listing::SessionMetaDataProvider;
use crate::datasets::storage::DatasetDb;
use crate::layers::storage::LayerProviderDb;
use crate::pro::users::{OidcRequestDb, UserDb, UserSession};

use async_trait::async_trait;

/// A pro contexts that extends the default context.
// TODO: avoid locking the individual DBs here IF they are already thread safe (e.g. guaranteed by postgres)
#[async_trait]
pub trait ProContext: Context<Session = UserSession> {
    type UserDB: UserDb;

    fn user_db(&self) -> Arc<Self::UserDB>;
    fn user_db_ref(&self) -> &Self::UserDB;
    fn oidc_request_db(&self) -> Option<&OidcRequestDb>;
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

    fn wrap_initialized_raster_operator(
        &self,
        op: Box<dyn geoengine_operators::engine::InitializedRasterOperator>,
        span: CreateSpan,
    ) -> Box<dyn geoengine_operators::engine::InitializedRasterOperator> {
        Box::new(InitializedProcessorStatistics::new(op, span))
    }

    fn wrap_initialized_vector_operator(
        &self,
        op: Box<dyn InitializedVectorOperator>,
        span: CreateSpan,
    ) -> Box<dyn InitializedVectorOperator> {
        Box::new(InitializedProcessorStatistics::new(op, span))
    }

    fn wrap_initialized_plot_operator(
        &self,
        op: Box<dyn InitializedPlotOperator>,
        _span: CreateSpan,
    ) -> Box<dyn InitializedPlotOperator> {
        // as plots do not produce a stream of results, we have nothing to count for now
        op
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
