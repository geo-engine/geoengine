mod db_types;
pub(crate) mod migrations;
mod postgres;

use std::str::FromStr;
use std::sync::Arc;

use geoengine_datatypes::dataset::{DataId, DataProviderId, ExternalDataId, LayerId};
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_operators::engine::{
    CreateSpan, ExecutionContext, ExecutionContextExtensions, InitializedPlotOperator,
    InitializedVectorOperator, MetaData, MetaDataProvider, RasterResultDescriptor,
    VectorResultDescriptor, WorkflowOperatorPath,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::pro::cache::cache_operator::InitializedCacheOperator;
use geoengine_operators::pro::meta::quota::QuotaCheck;
use geoengine_operators::pro::meta::wrapper::InitializedOperatorWrapper;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};

pub use postgres::ProPostgresContext;
use rayon::ThreadPool;

use crate::contexts::{ApplicationContext, GeoEngineDb};
use crate::datasets::storage::DatasetDb;
use crate::error::Result;

use crate::layers::storage::LayerProviderDb;
use crate::pro::users::{OidcManager, UserDb};

use async_trait::async_trait;

use super::permissions::PermissionDb;
use super::users::{RoleDb, UserAuth, UserSession};
use super::util::config::{Cache, QuotaTrackingMode};
use crate::util::config::get_config_element;

pub use postgres::ProPostgresDb;

/// A pro application contexts that extends the default context.
pub trait ProApplicationContext: ApplicationContext<Session = UserSession> + UserAuth {
    fn oidc_manager(&self) -> &OidcManager;
}

pub trait ProGeoEngineDb: GeoEngineDb + UserDb + PermissionDb + RoleDb {}

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
            extensions: ExecutionContextExtensions::default(),
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
        span: CreateSpan,
        path: WorkflowOperatorPath,
    ) -> Box<dyn geoengine_operators::engine::InitializedRasterOperator> {
        let wrapped = Box::new(InitializedOperatorWrapper::new(op, span, path))
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
        path: WorkflowOperatorPath,
    ) -> Box<dyn InitializedVectorOperator> {
        let wrapped = Box::new(InitializedOperatorWrapper::new(op, span, path))
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
        _path: WorkflowOperatorPath,
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

pub struct QuotaCheckerImpl<U: UserDb> {
    user_db: U,
}

#[async_trait]
impl<U: UserDb> QuotaCheck for QuotaCheckerImpl<U> {
    async fn ensure_quota_available(&self) -> geoengine_operators::util::Result<()> {
        // TODO: cache the result, s.th. other operators in the same workflow can re-use it
        let quota_check_enabled =
            crate::util::config::get_config_element::<crate::pro::util::config::Quota>()
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
                source: Box::new(crate::pro::quota::QuotaError::QuotaExhausted),
            });
        }

        Ok(())
    }
}
