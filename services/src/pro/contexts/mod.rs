mod db_types;
mod postgres;

use std::str::FromStr;
use std::sync::Arc;

use geoengine_datatypes::dataset::{DataId, DataProviderId, ExternalDataId, LayerId};
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::util::canonicalize_subpath;
use geoengine_operators::engine::{
    CreateSpan, ExecutionContext, InitializedPlotOperator, InitializedVectorOperator, MetaData,
    MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor, WorkflowOperatorPath,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::pro::cache::cache_operator::InitializedCacheOperator;
use geoengine_operators::pro::meta::quota::QuotaCheck;
use geoengine_operators::pro::meta::wrapper::InitializedOperatorWrapper;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};

pub use postgres::ProPostgresContext;
use rayon::ThreadPool;
use tokio::io::AsyncWriteExt;

use crate::contexts::{ApplicationContext, GeoEngineDb};
use crate::datasets::storage::DatasetDb;
use crate::error::Result;

use crate::layers::storage::LayerProviderDb;
use crate::pro::users::{OidcRequestDb, UserDb};
use crate::util::config::get_config_element;
use crate::util::path_with_base_path;

use async_trait::async_trait;

use super::permissions::PermissionDb;
use super::users::{RoleDb, UserAuth, UserSession};
use super::util::config::{Cache, QuotaTrackingMode};

pub use postgres::ProPostgresDb;

/// A pro application contexts that extends the default context.
pub trait ProApplicationContext: ApplicationContext<Session = UserSession> + UserAuth {
    fn oidc_request_db(&self) -> Option<&OidcRequestDb>;
}

pub trait ProGeoEngineDb: GeoEngineDb + UserDb + PermissionDb + RoleDb {}

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

    /// This method is meant to read a ml model from disk, specified by the config key `machinelearning.model_defs_path`.
    async fn read_ml_model(
        &self,
        model_sub_path: std::path::PathBuf,
    ) -> geoengine_operators::util::Result<String> {
        let cfg = get_config_element::<crate::util::config::MachineLearning>()
            .map_err(|_| geoengine_operators::error::Error::InvalidMachineLearningConfig)?;

        let model_base_path = cfg.model_defs_path;

        let model_path = canonicalize_subpath(&model_base_path, &model_sub_path)?;
        let model = tokio::fs::read_to_string(model_path).await?;

        Ok(model)
    }

    /// This method is meant to write a ml model to disk.
    /// The provided path for the model has to exist.
    async fn write_ml_model(
        &mut self,
        model_sub_path: std::path::PathBuf,
        ml_model_str: String,
    ) -> geoengine_operators::util::Result<()> {
        let cfg = get_config_element::<crate::util::config::MachineLearning>()
            .map_err(|_| geoengine_operators::error::Error::InvalidMachineLearningConfig)?;

        let model_base_path = cfg.model_defs_path;

        // make sure, that the model sub path is not escaping the config path
        let model_path = path_with_base_path(&model_base_path, &model_sub_path)
            .map_err(|_| geoengine_operators::error::Error::InvalidMlModelPath)?;

        let parent_dir = model_path
            .parent()
            .ok_or(geoengine_operators::error::Error::CouldNotGetMlModelDirectory)?;

        tokio::fs::create_dir_all(parent_dir).await?;

        // TODO: add routine or error, if a given modelpath would overwrite an existing model
        let mut file = tokio::fs::File::create(model_path).await?;
        file.write_all(ml_model_str.as_bytes()).await?;
        file.flush().await?;

        Ok(())
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

        Ok(dataset_id.into())
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
                    .load_layer_provider(external.provider_id.into())
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
                    .load_layer_provider(external.provider_id.into())
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
                    .load_layer_provider(external.provider_id.into())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use geoengine_datatypes::test_data;
    use serial_test::serial;

    use crate::contexts::SimpleApplicationContext;
    use crate::util::tests::with_temp_context;
    use crate::{contexts::SessionContext, util::config::set_config};

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    async fn read_model_test() {
        with_temp_context(|app_ctx, _| async move {
            let cfg = get_config_element::<crate::util::config::MachineLearning>().unwrap();
            let cfg_backup = &cfg.model_defs_path;

            set_config(
                "machinelearning.model_defs_path",
                test_data!("pro/ml").to_str().unwrap(),
            )
            .unwrap();
            let ctx =app_ctx
                .default_session_context()
                .await
                .unwrap();

            let exe_ctx = ctx.execution_context().unwrap();

            let model_path = PathBuf::from("xgboost/s2_10m_de_marburg/model.json");
            let mut model = exe_ctx.read_ml_model(model_path).await.unwrap();

            let actual: String = model.drain(0..277).collect();

            set_config(
                "machinelearning.model_defs_path",
                cfg_backup.to_str().unwrap(),
            )
            .unwrap();

            let expected = "{\"learner\":{\"attributes\":{},\"feature_names\":[],\"feature_types\":[],\"gradient_booster\":{\"model\":{\"gbtree_model_param\":{\"num_parallel_tree\":\"1\",\"num_trees\":\"16\",\"size_leaf_vector\":\"0\"},\"tree_info\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],\"trees\":[{\"base_weights\":[5.192308E-1,9.722222E-1";

            assert_eq!(actual, expected);
        }).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial]
    async fn write_model_test() {
        with_temp_context(|app_ctx, _| async move {
            let cfg = get_config_element::<crate::util::config::MachineLearning>().unwrap();
            let cfg_backup = cfg.model_defs_path;

            let tmp_dir = tempfile::tempdir().unwrap();
            let tmp_path = tmp_dir.path();
            std::fs::create_dir_all(tmp_path.join("pro/ml/xgboost")).unwrap();

            let temp_ml_path = tmp_path.join("pro/ml").to_str().unwrap().to_string();

            set_config("machinelearning.model_defs_path", temp_ml_path).unwrap();

            let ctx = app_ctx.default_session_context().await.unwrap();

            let mut exe_ctx = ctx.execution_context().unwrap();

            let model_path = PathBuf::from("xgboost/model.json");

            exe_ctx
                .write_ml_model(model_path, String::from("model content"))
                .await
                .unwrap();

            set_config(
                "machinelearning.model_defs_path",
                cfg_backup.to_str().unwrap(),
            )
            .unwrap();

            let actual = tokio::fs::read_to_string(tmp_path.join("pro/ml/xgboost/model.json"))
                .await
                .unwrap();

            assert_eq!(actual, "model content");
        })
        .await;
    }
}
