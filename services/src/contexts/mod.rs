use crate::error::Result;
use crate::layers::storage::{LayerDb, LayerProviderDb};
use crate::tasks::{TaskContext, TaskManager};
use crate::util::config::get_config_element;
use crate::util::path_with_base_path;
use crate::{projects::ProjectDb, workflows::registry::WorkflowRegistry};
use async_trait::async_trait;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::util::canonicalize_subpath;
use rayon::ThreadPool;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

mod in_memory;
mod session;
mod simple_context;

use crate::datasets::storage::DatasetDb;

use geoengine_datatypes::dataset::DataId;

use geoengine_datatypes::raster::TilingSpecification;
use geoengine_operators::engine::{
    ChunkByteSize, CreateSpan, ExecutionContext, InitializedPlotOperator,
    InitializedVectorOperator, MetaData, MetaDataProvider, QueryAbortRegistration,
    QueryAbortTrigger, QueryContext, QueryContextExtensions, RasterResultDescriptor,
    VectorResultDescriptor,
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

    fn query_context(&self, session: Self::Session) -> Result<Self::QueryContext>;

    fn execution_context(&self, session: Self::Session) -> Result<Self::ExecutionContext>;

    async fn session_by_id(&self, session_id: SessionId) -> Result<Self::Session>;
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

#[async_trait::async_trait]
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
        _span: CreateSpan,
    ) -> Box<dyn geoengine_operators::engine::InitializedRasterOperator> {
        op
    }

    fn wrap_initialized_vector_operator(
        &self,
        op: Box<dyn InitializedVectorOperator>,
        _span: CreateSpan,
    ) -> Box<dyn InitializedVectorOperator> {
        op
    }

    fn wrap_initialized_plot_operator(
        &self,
        op: Box<dyn InitializedPlotOperator>,
        _span: CreateSpan,
    ) -> Box<dyn InitializedPlotOperator> {
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
        let mut file = File::create(model_path).await?;
        file.write_all(ml_model_str.as_bytes()).await?;
        file.flush().await?;

        Ok(())
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

#[cfg(test)]

mod tests {
    use super::*;
    use std::path::PathBuf;

    use geoengine_datatypes::{test_data, util::test::TestDefault};
    use serial_test::serial;

    use crate::{
        contexts::{Context, InMemoryContext, SimpleSession},
        util::config::set_config,
    };

    #[tokio::test]
    #[serial]
    async fn read_model_test() {
        let cfg = get_config_element::<crate::util::config::MachineLearning>().unwrap();
        let cfg_backup = &cfg.model_defs_path;

        set_config(
            "machinelearning.model_defs_path",
            test_data!("pro/ml").to_str().unwrap(),
        )
        .unwrap();

        let ctx = InMemoryContext::test_default();
        let exe_ctx = ctx.execution_context(SimpleSession::default()).unwrap();

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
    }

    #[tokio::test]
    #[serial]
    async fn write_model_test() {
        let cfg = get_config_element::<crate::util::config::MachineLearning>().unwrap();
        let cfg_backup = cfg.model_defs_path;

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path();
        std::fs::create_dir_all(tmp_path.join("pro/ml/xgboost")).unwrap();

        let temp_ml_path = tmp_path.join("pro/ml").to_str().unwrap().to_string();

        set_config("machinelearning.model_defs_path", temp_ml_path).unwrap();
        let ctx = InMemoryContext::test_default();
        let mut exe_ctx = ctx.execution_context(SimpleSession::default()).unwrap();

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
    }
}
