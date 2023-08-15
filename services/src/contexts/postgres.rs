use crate::api::model::datatypes::DatasetName;
use crate::contexts::{ApplicationContext, QueryContextImpl, SessionId, SimpleSession};
use crate::contexts::{GeoEngineDb, SessionContext};
use crate::datasets::add_from_directory::{
    add_datasets_from_directory, add_providers_from_directory,
};
use crate::datasets::upload::{Volume, Volumes};
use crate::error::{self, Error, Result};
use crate::layers::add_from_directory::{
    add_layer_collections_from_directory, add_layers_from_directory, UNSORTED_COLLECTION_ID,
};
use crate::layers::storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID;

use crate::projects::{ProjectId, STRectangle};
use crate::tasks::{SimpleTaskManager, SimpleTaskManagerBackend, SimpleTaskManagerContext};
use crate::util::config::get_config_element;
use async_trait::async_trait;
use bb8_postgres::{
    bb8::Pool,
    bb8::PooledConnection,
    tokio_postgres::{error::SqlState, tls::MakeTlsConnect, tls::TlsConnect, Config, Socket},
    PostgresConnectionManager,
};
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_operators::engine::ChunkByteSize;
use geoengine_operators::util::create_rayon_thread_pool;
use log::{debug, info};
use rayon::ThreadPool;
use std::path::PathBuf;
use std::sync::Arc;

use super::{ExecutionContextImpl, Session, SimpleApplicationContext};

// TODO: distinguish user-facing errors from system-facing error messages

/// A context with references to Postgres backends of the database.
#[derive(Clone)]
pub struct PostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    default_session_id: SessionId,
    thread_pool: Arc<ThreadPool>,
    exe_ctx_tiling_spec: TilingSpecification,
    query_ctx_chunk_size: ChunkByteSize,
    task_manager: Arc<SimpleTaskManagerBackend>,
    pool: Pool<PostgresConnectionManager<Tls>>,
    volumes: Volumes,
}

enum DatabaseStatus {
    Unitialized,
    InitializedClearDatabase,
    InitializedKeepDatabase,
}

impl<Tls> PostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub async fn new_with_context_spec(
        config: Config,
        tls: Tls,
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
    ) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(config, tls);

        let pool = Pool::builder().build(pg_mgr).await?;
        let created_schema = Self::create_schema(pool.get().await?).await?;

        let session = if created_schema {
            let session = SimpleSession::default();
            Self::create_default_session(pool.get().await?, session.id()).await?;
            session
        } else {
            Self::load_default_session(pool.get().await?).await?
        };

        Ok(PostgresContext {
            default_session_id: session.id(),
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            pool,
            volumes: Default::default(),
        })
    }

    // TODO: check if the datasets exist already and don't output warnings when skipping them
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_data(
        config: Config,
        tls: Tls,
        dataset_defs_path: PathBuf,
        provider_defs_path: PathBuf,
        layer_defs_path: PathBuf,
        layer_collection_defs_path: PathBuf,
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
    ) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(config, tls);

        let pool = Pool::builder().build(pg_mgr).await?;
        let created_schema = Self::create_schema(pool.get().await?).await?;

        let session = if created_schema {
            let session = SimpleSession::default();
            Self::create_default_session(pool.get().await?, session.id()).await?;
            session
        } else {
            Self::load_default_session(pool.get().await?).await?
        };

        let app_ctx = PostgresContext {
            default_session_id: session.id(),
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            pool,
            volumes: Default::default(),
        };

        if created_schema {
            info!("Populating database with initial data...");

            let ctx = app_ctx.session_context(session);

            let mut db = ctx.db();
            add_layers_from_directory(&mut db, layer_defs_path).await;
            add_layer_collections_from_directory(&mut db, layer_collection_defs_path).await;

            add_datasets_from_directory(&mut db, dataset_defs_path).await;

            add_providers_from_directory(&mut db, provider_defs_path, &[]).await;
        }

        Ok(app_ctx)
    }

    async fn check_schema_status(
        conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
    ) -> Result<DatabaseStatus> {
        let stmt = match conn
            .prepare("SELECT clear_database_on_start from geoengine;")
            .await
        {
            Ok(stmt) => stmt,
            Err(e) => {
                if let Some(code) = e.code() {
                    if *code == SqlState::UNDEFINED_TABLE {
                        info!("Initializing schema.");
                        return Ok(DatabaseStatus::Unitialized);
                    }
                }
                return Err(error::Error::TokioPostgres { source: e });
            }
        };

        let row = conn.query_one(&stmt, &[]).await?;

        if row.get(0) {
            Ok(DatabaseStatus::InitializedClearDatabase)
        } else {
            Ok(DatabaseStatus::InitializedKeepDatabase)
        }
    }

    #[allow(clippy::too_many_lines)]
    /// Creates the database schema. Returns true if the schema was created, false if it already existed.
    pub(crate) async fn create_schema(
        mut conn: PooledConnection<'_, PostgresConnectionManager<Tls>>,
    ) -> Result<bool> {
        let postgres_config = get_config_element::<crate::util::config::Postgres>()?;

        let database_status = Self::check_schema_status(&conn).await?;

        match database_status {
            DatabaseStatus::InitializedClearDatabase if postgres_config.clear_database_on_start => {
                let schema_name = postgres_config.schema;
                info!("Clearing schema {}.", schema_name);
                conn.batch_execute(&format!(
                    "DROP SCHEMA {schema_name} CASCADE; CREATE SCHEMA {schema_name};"
                ))
                .await?;
            }
            DatabaseStatus::InitializedKeepDatabase if postgres_config.clear_database_on_start => {
                return Err(Error::ClearDatabaseOnStartupNotAllowed)
            }
            DatabaseStatus::InitializedClearDatabase | DatabaseStatus::InitializedKeepDatabase => {
                return Ok(false)
            }
            DatabaseStatus::Unitialized => (),
        };

        let tx = conn.build_transaction().start().await?;

        tx.batch_execute(include_str!("schema.sql")).await?;

        let stmt = tx
            .prepare(
                "
            INSERT INTO geoengine (clear_database_on_start) VALUES ($1);",
            )
            .await?;

        tx.execute(&stmt, &[&postgres_config.clear_database_on_start])
            .await?;

        let stmt = tx
            .prepare(
                r#"
            INSERT INTO layer_collections (
                id,
                name,
                description,
                properties
            ) VALUES (
                $1,
                'Layers',
                'All available Geo Engine layers',
                ARRAY[]::"PropertyType"[]
            );"#,
            )
            .await?;

        tx.execute(&stmt, &[&INTERNAL_LAYER_DB_ROOT_COLLECTION_ID])
            .await?;

        let stmt = tx
            .prepare(
                r#"INSERT INTO layer_collections (
                id,
                name,
                description,
                properties
            ) VALUES (
                $1,
                'Unsorted',
                'Unsorted Layers',
                ARRAY[]::"PropertyType"[]
            );"#,
            )
            .await?;

        tx.execute(&stmt, &[&UNSORTED_COLLECTION_ID]).await?;

        let stmt = tx
            .prepare(
                r#"
            INSERT INTO collection_children (parent, child) 
            VALUES ($1, $2);"#,
            )
            .await?;

        tx.execute(
            &stmt,
            &[
                &INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
                &UNSORTED_COLLECTION_ID,
            ],
        )
        .await?;

        tx.commit().await?;

        debug!("Created database schema");

        Ok(true)
    }

    async fn create_default_session(
        conn: PooledConnection<'_, PostgresConnectionManager<Tls>>,
        session_id: SessionId,
    ) -> Result<()> {
        let stmt = conn
            .prepare("INSERT INTO sessions (id, project_id, view) VALUES ($1, NULL ,NULL);")
            .await?;

        conn.execute(&stmt, &[&session_id]).await?;

        Ok(())
    }
    async fn load_default_session(
        conn: PooledConnection<'_, PostgresConnectionManager<Tls>>,
    ) -> Result<SimpleSession> {
        let stmt = conn
            .prepare("SELECT id, project_id, view FROM sessions LIMIT 1;")
            .await?;

        let row = conn.query_one(&stmt, &[]).await?;

        Ok(SimpleSession::new(row.get(0), row.get(1), row.get(2)))
    }
}

#[async_trait]
impl<Tls> SimpleApplicationContext for PostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn default_session_id(&self) -> SessionId {
        self.default_session_id
    }

    async fn default_session(&self) -> Result<SimpleSession> {
        Self::load_default_session(self.pool.get().await?).await
    }

    async fn update_default_session_project(&self, project: ProjectId) -> Result<()> {
        let conn = self.pool.get().await?;

        let stmt = conn
            .prepare("UPDATE sessions SET project_id = $1 WHERE id = $2;")
            .await?;

        conn.execute(&stmt, &[&project, &self.default_session_id])
            .await?;

        Ok(())
    }

    async fn update_default_session_view(&self, view: STRectangle) -> Result<()> {
        let conn = self.pool.get().await?;

        let stmt = conn
            .prepare("UPDATE sessions SET view = $1 WHERE id = $2;")
            .await?;

        conn.execute(&stmt, &[&view, &self.default_session_id])
            .await?;

        Ok(())
    }

    async fn default_session_context(&self) -> Result<Self::SessionContext> {
        Ok(self.session_context(self.session_by_id(self.default_session_id).await?))
    }
}

#[async_trait]
impl<Tls> ApplicationContext for PostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type SessionContext = PostgresSessionContext<Tls>;
    type Session = SimpleSession;

    fn session_context(&self, session: Self::Session) -> Self::SessionContext {
        PostgresSessionContext {
            session,
            context: self.clone(),
        }
    }

    async fn session_by_id(&self, session_id: SessionId) -> Result<Self::Session> {
        let mut conn = self.pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        let stmt = tx
            .prepare(
                "
            SELECT           
                project_id,
                view
            FROM sessions
            WHERE id = $1;",
            )
            .await?;

        let row = tx
            .query_one(&stmt, &[&session_id])
            .await
            .map_err(|_error| error::Error::InvalidSession)?;

        Ok(SimpleSession::new(session_id, row.get(0), row.get(1)))
    }
}

#[derive(Clone)]
pub struct PostgresSessionContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    session: SimpleSession,
    context: PostgresContext<Tls>,
}

#[async_trait]
impl<Tls> SessionContext for PostgresSessionContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Session = SimpleSession;
    type GeoEngineDB = PostgresDb<Tls>;

    type TaskContext = SimpleTaskManagerContext;
    type TaskManager = SimpleTaskManager; // this does not persist across restarts
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<Self::GeoEngineDB>;

    fn db(&self) -> Self::GeoEngineDB {
        PostgresDb::new(self.context.pool.clone())
    }

    fn tasks(&self) -> Self::TaskManager {
        SimpleTaskManager::new(self.context.task_manager.clone())
    }

    fn query_context(&self) -> Result<Self::QueryContext> {
        Ok(QueryContextImpl::new(
            self.context.query_ctx_chunk_size,
            self.context.thread_pool.clone(),
        ))
    }

    fn execution_context(&self) -> Result<Self::ExecutionContext> {
        Ok(ExecutionContextImpl::<PostgresDb<Tls>>::new(
            self.db(),
            self.context.thread_pool.clone(),
            self.context.exe_ctx_tiling_spec,
        ))
    }

    fn volumes(&self) -> Result<Vec<Volume>> {
        Ok(self.context.volumes.volumes.clone())
    }

    fn session(&self) -> &Self::Session {
        &self.session
    }
}

pub struct PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub(crate) conn_pool: Pool<PostgresConnectionManager<Tls>>,
}

impl<Tls> PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(conn_pool: Pool<PostgresConnectionManager<Tls>>) -> Self {
        Self { conn_pool }
    }

    /// Check whether the namepsace of the given dataset is allowed for insertion
    /// Check whether the namepsace of the given dataset is allowed for insertion
    pub(crate) fn check_namespace(id: &DatasetName) -> Result<()> {
        // due to a lack of users, etc., we only allow one namespace for now
        if id.namespace.is_none() {
            Ok(())
        } else {
            Err(Error::InvalidDatasetIdNamespace)
        }
    }
}

impl<Tls> GeoEngineDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::marker::PhantomData;
    use std::str::FromStr;

    use super::*;
    use crate::api::model::datatypes::{
        Breakpoint, ClassificationMeasurement, Colorizer, ContinuousMeasurement, DataProviderId,
        DatasetName, DefaultColors, LayerId, LinearGradient, LogarithmicGradient, Measurement,
        MultiLineString, MultiPoint, MultiPolygon, NoGeometry, NotNanF64, OverUnderColors, Palette,
        RasterPropertiesEntryType, RasterPropertiesKey, RgbaColor, SpatialPartition2D, StringPair,
    };
    use crate::api::model::operators::{
        GdalSourceTimePlaceholder, PlotResultDescriptor, TimeReference, UnixTimeStampType,
    };
    use crate::api::model::responses::datasets::DatasetIdAndName;
    use crate::api::model::services::AddDataset;
    use crate::api::model::{ColorizerTypeDbType, HashMapTextTextDbType};
    use crate::datasets::external::mock::{MockCollection, MockExternalLayerProviderDefinition};
    use crate::datasets::listing::{DatasetListOptions, DatasetListing, ProvenanceOutput};
    use crate::datasets::listing::{DatasetProvider, Provenance};
    use crate::datasets::storage::{DatasetStore, MetaDataDefinition};
    use crate::datasets::upload::{FileId, UploadId};
    use crate::datasets::upload::{FileUpload, Upload, UploadDb};
    use crate::layers::layer::{
        AddLayer, AddLayerCollection, CollectionItem, LayerCollection, LayerCollectionListOptions,
        LayerCollectionListing, LayerListing, ProviderLayerCollectionId, ProviderLayerId,
    };
    use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
    use crate::layers::storage::{
        LayerDb, LayerProviderDb, LayerProviderListing, LayerProviderListingOptions,
        INTERNAL_PROVIDER_ID,
    };
    use crate::projects::{
        ColorParam, CreateProject, DerivedColor, DerivedNumber, LayerUpdate, LineSymbology,
        LoadVersion, NumberParam, OrderBy, Plot, PlotUpdate, PointSymbology, PolygonSymbology,
        ProjectDb, ProjectFilter, ProjectId, ProjectLayer, ProjectListOptions, ProjectListing,
        RasterSymbology, STRectangle, StrokeParam, Symbology, TextSymbology, UpdateProject,
    };
    use crate::util::tests::register_ndvi_workflow_helper;
    use crate::util::tests::with_temp_context;
    use crate::workflows::registry::WorkflowRegistry;
    use crate::workflows::workflow::Workflow;
    use bb8_postgres::tokio_postgres::NoTls;
    use futures::join;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::primitives::CacheTtlSeconds;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, Coordinate2D, FeatureDataType, RasterQueryRectangle, SpatialResolution,
        TimeGranularity, TimeInstance, TimeInterval, TimeStep, VectorQueryRectangle,
    };
    use geoengine_datatypes::raster::RasterDataType;
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
    use geoengine_operators::engine::{
        MetaData, MetaDataProvider, MultipleRasterOrSingleVectorSource, PlotOperator,
        RasterResultDescriptor, StaticMetaData, TypedOperator, TypedResultDescriptor,
        VectorColumnInfo, VectorOperator, VectorResultDescriptor,
    };
    use geoengine_operators::mock::{MockPointSource, MockPointSourceParams};
    use geoengine_operators::plot::{Statistics, StatisticsParams};
    use geoengine_operators::source::{
        CsvHeader, FileNotFoundHandling, FormatSpecifics, GdalDatasetGeoTransform,
        GdalDatasetParameters, GdalLoadingInfo, GdalMetaDataList, GdalMetaDataRegular,
        GdalMetaDataStatic, GdalMetadataNetCdfCf, OgrSourceColumnSpec, OgrSourceDataset,
        OgrSourceDatasetTimeType, OgrSourceDurationSpec, OgrSourceErrorSpec, OgrSourceTimeFormat,
    };
    use geoengine_operators::util::input::MultiRasterOrVectorOperator::Raster;
    use ordered_float::NotNan;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test() {
        with_temp_context(|app_ctx, _| async move {
            let session = app_ctx.default_session().await.unwrap();

            create_projects(&app_ctx, &session).await;

            let projects = list_projects(&app_ctx, &session).await;

            let project_id = projects[0].id;

            update_projects(&app_ctx, &session, project_id).await;

            delete_project(&app_ctx, &session, project_id).await;
        })
        .await;
    }

    async fn delete_project(
        app_ctx: &PostgresContext<NoTls>,
        session: &SimpleSession,
        project_id: ProjectId,
    ) {
        let db = app_ctx.session_context(session.clone()).db();

        db.delete_project(project_id).await.unwrap();

        assert!(db.load_project(project_id).await.is_err());
    }

    #[allow(clippy::too_many_lines)]
    async fn update_projects(
        app_ctx: &PostgresContext<NoTls>,
        session: &SimpleSession,
        project_id: ProjectId,
    ) {
        let db = app_ctx.session_context(session.clone()).db();

        let project = db
            .load_project_version(project_id, LoadVersion::Latest)
            .await
            .unwrap();

        let layer_workflow_id = db
            .register_workflow(Workflow {
                operator: TypedOperator::Vector(
                    MockPointSource {
                        params: MockPointSourceParams {
                            points: vec![Coordinate2D::new(1., 2.); 3],
                        },
                    }
                    .boxed(),
                ),
            })
            .await
            .unwrap();

        assert!(db.load_workflow(&layer_workflow_id).await.is_ok());

        let plot_workflow_id = db
            .register_workflow(Workflow {
                operator: Statistics {
                    params: StatisticsParams {
                        column_names: vec![],
                    },
                    sources: MultipleRasterOrSingleVectorSource {
                        source: Raster(vec![]),
                    },
                }
                .boxed()
                .into(),
            })
            .await
            .unwrap();

        assert!(db.load_workflow(&plot_workflow_id).await.is_ok());

        // add a plot
        let update = UpdateProject {
            id: project.id,
            name: Some("Test9 Updated".into()),
            description: None,
            layers: Some(vec![LayerUpdate::UpdateOrInsert(ProjectLayer {
                workflow: layer_workflow_id,
                name: "TestLayer".into(),
                symbology: PointSymbology::default().into(),
                visibility: Default::default(),
            })]),
            plots: Some(vec![PlotUpdate::UpdateOrInsert(Plot {
                workflow: plot_workflow_id,
                name: "Test Plot".into(),
            })]),
            bounds: None,
            time_step: None,
        };
        db.update_project(update).await.unwrap();

        let versions = db.list_project_versions(project_id).await.unwrap();
        assert_eq!(versions.len(), 2);

        // add second plot
        let update = UpdateProject {
            id: project.id,
            name: Some("Test9 Updated".into()),
            description: None,
            layers: Some(vec![LayerUpdate::UpdateOrInsert(ProjectLayer {
                workflow: layer_workflow_id,
                name: "TestLayer".into(),
                symbology: PointSymbology::default().into(),
                visibility: Default::default(),
            })]),
            plots: Some(vec![
                PlotUpdate::UpdateOrInsert(Plot {
                    workflow: plot_workflow_id,
                    name: "Test Plot".into(),
                }),
                PlotUpdate::UpdateOrInsert(Plot {
                    workflow: plot_workflow_id,
                    name: "Test Plot".into(),
                }),
            ]),
            bounds: None,
            time_step: None,
        };
        db.update_project(update).await.unwrap();

        let versions = db.list_project_versions(project_id).await.unwrap();
        assert_eq!(versions.len(), 3);

        // delete plots
        let update = UpdateProject {
            id: project.id,
            name: None,
            description: None,
            layers: None,
            plots: Some(vec![]),
            bounds: None,
            time_step: None,
        };
        db.update_project(update).await.unwrap();

        let versions = db.list_project_versions(project_id).await.unwrap();
        assert_eq!(versions.len(), 4);
    }

    async fn list_projects(
        app_ctx: &PostgresContext<NoTls>,
        session: &SimpleSession,
    ) -> Vec<ProjectListing> {
        let options = ProjectListOptions {
            filter: ProjectFilter::None,
            order: OrderBy::NameDesc,
            offset: 0,
            limit: 2,
        };

        let db = app_ctx.session_context(session.clone()).db();

        let projects = db.list_projects(options).await.unwrap();

        assert_eq!(projects.len(), 2);
        assert_eq!(projects[0].name, "Test9");
        assert_eq!(projects[1].name, "Test8");
        projects
    }

    async fn create_projects(app_ctx: &PostgresContext<NoTls>, session: &SimpleSession) {
        let db = app_ctx.session_context(session.clone()).db();

        for i in 0..10 {
            let create = CreateProject {
                name: format!("Test{i}"),
                description: format!("Test{}", 10 - i),
                bounds: STRectangle::new(
                    SpatialReferenceOption::Unreferenced,
                    0.,
                    0.,
                    1.,
                    1.,
                    0,
                    1,
                )
                .unwrap(),
                time_step: None,
            };
            db.create_project(create).await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_persists_workflows() {
        with_temp_context(|app_ctx, _pg_config| async move {
            let workflow = Workflow {
                operator: TypedOperator::Vector(
                    MockPointSource {
                        params: MockPointSourceParams {
                            points: vec![Coordinate2D::new(1., 2.); 3],
                        },
                    }
                    .boxed(),
                ),
            };

            let session = app_ctx.default_session().await.unwrap();
        let ctx = app_ctx.session_context(session);

            let db = ctx
                .db();
            let id = db
                .register_workflow(workflow)
                .await
                .unwrap();

            drop(ctx);

            let workflow = db.load_workflow(&id).await.unwrap();

            let json = serde_json::to_string(&workflow).unwrap();
            assert_eq!(json, r#"{"type":"Vector","operator":{"type":"MockPointSource","params":{"points":[{"x":1.0,"y":2.0},{"x":1.0,"y":2.0},{"x":1.0,"y":2.0}]}}}"#);
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_persists_datasets() {
        with_temp_context(|app_ctx, _| async move {
            let loading_info = OgrSourceDataset {
                file_name: PathBuf::from("test.csv"),
                layer_name: "test.csv".to_owned(),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::Start {
                    start_field: "start".to_owned(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                },
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: Some(FormatSpecifics::Csv {
                        header: CsvHeader::Auto,
                    }),
                    x: "x".to_owned(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: vec![],
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            };

            let meta_data = MetaDataDefinition::OgrMetaData(StaticMetaData::<
                OgrSourceDataset,
                VectorResultDescriptor,
                VectorQueryRectangle,
            > {
                loading_info: loading_info.clone(),
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [(
                        "foo".to_owned(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Float,
                            measurement: Measurement::Unitless.into(),
                        },
                    )]
                    .into_iter()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            });

            let session = app_ctx.default_session().await.unwrap();

            let dataset_name = DatasetName::new(None, "my_dataset");

            let db = app_ctx.session_context(session.clone()).db();
            let wrap = db.wrap_meta_data(meta_data);
            let DatasetIdAndName {
                id: dataset_id,
                name: dataset_name,
            } = db
                .add_dataset(
                    AddDataset {
                        name: Some(dataset_name.clone()),
                        display_name: "Ogr Test".to_owned(),
                        description: "desc".to_owned(),
                        source_operator: "OgrSource".to_owned(),
                        symbology: None,
                        provenance: Some(vec![Provenance {
                            citation: "citation".to_owned(),
                            license: "license".to_owned(),
                            uri: "uri".to_owned(),
                        }]),
                    },
                    wrap,
                )
                .await
                .unwrap();

            let datasets = db
                .list_datasets(DatasetListOptions {
                    filter: None,
                    order: crate::datasets::listing::OrderBy::NameAsc,
                    offset: 0,
                    limit: 10,
                })
                .await
                .unwrap();

            assert_eq!(datasets.len(), 1);

            assert_eq!(
                datasets[0],
                DatasetListing {
                    id: dataset_id,
                    name: dataset_name,
                    display_name: "Ogr Test".to_owned(),
                    description: "desc".to_owned(),
                    source_operator: "OgrSource".to_owned(),
                    symbology: None,
                    tags: vec![],
                    result_descriptor: TypedResultDescriptor::Vector(VectorResultDescriptor {
                        data_type: VectorDataType::MultiPoint,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        columns: [(
                            "foo".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Float,
                                measurement: Measurement::Unitless.into()
                            }
                        )]
                        .into_iter()
                        .collect(),
                        time: None,
                        bbox: None,
                    })
                    .into(),
                },
            );

            let provenance = db.load_provenance(&dataset_id).await.unwrap();

            assert_eq!(
                provenance,
                ProvenanceOutput {
                    data: dataset_id.into(),
                    provenance: Some(vec![Provenance {
                        citation: "citation".to_owned(),
                        license: "license".to_owned(),
                        uri: "uri".to_owned(),
                    }])
                }
            );

            let meta_data: Box<dyn MetaData<OgrSourceDataset, _, _>> =
                db.meta_data(&dataset_id.into()).await.unwrap();

            assert_eq!(
                meta_data
                    .loading_info(VectorQueryRectangle {
                        spatial_bounds: BoundingBox2D::new_unchecked(
                            (-180., -90.).into(),
                            (180., 90.).into()
                        ),
                        time_interval: TimeInterval::default(),
                        spatial_resolution: SpatialResolution::zero_point_one(),
                    })
                    .await
                    .unwrap(),
                loading_info
            );
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_persists_uploads() {
        with_temp_context(|app_ctx, _| async move {
            let id = UploadId::from_str("2de18cd8-4a38-4111-a445-e3734bc18a80").unwrap();
            let input = Upload {
                id,
                files: vec![FileUpload {
                    id: FileId::from_str("e80afab0-831d-4d40-95d6-1e4dfd277e72").unwrap(),
                    name: "test.csv".to_owned(),
                    byte_size: 1337,
                }],
            };

            let session = app_ctx.default_session().await.unwrap();

            let db = app_ctx.session_context(session.clone()).db();

            db.create_upload(input.clone()).await.unwrap();

            let upload = db.load_upload(id).await.unwrap();

            assert_eq!(upload, input);
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_persists_layer_providers() {
        with_temp_context(|app_ctx, _| async move {
            let db = app_ctx.default_session_context().await.unwrap().db();

            let provider_id =
                DataProviderId::from_str("7b20c8d7-d754-4f8f-ad44-dddd25df22d2").unwrap();

            let loading_info = OgrSourceDataset {
                file_name: PathBuf::from("test.csv"),
                layer_name: "test.csv".to_owned(),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::Start {
                    start_field: "start".to_owned(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                },
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: Some(FormatSpecifics::Csv {
                        header: CsvHeader::Auto,
                    }),
                    x: "x".to_owned(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: vec![],
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            };

            let meta_data = MetaDataDefinition::OgrMetaData(StaticMetaData::<
                OgrSourceDataset,
                VectorResultDescriptor,
                VectorQueryRectangle,
            > {
                loading_info: loading_info.clone(),
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [(
                        "foo".to_owned(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Float,
                            measurement: Measurement::Unitless.into(),
                        },
                    )]
                    .into_iter()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            });

            let provider = MockExternalLayerProviderDefinition {
                id: provider_id,
                root_collection: MockCollection {
                    id: LayerCollectionId("b5f82c7c-9133-4ac1-b4ae-8faac3b9a6df".to_owned()),
                    name: "Mock Collection A".to_owned(),
                    description: "Some description".to_owned(),
                    collections: vec![MockCollection {
                        id: LayerCollectionId("21466897-37a1-4666-913a-50b5244699ad".to_owned()),
                        name: "Mock Collection B".to_owned(),
                        description: "Some description".to_owned(),
                        collections: vec![],
                        layers: vec![],
                    }],
                    layers: vec![],
                },
                data: [("myData".to_owned(), meta_data)].into_iter().collect(),
            };

            db.add_layer_provider(Box::new(provider)).await.unwrap();

            let providers = db
                .list_layer_providers(LayerProviderListingOptions {
                    offset: 0,
                    limit: 10,
                })
                .await
                .unwrap();

            assert_eq!(providers.len(), 1);

            assert_eq!(
                providers[0],
                LayerProviderListing {
                    id: provider_id,
                    name: "MockName".to_owned(),
                    description: "MockType".to_owned(),
                }
            );

            let provider = db.load_layer_provider(provider_id).await.unwrap();

            let datasets = provider
                .load_layer_collection(
                    &provider.get_root_layer_collection_id().await.unwrap(),
                    LayerCollectionListOptions {
                        offset: 0,
                        limit: 10,
                    },
                )
                .await
                .unwrap();

            assert_eq!(datasets.items.len(), 1);
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_loads_all_meta_data_types() {
        with_temp_context(|app_ctx, _| async move {
            let session = app_ctx.default_session().await.unwrap();

            let db = app_ctx.session_context(session.clone()).db();

            let vector_descriptor = VectorResultDescriptor {
                data_type: VectorDataType::Data,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: Default::default(),
                time: None,
                bbox: None,
            };

            let raster_descriptor = RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                measurement: Default::default(),
                time: None,
                bbox: None,
                resolution: None,
            };

            let vector_ds = AddDataset {
                name: None,
                display_name: "OgrDataset".to_string(),
                description: "My Ogr dataset".to_string(),
                source_operator: "OgrSource".to_string(),
                symbology: None,
                provenance: None,
            };

            let raster_ds = AddDataset {
                name: None,
                display_name: "GdalDataset".to_string(),
                description: "My Gdal dataset".to_string(),
                source_operator: "GdalSource".to_string(),
                symbology: None,
                provenance: None,
            };

            let gdal_params = GdalDatasetParameters {
                file_path: Default::default(),
                rasterband_channel: 0,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: Default::default(),
                    x_pixel_size: 0.0,
                    y_pixel_size: 0.0,
                },
                width: 0,
                height: 0,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: None,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: false,
                retry: None,
            };

            let meta = StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: Default::default(),
                    layer_name: String::new(),
                    data_type: None,
                    time: Default::default(),
                    default_geometry: None,
                    columns: None,
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: CacheTtlSeconds::default(),
                },
                result_descriptor: vector_descriptor.clone(),
                phantom: Default::default(),
            };

            let meta = db.wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let id = db.add_dataset(vector_ds, meta).await.unwrap().id;

            let meta: geoengine_operators::util::Result<
                Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
            > = db.meta_data(&id.into()).await;

            assert!(meta.is_ok());

            let meta = GdalMetaDataRegular {
                result_descriptor: raster_descriptor.clone(),
                params: gdal_params.clone(),
                time_placeholders: Default::default(),
                data_time: Default::default(),
                step: TimeStep {
                    granularity: TimeGranularity::Millis,
                    step: 0,
                },
                cache_ttl: CacheTtlSeconds::default(),
            };

            let meta = db.wrap_meta_data(MetaDataDefinition::GdalMetaDataRegular(meta));

            let id = db.add_dataset(raster_ds.clone(), meta).await.unwrap().id;

            let meta: geoengine_operators::util::Result<
                Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
            > = db.meta_data(&id.into()).await;

            assert!(meta.is_ok());

            let meta = GdalMetaDataStatic {
                time: None,
                params: gdal_params.clone(),
                result_descriptor: raster_descriptor.clone(),
                cache_ttl: CacheTtlSeconds::default(),
            };

            let meta = db.wrap_meta_data(MetaDataDefinition::GdalStatic(meta));

            let id = db.add_dataset(raster_ds.clone(), meta).await.unwrap().id;

            let meta: geoengine_operators::util::Result<
                Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
            > = db.meta_data(&id.into()).await;

            assert!(meta.is_ok());

            let meta = GdalMetaDataList {
                result_descriptor: raster_descriptor.clone(),
                params: vec![],
            };

            let meta = db.wrap_meta_data(MetaDataDefinition::GdalMetaDataList(meta));

            let id = db.add_dataset(raster_ds.clone(), meta).await.unwrap().id;

            let meta: geoengine_operators::util::Result<
                Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
            > = db.meta_data(&id.into()).await;

            assert!(meta.is_ok());

            let meta = GdalMetadataNetCdfCf {
                result_descriptor: raster_descriptor.clone(),
                params: gdal_params.clone(),
                start: TimeInstance::MIN,
                end: TimeInstance::MAX,
                step: TimeStep {
                    granularity: TimeGranularity::Millis,
                    step: 0,
                },
                band_offset: 0,
                cache_ttl: CacheTtlSeconds::default(),
            };

            let meta = db.wrap_meta_data(MetaDataDefinition::GdalMetadataNetCdfCf(meta));

            let id = db.add_dataset(raster_ds.clone(), meta).await.unwrap().id;

            let meta: geoengine_operators::util::Result<
                Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
            > = db.meta_data(&id.into()).await;

            assert!(meta.is_ok());
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_collects_layers() {
        with_temp_context(|app_ctx, _| async move {
            let session = app_ctx.default_session().await.unwrap();

            let layer_db = app_ctx.session_context(session).db();

            let workflow = Workflow {
                operator: TypedOperator::Vector(
                    MockPointSource {
                        params: MockPointSourceParams {
                            points: vec![Coordinate2D::new(1., 2.); 3],
                        },
                    }
                    .boxed(),
                ),
            };

            let root_collection_id = layer_db.get_root_layer_collection_id().await.unwrap();

            let layer1 = layer_db
                .add_layer(
                    AddLayer {
                        name: "Layer1".to_string(),
                        description: "Layer 1".to_string(),
                        symbology: None,
                        workflow: workflow.clone(),
                        metadata: [("meta".to_string(), "datum".to_string())].into(),
                        properties: vec![("proper".to_string(), "tee".to_string()).into()],
                    },
                    &root_collection_id,
                )
                .await
                .unwrap();

            assert_eq!(
                layer_db.load_layer(&layer1).await.unwrap(),
                crate::layers::layer::Layer {
                    id: ProviderLayerId {
                        provider_id: INTERNAL_PROVIDER_ID,
                        layer_id: layer1.clone(),
                    },
                    name: "Layer1".to_string(),
                    description: "Layer 1".to_string(),
                    symbology: None,
                    workflow: workflow.clone(),
                    metadata: [("meta".to_string(), "datum".to_string())].into(),
                    properties: vec![("proper".to_string(), "tee".to_string()).into()],
                }
            );

            let collection1_id = layer_db
                .add_layer_collection(
                    AddLayerCollection {
                        name: "Collection1".to_string(),
                        description: "Collection 1".to_string(),
                        properties: Default::default(),
                    },
                    &root_collection_id,
                )
                .await
                .unwrap();

            let layer2 = layer_db
                .add_layer(
                    AddLayer {
                        name: "Layer2".to_string(),
                        description: "Layer 2".to_string(),
                        symbology: None,
                        workflow: workflow.clone(),
                        metadata: Default::default(),
                        properties: Default::default(),
                    },
                    &collection1_id,
                )
                .await
                .unwrap();

            let collection2_id = layer_db
                .add_layer_collection(
                    AddLayerCollection {
                        name: "Collection2".to_string(),
                        description: "Collection 2".to_string(),
                        properties: Default::default(),
                    },
                    &collection1_id,
                )
                .await
                .unwrap();

            layer_db
                .add_collection_to_parent(&collection2_id, &collection1_id)
                .await
                .unwrap();

            let root_collection = layer_db
                .load_layer_collection(
                    &root_collection_id,
                    LayerCollectionListOptions {
                        offset: 0,
                        limit: 20,
                    },
                )
                .await
                .unwrap();

            assert_eq!(
                root_collection,
                LayerCollection {
                    id: ProviderLayerCollectionId {
                        provider_id: INTERNAL_PROVIDER_ID,
                        collection_id: root_collection_id,
                    },
                    name: "Layers".to_string(),
                    description: "All available Geo Engine layers".to_string(),
                    items: vec![
                        CollectionItem::Collection(LayerCollectionListing {
                            id: ProviderLayerCollectionId {
                                provider_id: INTERNAL_PROVIDER_ID,
                                collection_id: collection1_id.clone(),
                            },
                            name: "Collection1".to_string(),
                            description: "Collection 1".to_string(),
                            properties: Default::default(),
                        }),
                        CollectionItem::Collection(LayerCollectionListing {
                            id: ProviderLayerCollectionId {
                                provider_id: INTERNAL_PROVIDER_ID,
                                collection_id: LayerCollectionId(
                                    UNSORTED_COLLECTION_ID.to_string()
                                ),
                            },
                            name: "Unsorted".to_string(),
                            description: "Unsorted Layers".to_string(),
                            properties: Default::default(),
                        }),
                        CollectionItem::Layer(LayerListing {
                            id: ProviderLayerId {
                                provider_id: INTERNAL_PROVIDER_ID,
                                layer_id: layer1,
                            },
                            name: "Layer1".to_string(),
                            description: "Layer 1".to_string(),
                            properties: vec![("proper".to_string(), "tee".to_string()).into()],
                        })
                    ],
                    entry_label: None,
                    properties: vec![],
                }
            );

            let collection1 = layer_db
                .load_layer_collection(
                    &collection1_id,
                    LayerCollectionListOptions {
                        offset: 0,
                        limit: 20,
                    },
                )
                .await
                .unwrap();

            assert_eq!(
                collection1,
                LayerCollection {
                    id: ProviderLayerCollectionId {
                        provider_id: INTERNAL_PROVIDER_ID,
                        collection_id: collection1_id,
                    },
                    name: "Collection1".to_string(),
                    description: "Collection 1".to_string(),
                    items: vec![
                        CollectionItem::Collection(LayerCollectionListing {
                            id: ProviderLayerCollectionId {
                                provider_id: INTERNAL_PROVIDER_ID,
                                collection_id: collection2_id,
                            },
                            name: "Collection2".to_string(),
                            description: "Collection 2".to_string(),
                            properties: Default::default(),
                        }),
                        CollectionItem::Layer(LayerListing {
                            id: ProviderLayerId {
                                provider_id: INTERNAL_PROVIDER_ID,
                                layer_id: layer2,
                            },
                            name: "Layer2".to_string(),
                            description: "Layer 2".to_string(),
                            properties: vec![],
                        })
                    ],
                    entry_label: None,
                    properties: vec![],
                }
            );
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_removes_layer_collections() {
        with_temp_context(|app_ctx, _| async move {
            let session = app_ctx.default_session().await.unwrap();

            let layer_db = app_ctx.session_context(session).db();

            let layer = AddLayer {
                name: "layer".to_string(),
                description: "description".to_string(),
                workflow: Workflow {
                    operator: TypedOperator::Vector(
                        MockPointSource {
                            params: MockPointSourceParams {
                                points: vec![Coordinate2D::new(1., 2.); 3],
                            },
                        }
                        .boxed(),
                    ),
                },
                symbology: None,
                metadata: Default::default(),
                properties: Default::default(),
            };

            let root_collection = &layer_db.get_root_layer_collection_id().await.unwrap();

            let collection = AddLayerCollection {
                name: "top collection".to_string(),
                description: "description".to_string(),
                properties: Default::default(),
            };

            let top_c_id = layer_db
                .add_layer_collection(collection, root_collection)
                .await
                .unwrap();

            let l_id = layer_db.add_layer(layer, &top_c_id).await.unwrap();

            let collection = AddLayerCollection {
                name: "empty collection".to_string(),
                description: "description".to_string(),
                properties: Default::default(),
            };

            let empty_c_id = layer_db
                .add_layer_collection(collection, &top_c_id)
                .await
                .unwrap();

            let items = layer_db
                .load_layer_collection(
                    &top_c_id,
                    LayerCollectionListOptions {
                        offset: 0,
                        limit: 20,
                    },
                )
                .await
                .unwrap();

            assert_eq!(
                items,
                LayerCollection {
                    id: ProviderLayerCollectionId {
                        provider_id: INTERNAL_PROVIDER_ID,
                        collection_id: top_c_id.clone(),
                    },
                    name: "top collection".to_string(),
                    description: "description".to_string(),
                    items: vec![
                        CollectionItem::Collection(LayerCollectionListing {
                            id: ProviderLayerCollectionId {
                                provider_id: INTERNAL_PROVIDER_ID,
                                collection_id: empty_c_id.clone(),
                            },
                            name: "empty collection".to_string(),
                            description: "description".to_string(),
                            properties: Default::default(),
                        }),
                        CollectionItem::Layer(LayerListing {
                            id: ProviderLayerId {
                                provider_id: INTERNAL_PROVIDER_ID,
                                layer_id: l_id.clone(),
                            },
                            name: "layer".to_string(),
                            description: "description".to_string(),
                            properties: vec![],
                        })
                    ],
                    entry_label: None,
                    properties: vec![],
                }
            );

            // remove empty collection
            layer_db.remove_layer_collection(&empty_c_id).await.unwrap();

            let items = layer_db
                .load_layer_collection(
                    &top_c_id,
                    LayerCollectionListOptions {
                        offset: 0,
                        limit: 20,
                    },
                )
                .await
                .unwrap();

            assert_eq!(
                items,
                LayerCollection {
                    id: ProviderLayerCollectionId {
                        provider_id: INTERNAL_PROVIDER_ID,
                        collection_id: top_c_id.clone(),
                    },
                    name: "top collection".to_string(),
                    description: "description".to_string(),
                    items: vec![CollectionItem::Layer(LayerListing {
                        id: ProviderLayerId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            layer_id: l_id.clone(),
                        },
                        name: "layer".to_string(),
                        description: "description".to_string(),
                        properties: vec![],
                    })],
                    entry_label: None,
                    properties: vec![],
                }
            );

            // remove top (not root) collection
            layer_db.remove_layer_collection(&top_c_id).await.unwrap();

            layer_db
                .load_layer_collection(
                    &top_c_id,
                    LayerCollectionListOptions {
                        offset: 0,
                        limit: 20,
                    },
                )
                .await
                .unwrap_err();

            // should be deleted automatically
            layer_db.load_layer(&l_id).await.unwrap_err();

            // it is not allowed to remove the root collection
            layer_db
                .remove_layer_collection(root_collection)
                .await
                .unwrap_err();
            layer_db
                .load_layer_collection(
                    root_collection,
                    LayerCollectionListOptions {
                        offset: 0,
                        limit: 20,
                    },
                )
                .await
                .unwrap();
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn it_removes_collections_from_collections() {
        with_temp_context(|app_ctx, _| async move {
            let session = app_ctx.default_session().await.unwrap();

            let db = app_ctx.session_context(session).db();

            let root_collection_id = &db.get_root_layer_collection_id().await.unwrap();

            let mid_collection_id = db
                .add_layer_collection(
                    AddLayerCollection {
                        name: "mid collection".to_string(),
                        description: "description".to_string(),
                        properties: Default::default(),
                    },
                    root_collection_id,
                )
                .await
                .unwrap();

            let bottom_collection_id = db
                .add_layer_collection(
                    AddLayerCollection {
                        name: "bottom collection".to_string(),
                        description: "description".to_string(),
                        properties: Default::default(),
                    },
                    &mid_collection_id,
                )
                .await
                .unwrap();

            let layer_id = db
                .add_layer(
                    AddLayer {
                        name: "layer".to_string(),
                        description: "description".to_string(),
                        workflow: Workflow {
                            operator: TypedOperator::Vector(
                                MockPointSource {
                                    params: MockPointSourceParams {
                                        points: vec![Coordinate2D::new(1., 2.); 3],
                                    },
                                }
                                .boxed(),
                            ),
                        },
                        symbology: None,
                        metadata: Default::default(),
                        properties: Default::default(),
                    },
                    &mid_collection_id,
                )
                .await
                .unwrap();

            // removing the mid collection…
            db.remove_layer_collection_from_parent(&mid_collection_id, root_collection_id)
                .await
                .unwrap();

            // …should remove itself
            db.load_layer_collection(&mid_collection_id, LayerCollectionListOptions::default())
                .await
                .unwrap_err();

            // …should remove the bottom collection
            db.load_layer_collection(&bottom_collection_id, LayerCollectionListOptions::default())
                .await
                .unwrap_err();

            // … and should remove the layer of the bottom collection
            db.load_layer(&layer_id).await.unwrap_err();

            // the root collection is still there
            db.load_layer_collection(root_collection_id, LayerCollectionListOptions::default())
                .await
                .unwrap();
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn it_removes_layers_from_collections() {
        with_temp_context(|app_ctx, _| async move {
            let session = app_ctx.default_session().await.unwrap();

            let db = app_ctx.session_context(session).db();

            let root_collection = &db.get_root_layer_collection_id().await.unwrap();

            let another_collection = db
                .add_layer_collection(
                    AddLayerCollection {
                        name: "top collection".to_string(),
                        description: "description".to_string(),
                        properties: Default::default(),
                    },
                    root_collection,
                )
                .await
                .unwrap();

            let layer_in_one_collection = db
                .add_layer(
                    AddLayer {
                        name: "layer 1".to_string(),
                        description: "description".to_string(),
                        workflow: Workflow {
                            operator: TypedOperator::Vector(
                                MockPointSource {
                                    params: MockPointSourceParams {
                                        points: vec![Coordinate2D::new(1., 2.); 3],
                                    },
                                }
                                .boxed(),
                            ),
                        },
                        symbology: None,
                        metadata: Default::default(),
                        properties: Default::default(),
                    },
                    &another_collection,
                )
                .await
                .unwrap();

            let layer_in_two_collections = db
                .add_layer(
                    AddLayer {
                        name: "layer 2".to_string(),
                        description: "description".to_string(),
                        workflow: Workflow {
                            operator: TypedOperator::Vector(
                                MockPointSource {
                                    params: MockPointSourceParams {
                                        points: vec![Coordinate2D::new(1., 2.); 3],
                                    },
                                }
                                .boxed(),
                            ),
                        },
                        symbology: None,
                        metadata: Default::default(),
                        properties: Default::default(),
                    },
                    &another_collection,
                )
                .await
                .unwrap();

            db.add_layer_to_collection(&layer_in_two_collections, root_collection)
                .await
                .unwrap();

            // remove first layer --> should be deleted entirely

            db.remove_layer_from_collection(&layer_in_one_collection, &another_collection)
                .await
                .unwrap();

            let number_of_layer_in_collection = db
                .load_layer_collection(
                    &another_collection,
                    LayerCollectionListOptions {
                        offset: 0,
                        limit: 20,
                    },
                )
                .await
                .unwrap()
                .items
                .len();
            assert_eq!(
                number_of_layer_in_collection,
                1 /* only the other collection should be here */
            );

            db.load_layer(&layer_in_one_collection).await.unwrap_err();

            // remove second layer --> should only be gone in collection

            db.remove_layer_from_collection(&layer_in_two_collections, &another_collection)
                .await
                .unwrap();

            let number_of_layer_in_collection = db
                .load_layer_collection(
                    &another_collection,
                    LayerCollectionListOptions {
                        offset: 0,
                        limit: 20,
                    },
                )
                .await
                .unwrap()
                .items
                .len();
            assert_eq!(
                number_of_layer_in_collection,
                0 /* both layers were deleted */
            );

            db.load_layer(&layer_in_two_collections).await.unwrap();
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn it_deletes_dataset() {
        with_temp_context(|app_ctx, _| async move {
            let loading_info = OgrSourceDataset {
                file_name: PathBuf::from("test.csv"),
                layer_name: "test.csv".to_owned(),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::Start {
                    start_field: "start".to_owned(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                },
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: Some(FormatSpecifics::Csv {
                        header: CsvHeader::Auto,
                    }),
                    x: "x".to_owned(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: vec![],
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            };

            let meta_data = MetaDataDefinition::OgrMetaData(StaticMetaData::<
                OgrSourceDataset,
                VectorResultDescriptor,
                VectorQueryRectangle,
            > {
                loading_info: loading_info.clone(),
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [(
                        "foo".to_owned(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Float,
                            measurement: Measurement::Unitless.into(),
                        },
                    )]
                    .into_iter()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            });

            let session = app_ctx.default_session().await.unwrap();

            let dataset_name = DatasetName::new(None, "my_dataset");

            let db = app_ctx.session_context(session.clone()).db();
            let wrap = db.wrap_meta_data(meta_data);
            let dataset_id = db
                .add_dataset(
                    AddDataset {
                        name: Some(dataset_name),
                        display_name: "Ogr Test".to_owned(),
                        description: "desc".to_owned(),
                        source_operator: "OgrSource".to_owned(),
                        symbology: None,
                        provenance: Some(vec![Provenance {
                            citation: "citation".to_owned(),
                            license: "license".to_owned(),
                            uri: "uri".to_owned(),
                        }]),
                    },
                    wrap,
                )
                .await
                .unwrap()
                .id;

            assert!(db.load_dataset(&dataset_id).await.is_ok());

            db.delete_dataset(dataset_id).await.unwrap();

            assert!(db.load_dataset(&dataset_id).await.is_err());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn it_deletes_admin_dataset() {
        with_temp_context(|app_ctx, _| async move {
            let dataset_name = DatasetName::new(None, "my_dataset");

            let loading_info = OgrSourceDataset {
                file_name: PathBuf::from("test.csv"),
                layer_name: "test.csv".to_owned(),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::Start {
                    start_field: "start".to_owned(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                },
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: Some(FormatSpecifics::Csv {
                        header: CsvHeader::Auto,
                    }),
                    x: "x".to_owned(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: vec![],
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            };

            let meta_data = MetaDataDefinition::OgrMetaData(StaticMetaData::<
                OgrSourceDataset,
                VectorResultDescriptor,
                VectorQueryRectangle,
            > {
                loading_info: loading_info.clone(),
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [(
                        "foo".to_owned(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Float,
                            measurement: Measurement::Unitless.into(),
                        },
                    )]
                    .into_iter()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            });

            let session = app_ctx.default_session().await.unwrap();

            let db = app_ctx.session_context(session).db();
            let wrap = db.wrap_meta_data(meta_data);
            let dataset_id = db
                .add_dataset(
                    AddDataset {
                        name: Some(dataset_name),
                        display_name: "Ogr Test".to_owned(),
                        description: "desc".to_owned(),
                        source_operator: "OgrSource".to_owned(),
                        symbology: None,
                        provenance: Some(vec![Provenance {
                            citation: "citation".to_owned(),
                            license: "license".to_owned(),
                            uri: "uri".to_owned(),
                        }]),
                    },
                    wrap,
                )
                .await
                .unwrap()
                .id;

            assert!(db.load_dataset(&dataset_id).await.is_ok());

            db.delete_dataset(dataset_id).await.unwrap();

            assert!(db.load_dataset(&dataset_id).await.is_err());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_missing_layer_dataset_in_collection_listing() {
        with_temp_context(|app_ctx, _| async move {
            let session = app_ctx.default_session().await.unwrap();
            let db = app_ctx.session_context(session).db();

            let root_collection_id = &db.get_root_layer_collection_id().await.unwrap();

            let top_collection_id = db
                .add_layer_collection(
                    AddLayerCollection {
                        name: "top collection".to_string(),
                        description: "description".to_string(),
                        properties: Default::default(),
                    },
                    root_collection_id,
                )
                .await
                .unwrap();

            let faux_layer = LayerId("faux".to_string());

            // this should fail
            db.add_layer_to_collection(&faux_layer, &top_collection_id)
                .await
                .unwrap_err();

            let root_collection_layers = db
                .load_layer_collection(
                    &top_collection_id,
                    LayerCollectionListOptions {
                        offset: 0,
                        limit: 20,
                    },
                )
                .await
                .unwrap();

            assert_eq!(
                root_collection_layers,
                LayerCollection {
                    id: ProviderLayerCollectionId {
                        provider_id: DataProviderId(
                            "ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74".try_into().unwrap()
                        ),
                        collection_id: top_collection_id.clone(),
                    },
                    name: "top collection".to_string(),
                    description: "description".to_string(),
                    items: vec![],
                    entry_label: None,
                    properties: vec![],
                }
            );
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_updates_project_layer_symbology() {
        with_temp_context(|app_ctx, _| async move {
            let session = app_ctx.default_session().await.unwrap();

            let (_, workflow_id) = register_ndvi_workflow_helper(&app_ctx).await;

            let db = app_ctx.session_context(session.clone()).db();

            let create_project: CreateProject = serde_json::from_value(json!({
                "name": "Default",
                "description": "Default project",
                "bounds": {
                    "boundingBox": {
                        "lowerLeftCoordinate": {
                            "x": -180,
                            "y": -90
                        },
                        "upperRightCoordinate": {
                            "x": 180,
                            "y": 90
                        }
                    },
                    "spatialReference": "EPSG:4326",
                    "timeInterval": {
                        "start": 1_396_353_600_000i64,
                        "end": 1_396_353_600_000i64
                    }
                },
                "timeStep": {
                    "step": 1,
                    "granularity": "months"
                }
            }))
            .unwrap();

            let project_id = db.create_project(create_project).await.unwrap();

            let update: UpdateProject = serde_json::from_value(json!({
                "id": project_id.to_string(),
                "layers": [{
                    "name": "NDVI",
                    "workflow": workflow_id.to_string(),
                    "visibility": {
                        "data": true,
                        "legend": false
                    },
                    "symbology": {
                        "type": "raster",
                        "opacity": 1,
                        "colorizer": {
                            "type": "linearGradient",
                            "breakpoints": [{
                                "value": 1,
                                "color": [0, 0, 0, 255]
                            }, {
                                "value": 255,
                                "color": [255, 255, 255, 255]
                            }],
                            "noDataColor": [0, 0, 0, 0],
                            "overColor": [255, 255, 255, 127],
                            "underColor": [255, 255, 255, 127]
                        }
                    }
                }]
            }))
            .unwrap();

            db.update_project(update).await.unwrap();

            let update: UpdateProject = serde_json::from_value(json!({
                "id": project_id.to_string(),
                "layers": [{
                    "name": "NDVI",
                    "workflow": workflow_id.to_string(),
                    "visibility": {
                        "data": true,
                        "legend": false
                    },
                    "symbology": {
                        "type": "raster",
                        "opacity": 1,
                        "colorizer": {
                            "type": "linearGradient",
                            "breakpoints": [{
                                "value": 1,
                                "color": [0, 0, 4, 255]
                            }, {
                                "value": 17.866_666_666_666_667,
                                "color": [11, 9, 36, 255]
                            }, {
                                "value": 34.733_333_333_333_334,
                                "color": [32, 17, 75, 255]
                            }, {
                                "value": 51.6,
                                "color": [59, 15, 112, 255]
                            }, {
                                "value": 68.466_666_666_666_67,
                                "color": [87, 21, 126, 255]
                            }, {
                                "value": 85.333_333_333_333_33,
                                "color": [114, 31, 129, 255]
                            }, {
                                "value": 102.199_999_999_999_99,
                                "color": [140, 41, 129, 255]
                            }, {
                                "value": 119.066_666_666_666_65,
                                "color": [168, 50, 125, 255]
                            }, {
                                "value": 135.933_333_333_333_34,
                                "color": [196, 60, 117, 255]
                            }, {
                                "value": 152.799_999_999_999_98,
                                "color": [222, 73, 104, 255]
                            }, {
                                "value": 169.666_666_666_666_66,
                                "color": [241, 96, 93, 255]
                            }, {
                                "value": 186.533_333_333_333_33,
                                "color": [250, 127, 94, 255]
                            }, {
                                "value": 203.399_999_999_999_98,
                                "color": [254, 159, 109, 255]
                            }, {
                                "value": 220.266_666_666_666_65,
                                "color": [254, 191, 132, 255]
                            }, {
                                "value": 237.133_333_333_333_3,
                                "color": [253, 222, 160, 255]
                            }, {
                                "value": 254,
                                "color": [252, 253, 191, 255]
                            }],
                            "noDataColor": [0, 0, 0, 0],
                            "overColor": [255, 255, 255, 127],
                            "underColor": [255, 255, 255, 127]
                        }
                    }
                }]
            }))
            .unwrap();

            db.update_project(update).await.unwrap();

            let update: UpdateProject = serde_json::from_value(json!({
                "id": project_id.to_string(),
                "layers": [{
                    "name": "NDVI",
                    "workflow": workflow_id.to_string(),
                    "visibility": {
                        "data": true,
                        "legend": false
                    },
                    "symbology": {
                        "type": "raster",
                        "opacity": 1,
                        "colorizer": {
                            "type": "linearGradient",
                            "breakpoints": [{
                                "value": 1,
                                "color": [0, 0, 4, 255]
                            }, {
                                "value": 17.866_666_666_666_667,
                                "color": [11, 9, 36, 255]
                            }, {
                                "value": 34.733_333_333_333_334,
                                "color": [32, 17, 75, 255]
                            }, {
                                "value": 51.6,
                                "color": [59, 15, 112, 255]
                            }, {
                                "value": 68.466_666_666_666_67,
                                "color": [87, 21, 126, 255]
                            }, {
                                "value": 85.333_333_333_333_33,
                                "color": [114, 31, 129, 255]
                            }, {
                                "value": 102.199_999_999_999_99,
                                "color": [140, 41, 129, 255]
                            }, {
                                "value": 119.066_666_666_666_65,
                                "color": [168, 50, 125, 255]
                            }, {
                                "value": 135.933_333_333_333_34,
                                "color": [196, 60, 117, 255]
                            }, {
                                "value": 152.799_999_999_999_98,
                                "color": [222, 73, 104, 255]
                            }, {
                                "value": 169.666_666_666_666_66,
                                "color": [241, 96, 93, 255]
                            }, {
                                "value": 186.533_333_333_333_33,
                                "color": [250, 127, 94, 255]
                            }, {
                                "value": 203.399_999_999_999_98,
                                "color": [254, 159, 109, 255]
                            }, {
                                "value": 220.266_666_666_666_65,
                                "color": [254, 191, 132, 255]
                            }, {
                                "value": 237.133_333_333_333_3,
                                "color": [253, 222, 160, 255]
                            }, {
                                "value": 254,
                                "color": [252, 253, 191, 255]
                            }],
                            "noDataColor": [0, 0, 0, 0],
                            "overColor": [255, 255, 255, 127],
                            "underColor": [255, 255, 255, 127]
                        }
                    }
                }]
            }))
            .unwrap();

            db.update_project(update).await.unwrap();

            let update: UpdateProject = serde_json::from_value(json!({
                "id": project_id.to_string(),
                "layers": [{
                    "name": "NDVI",
                    "workflow": workflow_id.to_string(),
                    "visibility": {
                        "data": true,
                        "legend": false
                    },
                    "symbology": {
                        "type": "raster",
                        "opacity": 1,
                        "colorizer": {
                            "type": "linearGradient",
                            "breakpoints": [{
                                "value": 1,
                                "color": [0, 0, 4, 255]
                            }, {
                                "value": 17.933_333_333_333_334,
                                "color": [11, 9, 36, 255]
                            }, {
                                "value": 34.866_666_666_666_67,
                                "color": [32, 17, 75, 255]
                            }, {
                                "value": 51.800_000_000_000_004,
                                "color": [59, 15, 112, 255]
                            }, {
                                "value": 68.733_333_333_333_33,
                                "color": [87, 21, 126, 255]
                            }, {
                                "value": 85.666_666_666_666_66,
                                "color": [114, 31, 129, 255]
                            }, {
                                "value": 102.6,
                                "color": [140, 41, 129, 255]
                            }, {
                                "value": 119.533_333_333_333_32,
                                "color": [168, 50, 125, 255]
                            }, {
                                "value": 136.466_666_666_666_67,
                                "color": [196, 60, 117, 255]
                            }, {
                                "value": 153.4,
                                "color": [222, 73, 104, 255]
                            }, {
                                "value": 170.333_333_333_333_31,
                                "color": [241, 96, 93, 255]
                            }, {
                                "value": 187.266_666_666_666_65,
                                "color": [250, 127, 94, 255]
                            }, {
                                "value": 204.2,
                                "color": [254, 159, 109, 255]
                            }, {
                                "value": 221.133_333_333_333_33,
                                "color": [254, 191, 132, 255]
                            }, {
                                "value": 238.066_666_666_666_63,
                                "color": [253, 222, 160, 255]
                            }, {
                                "value": 255,
                                "color": [252, 253, 191, 255]
                            }],
                            "noDataColor": [0, 0, 0, 0],
                            "overColor": [255, 255, 255, 127],
                            "underColor": [255, 255, 255, 127]
                        }
                    }
                }]
            }))
            .unwrap();

            let update = update;

            // run two updates concurrently
            let (r0, r1) = join!(db.update_project(update.clone()), db.update_project(update));

            assert!(r0.is_ok());
            assert!(r1.is_ok());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn it_resolves_dataset_names_to_ids() {
        with_temp_context(|app_ctx, _| async move {
            let session = app_ctx.default_session().await.unwrap();
            let db = app_ctx.session_context(session.clone()).db();

            let loading_info = OgrSourceDataset {
                file_name: PathBuf::from("test.csv"),
                layer_name: "test.csv".to_owned(),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::Start {
                    start_field: "start".to_owned(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                },
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: Some(FormatSpecifics::Csv {
                        header: CsvHeader::Auto,
                    }),
                    x: "x".to_owned(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: vec![],
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            };

            let meta_data = MetaDataDefinition::OgrMetaData(StaticMetaData::<
                OgrSourceDataset,
                VectorResultDescriptor,
                VectorQueryRectangle,
            > {
                loading_info: loading_info.clone(),
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [(
                        "foo".to_owned(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Float,
                            measurement: Measurement::Unitless.into(),
                        },
                    )]
                    .into_iter()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            });

            let DatasetIdAndName {
                id: dataset_id1,
                name: dataset_name1,
            } = db
                .add_dataset(
                    AddDataset {
                        name: Some(DatasetName::new(None, "my_dataset".to_owned())),
                        display_name: "Ogr Test".to_owned(),
                        description: "desc".to_owned(),
                        source_operator: "OgrSource".to_owned(),
                        symbology: None,
                        provenance: Some(vec![Provenance {
                            citation: "citation".to_owned(),
                            license: "license".to_owned(),
                            uri: "uri".to_owned(),
                        }]),
                    },
                    db.wrap_meta_data(meta_data.clone()),
                )
                .await
                .unwrap();

            assert_eq!(
                db.resolve_dataset_name_to_id(&dataset_name1).await.unwrap(),
                dataset_id1
            );
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn test_postgres_type_serialization() {
        pub async fn test_type<T>(
            conn: &PooledConnection<'_, PostgresConnectionManager<tokio_postgres::NoTls>>,
            sql_type: &str,
            checks: impl IntoIterator<Item = T>,
        ) where
            T: PartialEq + postgres_types::FromSqlOwned + postgres_types::ToSql + Sync,
        {
            const UNQUOTED: [&str; 3] = ["double precision", "int", "point[]"];

            // don't quote built-in types
            let quote = if UNQUOTED.contains(&sql_type) || sql_type.contains('[') {
                ""
            } else {
                "\""
            };

            for value in checks {
                let stmt = conn
                    .prepare(&format!("SELECT $1::{quote}{sql_type}{quote}"))
                    .await
                    .unwrap();
                let result: T = conn.query_one(&stmt, &[&value]).await.unwrap().get(0);

                assert_eq!(value, result);
            }
        }

        with_temp_context(|app_ctx, _| async move {
            let pool = app_ctx.pool.get().await.unwrap();

            test_type(&pool, "RgbaColor", [RgbaColor([0, 1, 2, 3])]).await;

            test_type(
                &pool,
                "double precision",
                [NotNanF64::from(NotNan::<f64>::new(1.0).unwrap())],
            )
            .await;

            test_type(
                &pool,
                "Breakpoint",
                [Breakpoint {
                    value: NotNan::<f64>::new(1.0).unwrap().into(),
                    color: RgbaColor([0, 0, 0, 0]),
                }],
            )
            .await;

            test_type(
                &pool,
                "DefaultColors",
                [
                    DefaultColors::DefaultColor {
                        default_color: RgbaColor([0, 10, 20, 30]),
                    },
                    DefaultColors::OverUnder(OverUnderColors {
                        over_color: RgbaColor([1, 2, 3, 4]),
                        under_color: RgbaColor([5, 6, 7, 8]),
                    }),
                ],
            )
            .await;

            test_type(
                &pool,
                "ColorizerType",
                [
                    ColorizerTypeDbType::LinearGradient,
                    ColorizerTypeDbType::LogarithmicGradient,
                    ColorizerTypeDbType::Palette,
                    ColorizerTypeDbType::Rgba,
                ],
            )
            .await;

            test_type(
                &pool,
                "Colorizer",
                [
                    Colorizer::LinearGradient(LinearGradient {
                        breakpoints: vec![
                            Breakpoint {
                                value: NotNan::<f64>::new(-10.0).unwrap().into(),
                                color: RgbaColor([0, 0, 0, 0]),
                            },
                            Breakpoint {
                                value: NotNan::<f64>::new(2.0).unwrap().into(),
                                color: RgbaColor([255, 0, 0, 255]),
                            },
                        ],
                        no_data_color: RgbaColor([0, 10, 20, 30]),
                        color_fields: DefaultColors::OverUnder(OverUnderColors {
                            over_color: RgbaColor([1, 2, 3, 4]),
                            under_color: RgbaColor([5, 6, 7, 8]),
                        }),
                    }),
                    Colorizer::LogarithmicGradient(LogarithmicGradient {
                        breakpoints: vec![
                            Breakpoint {
                                value: NotNan::<f64>::new(1.0).unwrap().into(),
                                color: RgbaColor([0, 0, 0, 0]),
                            },
                            Breakpoint {
                                value: NotNan::<f64>::new(2.0).unwrap().into(),
                                color: RgbaColor([255, 0, 0, 255]),
                            },
                        ],
                        no_data_color: RgbaColor([0, 10, 20, 30]),
                        color_fields: DefaultColors::OverUnder(OverUnderColors {
                            over_color: RgbaColor([1, 2, 3, 4]),
                            under_color: RgbaColor([5, 6, 7, 8]),
                        }),
                    }),
                    Colorizer::Palette {
                        colors: Palette(
                            [
                                (NotNan::<f64>::new(1.0).unwrap(), RgbaColor([0, 0, 0, 0])),
                                (
                                    NotNan::<f64>::new(2.0).unwrap(),
                                    RgbaColor([255, 0, 0, 255]),
                                ),
                                (NotNan::<f64>::new(3.0).unwrap(), RgbaColor([0, 10, 20, 30])),
                            ]
                            .into(),
                        ),
                        no_data_color: RgbaColor([1, 2, 3, 4]),
                        default_color: RgbaColor([5, 6, 7, 8]),
                    },
                    Colorizer::Rgba,
                ],
            )
            .await;

            test_type(
                &pool,
                "ColorParam",
                [
                    ColorParam::Static {
                        color: RgbaColor([0, 10, 20, 30]).into(),
                    },
                    ColorParam::Derived(DerivedColor {
                        attribute: "foobar".to_string(),
                        colorizer: Colorizer::Rgba,
                    }),
                ],
            )
            .await;

            test_type(
                &pool,
                "NumberParam",
                [
                    NumberParam::Static { value: 42 },
                    NumberParam::Derived(DerivedNumber {
                        attribute: "foobar".to_string(),
                        factor: 1.0,
                        default_value: 42.,
                    }),
                ],
            )
            .await;

            test_type(
                &pool,
                "StrokeParam",
                [StrokeParam {
                    width: NumberParam::Static { value: 42 },
                    color: ColorParam::Static {
                        color: RgbaColor([0, 10, 20, 30]).into(),
                    },
                }],
            )
            .await;

            test_type(
                &pool,
                "TextSymbology",
                [TextSymbology {
                    attribute: "attribute".to_string(),
                    fill_color: ColorParam::Static {
                        color: RgbaColor([0, 10, 20, 30]).into(),
                    },
                    stroke: StrokeParam {
                        width: NumberParam::Static { value: 42 },
                        color: ColorParam::Static {
                            color: RgbaColor([0, 10, 20, 30]).into(),
                        },
                    },
                }],
            )
            .await;

            test_type(
                &pool,
                "Symbology",
                [
                    Symbology::Point(PointSymbology {
                        fill_color: ColorParam::Static {
                            color: RgbaColor([0, 10, 20, 30]).into(),
                        },
                        stroke: StrokeParam {
                            width: NumberParam::Static { value: 42 },
                            color: ColorParam::Static {
                                color: RgbaColor([0, 10, 20, 30]).into(),
                            },
                        },
                        radius: NumberParam::Static { value: 42 },
                        text: Some(TextSymbology {
                            attribute: "attribute".to_string(),
                            fill_color: ColorParam::Static {
                                color: RgbaColor([0, 10, 20, 30]).into(),
                            },
                            stroke: StrokeParam {
                                width: NumberParam::Static { value: 42 },
                                color: ColorParam::Static {
                                    color: RgbaColor([0, 10, 20, 30]).into(),
                                },
                            },
                        }),
                    }),
                    Symbology::Line(LineSymbology {
                        stroke: StrokeParam {
                            width: NumberParam::Static { value: 42 },
                            color: ColorParam::Static {
                                color: RgbaColor([0, 10, 20, 30]).into(),
                            },
                        },
                        text: Some(TextSymbology {
                            attribute: "attribute".to_string(),
                            fill_color: ColorParam::Static {
                                color: RgbaColor([0, 10, 20, 30]).into(),
                            },
                            stroke: StrokeParam {
                                width: NumberParam::Static { value: 42 },
                                color: ColorParam::Static {
                                    color: RgbaColor([0, 10, 20, 30]).into(),
                                },
                            },
                        }),
                        auto_simplified: true,
                    }),
                    Symbology::Polygon(PolygonSymbology {
                        fill_color: ColorParam::Static {
                            color: RgbaColor([0, 10, 20, 30]).into(),
                        },
                        stroke: StrokeParam {
                            width: NumberParam::Static { value: 42 },
                            color: ColorParam::Static {
                                color: RgbaColor([0, 10, 20, 30]).into(),
                            },
                        },
                        text: Some(TextSymbology {
                            attribute: "attribute".to_string(),
                            fill_color: ColorParam::Static {
                                color: RgbaColor([0, 10, 20, 30]).into(),
                            },
                            stroke: StrokeParam {
                                width: NumberParam::Static { value: 42 },
                                color: ColorParam::Static {
                                    color: RgbaColor([0, 10, 20, 30]).into(),
                                },
                            },
                        }),
                        auto_simplified: true,
                    }),
                    Symbology::Raster(RasterSymbology {
                        opacity: 1.0,
                        colorizer: Colorizer::LinearGradient(LinearGradient {
                            breakpoints: vec![
                                Breakpoint {
                                    value: NotNan::<f64>::new(-10.0).unwrap().into(),
                                    color: RgbaColor([0, 0, 0, 0]),
                                },
                                Breakpoint {
                                    value: NotNan::<f64>::new(2.0).unwrap().into(),
                                    color: RgbaColor([255, 0, 0, 255]),
                                },
                            ],
                            no_data_color: RgbaColor([0, 10, 20, 30]),
                            color_fields: DefaultColors::OverUnder(OverUnderColors {
                                over_color: RgbaColor([1, 2, 3, 4]),
                                under_color: RgbaColor([5, 6, 7, 8]),
                            }),
                        }),
                    }),
                ],
            )
            .await;

            test_type(
                &pool,
                "RasterDataType",
                [
                    crate::api::model::datatypes::RasterDataType::U8,
                    crate::api::model::datatypes::RasterDataType::U16,
                    crate::api::model::datatypes::RasterDataType::U32,
                    crate::api::model::datatypes::RasterDataType::U64,
                    crate::api::model::datatypes::RasterDataType::I8,
                    crate::api::model::datatypes::RasterDataType::I16,
                    crate::api::model::datatypes::RasterDataType::I32,
                    crate::api::model::datatypes::RasterDataType::I64,
                    crate::api::model::datatypes::RasterDataType::F32,
                    crate::api::model::datatypes::RasterDataType::F64,
                ],
            )
            .await;

            test_type(
                &pool,
                "Measurement",
                [
                    Measurement::Unitless,
                    Measurement::Continuous(ContinuousMeasurement {
                        measurement: "Temperature".to_string(),
                        unit: Some("°C".to_string()),
                    }),
                    Measurement::Classification(ClassificationMeasurement {
                        measurement: "Color".to_string(),
                        classes: [(1, "Grayscale".to_string()), (2, "Colorful".to_string())].into(),
                    }),
                ],
            )
            .await;

            test_type(
                &pool,
                "Coordinate2D",
                [crate::api::model::datatypes::Coordinate2D::from(
                    Coordinate2D::new(0.0f64, 1.),
                )],
            )
            .await;

            test_type(
                &pool,
                "SpatialPartition2D",
                [crate::api::model::datatypes::SpatialPartition2D {
                    upper_left_coordinate: Coordinate2D::new(0.0f64, 1.).into(),
                    lower_right_coordinate: Coordinate2D::new(2., 0.5).into(),
                }],
            )
            .await;

            test_type(
                &pool,
                "BoundingBox2D",
                [crate::api::model::datatypes::BoundingBox2D {
                    lower_left_coordinate: Coordinate2D::new(0.0f64, 0.5).into(),
                    upper_right_coordinate: Coordinate2D::new(2., 1.0).into(),
                }],
            )
            .await;

            test_type(
                &pool,
                "SpatialResolution",
                [crate::api::model::datatypes::SpatialResolution { x: 1.2, y: 2.3 }],
            )
            .await;

            test_type(
                &pool,
                "VectorDataType",
                [
                    crate::api::model::datatypes::VectorDataType::Data,
                    crate::api::model::datatypes::VectorDataType::MultiPoint,
                    crate::api::model::datatypes::VectorDataType::MultiLineString,
                    crate::api::model::datatypes::VectorDataType::MultiPolygon,
                ],
            )
            .await;

            test_type(
                &pool,
                "FeatureDataType",
                [
                    crate::api::model::datatypes::FeatureDataType::Category,
                    crate::api::model::datatypes::FeatureDataType::Int,
                    crate::api::model::datatypes::FeatureDataType::Float,
                    crate::api::model::datatypes::FeatureDataType::Text,
                    crate::api::model::datatypes::FeatureDataType::Bool,
                    crate::api::model::datatypes::FeatureDataType::DateTime,
                ],
            )
            .await;

            test_type(
                &pool,
                "TimeInterval",
                [crate::api::model::datatypes::TimeInterval::from(
                    TimeInterval::default(),
                )],
            )
            .await;

            test_type(
                &pool,
                "SpatialReference",
                [
                    crate::api::model::datatypes::SpatialReferenceOption::Unreferenced,
                    crate::api::model::datatypes::SpatialReferenceOption::SpatialReference(
                        SpatialReference::epsg_4326().into(),
                    ),
                ],
            )
            .await;

            test_type(
                &pool,
                "PlotResultDescriptor",
                [PlotResultDescriptor {
                    spatial_reference: SpatialReferenceOption::Unreferenced.into(),
                    time: None,
                    bbox: None,
                }],
            )
            .await;

            test_type(
                &pool,
                "VectorResultDescriptor",
                [crate::api::model::operators::VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint.into(),
                    spatial_reference: SpatialReferenceOption::SpatialReference(
                        SpatialReference::epsg_4326(),
                    )
                    .into(),
                    columns: [(
                        "foo".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Unitless.into(),
                        }
                        .into(),
                    )]
                    .into(),
                    time: Some(TimeInterval::default().into()),
                    bbox: Some(
                        BoundingBox2D::new(
                            Coordinate2D::new(0.0f64, 0.5),
                            Coordinate2D::new(2., 1.0),
                        )
                        .unwrap()
                        .into(),
                    ),
                }],
            )
            .await;

            test_type(
                &pool,
                "RasterResultDescriptor",
                [crate::api::model::operators::RasterResultDescriptor {
                    data_type: RasterDataType::U8.into(),
                    spatial_reference: SpatialReferenceOption::SpatialReference(
                        SpatialReference::epsg_4326(),
                    )
                    .into(),
                    measurement: Measurement::Unitless,
                    time: Some(TimeInterval::default().into()),
                    bbox: Some(SpatialPartition2D {
                        upper_left_coordinate: Coordinate2D::new(0.0f64, 1.).into(),
                        lower_right_coordinate: Coordinate2D::new(2., 0.5).into(),
                    }),
                    resolution: Some(SpatialResolution { x: 1.2, y: 2.3 }.into()),
                }],
            )
            .await;

            test_type(
                &pool,
                "ResultDescriptor",
                [
                    crate::api::model::operators::TypedResultDescriptor::Vector(
                        VectorResultDescriptor {
                            data_type: VectorDataType::MultiPoint,
                            spatial_reference: SpatialReferenceOption::SpatialReference(
                                SpatialReference::epsg_4326(),
                            ),
                            columns: [(
                                "foo".to_string(),
                                VectorColumnInfo {
                                    data_type: FeatureDataType::Int,
                                    measurement: Measurement::Unitless.into(),
                                },
                            )]
                            .into(),
                            time: Some(TimeInterval::default()),
                            bbox: Some(
                                BoundingBox2D::new(
                                    Coordinate2D::new(0.0f64, 0.5),
                                    Coordinate2D::new(2., 1.0),
                                )
                                .unwrap(),
                            ),
                        }
                        .into(),
                    ),
                    crate::api::model::operators::TypedResultDescriptor::Raster(
                        crate::api::model::operators::RasterResultDescriptor {
                            data_type: RasterDataType::U8.into(),
                            spatial_reference: SpatialReferenceOption::SpatialReference(
                                SpatialReference::epsg_4326(),
                            )
                            .into(),
                            measurement: Measurement::Unitless,
                            time: Some(TimeInterval::default().into()),
                            bbox: Some(SpatialPartition2D {
                                upper_left_coordinate: Coordinate2D::new(0.0f64, 1.).into(),
                                lower_right_coordinate: Coordinate2D::new(2., 0.5).into(),
                            }),
                            resolution: Some(SpatialResolution { x: 1.2, y: 2.3 }.into()),
                        },
                    ),
                    crate::api::model::operators::TypedResultDescriptor::Plot(
                        PlotResultDescriptor {
                            spatial_reference: SpatialReferenceOption::Unreferenced.into(),
                            time: None,
                            bbox: None,
                        },
                    ),
                ],
            )
            .await;

            test_type(
                &pool,
                "\"TextTextKeyValue\"[]",
                [HashMapTextTextDbType::from(
                    &HashMap::<String, String>::from([
                        ("foo".to_string(), "bar".to_string()),
                        ("baz".to_string(), "fuu".to_string()),
                    ]),
                )],
            )
            .await;

            test_type(
                &pool,
                "MockDatasetDataSourceLoadingInfo",
                [
                    crate::api::model::operators::MockDatasetDataSourceLoadingInfo {
                        points: vec![
                            Coordinate2D::new(0.0f64, 0.5).into(),
                            Coordinate2D::new(2., 1.0).into(),
                        ],
                    },
                ],
            )
            .await;

            test_type(
                &pool,
                "OgrSourceTimeFormat",
                [
                    crate::api::model::operators::OgrSourceTimeFormat::Auto,
                    crate::api::model::operators::OgrSourceTimeFormat::Custom {
                        custom_format:
                            geoengine_datatypes::primitives::DateTimeParseFormat::custom(
                                "%Y-%m-%dT%H:%M:%S%.3fZ".to_string(),
                            )
                            .into(),
                    },
                    crate::api::model::operators::OgrSourceTimeFormat::UnixTimeStamp {
                        timestamp_type: UnixTimeStampType::EpochSeconds,
                        fmt: geoengine_datatypes::primitives::DateTimeParseFormat::unix().into(),
                    },
                ],
            )
            .await;

            test_type(
                &pool,
                "OgrSourceDurationSpec",
                [
                    crate::api::model::operators::OgrSourceDurationSpec::Infinite,
                    crate::api::model::operators::OgrSourceDurationSpec::Zero,
                    crate::api::model::operators::OgrSourceDurationSpec::Value(
                        TimeStep {
                            granularity: TimeGranularity::Millis,
                            step: 1000,
                        }
                        .into(),
                    ),
                ],
            )
            .await;

            test_type(
                &pool,
                "OgrSourceDatasetTimeType",
                [
                    crate::api::model::operators::OgrSourceDatasetTimeType::None,
                    crate::api::model::operators::OgrSourceDatasetTimeType::Start {
                        start_field: "start".to_string(),
                        start_format: crate::api::model::operators::OgrSourceTimeFormat::Auto,
                        duration: crate::api::model::operators::OgrSourceDurationSpec::Zero,
                    },
                    crate::api::model::operators::OgrSourceDatasetTimeType::StartEnd {
                        start_field: "start".to_string(),
                        start_format: crate::api::model::operators::OgrSourceTimeFormat::Auto,
                        end_field: "end".to_string(),
                        end_format: crate::api::model::operators::OgrSourceTimeFormat::Auto,
                    },
                    crate::api::model::operators::OgrSourceDatasetTimeType::StartDuration {
                        start_field: "start".to_string(),
                        start_format: crate::api::model::operators::OgrSourceTimeFormat::Auto,
                        duration_field: "duration".to_string(),
                    },
                ],
            )
            .await;

            test_type(
                &pool,
                "FormatSpecifics",
                [crate::api::model::operators::FormatSpecifics::Csv {
                    header: CsvHeader::Yes.into(),
                }],
            )
            .await;

            test_type(
                &pool,
                "OgrSourceColumnSpec",
                [crate::api::model::operators::OgrSourceColumnSpec {
                    format_specifics: Some(crate::api::model::operators::FormatSpecifics::Csv {
                        header: CsvHeader::Auto.into(),
                    }),
                    x: "x".to_string(),
                    y: Some("y".to_string()),
                    int: vec!["int".to_string()],
                    float: vec!["float".to_string()],
                    text: vec!["text".to_string()],
                    bool: vec!["bool".to_string()],
                    datetime: vec!["datetime".to_string()],
                    rename: Some(
                        [
                            ("xx".to_string(), "xx_renamed".to_string()),
                            ("yx".to_string(), "yy_renamed".to_string()),
                        ]
                        .into(),
                    ),
                }],
            )
            .await;

            test_type(
                &pool,
                "point[]",
                [MultiPoint {
                    coordinates: vec![
                        Coordinate2D::new(0.0f64, 0.5).into(),
                        Coordinate2D::new(2., 1.0).into(),
                    ],
                }],
            )
            .await;

            test_type(
                &pool,
                "path[]",
                [MultiLineString {
                    coordinates: vec![
                        vec![
                            Coordinate2D::new(0.0f64, 0.5).into(),
                            Coordinate2D::new(2., 1.0).into(),
                        ],
                        vec![
                            Coordinate2D::new(0.0f64, 0.5).into(),
                            Coordinate2D::new(2., 1.0).into(),
                        ],
                    ],
                }],
            )
            .await;

            test_type(
                &pool,
                "\"Polygon\"[]",
                [MultiPolygon {
                    polygons: vec![
                        vec![
                            vec![
                                Coordinate2D::new(0.0f64, 0.5).into(),
                                Coordinate2D::new(2., 1.0).into(),
                                Coordinate2D::new(2., 1.0).into(),
                                Coordinate2D::new(0.0f64, 0.5).into(),
                            ],
                            vec![
                                Coordinate2D::new(0.0f64, 0.5).into(),
                                Coordinate2D::new(2., 1.0).into(),
                                Coordinate2D::new(2., 1.0).into(),
                                Coordinate2D::new(0.0f64, 0.5).into(),
                            ],
                        ],
                        vec![
                            vec![
                                Coordinate2D::new(0.0f64, 0.5).into(),
                                Coordinate2D::new(2., 1.0).into(),
                                Coordinate2D::new(2., 1.0).into(),
                                Coordinate2D::new(0.0f64, 0.5).into(),
                            ],
                            vec![
                                Coordinate2D::new(0.0f64, 0.5).into(),
                                Coordinate2D::new(2., 1.0).into(),
                                Coordinate2D::new(2., 1.0).into(),
                                Coordinate2D::new(0.0f64, 0.5).into(),
                            ],
                        ],
                    ],
                }],
            )
            .await;

            test_type(
                &pool,
                "TypedGeometry",
                [
                    crate::api::model::operators::TypedGeometry::Data(NoGeometry),
                    crate::api::model::operators::TypedGeometry::MultiPoint(MultiPoint {
                        coordinates: vec![
                            Coordinate2D::new(0.0f64, 0.5).into(),
                            Coordinate2D::new(2., 1.0).into(),
                        ],
                    }),
                    crate::api::model::operators::TypedGeometry::MultiLineString(MultiLineString {
                        coordinates: vec![
                            vec![
                                Coordinate2D::new(0.0f64, 0.5).into(),
                                Coordinate2D::new(2., 1.0).into(),
                            ],
                            vec![
                                Coordinate2D::new(0.0f64, 0.5).into(),
                                Coordinate2D::new(2., 1.0).into(),
                            ],
                        ],
                    }),
                    crate::api::model::operators::TypedGeometry::MultiPolygon(MultiPolygon {
                        polygons: vec![
                            vec![
                                vec![
                                    Coordinate2D::new(0.0f64, 0.5).into(),
                                    Coordinate2D::new(2., 1.0).into(),
                                    Coordinate2D::new(2., 1.0).into(),
                                    Coordinate2D::new(0.0f64, 0.5).into(),
                                ],
                                vec![
                                    Coordinate2D::new(0.0f64, 0.5).into(),
                                    Coordinate2D::new(2., 1.0).into(),
                                    Coordinate2D::new(2., 1.0).into(),
                                    Coordinate2D::new(0.0f64, 0.5).into(),
                                ],
                            ],
                            vec![
                                vec![
                                    Coordinate2D::new(0.0f64, 0.5).into(),
                                    Coordinate2D::new(2., 1.0).into(),
                                    Coordinate2D::new(2., 1.0).into(),
                                    Coordinate2D::new(0.0f64, 0.5).into(),
                                ],
                                vec![
                                    Coordinate2D::new(0.0f64, 0.5).into(),
                                    Coordinate2D::new(2., 1.0).into(),
                                    Coordinate2D::new(2., 1.0).into(),
                                    Coordinate2D::new(0.0f64, 0.5).into(),
                                ],
                            ],
                        ],
                    }),
                ],
            )
            .await;

            test_type(&pool, "int", [CacheTtlSeconds::new(100)]).await;

            test_type(
                &pool,
                "OgrSourceDataset",
                [crate::api::model::operators::OgrSourceDataset {
                    file_name: "test".into(),
                    layer_name: "test".to_string(),
                    data_type: Some(VectorDataType::MultiPoint.into()),
                    time: crate::api::model::operators::OgrSourceDatasetTimeType::Start {
                        start_field: "start".to_string(),
                        start_format: crate::api::model::operators::OgrSourceTimeFormat::Auto,
                        duration: crate::api::model::operators::OgrSourceDurationSpec::Zero,
                    },
                    default_geometry: Some(
                        crate::api::model::operators::TypedGeometry::MultiPoint(MultiPoint {
                            coordinates: vec![
                                Coordinate2D::new(0.0f64, 0.5).into(),
                                Coordinate2D::new(2., 1.0).into(),
                            ],
                        }),
                    ),
                    columns: Some(crate::api::model::operators::OgrSourceColumnSpec {
                        format_specifics: Some(
                            crate::api::model::operators::FormatSpecifics::Csv {
                                header: CsvHeader::Auto.into(),
                            },
                        ),
                        x: "x".to_string(),
                        y: Some("y".to_string()),
                        int: vec!["int".to_string()],
                        float: vec!["float".to_string()],
                        text: vec!["text".to_string()],
                        bool: vec!["bool".to_string()],
                        datetime: vec!["datetime".to_string()],
                        rename: Some(
                            [
                                ("xx".to_string(), "xx_renamed".to_string()),
                                ("yx".to_string(), "yy_renamed".to_string()),
                            ]
                            .into(),
                        ),
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: true,
                    on_error: crate::api::model::operators::OgrSourceErrorSpec::Abort,
                    sql_query: None,
                    attribute_query: Some("foo = 'bar'".to_string()),
                    cache_ttl: CacheTtlSeconds::new(5),
                }],
            )
            .await;

            test_type(
                &pool,
                "MockMetaData",
                [crate::api::model::operators::MockMetaData {
                    loading_info: crate::api::model::operators::MockDatasetDataSourceLoadingInfo {
                        points: vec![
                            Coordinate2D::new(0.0f64, 0.5).into(),
                            Coordinate2D::new(2., 1.0).into(),
                        ],
                    },
                    result_descriptor: VectorResultDescriptor {
                        data_type: VectorDataType::MultiPoint,
                        spatial_reference: SpatialReferenceOption::SpatialReference(
                            SpatialReference::epsg_4326(),
                        ),
                        columns: [(
                            "foo".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Int,
                                measurement: Measurement::Unitless.into(),
                            },
                        )]
                        .into(),
                        time: Some(TimeInterval::default()),
                        bbox: Some(
                            BoundingBox2D::new(
                                Coordinate2D::new(0.0f64, 0.5),
                                Coordinate2D::new(2., 1.0),
                            )
                            .unwrap(),
                        ),
                    }
                    .into(),
                    phantom: PhantomData,
                }],
            )
            .await;

            test_type(
                &pool,
                "OgrMetaData",
                [crate::api::model::operators::OgrMetaData {
                    loading_info: crate::api::model::operators::OgrSourceDataset {
                        file_name: "test".into(),
                        layer_name: "test".to_string(),
                        data_type: Some(VectorDataType::MultiPoint.into()),
                        time: crate::api::model::operators::OgrSourceDatasetTimeType::Start {
                            start_field: "start".to_string(),
                            start_format: crate::api::model::operators::OgrSourceTimeFormat::Auto,
                            duration: crate::api::model::operators::OgrSourceDurationSpec::Zero,
                        },
                        default_geometry: Some(
                            crate::api::model::operators::TypedGeometry::MultiPoint(MultiPoint {
                                coordinates: vec![
                                    Coordinate2D::new(0.0f64, 0.5).into(),
                                    Coordinate2D::new(2., 1.0).into(),
                                ],
                            }),
                        ),
                        columns: Some(crate::api::model::operators::OgrSourceColumnSpec {
                            format_specifics: Some(
                                crate::api::model::operators::FormatSpecifics::Csv {
                                    header: CsvHeader::Auto.into(),
                                },
                            ),
                            x: "x".to_string(),
                            y: Some("y".to_string()),
                            int: vec!["int".to_string()],
                            float: vec!["float".to_string()],
                            text: vec!["text".to_string()],
                            bool: vec!["bool".to_string()],
                            datetime: vec!["datetime".to_string()],
                            rename: Some(
                                [
                                    ("xx".to_string(), "xx_renamed".to_string()),
                                    ("yx".to_string(), "yy_renamed".to_string()),
                                ]
                                .into(),
                            ),
                        }),
                        force_ogr_time_filter: false,
                        force_ogr_spatial_filter: true,
                        on_error: crate::api::model::operators::OgrSourceErrorSpec::Abort,
                        sql_query: None,
                        attribute_query: Some("foo = 'bar'".to_string()),
                        cache_ttl: CacheTtlSeconds::new(5),
                    },
                    result_descriptor: VectorResultDescriptor {
                        data_type: VectorDataType::MultiPoint,
                        spatial_reference: SpatialReferenceOption::SpatialReference(
                            SpatialReference::epsg_4326(),
                        ),
                        columns: [(
                            "foo".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Int,
                                measurement: Measurement::Unitless.into(),
                            },
                        )]
                        .into(),
                        time: Some(TimeInterval::default()),
                        bbox: Some(
                            BoundingBox2D::new(
                                Coordinate2D::new(0.0f64, 0.5),
                                Coordinate2D::new(2., 1.0),
                            )
                            .unwrap(),
                        ),
                    }
                    .into(),
                    phantom: PhantomData,
                }],
            )
            .await;

            test_type(
                &pool,
                "GdalDatasetGeoTransform",
                [crate::api::model::operators::GdalDatasetGeoTransform {
                    origin_coordinate: Coordinate2D::new(0.0f64, 0.5).into(),
                    x_pixel_size: 1.0,
                    y_pixel_size: 2.0,
                }],
            )
            .await;

            test_type(
                &pool,
                "FileNotFoundHandling",
                [
                    crate::api::model::operators::FileNotFoundHandling::NoData,
                    crate::api::model::operators::FileNotFoundHandling::Error,
                ],
            )
            .await;

            test_type(
                &pool,
                "GdalMetadataMapping",
                [crate::api::model::operators::GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: None,
                        key: "foo".to_string(),
                    },
                    target_key: RasterPropertiesKey {
                        domain: Some("bar".to_string()),
                        key: "foo".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                }],
            )
            .await;

            test_type(
                &pool,
                "StringPair",
                [StringPair::from(("foo".to_string(), "bar".to_string()))],
            )
            .await;

            test_type(
                &pool,
                "GdalDatasetParameters",
                [crate::api::model::operators::GdalDatasetParameters {
                    file_path: "text".into(),
                    rasterband_channel: 1,
                    geo_transform: crate::api::model::operators::GdalDatasetGeoTransform {
                        origin_coordinate: Coordinate2D::new(0.0f64, 0.5).into(),
                        x_pixel_size: 1.0,
                        y_pixel_size: 2.0,
                    },
                    width: 42,
                    height: 23,
                    file_not_found_handling:
                        crate::api::model::operators::FileNotFoundHandling::NoData,
                    no_data_value: Some(42.0),
                    properties_mapping: Some(vec![
                        crate::api::model::operators::GdalMetadataMapping {
                            source_key: RasterPropertiesKey {
                                domain: None,
                                key: "foo".to_string(),
                            },
                            target_key: RasterPropertiesKey {
                                domain: Some("bar".to_string()),
                                key: "foo".to_string(),
                            },
                            target_type: RasterPropertiesEntryType::String,
                        },
                    ]),
                    gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                    gdal_config_options: Some(vec![
                        crate::api::model::operators::GdalConfigOption::from((
                            "foo".to_string(),
                            "bar".to_string(),
                        )),
                    ]),
                    allow_alphaband_as_mask: false,
                }],
            )
            .await;

            test_type(
                &pool,
                "TextGdalSourceTimePlaceholderKeyValue",
                [crate::api::model::TextGdalSourceTimePlaceholderKeyValue {
                    key: "foo".to_string(),
                    value: GdalSourceTimePlaceholder {
                        format: geoengine_datatypes::primitives::DateTimeParseFormat::unix().into(),
                        reference: TimeReference::Start,
                    },
                }],
            )
            .await;

            test_type(
                &pool,
                "GdalMetaDataRegular",
                [crate::api::model::operators::GdalMetaDataRegular {
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Continuous(ContinuousMeasurement {
                            measurement: "Temperature".to_string(),
                            unit: Some("°C".to_string()),
                        })
                        .into(),
                        time: TimeInterval::new_unchecked(0, 1).into(),
                        bbox: Some(
                            crate::api::model::datatypes::SpatialPartition2D {
                                upper_left_coordinate: Coordinate2D::new(0.0f64, 1.).into(),
                                lower_right_coordinate: Coordinate2D::new(2., 0.5).into(),
                            }
                            .into(),
                        ),
                        resolution: Some(SpatialResolution::zero_point_one()),
                    }
                    .into(),
                    params: crate::api::model::operators::GdalDatasetParameters {
                        file_path: "text".into(),
                        rasterband_channel: 1,
                        geo_transform: crate::api::model::operators::GdalDatasetGeoTransform {
                            origin_coordinate: Coordinate2D::new(0.0f64, 0.5).into(),
                            x_pixel_size: 1.0,
                            y_pixel_size: 2.0,
                        },
                        width: 42,
                        height: 23,
                        file_not_found_handling:
                            crate::api::model::operators::FileNotFoundHandling::NoData,
                        no_data_value: Some(42.0),
                        properties_mapping: Some(vec![
                            crate::api::model::operators::GdalMetadataMapping {
                                source_key: RasterPropertiesKey {
                                    domain: None,
                                    key: "foo".to_string(),
                                },
                                target_key: RasterPropertiesKey {
                                    domain: Some("bar".to_string()),
                                    key: "foo".to_string(),
                                },
                                target_type: RasterPropertiesEntryType::String,
                            },
                        ]),
                        gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                        gdal_config_options: Some(vec![
                            crate::api::model::operators::GdalConfigOption::from((
                                "foo".to_string(),
                                "bar".to_string(),
                            )),
                        ]),
                        allow_alphaband_as_mask: false,
                    },
                    time_placeholders: [(
                        "foo".to_string(),
                        GdalSourceTimePlaceholder {
                            format: geoengine_datatypes::primitives::DateTimeParseFormat::unix()
                                .into(),
                            reference: TimeReference::Start,
                        },
                    )]
                    .into(),
                    data_time: TimeInterval::new_unchecked(0, 1).into(),
                    step: TimeStep {
                        granularity: TimeGranularity::Millis,
                        step: 1,
                    }
                    .into(),
                    cache_ttl: CacheTtlSeconds::max(),
                }],
            )
            .await;

            test_type(
                &pool,
                "GdalMetaDataStatic",
                [crate::api::model::operators::GdalMetaDataStatic {
                    time: Some(TimeInterval::new_unchecked(0, 1).into()),
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Continuous(ContinuousMeasurement {
                            measurement: "Temperature".to_string(),
                            unit: Some("°C".to_string()),
                        })
                        .into(),
                        time: TimeInterval::new_unchecked(0, 1).into(),
                        bbox: Some(
                            crate::api::model::datatypes::SpatialPartition2D {
                                upper_left_coordinate: Coordinate2D::new(0.0f64, 1.).into(),
                                lower_right_coordinate: Coordinate2D::new(2., 0.5).into(),
                            }
                            .into(),
                        ),
                        resolution: Some(SpatialResolution::zero_point_one()),
                    }
                    .into(),
                    params: crate::api::model::operators::GdalDatasetParameters {
                        file_path: "text".into(),
                        rasterband_channel: 1,
                        geo_transform: crate::api::model::operators::GdalDatasetGeoTransform {
                            origin_coordinate: Coordinate2D::new(0.0f64, 0.5).into(),
                            x_pixel_size: 1.0,
                            y_pixel_size: 2.0,
                        },
                        width: 42,
                        height: 23,
                        file_not_found_handling:
                            crate::api::model::operators::FileNotFoundHandling::NoData,
                        no_data_value: Some(42.0),
                        properties_mapping: Some(vec![
                            crate::api::model::operators::GdalMetadataMapping {
                                source_key: RasterPropertiesKey {
                                    domain: None,
                                    key: "foo".to_string(),
                                },
                                target_key: RasterPropertiesKey {
                                    domain: Some("bar".to_string()),
                                    key: "foo".to_string(),
                                },
                                target_type: RasterPropertiesEntryType::String,
                            },
                        ]),
                        gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                        gdal_config_options: Some(vec![
                            crate::api::model::operators::GdalConfigOption::from((
                                "foo".to_string(),
                                "bar".to_string(),
                            )),
                        ]),
                        allow_alphaband_as_mask: false,
                    },
                    cache_ttl: CacheTtlSeconds::max(),
                }],
            )
            .await;

            test_type(
                &pool,
                "GdalMetadataNetCdfCf",
                [crate::api::model::operators::GdalMetadataNetCdfCf {
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Continuous(ContinuousMeasurement {
                            measurement: "Temperature".to_string(),
                            unit: Some("°C".to_string()),
                        })
                        .into(),
                        time: TimeInterval::new_unchecked(0, 1).into(),
                        bbox: Some(
                            crate::api::model::datatypes::SpatialPartition2D {
                                upper_left_coordinate: Coordinate2D::new(0.0f64, 1.).into(),
                                lower_right_coordinate: Coordinate2D::new(2., 0.5).into(),
                            }
                            .into(),
                        ),
                        resolution: Some(SpatialResolution::zero_point_one()),
                    }
                    .into(),
                    params: crate::api::model::operators::GdalDatasetParameters {
                        file_path: "text".into(),
                        rasterband_channel: 1,
                        geo_transform: crate::api::model::operators::GdalDatasetGeoTransform {
                            origin_coordinate: Coordinate2D::new(0.0f64, 0.5).into(),
                            x_pixel_size: 1.0,
                            y_pixel_size: 2.0,
                        },
                        width: 42,
                        height: 23,
                        file_not_found_handling:
                            crate::api::model::operators::FileNotFoundHandling::NoData,
                        no_data_value: Some(42.0),
                        properties_mapping: Some(vec![
                            crate::api::model::operators::GdalMetadataMapping {
                                source_key: RasterPropertiesKey {
                                    domain: None,
                                    key: "foo".to_string(),
                                },
                                target_key: RasterPropertiesKey {
                                    domain: Some("bar".to_string()),
                                    key: "foo".to_string(),
                                },
                                target_type: RasterPropertiesEntryType::String,
                            },
                        ]),
                        gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                        gdal_config_options: Some(vec![
                            crate::api::model::operators::GdalConfigOption::from((
                                "foo".to_string(),
                                "bar".to_string(),
                            )),
                        ]),
                        allow_alphaband_as_mask: false,
                    },
                    start: TimeInstance::from_millis(0).unwrap().into(),
                    end: TimeInstance::from_millis(1000).unwrap().into(),
                    cache_ttl: CacheTtlSeconds::max(),
                    step: TimeStep {
                        granularity: TimeGranularity::Millis,
                        step: 1,
                    }
                    .into(),
                    band_offset: 3,
                }],
            )
            .await;

            test_type(
                &pool,
                "GdalMetaDataList",
                [crate::api::model::operators::GdalMetaDataList {
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Continuous(ContinuousMeasurement {
                            measurement: "Temperature".to_string(),
                            unit: Some("°C".to_string()),
                        })
                        .into(),
                        time: TimeInterval::new_unchecked(0, 1).into(),
                        bbox: Some(
                            crate::api::model::datatypes::SpatialPartition2D {
                                upper_left_coordinate: Coordinate2D::new(0.0f64, 1.).into(),
                                lower_right_coordinate: Coordinate2D::new(2., 0.5).into(),
                            }
                            .into(),
                        ),
                        resolution: Some(SpatialResolution::zero_point_one()),
                    }
                    .into(),
                    params: vec![crate::api::model::operators::GdalLoadingInfoTemporalSlice {
                        time: TimeInterval::new_unchecked(0, 1).into(),
                        params: Some(crate::api::model::operators::GdalDatasetParameters {
                            file_path: "text".into(),
                            rasterband_channel: 1,
                            geo_transform: crate::api::model::operators::GdalDatasetGeoTransform {
                                origin_coordinate: Coordinate2D::new(0.0f64, 0.5).into(),
                                x_pixel_size: 1.0,
                                y_pixel_size: 2.0,
                            },
                            width: 42,
                            height: 23,
                            file_not_found_handling:
                                crate::api::model::operators::FileNotFoundHandling::NoData,
                            no_data_value: Some(42.0),
                            properties_mapping: Some(vec![
                                crate::api::model::operators::GdalMetadataMapping {
                                    source_key: RasterPropertiesKey {
                                        domain: None,
                                        key: "foo".to_string(),
                                    },
                                    target_key: RasterPropertiesKey {
                                        domain: Some("bar".to_string()),
                                        key: "foo".to_string(),
                                    },
                                    target_type: RasterPropertiesEntryType::String,
                                },
                            ]),
                            gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                            gdal_config_options: Some(vec![
                                crate::api::model::operators::GdalConfigOption::from((
                                    "foo".to_string(),
                                    "bar".to_string(),
                                )),
                            ]),
                            allow_alphaband_as_mask: false,
                        }),
                        cache_ttl: CacheTtlSeconds::max(),
                    }],
                }],
            )
            .await;

            test_type(
                &pool,
                "MetaDataDefinition",
                [
                    crate::datasets::storage::MetaDataDefinition::MockMetaData(
                        crate::api::model::operators::MockMetaData {
                            loading_info:
                                crate::api::model::operators::MockDatasetDataSourceLoadingInfo {
                                    points: vec![
                                        Coordinate2D::new(0.0f64, 0.5).into(),
                                        Coordinate2D::new(2., 1.0).into(),
                                    ],
                                },
                            result_descriptor: VectorResultDescriptor {
                                data_type: VectorDataType::MultiPoint,
                                spatial_reference: SpatialReferenceOption::SpatialReference(
                                    SpatialReference::epsg_4326(),
                                ),
                                columns: [(
                                    "foo".to_string(),
                                    VectorColumnInfo {
                                        data_type: FeatureDataType::Int,
                                        measurement: Measurement::Unitless.into(),
                                    },
                                )]
                                .into(),
                                time: Some(TimeInterval::default()),
                                bbox: Some(
                                    BoundingBox2D::new(
                                        Coordinate2D::new(0.0f64, 0.5),
                                        Coordinate2D::new(2., 1.0),
                                    )
                                    .unwrap(),
                                ),
                            }
                            .into(),
                            phantom: PhantomData,
                        }.into(),
                    ),
                    crate::api::model::services::MetaDataDefinition::OgrMetaData(crate::api::model::operators::OgrMetaData {
                        loading_info: crate::api::model::operators::OgrSourceDataset {
                            file_name: "test".into(),
                            layer_name: "test".to_string(),
                            data_type: Some(VectorDataType::MultiPoint.into()),
                            time: crate::api::model::operators::OgrSourceDatasetTimeType::Start {
                                start_field: "start".to_string(),
                                start_format: crate::api::model::operators::OgrSourceTimeFormat::Auto,
                                duration: crate::api::model::operators::OgrSourceDurationSpec::Zero,
                            },
                            default_geometry: Some(
                                crate::api::model::operators::TypedGeometry::MultiPoint(MultiPoint {
                                    coordinates: vec![
                                        Coordinate2D::new(0.0f64, 0.5).into(),
                                        Coordinate2D::new(2., 1.0).into(),
                                    ],
                                }),
                            ),
                            columns: Some(crate::api::model::operators::OgrSourceColumnSpec {
                                format_specifics: Some(
                                    crate::api::model::operators::FormatSpecifics::Csv {
                                        header: CsvHeader::Auto.into(),
                                    },
                                ),
                                x: "x".to_string(),
                                y: Some("y".to_string()),
                                int: vec!["int".to_string()],
                                float: vec!["float".to_string()],
                                text: vec!["text".to_string()],
                                bool: vec!["bool".to_string()],
                                datetime: vec!["datetime".to_string()],
                                rename: Some(
                                    [
                                        ("xx".to_string(), "xx_renamed".to_string()),
                                        ("yx".to_string(), "yy_renamed".to_string()),
                                    ]
                                    .into(),
                                ),
                            }),
                            force_ogr_time_filter: false,
                            force_ogr_spatial_filter: true,
                            on_error: crate::api::model::operators::OgrSourceErrorSpec::Abort,
                            sql_query: None,
                            attribute_query: Some("foo = 'bar'".to_string()),
                            cache_ttl: CacheTtlSeconds::new(5),
                        },
                        result_descriptor: VectorResultDescriptor {
                            data_type: VectorDataType::MultiPoint,
                            spatial_reference: SpatialReferenceOption::SpatialReference(
                                SpatialReference::epsg_4326(),
                            ),
                            columns: [(
                                "foo".to_string(),
                                VectorColumnInfo {
                                    data_type: FeatureDataType::Int,
                                    measurement: Measurement::Unitless.into(),
                                },
                            )]
                            .into(),
                            time: Some(TimeInterval::default()),
                            bbox: Some(
                                BoundingBox2D::new(
                                    Coordinate2D::new(0.0f64, 0.5),
                                    Coordinate2D::new(2., 1.0),
                                )
                                .unwrap(),
                            ),
                        }
                        .into(),
                        phantom: PhantomData,
                    }).into(),
                    crate::api::model::services::MetaDataDefinition::GdalMetaDataRegular(crate::api::model::operators::GdalMetaDataRegular {
                        result_descriptor: RasterResultDescriptor {
                            data_type: RasterDataType::U8,
                            spatial_reference: SpatialReference::epsg_4326().into(),
                            measurement: Measurement::Continuous(ContinuousMeasurement {
                                measurement: "Temperature".to_string(),
                                unit: Some("°C".to_string()),
                            })
                            .into(),
                            time: TimeInterval::new_unchecked(0, 1).into(),
                            bbox: Some(
                                crate::api::model::datatypes::SpatialPartition2D {
                                    upper_left_coordinate: Coordinate2D::new(0.0f64, 1.).into(),
                                    lower_right_coordinate: Coordinate2D::new(2., 0.5).into(),
                                }
                                .into(),
                            ),
                            resolution: Some(SpatialResolution::zero_point_one()),
                        }
                        .into(),
                        params: crate::api::model::operators::GdalDatasetParameters {
                            file_path: "text".into(),
                            rasterband_channel: 1,
                            geo_transform: crate::api::model::operators::GdalDatasetGeoTransform {
                                origin_coordinate: Coordinate2D::new(0.0f64, 0.5).into(),
                                x_pixel_size: 1.0,
                                y_pixel_size: 2.0,
                            },
                            width: 42,
                            height: 23,
                            file_not_found_handling:
                                crate::api::model::operators::FileNotFoundHandling::NoData,
                            no_data_value: Some(42.0),
                            properties_mapping: Some(vec![
                                crate::api::model::operators::GdalMetadataMapping {
                                    source_key: RasterPropertiesKey {
                                        domain: None,
                                        key: "foo".to_string(),
                                    },
                                    target_key: RasterPropertiesKey {
                                        domain: Some("bar".to_string()),
                                        key: "foo".to_string(),
                                    },
                                    target_type: RasterPropertiesEntryType::String,
                                },
                            ]),
                            gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                            gdal_config_options: Some(vec![
                                crate::api::model::operators::GdalConfigOption::from((
                                    "foo".to_string(),
                                    "bar".to_string(),
                                )),
                            ]),
                            allow_alphaband_as_mask: false,
                        },
                        time_placeholders: [(
                            "foo".to_string(),
                            GdalSourceTimePlaceholder {
                                format: geoengine_datatypes::primitives::DateTimeParseFormat::unix()
                                    .into(),
                                reference: TimeReference::Start,
                            },
                        )]
                        .into(),
                        data_time: TimeInterval::new_unchecked(0, 1).into(),
                        step: TimeStep {
                            granularity: TimeGranularity::Millis,
                            step: 1,
                        }
                        .into(),
                        cache_ttl: CacheTtlSeconds::max(),
                    }).into(),
                    crate::api::model::services::MetaDataDefinition::GdalStatic(crate::api::model::operators::GdalMetaDataStatic {
                        time: Some(TimeInterval::new_unchecked(0, 1).into()),
                        result_descriptor: RasterResultDescriptor {
                            data_type: RasterDataType::U8,
                            spatial_reference: SpatialReference::epsg_4326().into(),
                            measurement: Measurement::Continuous(ContinuousMeasurement {
                                measurement: "Temperature".to_string(),
                                unit: Some("°C".to_string()),
                            })
                            .into(),
                            time: TimeInterval::new_unchecked(0, 1).into(),
                            bbox: Some(
                                crate::api::model::datatypes::SpatialPartition2D {
                                    upper_left_coordinate: Coordinate2D::new(0.0f64, 1.).into(),
                                    lower_right_coordinate: Coordinate2D::new(2., 0.5).into(),
                                }
                                .into(),
                            ),
                            resolution: Some(SpatialResolution::zero_point_one()),
                        }
                        .into(),
                        params: crate::api::model::operators::GdalDatasetParameters {
                            file_path: "text".into(),
                            rasterband_channel: 1,
                            geo_transform: crate::api::model::operators::GdalDatasetGeoTransform {
                                origin_coordinate: Coordinate2D::new(0.0f64, 0.5).into(),
                                x_pixel_size: 1.0,
                                y_pixel_size: 2.0,
                            },
                            width: 42,
                            height: 23,
                            file_not_found_handling:
                                crate::api::model::operators::FileNotFoundHandling::NoData,
                            no_data_value: Some(42.0),
                            properties_mapping: Some(vec![
                                crate::api::model::operators::GdalMetadataMapping {
                                    source_key: RasterPropertiesKey {
                                        domain: None,
                                        key: "foo".to_string(),
                                    },
                                    target_key: RasterPropertiesKey {
                                        domain: Some("bar".to_string()),
                                        key: "foo".to_string(),
                                    },
                                    target_type: RasterPropertiesEntryType::String,
                                },
                            ]),
                            gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                            gdal_config_options: Some(vec![
                                crate::api::model::operators::GdalConfigOption::from((
                                    "foo".to_string(),
                                    "bar".to_string(),
                                )),
                            ]),
                            allow_alphaband_as_mask: false,
                        },
                        cache_ttl: CacheTtlSeconds::max(),
                    }).into(),
                    crate::api::model::services::MetaDataDefinition::GdalMetadataNetCdfCf(crate::api::model::operators::GdalMetadataNetCdfCf {
                        result_descriptor: RasterResultDescriptor {
                            data_type: RasterDataType::U8,
                            spatial_reference: SpatialReference::epsg_4326().into(),
                            measurement: Measurement::Continuous(ContinuousMeasurement {
                                measurement: "Temperature".to_string(),
                                unit: Some("°C".to_string()),
                            })
                            .into(),
                            time: TimeInterval::new_unchecked(0, 1).into(),
                            bbox: Some(
                                crate::api::model::datatypes::SpatialPartition2D {
                                    upper_left_coordinate: Coordinate2D::new(0.0f64, 1.).into(),
                                    lower_right_coordinate: Coordinate2D::new(2., 0.5).into(),
                                }
                                .into(),
                            ),
                            resolution: Some(SpatialResolution::zero_point_one()),
                        }
                        .into(),
                        params: crate::api::model::operators::GdalDatasetParameters {
                            file_path: "text".into(),
                            rasterband_channel: 1,
                            geo_transform: crate::api::model::operators::GdalDatasetGeoTransform {
                                origin_coordinate: Coordinate2D::new(0.0f64, 0.5).into(),
                                x_pixel_size: 1.0,
                                y_pixel_size: 2.0,
                            },
                            width: 42,
                            height: 23,
                            file_not_found_handling:
                                crate::api::model::operators::FileNotFoundHandling::NoData,
                            no_data_value: Some(42.0),
                            properties_mapping: Some(vec![
                                crate::api::model::operators::GdalMetadataMapping {
                                    source_key: RasterPropertiesKey {
                                        domain: None,
                                        key: "foo".to_string(),
                                    },
                                    target_key: RasterPropertiesKey {
                                        domain: Some("bar".to_string()),
                                        key: "foo".to_string(),
                                    },
                                    target_type: RasterPropertiesEntryType::String,
                                },
                            ]),
                            gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                            gdal_config_options: Some(vec![
                                crate::api::model::operators::GdalConfigOption::from((
                                    "foo".to_string(),
                                    "bar".to_string(),
                                )),
                            ]),
                            allow_alphaband_as_mask: false,
                        },
                        start: TimeInstance::from_millis(0).unwrap().into(),
                        end: TimeInstance::from_millis(1000).unwrap().into(),
                        cache_ttl: CacheTtlSeconds::max(),
                        step: TimeStep {
                            granularity: TimeGranularity::Millis,
                            step: 1,
                        }
                        .into(),
                        band_offset: 3,
                    }).into(),
                    crate::api::model::services::MetaDataDefinition::GdalMetaDataList(
                        crate::api::model::operators::GdalMetaDataList {
                            result_descriptor: RasterResultDescriptor {
                                data_type: RasterDataType::U8,
                                spatial_reference: SpatialReference::epsg_4326().into(),
                                measurement: Measurement::Continuous(ContinuousMeasurement {
                                    measurement: "Temperature".to_string(),
                                    unit: Some("°C".to_string()),
                                })
                                .into(),
                                time: TimeInterval::new_unchecked(0, 1).into(),
                                bbox: Some(
                                    crate::api::model::datatypes::SpatialPartition2D {
                                        upper_left_coordinate: Coordinate2D::new(0.0f64, 1.).into(),
                                        lower_right_coordinate: Coordinate2D::new(2., 0.5).into(),
                                    }
                                    .into(),
                                ),
                                resolution: Some(SpatialResolution::zero_point_one()),
                            }
                            .into(),
                            params: vec![crate::api::model::operators::GdalLoadingInfoTemporalSlice {
                                time: TimeInterval::new_unchecked(0, 1).into(),
                                params: Some(crate::api::model::operators::GdalDatasetParameters {
                                    file_path: "text".into(),
                                    rasterband_channel: 1,
                                    geo_transform: crate::api::model::operators::GdalDatasetGeoTransform {
                                        origin_coordinate: Coordinate2D::new(0.0f64, 0.5).into(),
                                        x_pixel_size: 1.0,
                                        y_pixel_size: 2.0,
                                    },
                                    width: 42,
                                    height: 23,
                                    file_not_found_handling:
                                        crate::api::model::operators::FileNotFoundHandling::NoData,
                                    no_data_value: Some(42.0),
                                    properties_mapping: Some(vec![
                                        crate::api::model::operators::GdalMetadataMapping {
                                            source_key: RasterPropertiesKey {
                                                domain: None,
                                                key: "foo".to_string(),
                                            },
                                            target_key: RasterPropertiesKey {
                                                domain: Some("bar".to_string()),
                                                key: "foo".to_string(),
                                            },
                                            target_type: RasterPropertiesEntryType::String,
                                        },
                                    ]),
                                    gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                                    gdal_config_options: Some(vec![
                                        crate::api::model::operators::GdalConfigOption::from((
                                            "foo".to_string(),
                                            "bar".to_string(),
                                        )),
                                    ]),
                                    allow_alphaband_as_mask: false,
                                }),
                                cache_ttl: CacheTtlSeconds::max(),
                            }],
                        },
                    ).into(),
                ],
            )
            .await;
        })
        .await;
    }
}
