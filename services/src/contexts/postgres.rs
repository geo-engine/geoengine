use super::migrations::{all_migrations, CurrentSchemaMigration, MigrationResult};
use super::{initialize_database, ExecutionContextImpl, Session, SimpleApplicationContext};
use crate::api::cli::{add_datasets_from_directory, add_providers_from_directory};
use crate::api::model::services::Volume;
use crate::contexts::{ApplicationContext, QueryContextImpl, SessionId, SimpleSession};
use crate::contexts::{GeoEngineDb, SessionContext};
use crate::datasets::upload::Volumes;
use crate::datasets::DatasetName;
use crate::error::{self, Error, Result};
use crate::layers::add_from_directory::{
    add_layer_collections_from_directory, add_layers_from_directory,
};
use crate::projects::{ProjectId, STRectangle};
use crate::tasks::{SimpleTaskManager, SimpleTaskManagerBackend, SimpleTaskManagerContext};
use crate::util::config;
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
use log::info;
use rayon::ThreadPool;
use snafu::ensure;
use std::path::PathBuf;
use std::sync::Arc;

// TODO: distinguish user-facing errors from system-facing error messages

/// A context with references to Postgres backends of the database.
#[derive(Clone)]
pub struct PostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
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
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub async fn new_with_context_spec(
        config: Config,
        tls: Tls,
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
        volumes: Volumes,
    ) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(config, tls);

        let pool = Pool::builder().build(pg_mgr).await?;
        let created_schema = Self::create_database(pool.get().await?).await?;

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
            volumes,
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
        let created_schema = Self::create_database(pool.get().await?).await?;

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

            add_providers_from_directory(&mut db, provider_defs_path).await;
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

    /// Clears the database if the Settings demand and the database properties allows it.
    pub(crate) async fn maybe_clear_database(
        conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
    ) -> Result<()> {
        let postgres_config = get_config_element::<crate::util::config::Postgres>()?;
        let database_status = Self::check_schema_status(conn).await?;
        let schema_name = postgres_config.schema;

        match database_status {
            DatabaseStatus::InitializedClearDatabase
                if postgres_config.clear_database_on_start && schema_name != "pg_temp" =>
            {
                info!("Clearing schema {}.", schema_name);
                conn.batch_execute(&format!("DROP SCHEMA {schema_name} CASCADE;"))
                    .await?;
            }
            DatabaseStatus::InitializedKeepDatabase if postgres_config.clear_database_on_start => {
                return Err(Error::ClearDatabaseOnStartupNotAllowed)
            }
            DatabaseStatus::InitializedClearDatabase
            | DatabaseStatus::InitializedKeepDatabase
            | DatabaseStatus::Unitialized => (),
        };

        Ok(())
    }

    /// Creates the database schema. Returns true if the schema was created, false if it already existed.
    pub(crate) async fn create_database(
        mut conn: PooledConnection<'_, PostgresConnectionManager<Tls>>,
    ) -> Result<bool> {
        Self::maybe_clear_database(&conn).await?;

        let migration = initialize_database(
            &mut conn,
            Box::new(CurrentSchemaMigration),
            &all_migrations(),
        )
        .await?;

        Ok(migration == MigrationResult::CreatedDatabase)
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
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
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
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
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
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
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
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
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
        Ok(self
            .context
            .volumes
            .volumes
            .iter()
            .map(|v| Volume {
                name: v.name.0.clone(),
                path: Some(v.path.to_string_lossy().to_string()),
            })
            .collect())
    }

    fn session(&self) -> &Self::Session {
        &self.session
    }
}

#[derive(Debug)]
pub struct PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub(crate) conn_pool: Pool<PostgresConnectionManager<Tls>>,
}

impl<Tls> PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(conn_pool: Pool<PostgresConnectionManager<Tls>>) -> Self {
        Self { conn_pool }
    }

    /// Check whether the namespace of the given dataset is allowed for insertion
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
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
}

impl TryFrom<config::Postgres> for Config {
    type Error = Error;

    fn try_from(db_config: config::Postgres) -> Result<Self> {
        ensure!(
            db_config.schema != "public",
            crate::error::InvalidDatabaseSchema
        );

        let mut pg_config = Config::new();
        pg_config
            .user(&db_config.user)
            .password(&db_config.password)
            .host(&db_config.host)
            .dbname(&db_config.database)
            .port(db_config.port)
            // fix schema by providing `search_path` option
            .options(&format!("-c search_path={}", db_config.schema));
        Ok(pg_config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasets::external::aruna::ArunaDataProviderDefinition;
    use crate::datasets::external::gbif::{GbifDataProvider, GbifDataProviderDefinition};
    use crate::datasets::external::gfbio_abcd::GfbioAbcdDataProviderDefinition;
    use crate::datasets::external::gfbio_collections::GfbioCollectionsDataProviderDefinition;
    use crate::datasets::external::netcdfcf::{
        EbvPortalDataProviderDefinition, NetCdfCfDataProviderDefinition,
    };
    use crate::datasets::external::pangaea::PangaeaDataProviderDefinition;
    use crate::datasets::listing::{DatasetListOptions, DatasetListing, ProvenanceOutput};
    use crate::datasets::listing::{DatasetProvider, Provenance};
    use crate::datasets::storage::{DatasetStore, MetaDataDefinition};
    use crate::datasets::upload::{FileId, UploadId};
    use crate::datasets::upload::{FileUpload, Upload, UploadDb};
    use crate::datasets::{AddDataset, DatasetIdAndName};
    use crate::ge_context;
    use crate::layers::add_from_directory::UNSORTED_COLLECTION_ID;
    use crate::layers::external::TypedDataProviderDefinition;
    use crate::layers::layer::{
        AddLayer, AddLayerCollection, CollectionItem, LayerCollection, LayerCollectionListOptions,
        LayerCollectionListing, LayerListing, Property, ProviderLayerCollectionId, ProviderLayerId,
    };
    use crate::layers::listing::{
        LayerCollectionId, LayerCollectionProvider, SearchParameters, SearchType,
    };
    use crate::layers::storage::{
        LayerDb, LayerProviderDb, LayerProviderListing, LayerProviderListingOptions,
        INTERNAL_PROVIDER_ID,
    };
    use crate::projects::{
        ColorParam, CreateProject, DerivedColor, DerivedNumber, LayerUpdate, LineSymbology,
        LoadVersion, NumberParam, OrderBy, Plot, PlotUpdate, PointSymbology, PolygonSymbology,
        ProjectDb, ProjectId, ProjectLayer, ProjectListOptions, ProjectListing, RasterSymbology,
        STRectangle, StrokeParam, Symbology, TextSymbology, UpdateProject,
    };
    use crate::util::encryption::U96;
    use crate::util::postgres::{assert_sql_type, DatabaseConnectionConfig};
    use crate::util::tests::register_ndvi_workflow_helper;
    use crate::workflows::registry::WorkflowRegistry;
    use crate::workflows::workflow::Workflow;
    use aes_gcm::aead::generic_array::arr;
    use bb8_postgres::tokio_postgres::NoTls;
    use futures::join;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::dataset::{DataProviderId, LayerId};
    use geoengine_datatypes::operations::image::{
        Breakpoint, Colorizer, RasterColorizer, RgbaColor,
    };
    use geoengine_datatypes::primitives::{
        BoundingBox2D, ClassificationMeasurement, ColumnSelection, ContinuousMeasurement,
        Coordinate2D, DateTimeParseFormat, FeatureDataType, MultiLineString, MultiPoint,
        MultiPolygon, NoGeometry, RasterQueryRectangle, SpatialPartition2D, SpatialResolution,
        TimeGranularity, TimeInstance, TimeInterval, TimeStep, TypedGeometry, VectorQueryRectangle,
    };
    use geoengine_datatypes::primitives::{CacheTtlSeconds, Measurement};
    use geoengine_datatypes::raster::{
        RasterDataType, RasterPropertiesEntryType, RasterPropertiesKey,
    };
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_datatypes::util::{NotNanF64, StringPair};
    use geoengine_operators::engine::{
        MetaData, MetaDataProvider, MultipleRasterOrSingleVectorSource, PlotOperator,
        PlotResultDescriptor, RasterBandDescriptor, RasterBandDescriptors, RasterResultDescriptor,
        StaticMetaData, TypedOperator, TypedResultDescriptor, VectorColumnInfo, VectorOperator,
        VectorResultDescriptor,
    };
    use geoengine_operators::mock::{
        MockDatasetDataSourceLoadingInfo, MockPointSource, MockPointSourceParams,
    };
    use geoengine_operators::plot::{Statistics, StatisticsParams};
    use geoengine_operators::source::{
        CsvHeader, FileNotFoundHandling, FormatSpecifics, GdalDatasetGeoTransform,
        GdalDatasetParameters, GdalLoadingInfo, GdalLoadingInfoTemporalSlice, GdalMetaDataList,
        GdalMetaDataRegular, GdalMetaDataStatic, GdalMetadataMapping, GdalMetadataNetCdfCf,
        GdalRetryOptions, GdalSourceTimePlaceholder, OgrSourceColumnSpec, OgrSourceDataset,
        OgrSourceDatasetTimeType, OgrSourceDurationSpec, OgrSourceErrorSpec, OgrSourceTimeFormat,
        TimeReference, UnixTimeStampType,
    };
    use geoengine_operators::util::input::MultiRasterOrVectorOperator::Raster;
    use ordered_float::NotNan;
    use serde_json::json;
    use std::marker::PhantomData;
    use std::str::FromStr;
    use tokio_postgres::config::Host;

    #[ge_context::test]
    async fn test(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.default_session().await.unwrap();

        create_projects(&app_ctx, &session).await;

        let projects = list_projects(&app_ctx, &session).await;

        let project_id = projects[0].id;

        update_projects(&app_ctx, &session, project_id).await;

        delete_project(&app_ctx, &session, project_id).await;
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
                        percentiles: vec![],
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

    #[ge_context::test]
    async fn it_persists_workflows(app_ctx: PostgresContext<NoTls>) {
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

        let db = ctx.db();
        let id = db.register_workflow(workflow).await.unwrap();

        drop(ctx);

        let workflow = db.load_workflow(&id).await.unwrap();

        let json = serde_json::to_string(&workflow).unwrap();
        assert_eq!(
            json,
            r#"{"type":"Vector","operator":{"type":"MockPointSource","params":{"points":[{"x":1.0,"y":2.0},{"x":1.0,"y":2.0},{"x":1.0,"y":2.0}]}}}"#
        );
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_persists_datasets(app_ctx: PostgresContext<NoTls>) {
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
                        measurement: Measurement::Unitless,
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
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data,
            )
            .await
            .unwrap();

        let datasets = db
            .list_datasets(DatasetListOptions {
                filter: None,
                order: crate::datasets::listing::OrderBy::NameAsc,
                offset: 0,
                limit: 10,
                tags: None,
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
                tags: vec!["upload".to_owned(), "test".to_owned()],
                result_descriptor: TypedResultDescriptor::Vector(VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [(
                        "foo".to_owned(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Float,
                            measurement: Measurement::Unitless
                        }
                    )]
                    .into_iter()
                    .collect(),
                    time: None,
                    bbox: None,
                })
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
                    attributes: ColumnSelection::all()
                })
                .await
                .unwrap(),
            loading_info
        );
    }

    #[ge_context::test]
    async fn it_persists_uploads(app_ctx: PostgresContext<NoTls>) {
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
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_persists_layer_providers(app_ctx: PostgresContext<NoTls>) {
        let db = app_ctx.default_session_context().await.unwrap().db();

        let provider = NetCdfCfDataProviderDefinition {
            name: "netcdfcf".to_string(),
            description: "NetCdfCfProviderDefinition".to_string(),
            priority: Some(21),
            data: test_data!("netcdf4d/").into(),
            overviews: test_data!("netcdf4d/overviews/").into(),
            cache_ttl: CacheTtlSeconds::new(0),
        };

        let provider_id = db.add_layer_provider(provider.into()).await.unwrap();

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
                name: "netcdfcf".to_owned(),
                priority: 21,
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

        assert_eq!(datasets.items.len(), 5, "{:?}", datasets.items);
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_loads_all_meta_data_types(app_ctx: PostgresContext<NoTls>) {
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
            time: None,
            bbox: None,
            resolution: None,
            bands: RasterBandDescriptors::new_single_band(),
        };

        let vector_ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
        };

        let raster_ds = AddDataset {
            name: None,
            display_name: "GdalDataset".to_string(),
            description: "My Gdal dataset".to_string(),
            source_operator: "GdalSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
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

        let id = db.add_dataset(vector_ds, meta.into()).await.unwrap().id;

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

        let id = db
            .add_dataset(raster_ds.clone(), meta.into())
            .await
            .unwrap()
            .id;

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

        let id = db
            .add_dataset(raster_ds.clone(), meta.into())
            .await
            .unwrap()
            .id;

        let meta: geoengine_operators::util::Result<
            Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        > = db.meta_data(&id.into()).await;

        assert!(meta.is_ok());

        let meta = GdalMetaDataList {
            result_descriptor: raster_descriptor.clone(),
            params: vec![],
        };

        let id = db
            .add_dataset(raster_ds.clone(), meta.into())
            .await
            .unwrap()
            .id;

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

        let id = db
            .add_dataset(raster_ds.clone(), meta.into())
            .await
            .unwrap()
            .id;

        let meta: geoengine_operators::util::Result<
            Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        > = db.meta_data(&id.into()).await;

        assert!(meta.is_ok());
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_collects_layers(app_ctx: PostgresContext<NoTls>) {
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
                            collection_id: LayerCollectionId(UNSORTED_COLLECTION_ID.to_string()),
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
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_searches_layers(app_ctx: PostgresContext<NoTls>) {
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

        let root_collection_all = layer_db
            .search(
                &root_collection_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: String::new(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            root_collection_all,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: INTERNAL_PROVIDER_ID,
                    collection_id: root_collection_id.clone(),
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
                            collection_id: collection2_id.clone(),
                        },
                        name: "Collection2".to_string(),
                        description: "Collection 2".to_string(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            collection_id: LayerCollectionId(
                                "ffb2dd9e-f5ad-427c-b7f1-c9a0c7a0ae3f".to_string()
                            ),
                        },
                        name: "Unsorted".to_string(),
                        description: "Unsorted Layers".to_string(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Layer(LayerListing {
                        id: ProviderLayerId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            layer_id: layer1.clone(),
                        },
                        name: "Layer1".to_string(),
                        description: "Layer 1".to_string(),
                        properties: vec![("proper".to_string(), "tee".to_string()).into()],
                    }),
                    CollectionItem::Layer(LayerListing {
                        id: ProviderLayerId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            layer_id: layer2.clone(),
                        },
                        name: "Layer2".to_string(),
                        description: "Layer 2".to_string(),
                        properties: vec![],
                    }),
                ],
                entry_label: None,
                properties: vec![],
            }
        );

        let root_collection_filtered = layer_db
            .search(
                &root_collection_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "lection".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            root_collection_filtered,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: INTERNAL_PROVIDER_ID,
                    collection_id: root_collection_id.clone(),
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
                            collection_id: collection2_id.clone(),
                        },
                        name: "Collection2".to_string(),
                        description: "Collection 2".to_string(),
                        properties: Default::default(),
                    }),
                ],
                entry_label: None,
                properties: vec![],
            }
        );

        let collection1_all = layer_db
            .search(
                &collection1_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: String::new(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection1_all,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: INTERNAL_PROVIDER_ID,
                    collection_id: collection1_id.clone(),
                },
                name: "Collection1".to_string(),
                description: "Collection 1".to_string(),
                items: vec![
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            collection_id: collection2_id.clone(),
                        },
                        name: "Collection2".to_string(),
                        description: "Collection 2".to_string(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Layer(LayerListing {
                        id: ProviderLayerId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            layer_id: layer2.clone(),
                        },
                        name: "Layer2".to_string(),
                        description: "Layer 2".to_string(),
                        properties: vec![],
                    }),
                ],
                entry_label: None,
                properties: vec![],
            }
        );

        let collection1_filtered_fulltext = layer_db
            .search(
                &collection1_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "ay".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection1_filtered_fulltext,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: INTERNAL_PROVIDER_ID,
                    collection_id: collection1_id.clone(),
                },
                name: "Collection1".to_string(),
                description: "Collection 1".to_string(),
                items: vec![CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: INTERNAL_PROVIDER_ID,
                        layer_id: layer2.clone(),
                    },
                    name: "Layer2".to_string(),
                    description: "Layer 2".to_string(),
                    properties: vec![],
                }),],
                entry_label: None,
                properties: vec![],
            }
        );

        let collection1_filtered_prefix = layer_db
            .search(
                &collection1_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "ay".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection1_filtered_prefix,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: INTERNAL_PROVIDER_ID,
                    collection_id: collection1_id.clone(),
                },
                name: "Collection1".to_string(),
                description: "Collection 1".to_string(),
                items: vec![],
                entry_label: None,
                properties: vec![],
            }
        );

        let collection1_filtered_prefix2 = layer_db
            .search(
                &collection1_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "Lay".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection1_filtered_prefix2,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: INTERNAL_PROVIDER_ID,
                    collection_id: collection1_id.clone(),
                },
                name: "Collection1".to_string(),
                description: "Collection 1".to_string(),
                items: vec![CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: INTERNAL_PROVIDER_ID,
                        layer_id: layer2.clone(),
                    },
                    name: "Layer2".to_string(),
                    description: "Layer 2".to_string(),
                    properties: vec![],
                }),],
                entry_label: None,
                properties: vec![],
            }
        );
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_autocompletes_layers(app_ctx: PostgresContext<NoTls>) {
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

        let _layer1 = layer_db
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

        let _layer2 = layer_db
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

        let _collection2_id = layer_db
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

        let root_collection_all = layer_db
            .autocomplete_search(
                &root_collection_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: String::new(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            root_collection_all,
            vec![
                "Collection1".to_string(),
                "Collection2".to_string(),
                "Layer1".to_string(),
                "Layer2".to_string(),
                "Unsorted".to_string(),
            ]
        );

        let root_collection_filtered = layer_db
            .autocomplete_search(
                &root_collection_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "lection".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            root_collection_filtered,
            vec!["Collection1".to_string(), "Collection2".to_string(),]
        );

        let collection1_all = layer_db
            .autocomplete_search(
                &collection1_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: String::new(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection1_all,
            vec!["Collection2".to_string(), "Layer2".to_string(),]
        );

        let collection1_filtered_fulltext = layer_db
            .autocomplete_search(
                &collection1_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "ay".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(collection1_filtered_fulltext, vec!["Layer2".to_string(),]);

        let collection1_filtered_prefix = layer_db
            .autocomplete_search(
                &collection1_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "ay".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(collection1_filtered_prefix, Vec::<String>::new());

        let collection1_filtered_prefix2 = layer_db
            .autocomplete_search(
                &collection1_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "Lay".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(collection1_filtered_prefix2, vec!["Layer2".to_string(),]);
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_reports_search_capabilities(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.default_session().await.unwrap();

        let layer_db = app_ctx.session_context(session).db();

        let capabilities = layer_db.capabilities().search;

        let root_collection_id = layer_db.get_root_layer_collection_id().await.unwrap();

        if capabilities.search_types.fulltext {
            assert!(layer_db
                .search(
                    &root_collection_id,
                    SearchParameters {
                        search_type: SearchType::Fulltext,
                        search_string: String::new(),
                        limit: 10,
                        offset: 0,
                    },
                )
                .await
                .is_ok());

            if capabilities.autocomplete {
                assert!(layer_db
                    .autocomplete_search(
                        &root_collection_id,
                        SearchParameters {
                            search_type: SearchType::Fulltext,
                            search_string: String::new(),
                            limit: 10,
                            offset: 0,
                        },
                    )
                    .await
                    .is_ok());
            } else {
                assert!(layer_db
                    .autocomplete_search(
                        &root_collection_id,
                        SearchParameters {
                            search_type: SearchType::Fulltext,
                            search_string: String::new(),
                            limit: 10,
                            offset: 0,
                        },
                    )
                    .await
                    .is_err());
            }
        }
        if capabilities.search_types.prefix {
            assert!(layer_db
                .search(
                    &root_collection_id,
                    SearchParameters {
                        search_type: SearchType::Prefix,
                        search_string: String::new(),
                        limit: 10,
                        offset: 0,
                    },
                )
                .await
                .is_ok());

            if capabilities.autocomplete {
                assert!(layer_db
                    .autocomplete_search(
                        &root_collection_id,
                        SearchParameters {
                            search_type: SearchType::Prefix,
                            search_string: String::new(),
                            limit: 10,
                            offset: 0,
                        },
                    )
                    .await
                    .is_ok());
            } else {
                assert!(layer_db
                    .autocomplete_search(
                        &root_collection_id,
                        SearchParameters {
                            search_type: SearchType::Prefix,
                            search_string: String::new(),
                            limit: 10,
                            offset: 0,
                        },
                    )
                    .await
                    .is_err());
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_removes_layer_collections(app_ctx: PostgresContext<NoTls>) {
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
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_removes_collections_from_collections(app_ctx: PostgresContext<NoTls>) {
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
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_removes_layers_from_collections(app_ctx: PostgresContext<NoTls>) {
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
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_deletes_dataset(app_ctx: PostgresContext<NoTls>) {
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
                        measurement: Measurement::Unitless,
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
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data,
            )
            .await
            .unwrap()
            .id;

        assert!(db.load_dataset(&dataset_id).await.is_ok());

        db.delete_dataset(dataset_id).await.unwrap();

        assert!(db.load_dataset(&dataset_id).await.is_err());
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_deletes_admin_dataset(app_ctx: PostgresContext<NoTls>) {
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
                        measurement: Measurement::Unitless,
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
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data,
            )
            .await
            .unwrap()
            .id;

        assert!(db.load_dataset(&dataset_id).await.is_ok());

        db.delete_dataset(dataset_id).await.unwrap();

        assert!(db.load_dataset(&dataset_id).await.is_err());
    }

    #[ge_context::test]
    async fn test_missing_layer_dataset_in_collection_listing(app_ctx: PostgresContext<NoTls>) {
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
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_updates_project_layer_symbology(app_ctx: PostgresContext<NoTls>) {
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
                    "rasterColorizer": {
                        "type": "singleBand",
                        "band": 0,
                        "bandColorizer": {
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
                    "rasterColorizer": {
                        "type": "singleBand",
                        "band": 0,
                        "bandColorizer": {
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
                    "rasterColorizer": {
                        "type": "singleBand",
                        "band": 0,
                        "bandColorizer": {
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
                    "rasterColorizer": {
                        "type": "singleBand",
                        "band": 0,
                        "bandColorizer": {
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
                }
            }]
        }))
        .unwrap();

        // run two updates concurrently
        let (r0, r1) = join!(db.update_project(update.clone()), db.update_project(update));

        assert!(r0.is_ok());
        assert!(r1.is_ok());
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_resolves_dataset_names_to_ids(app_ctx: PostgresContext<NoTls>) {
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
                        measurement: Measurement::Unitless,
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
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data.clone(),
            )
            .await
            .unwrap();

        assert_eq!(
            db.resolve_dataset_name_to_id(&dataset_name1)
                .await
                .unwrap()
                .unwrap(),
            dataset_id1
        );
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn test_postgres_type_serialization(app_ctx: PostgresContext<NoTls>) {
        let pool = app_ctx.pool.get().await.unwrap();

        assert_sql_type(&pool, "RgbaColor", [RgbaColor::new(0, 1, 2, 3)]).await;

        assert_sql_type(
            &pool,
            "double precision",
            [NotNanF64::from(NotNan::<f64>::new(1.0).unwrap())],
        )
        .await;

        assert_sql_type(
            &pool,
            "Breakpoint",
            [Breakpoint {
                value: NotNan::<f64>::new(1.0).unwrap(),
                color: RgbaColor::new(0, 0, 0, 0),
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "Colorizer",
            [
                Colorizer::LinearGradient {
                    breakpoints: vec![
                        Breakpoint {
                            value: NotNan::<f64>::new(-10.0).unwrap(),
                            color: RgbaColor::new(0, 0, 0, 0),
                        },
                        Breakpoint {
                            value: NotNan::<f64>::new(2.0).unwrap(),
                            color: RgbaColor::new(255, 0, 0, 255),
                        },
                    ],
                    no_data_color: RgbaColor::new(0, 10, 20, 30),
                    over_color: RgbaColor::new(1, 2, 3, 4),
                    under_color: RgbaColor::new(5, 6, 7, 8),
                },
                Colorizer::LogarithmicGradient {
                    breakpoints: vec![
                        Breakpoint {
                            value: NotNan::<f64>::new(1.0).unwrap(),
                            color: RgbaColor::new(0, 0, 0, 0),
                        },
                        Breakpoint {
                            value: NotNan::<f64>::new(2.0).unwrap(),
                            color: RgbaColor::new(255, 0, 0, 255),
                        },
                    ],
                    no_data_color: RgbaColor::new(0, 10, 20, 30),
                    over_color: RgbaColor::new(1, 2, 3, 4),
                    under_color: RgbaColor::new(5, 6, 7, 8),
                },
                Colorizer::palette(
                    [
                        (NotNan::<f64>::new(1.0).unwrap(), RgbaColor::new(0, 0, 0, 0)),
                        (
                            NotNan::<f64>::new(2.0).unwrap(),
                            RgbaColor::new(255, 0, 0, 255),
                        ),
                        (
                            NotNan::<f64>::new(3.0).unwrap(),
                            RgbaColor::new(0, 10, 20, 30),
                        ),
                    ]
                    .into(),
                    RgbaColor::new(1, 2, 3, 4),
                    RgbaColor::new(5, 6, 7, 8),
                )
                .unwrap(),
            ],
        )
        .await;

        assert_sql_type(
            &pool,
            "ColorParam",
            [
                ColorParam::Static {
                    color: RgbaColor::new(0, 10, 20, 30),
                },
                ColorParam::Derived(DerivedColor {
                    attribute: "foobar".to_string(),
                    colorizer: Colorizer::test_default(),
                }),
            ],
        )
        .await;

        assert_sql_type(
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

        assert_sql_type(
            &pool,
            "StrokeParam",
            [StrokeParam {
                width: NumberParam::Static { value: 42 },
                color: ColorParam::Static {
                    color: RgbaColor::new(0, 10, 20, 30),
                },
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "TextSymbology",
            [TextSymbology {
                attribute: "attribute".to_string(),
                fill_color: ColorParam::Static {
                    color: RgbaColor::new(0, 10, 20, 30),
                },
                stroke: StrokeParam {
                    width: NumberParam::Static { value: 42 },
                    color: ColorParam::Static {
                        color: RgbaColor::new(0, 10, 20, 30),
                    },
                },
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "Symbology",
            [
                Symbology::Point(PointSymbology {
                    fill_color: ColorParam::Static {
                        color: RgbaColor::new(0, 10, 20, 30),
                    },
                    stroke: StrokeParam {
                        width: NumberParam::Static { value: 42 },
                        color: ColorParam::Static {
                            color: RgbaColor::new(0, 10, 20, 30),
                        },
                    },
                    radius: NumberParam::Static { value: 42 },
                    text: Some(TextSymbology {
                        attribute: "attribute".to_string(),
                        fill_color: ColorParam::Static {
                            color: RgbaColor::new(0, 10, 20, 30),
                        },
                        stroke: StrokeParam {
                            width: NumberParam::Static { value: 42 },
                            color: ColorParam::Static {
                                color: RgbaColor::new(0, 10, 20, 30),
                            },
                        },
                    }),
                }),
                Symbology::Line(LineSymbology {
                    stroke: StrokeParam {
                        width: NumberParam::Static { value: 42 },
                        color: ColorParam::Static {
                            color: RgbaColor::new(0, 10, 20, 30),
                        },
                    },
                    text: Some(TextSymbology {
                        attribute: "attribute".to_string(),
                        fill_color: ColorParam::Static {
                            color: RgbaColor::new(0, 10, 20, 30),
                        },
                        stroke: StrokeParam {
                            width: NumberParam::Static { value: 42 },
                            color: ColorParam::Static {
                                color: RgbaColor::new(0, 10, 20, 30),
                            },
                        },
                    }),
                    auto_simplified: true,
                }),
                Symbology::Polygon(PolygonSymbology {
                    fill_color: ColorParam::Static {
                        color: RgbaColor::new(0, 10, 20, 30),
                    },
                    stroke: StrokeParam {
                        width: NumberParam::Static { value: 42 },
                        color: ColorParam::Static {
                            color: RgbaColor::new(0, 10, 20, 30),
                        },
                    },
                    text: Some(TextSymbology {
                        attribute: "attribute".to_string(),
                        fill_color: ColorParam::Static {
                            color: RgbaColor::new(0, 10, 20, 30),
                        },
                        stroke: StrokeParam {
                            width: NumberParam::Static { value: 42 },
                            color: ColorParam::Static {
                                color: RgbaColor::new(0, 10, 20, 30),
                            },
                        },
                    }),
                    auto_simplified: true,
                }),
                Symbology::Raster(RasterSymbology {
                    opacity: 1.0,
                    raster_colorizer: RasterColorizer::SingleBand {
                        band: 0,
                        band_colorizer: Colorizer::LinearGradient {
                            breakpoints: vec![
                                Breakpoint {
                                    value: NotNan::<f64>::new(-10.0).unwrap(),
                                    color: RgbaColor::new(0, 0, 0, 0),
                                },
                                Breakpoint {
                                    value: NotNan::<f64>::new(2.0).unwrap(),
                                    color: RgbaColor::new(255, 0, 0, 255),
                                },
                            ],
                            no_data_color: RgbaColor::new(0, 10, 20, 30),
                            over_color: RgbaColor::new(1, 2, 3, 4),
                            under_color: RgbaColor::new(5, 6, 7, 8),
                        },
                    },
                }),
            ],
        )
        .await;

        assert_sql_type(
            &pool,
            "RasterDataType",
            [
                RasterDataType::U8,
                RasterDataType::U16,
                RasterDataType::U32,
                RasterDataType::U64,
                RasterDataType::I8,
                RasterDataType::I16,
                RasterDataType::I32,
                RasterDataType::I64,
                RasterDataType::F32,
                RasterDataType::F64,
            ],
        )
        .await;

        assert_sql_type(
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

        assert_sql_type(&pool, "Coordinate2D", [Coordinate2D::new(0.0f64, 1.)]).await;

        assert_sql_type(
            &pool,
            "SpatialPartition2D",
            [
                SpatialPartition2D::new(Coordinate2D::new(0.0f64, 1.), Coordinate2D::new(2., 0.5))
                    .unwrap(),
            ],
        )
        .await;

        assert_sql_type(
            &pool,
            "BoundingBox2D",
            [
                BoundingBox2D::new(Coordinate2D::new(0.0f64, 0.5), Coordinate2D::new(2., 1.0))
                    .unwrap(),
            ],
        )
        .await;

        assert_sql_type(
            &pool,
            "SpatialResolution",
            [SpatialResolution { x: 1.2, y: 2.3 }],
        )
        .await;

        assert_sql_type(
            &pool,
            "VectorDataType",
            [
                VectorDataType::Data,
                VectorDataType::MultiPoint,
                VectorDataType::MultiLineString,
                VectorDataType::MultiPolygon,
            ],
        )
        .await;

        assert_sql_type(
            &pool,
            "FeatureDataType",
            [
                FeatureDataType::Category,
                FeatureDataType::Int,
                FeatureDataType::Float,
                FeatureDataType::Text,
                FeatureDataType::Bool,
                FeatureDataType::DateTime,
            ],
        )
        .await;

        assert_sql_type(&pool, "TimeInterval", [TimeInterval::default()]).await;

        assert_sql_type(
            &pool,
            "SpatialReference",
            [
                SpatialReferenceOption::Unreferenced,
                SpatialReferenceOption::SpatialReference(SpatialReference::epsg_4326()),
            ],
        )
        .await;

        assert_sql_type(
            &pool,
            "PlotResultDescriptor",
            [PlotResultDescriptor {
                spatial_reference: SpatialReferenceOption::Unreferenced,
                time: None,
                bbox: None,
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "VectorResultDescriptor",
            [VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReferenceOption::SpatialReference(
                    SpatialReference::epsg_4326(),
                ),
                columns: [(
                    "foo".to_string(),
                    VectorColumnInfo {
                        data_type: FeatureDataType::Int,
                        measurement: Measurement::Unitless,
                    },
                )]
                .into(),
                time: Some(TimeInterval::default()),
                bbox: Some(
                    BoundingBox2D::new(Coordinate2D::new(0.0f64, 0.5), Coordinate2D::new(2., 1.0))
                        .unwrap(),
                ),
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "RasterResultDescriptor",
            [RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReferenceOption::SpatialReference(
                    SpatialReference::epsg_4326(),
                ),
                time: Some(TimeInterval::default()),
                bbox: Some(
                    SpatialPartition2D::new(
                        Coordinate2D::new(0.0f64, 1.),
                        Coordinate2D::new(2., 0.5),
                    )
                    .unwrap(),
                ),
                resolution: Some(SpatialResolution { x: 1.2, y: 2.3 }),
                bands: RasterBandDescriptors::new_single_band(),
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "ResultDescriptor",
            [
                TypedResultDescriptor::Vector(VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReferenceOption::SpatialReference(
                        SpatialReference::epsg_4326(),
                    ),
                    columns: [(
                        "foo".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Unitless,
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
                }),
                TypedResultDescriptor::Raster(RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReferenceOption::SpatialReference(
                        SpatialReference::epsg_4326(),
                    ),
                    time: Some(TimeInterval::default()),
                    bbox: Some(
                        SpatialPartition2D::new(
                            Coordinate2D::new(0.0f64, 1.),
                            Coordinate2D::new(2., 0.5),
                        )
                        .unwrap(),
                    ),
                    resolution: Some(SpatialResolution { x: 1.2, y: 2.3 }),
                    bands: RasterBandDescriptors::new_single_band(),
                }),
                TypedResultDescriptor::Plot(PlotResultDescriptor {
                    spatial_reference: SpatialReferenceOption::Unreferenced,
                    time: None,
                    bbox: None,
                }),
            ],
        )
        .await;

        assert_sql_type(
            &pool,
            "MockDatasetDataSourceLoadingInfo",
            [MockDatasetDataSourceLoadingInfo {
                points: vec![Coordinate2D::new(0.0f64, 0.5), Coordinate2D::new(2., 1.0)],
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "OgrSourceTimeFormat",
            [
                OgrSourceTimeFormat::Auto,
                OgrSourceTimeFormat::Custom {
                    custom_format: geoengine_datatypes::primitives::DateTimeParseFormat::custom(
                        "%Y-%m-%dT%H:%M:%S%.3fZ".to_string(),
                    ),
                },
                OgrSourceTimeFormat::UnixTimeStamp {
                    timestamp_type: UnixTimeStampType::EpochSeconds,
                    fmt: geoengine_datatypes::primitives::DateTimeParseFormat::unix(),
                },
            ],
        )
        .await;

        assert_sql_type(
            &pool,
            "OgrSourceDurationSpec",
            [
                OgrSourceDurationSpec::Infinite,
                OgrSourceDurationSpec::Zero,
                OgrSourceDurationSpec::Value(TimeStep {
                    granularity: TimeGranularity::Millis,
                    step: 1000,
                }),
            ],
        )
        .await;

        assert_sql_type(
            &pool,
            "OgrSourceDatasetTimeType",
            [
                OgrSourceDatasetTimeType::None,
                OgrSourceDatasetTimeType::Start {
                    start_field: "start".to_string(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                },
                OgrSourceDatasetTimeType::StartEnd {
                    start_field: "start".to_string(),
                    start_format: OgrSourceTimeFormat::Auto,
                    end_field: "end".to_string(),
                    end_format: OgrSourceTimeFormat::Auto,
                },
                OgrSourceDatasetTimeType::StartDuration {
                    start_field: "start".to_string(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration_field: "duration".to_string(),
                },
            ],
        )
        .await;

        assert_sql_type(
            &pool,
            "FormatSpecifics",
            [FormatSpecifics::Csv {
                header: CsvHeader::Yes,
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "OgrSourceColumnSpec",
            [OgrSourceColumnSpec {
                format_specifics: Some(FormatSpecifics::Csv {
                    header: CsvHeader::Auto,
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

        assert_sql_type(
            &pool,
            "point[]",
            [MultiPoint::new(vec![
                Coordinate2D::new(0.0f64, 0.5),
                Coordinate2D::new(2., 1.0),
            ])
            .unwrap()],
        )
        .await;

        assert_sql_type(
            &pool,
            "path[]",
            [MultiLineString::new(vec![
                vec![Coordinate2D::new(0.0f64, 0.5), Coordinate2D::new(2., 1.0)],
                vec![Coordinate2D::new(0.0f64, 0.5), Coordinate2D::new(2., 1.0)],
            ])
            .unwrap()],
        )
        .await;

        assert_sql_type(
            &pool,
            "\"Polygon\"[]",
            [MultiPolygon::new(vec![
                vec![
                    vec![
                        Coordinate2D::new(0.0f64, 0.5),
                        Coordinate2D::new(2., 1.0),
                        Coordinate2D::new(2., 1.0),
                        Coordinate2D::new(0.0f64, 0.5),
                    ],
                    vec![
                        Coordinate2D::new(0.0f64, 0.5),
                        Coordinate2D::new(2., 1.0),
                        Coordinate2D::new(2., 1.0),
                        Coordinate2D::new(0.0f64, 0.5),
                    ],
                ],
                vec![
                    vec![
                        Coordinate2D::new(0.0f64, 0.5),
                        Coordinate2D::new(2., 1.0),
                        Coordinate2D::new(2., 1.0),
                        Coordinate2D::new(0.0f64, 0.5),
                    ],
                    vec![
                        Coordinate2D::new(0.0f64, 0.5),
                        Coordinate2D::new(2., 1.0),
                        Coordinate2D::new(2., 1.0),
                        Coordinate2D::new(0.0f64, 0.5),
                    ],
                ],
            ])
            .unwrap()],
        )
        .await;

        assert_sql_type(
            &pool,
            "TypedGeometry",
            [
                TypedGeometry::Data(NoGeometry),
                TypedGeometry::MultiPoint(
                    MultiPoint::new(vec![
                        Coordinate2D::new(0.0f64, 0.5),
                        Coordinate2D::new(2., 1.0),
                    ])
                    .unwrap(),
                ),
                TypedGeometry::MultiLineString(
                    MultiLineString::new(vec![
                        vec![Coordinate2D::new(0.0f64, 0.5), Coordinate2D::new(2., 1.0)],
                        vec![Coordinate2D::new(0.0f64, 0.5), Coordinate2D::new(2., 1.0)],
                    ])
                    .unwrap(),
                ),
                TypedGeometry::MultiPolygon(
                    MultiPolygon::new(vec![
                        vec![
                            vec![
                                Coordinate2D::new(0.0f64, 0.5),
                                Coordinate2D::new(2., 1.0),
                                Coordinate2D::new(2., 1.0),
                                Coordinate2D::new(0.0f64, 0.5),
                            ],
                            vec![
                                Coordinate2D::new(0.0f64, 0.5),
                                Coordinate2D::new(2., 1.0),
                                Coordinate2D::new(2., 1.0),
                                Coordinate2D::new(0.0f64, 0.5),
                            ],
                        ],
                        vec![
                            vec![
                                Coordinate2D::new(0.0f64, 0.5),
                                Coordinate2D::new(2., 1.0),
                                Coordinate2D::new(2., 1.0),
                                Coordinate2D::new(0.0f64, 0.5),
                            ],
                            vec![
                                Coordinate2D::new(0.0f64, 0.5),
                                Coordinate2D::new(2., 1.0),
                                Coordinate2D::new(2., 1.0),
                                Coordinate2D::new(0.0f64, 0.5),
                            ],
                        ],
                    ])
                    .unwrap(),
                ),
            ],
        )
        .await;

        assert_sql_type(&pool, "int", [CacheTtlSeconds::new(100)]).await;

        assert_sql_type(
            &pool,
            "OgrSourceDataset",
            [OgrSourceDataset {
                file_name: "test".into(),
                layer_name: "test".to_string(),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::Start {
                    start_field: "start".to_string(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                },
                default_geometry: Some(TypedGeometry::MultiPoint(
                    MultiPoint::new(vec![
                        Coordinate2D::new(0.0f64, 0.5),
                        Coordinate2D::new(2., 1.0),
                    ])
                    .unwrap(),
                )),
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: Some(FormatSpecifics::Csv {
                        header: CsvHeader::Auto,
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
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: true,
                on_error: OgrSourceErrorSpec::Abort,
                sql_query: None,
                attribute_query: Some("foo = 'bar'".to_string()),
                cache_ttl: CacheTtlSeconds::new(5),
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "MockMetaData",
            [StaticMetaData::<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            > {
                loading_info: MockDatasetDataSourceLoadingInfo {
                    points: vec![Coordinate2D::new(0.0f64, 0.5), Coordinate2D::new(2., 1.0)],
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
                            measurement: Measurement::Unitless,
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
                },
                phantom: PhantomData,
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "OgrMetaData",
            [
                StaticMetaData::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle> {
                    loading_info: OgrSourceDataset {
                        file_name: "test".into(),
                        layer_name: "test".to_string(),
                        data_type: Some(VectorDataType::MultiPoint),
                        time: OgrSourceDatasetTimeType::Start {
                            start_field: "start".to_string(),
                            start_format: OgrSourceTimeFormat::Auto,
                            duration: OgrSourceDurationSpec::Zero,
                        },
                        default_geometry: Some(TypedGeometry::MultiPoint(
                            MultiPoint::new(vec![
                                Coordinate2D::new(0.0f64, 0.5),
                                Coordinate2D::new(2., 1.0),
                            ])
                            .unwrap(),
                        )),
                        columns: Some(OgrSourceColumnSpec {
                            format_specifics: Some(FormatSpecifics::Csv {
                                header: CsvHeader::Auto,
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
                        }),
                        force_ogr_time_filter: false,
                        force_ogr_spatial_filter: true,
                        on_error: OgrSourceErrorSpec::Abort,
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
                                measurement: Measurement::Unitless,
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
                    },
                    phantom: PhantomData,
                },
            ],
        )
        .await;

        assert_sql_type(
            &pool,
            "GdalDatasetGeoTransform",
            [GdalDatasetGeoTransform {
                origin_coordinate: Coordinate2D::new(0.0f64, 0.5),
                x_pixel_size: 1.0,
                y_pixel_size: 2.0,
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "FileNotFoundHandling",
            [FileNotFoundHandling::NoData, FileNotFoundHandling::Error],
        )
        .await;

        assert_sql_type(
            &pool,
            "GdalMetadataMapping",
            [GdalMetadataMapping {
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

        assert_sql_type(
            &pool,
            "StringPair",
            [StringPair::from(("foo".to_string(), "bar".to_string()))],
        )
        .await;

        assert_sql_type(
            &pool,
            "GdalDatasetParameters",
            [GdalDatasetParameters {
                file_path: "text".into(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: Coordinate2D::new(0.0f64, 0.5),
                    x_pixel_size: 1.0,
                    y_pixel_size: 2.0,
                },
                width: 42,
                height: 23,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: Some(42.0),
                properties_mapping: Some(vec![GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: None,
                        key: "foo".to_string(),
                    },
                    target_key: RasterPropertiesKey {
                        domain: Some("bar".to_string()),
                        key: "foo".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                }]),
                gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                gdal_config_options: Some(vec![("foo".to_string(), "bar".to_string())]),
                allow_alphaband_as_mask: false,
                retry: Some(GdalRetryOptions { max_retries: 3 }),
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "GdalMetaDataRegular",
            [GdalMetaDataRegular {
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeInterval::new_unchecked(0, 1).into(),
                    bbox: Some(
                        SpatialPartition2D::new(
                            Coordinate2D::new(0.0f64, 1.),
                            Coordinate2D::new(2., 0.5),
                        )
                        .unwrap(),
                    ),
                    resolution: Some(SpatialResolution::zero_point_one()),
                    bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                        "band".into(),
                        Measurement::Continuous(ContinuousMeasurement {
                            measurement: "Temperature".to_string(),
                            unit: Some("°C".to_string()),
                        }),
                    )])
                    .unwrap(),
                },
                params: GdalDatasetParameters {
                    file_path: "text".into(),
                    rasterband_channel: 1,
                    geo_transform: GdalDatasetGeoTransform {
                        origin_coordinate: Coordinate2D::new(0.0f64, 0.5),
                        x_pixel_size: 1.0,
                        y_pixel_size: 2.0,
                    },
                    width: 42,
                    height: 23,
                    file_not_found_handling: FileNotFoundHandling::NoData,
                    no_data_value: Some(42.0),
                    properties_mapping: Some(vec![GdalMetadataMapping {
                        source_key: RasterPropertiesKey {
                            domain: None,
                            key: "foo".to_string(),
                        },
                        target_key: RasterPropertiesKey {
                            domain: Some("bar".to_string()),
                            key: "foo".to_string(),
                        },
                        target_type: RasterPropertiesEntryType::String,
                    }]),
                    gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                    gdal_config_options: Some(vec![("foo".to_string(), "bar".to_string())]),
                    allow_alphaband_as_mask: false,
                    retry: Some(GdalRetryOptions { max_retries: 3 }),
                },
                time_placeholders: [(
                    "foo".to_string(),
                    GdalSourceTimePlaceholder {
                        format: geoengine_datatypes::primitives::DateTimeParseFormat::unix(),
                        reference: TimeReference::Start,
                    },
                )]
                .into(),
                data_time: TimeInterval::new_unchecked(0, 1),
                step: TimeStep {
                    granularity: TimeGranularity::Millis,
                    step: 1,
                },
                cache_ttl: CacheTtlSeconds::max(),
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "GdalMetaDataStatic",
            [GdalMetaDataStatic {
                time: Some(TimeInterval::new_unchecked(0, 1)),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeInterval::new_unchecked(0, 1).into(),
                    bbox: Some(
                        SpatialPartition2D::new(
                            Coordinate2D::new(0.0f64, 1.),
                            Coordinate2D::new(2., 0.5),
                        )
                        .unwrap(),
                    ),
                    resolution: Some(SpatialResolution::zero_point_one()),
                    bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                        "band".into(),
                        Measurement::Continuous(ContinuousMeasurement {
                            measurement: "Temperature".to_string(),
                            unit: Some("°C".to_string()),
                        }),
                    )])
                    .unwrap(),
                },
                params: GdalDatasetParameters {
                    file_path: "text".into(),
                    rasterband_channel: 1,
                    geo_transform: GdalDatasetGeoTransform {
                        origin_coordinate: Coordinate2D::new(0.0f64, 0.5),
                        x_pixel_size: 1.0,
                        y_pixel_size: 2.0,
                    },
                    width: 42,
                    height: 23,
                    file_not_found_handling: FileNotFoundHandling::NoData,
                    no_data_value: Some(42.0),
                    properties_mapping: Some(vec![GdalMetadataMapping {
                        source_key: RasterPropertiesKey {
                            domain: None,
                            key: "foo".to_string(),
                        },
                        target_key: RasterPropertiesKey {
                            domain: Some("bar".to_string()),
                            key: "foo".to_string(),
                        },
                        target_type: RasterPropertiesEntryType::String,
                    }]),
                    gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                    gdal_config_options: Some(vec![("foo".to_string(), "bar".to_string())]),
                    allow_alphaband_as_mask: false,
                    retry: Some(GdalRetryOptions { max_retries: 3 }),
                },
                cache_ttl: CacheTtlSeconds::max(),
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "GdalMetadataNetCdfCf",
            [GdalMetadataNetCdfCf {
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeInterval::new_unchecked(0, 1).into(),
                    bbox: Some(
                        SpatialPartition2D::new(
                            Coordinate2D::new(0.0f64, 1.),
                            Coordinate2D::new(2., 0.5),
                        )
                        .unwrap(),
                    ),
                    resolution: Some(SpatialResolution::zero_point_one()),
                    bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                        "band".into(),
                        Measurement::Continuous(ContinuousMeasurement {
                            measurement: "Temperature".to_string(),
                            unit: Some("°C".to_string()),
                        }),
                    )])
                    .unwrap(),
                },
                params: GdalDatasetParameters {
                    file_path: "text".into(),
                    rasterband_channel: 1,
                    geo_transform: GdalDatasetGeoTransform {
                        origin_coordinate: Coordinate2D::new(0.0f64, 0.5),
                        x_pixel_size: 1.0,
                        y_pixel_size: 2.0,
                    },
                    width: 42,
                    height: 23,
                    file_not_found_handling: FileNotFoundHandling::NoData,
                    no_data_value: Some(42.0),
                    properties_mapping: Some(vec![GdalMetadataMapping {
                        source_key: RasterPropertiesKey {
                            domain: None,
                            key: "foo".to_string(),
                        },
                        target_key: RasterPropertiesKey {
                            domain: Some("bar".to_string()),
                            key: "foo".to_string(),
                        },
                        target_type: RasterPropertiesEntryType::String,
                    }]),
                    gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                    gdal_config_options: Some(vec![("foo".to_string(), "bar".to_string())]),
                    allow_alphaband_as_mask: false,
                    retry: Some(GdalRetryOptions { max_retries: 3 }),
                },
                start: TimeInstance::from_millis(0).unwrap(),
                end: TimeInstance::from_millis(1000).unwrap(),
                cache_ttl: CacheTtlSeconds::max(),
                step: TimeStep {
                    granularity: TimeGranularity::Millis,
                    step: 1,
                },
                band_offset: 3,
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "GdalMetaDataList",
            [GdalMetaDataList {
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeInterval::new_unchecked(0, 1).into(),
                    bbox: Some(
                        SpatialPartition2D::new(
                            Coordinate2D::new(0.0f64, 1.),
                            Coordinate2D::new(2., 0.5),
                        )
                        .unwrap(),
                    ),
                    resolution: Some(SpatialResolution::zero_point_one()),
                    bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                        "band".into(),
                        Measurement::Continuous(ContinuousMeasurement {
                            measurement: "Temperature".to_string(),
                            unit: Some("°C".to_string()),
                        }),
                    )])
                    .unwrap(),
                },
                params: vec![GdalLoadingInfoTemporalSlice {
                    time: TimeInterval::new_unchecked(0, 1),
                    params: Some(GdalDatasetParameters {
                        file_path: "text".into(),
                        rasterband_channel: 1,
                        geo_transform: GdalDatasetGeoTransform {
                            origin_coordinate: Coordinate2D::new(0.0f64, 0.5),
                            x_pixel_size: 1.0,
                            y_pixel_size: 2.0,
                        },
                        width: 42,
                        height: 23,
                        file_not_found_handling: FileNotFoundHandling::NoData,
                        no_data_value: Some(42.0),
                        properties_mapping: Some(vec![GdalMetadataMapping {
                            source_key: RasterPropertiesKey {
                                domain: None,
                                key: "foo".to_string(),
                            },
                            target_key: RasterPropertiesKey {
                                domain: Some("bar".to_string()),
                                key: "foo".to_string(),
                            },
                            target_type: RasterPropertiesEntryType::String,
                        }]),
                        gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                        gdal_config_options: Some(vec![("foo".to_string(), "bar".to_string())]),
                        allow_alphaband_as_mask: false,
                        retry: Some(GdalRetryOptions { max_retries: 3 }),
                    }),
                    cache_ttl: CacheTtlSeconds::max(),
                }],
            }],
        )
        .await;

        assert_sql_type(
            &pool,
            "MetaDataDefinition",
            [
                MetaDataDefinition::MockMetaData(StaticMetaData::<
                    MockDatasetDataSourceLoadingInfo,
                    VectorResultDescriptor,
                    VectorQueryRectangle,
                > {
                    loading_info: MockDatasetDataSourceLoadingInfo {
                        points: vec![Coordinate2D::new(0.0f64, 0.5), Coordinate2D::new(2., 1.0)],
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
                                measurement: Measurement::Unitless,
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
                    },
                    phantom: PhantomData,
                }),
                MetaDataDefinition::OgrMetaData(StaticMetaData::<
                    OgrSourceDataset,
                    VectorResultDescriptor,
                    VectorQueryRectangle,
                > {
                    loading_info: OgrSourceDataset {
                        file_name: "test".into(),
                        layer_name: "test".to_string(),
                        data_type: Some(VectorDataType::MultiPoint),
                        time: OgrSourceDatasetTimeType::Start {
                            start_field: "start".to_string(),
                            start_format: OgrSourceTimeFormat::Auto,
                            duration: OgrSourceDurationSpec::Zero,
                        },
                        default_geometry: Some(TypedGeometry::MultiPoint(
                            MultiPoint::new(vec![
                                Coordinate2D::new(0.0f64, 0.5),
                                Coordinate2D::new(2., 1.0),
                            ])
                            .unwrap(),
                        )),
                        columns: Some(OgrSourceColumnSpec {
                            format_specifics: Some(FormatSpecifics::Csv {
                                header: CsvHeader::Auto,
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
                        }),
                        force_ogr_time_filter: false,
                        force_ogr_spatial_filter: true,
                        on_error: OgrSourceErrorSpec::Abort,
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
                                measurement: Measurement::Unitless,
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
                    },
                    phantom: PhantomData,
                }),
                MetaDataDefinition::GdalMetaDataRegular(GdalMetaDataRegular {
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        time: TimeInterval::new_unchecked(0, 1).into(),
                        bbox: Some(
                            SpatialPartition2D::new(
                                Coordinate2D::new(0.0f64, 1.),
                                Coordinate2D::new(2., 0.5),
                            )
                            .unwrap(),
                        ),
                        resolution: Some(SpatialResolution::zero_point_one()),
                        bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                            "band".into(),
                            Measurement::Continuous(ContinuousMeasurement {
                                measurement: "Temperature".to_string(),
                                unit: Some("°C".to_string()),
                            }),
                        )])
                        .unwrap(),
                    },
                    params: GdalDatasetParameters {
                        file_path: "text".into(),
                        rasterband_channel: 1,
                        geo_transform: GdalDatasetGeoTransform {
                            origin_coordinate: Coordinate2D::new(0.0f64, 0.5),
                            x_pixel_size: 1.0,
                            y_pixel_size: 2.0,
                        },
                        width: 42,
                        height: 23,
                        file_not_found_handling: FileNotFoundHandling::NoData,
                        no_data_value: Some(42.0),
                        properties_mapping: Some(vec![GdalMetadataMapping {
                            source_key: RasterPropertiesKey {
                                domain: None,
                                key: "foo".to_string(),
                            },
                            target_key: RasterPropertiesKey {
                                domain: Some("bar".to_string()),
                                key: "foo".to_string(),
                            },
                            target_type: RasterPropertiesEntryType::String,
                        }]),
                        gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                        gdal_config_options: Some(vec![("foo".to_string(), "bar".to_string())]),
                        allow_alphaband_as_mask: false,
                        retry: Some(GdalRetryOptions { max_retries: 3 }),
                    },
                    time_placeholders: [(
                        "foo".to_string(),
                        GdalSourceTimePlaceholder {
                            format: DateTimeParseFormat::unix(),
                            reference: TimeReference::Start,
                        },
                    )]
                    .into(),
                    data_time: TimeInterval::new_unchecked(0, 1),
                    step: TimeStep {
                        granularity: TimeGranularity::Millis,
                        step: 1,
                    },
                    cache_ttl: CacheTtlSeconds::max(),
                }),
                MetaDataDefinition::GdalStatic(GdalMetaDataStatic {
                    time: Some(TimeInterval::new_unchecked(0, 1)),
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        time: TimeInterval::new_unchecked(0, 1).into(),
                        bbox: Some(
                            SpatialPartition2D::new(
                                Coordinate2D::new(0.0f64, 1.),
                                Coordinate2D::new(2., 0.5),
                            )
                            .unwrap(),
                        ),
                        resolution: Some(SpatialResolution::zero_point_one()),
                        bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                            "band".into(),
                            Measurement::Continuous(ContinuousMeasurement {
                                measurement: "Temperature".to_string(),
                                unit: Some("°C".to_string()),
                            }),
                        )])
                        .unwrap(),
                    },
                    params: GdalDatasetParameters {
                        file_path: "text".into(),
                        rasterband_channel: 1,
                        geo_transform: GdalDatasetGeoTransform {
                            origin_coordinate: Coordinate2D::new(0.0f64, 0.5),
                            x_pixel_size: 1.0,
                            y_pixel_size: 2.0,
                        },
                        width: 42,
                        height: 23,
                        file_not_found_handling: FileNotFoundHandling::NoData,
                        no_data_value: Some(42.0),
                        properties_mapping: Some(vec![GdalMetadataMapping {
                            source_key: RasterPropertiesKey {
                                domain: None,
                                key: "foo".to_string(),
                            },
                            target_key: RasterPropertiesKey {
                                domain: Some("bar".to_string()),
                                key: "foo".to_string(),
                            },
                            target_type: RasterPropertiesEntryType::String,
                        }]),
                        gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                        gdal_config_options: Some(vec![("foo".to_string(), "bar".to_string())]),
                        allow_alphaband_as_mask: false,
                        retry: Some(GdalRetryOptions { max_retries: 3 }),
                    },
                    cache_ttl: CacheTtlSeconds::max(),
                }),
                MetaDataDefinition::GdalMetadataNetCdfCf(GdalMetadataNetCdfCf {
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        time: TimeInterval::new_unchecked(0, 1).into(),
                        bbox: Some(
                            SpatialPartition2D::new(
                                Coordinate2D::new(0.0f64, 1.),
                                Coordinate2D::new(2., 0.5),
                            )
                            .unwrap(),
                        ),
                        resolution: Some(SpatialResolution::zero_point_one()),
                        bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                            "band".into(),
                            Measurement::Continuous(ContinuousMeasurement {
                                measurement: "Temperature".to_string(),
                                unit: Some("°C".to_string()),
                            }),
                        )])
                        .unwrap(),
                    },
                    params: GdalDatasetParameters {
                        file_path: "text".into(),
                        rasterband_channel: 1,
                        geo_transform: GdalDatasetGeoTransform {
                            origin_coordinate: Coordinate2D::new(0.0f64, 0.5),
                            x_pixel_size: 1.0,
                            y_pixel_size: 2.0,
                        },
                        width: 42,
                        height: 23,
                        file_not_found_handling: FileNotFoundHandling::NoData,
                        no_data_value: Some(42.0),
                        properties_mapping: Some(vec![GdalMetadataMapping {
                            source_key: RasterPropertiesKey {
                                domain: None,
                                key: "foo".to_string(),
                            },
                            target_key: RasterPropertiesKey {
                                domain: Some("bar".to_string()),
                                key: "foo".to_string(),
                            },
                            target_type: RasterPropertiesEntryType::String,
                        }]),
                        gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                        gdal_config_options: Some(vec![("foo".to_string(), "bar".to_string())]),
                        allow_alphaband_as_mask: false,
                        retry: Some(GdalRetryOptions { max_retries: 3 }),
                    },
                    start: TimeInstance::from_millis(0).unwrap(),
                    end: TimeInstance::from_millis(1000).unwrap(),
                    cache_ttl: CacheTtlSeconds::max(),
                    step: TimeStep {
                        granularity: TimeGranularity::Millis,
                        step: 1,
                    },
                    band_offset: 3,
                }),
                MetaDataDefinition::GdalMetaDataList(GdalMetaDataList {
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        time: TimeInterval::new_unchecked(0, 1).into(),
                        bbox: Some(
                            SpatialPartition2D::new(
                                Coordinate2D::new(0.0f64, 1.),
                                Coordinate2D::new(2., 0.5),
                            )
                            .unwrap(),
                        ),
                        resolution: Some(SpatialResolution::zero_point_one()),
                        bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                            "band".into(),
                            Measurement::Continuous(ContinuousMeasurement {
                                measurement: "Temperature".to_string(),
                                unit: Some("°C".to_string()),
                            }),
                        )])
                        .unwrap(),
                    },
                    params: vec![GdalLoadingInfoTemporalSlice {
                        time: TimeInterval::new_unchecked(0, 1),
                        params: Some(GdalDatasetParameters {
                            file_path: "text".into(),
                            rasterband_channel: 1,
                            geo_transform: GdalDatasetGeoTransform {
                                origin_coordinate: Coordinate2D::new(0.0f64, 0.5),
                                x_pixel_size: 1.0,
                                y_pixel_size: 2.0,
                            },
                            width: 42,
                            height: 23,
                            file_not_found_handling: FileNotFoundHandling::NoData,
                            no_data_value: Some(42.0),
                            properties_mapping: Some(vec![GdalMetadataMapping {
                                source_key: RasterPropertiesKey {
                                    domain: None,
                                    key: "foo".to_string(),
                                },
                                target_key: RasterPropertiesKey {
                                    domain: Some("bar".to_string()),
                                    key: "foo".to_string(),
                                },
                                target_type: RasterPropertiesEntryType::String,
                            }]),
                            gdal_open_options: Some(vec!["foo".to_string(), "bar".to_string()]),
                            gdal_config_options: Some(vec![("foo".to_string(), "bar".to_string())]),
                            allow_alphaband_as_mask: false,
                            retry: None,
                        }),
                        cache_ttl: CacheTtlSeconds::max(),
                    }],
                }),
            ],
        )
        .await;

        assert_sql_type(
            &pool,
            "bytea",
            [U96::from(
                arr![u8; 13, 227, 191, 247, 123, 193, 214, 165, 185, 37, 101, 24],
            )],
        )
        .await;

        test_data_provider_definition_types(&pool).await;
    }

    #[test]
    fn test_postgres_config_translation() {
        let host = "localhost";
        let port = 8095;
        let ge_default = "geoengine";
        let schema = "geoengine";

        let db_config = config::Postgres {
            host: host.to_string(),
            port,
            database: ge_default.to_string(),
            schema: schema.to_string(),
            user: ge_default.to_string(),
            password: ge_default.to_string(),
            clear_database_on_start: false,
        };

        let pg_config = Config::try_from(db_config).unwrap();

        assert_eq!(ge_default, pg_config.get_user().unwrap());
        assert_eq!(
            <str as AsRef<[u8]>>::as_ref(ge_default).to_vec(),
            pg_config.get_password().unwrap()
        );
        assert_eq!(ge_default, pg_config.get_dbname().unwrap());
        assert_eq!(
            &format!("-c search_path={schema}"),
            pg_config.get_options().unwrap()
        );
        assert_eq!(vec![Host::Tcp(host.to_string())], pg_config.get_hosts());
        assert_eq!(vec![port], pg_config.get_ports());
    }

    #[allow(clippy::too_many_lines)]
    async fn test_data_provider_definition_types(
        pool: &PooledConnection<'_, PostgresConnectionManager<tokio_postgres::NoTls>>,
    ) {
        assert_sql_type(
            pool,
            "ArunaDataProviderDefinition",
            [ArunaDataProviderDefinition {
                id: DataProviderId::from_str("86a7f7ce-1bab-4ce9-a32b-172c0f958ee0").unwrap(),
                name: "NFDI".to_string(),
                description: "NFDI".to_string(),
                priority: Some(33),
                api_url: "http://test".to_string(),
                project_id: "project".to_string(),
                api_token: "api_token".to_string(),
                filter_label: "filter".to_string(),
                cache_ttl: CacheTtlSeconds::new(0),
            }],
        )
        .await;

        assert_sql_type(
            pool,
            "GbifDataProviderDefinition",
            [GbifDataProviderDefinition {
                name: "GBIF".to_string(),
                description: "GFBio".to_string(),
                priority: None,
                db_config: DatabaseConnectionConfig {
                    host: "testhost".to_string(),
                    port: 1234,
                    database: "testdb".to_string(),
                    schema: "testschema".to_string(),
                    user: "testuser".to_string(),
                    password: "testpass".to_string(),
                },
                cache_ttl: CacheTtlSeconds::new(0),
                autocomplete_timeout: 3,
                columns: GbifDataProvider::all_columns(),
            }],
        )
        .await;

        assert_sql_type(
            pool,
            "GfbioAbcdDataProviderDefinition",
            [GfbioAbcdDataProviderDefinition {
                name: "GFbio".to_string(),
                description: "GFBio".to_string(),
                priority: None,
                db_config: DatabaseConnectionConfig {
                    host: "testhost".to_string(),
                    port: 1234,
                    database: "testdb".to_string(),
                    schema: "testschema".to_string(),
                    user: "testuser".to_string(),
                    password: "testpass".to_string(),
                },
                cache_ttl: CacheTtlSeconds::new(0),
            }],
        )
        .await;

        assert_sql_type(
            pool,
            "GfbioCollectionsDataProviderDefinition",
            [GfbioCollectionsDataProviderDefinition {
                name: "GFbio".to_string(),
                description: "GFBio".to_string(),
                priority: None,
                collection_api_url: "http://testhost".try_into().unwrap(),
                collection_api_auth_token: "token".to_string(),
                abcd_db_config: DatabaseConnectionConfig {
                    host: "testhost".to_string(),
                    port: 1234,
                    database: "testdb".to_string(),
                    schema: "testschema".to_string(),
                    user: "testuser".to_string(),
                    password: "testpass".to_string(),
                },
                pangaea_url: "http://panaea".try_into().unwrap(),
                cache_ttl: CacheTtlSeconds::new(0),
            }],
        )
        .await;

        assert_sql_type(pool, "\"PropertyType\"[]", [Vec::<Property>::new()]).await;

        assert_sql_type(
            pool,
            "EbvPortalDataProviderDefinition",
            [EbvPortalDataProviderDefinition {
                name: "ebv".to_string(),
                description: "EBV".to_string(),
                priority: None,
                data: "a_path".into(),
                base_url: "http://base".try_into().unwrap(),
                overviews: "another_path".into(),
                cache_ttl: CacheTtlSeconds::new(0),
            }],
        )
        .await;

        assert_sql_type(
            pool,
            "NetCdfCfDataProviderDefinition",
            [NetCdfCfDataProviderDefinition {
                name: "netcdfcf".to_string(),
                description: "netcdfcf".to_string(),
                priority: Some(33),
                data: "a_path".into(),
                overviews: "another_path".into(),
                cache_ttl: CacheTtlSeconds::new(0),
            }],
        )
        .await;

        assert_sql_type(
            pool,
            "PangaeaDataProviderDefinition",
            [PangaeaDataProviderDefinition {
                name: "pangaea".to_string(),
                description: "pangaea".to_string(),
                priority: None,
                base_url: "http://base".try_into().unwrap(),
                cache_ttl: CacheTtlSeconds::new(0),
            }],
        )
        .await;

        assert_sql_type(
            pool,
            "DataProviderDefinition",
            [
                TypedDataProviderDefinition::ArunaDataProviderDefinition(
                    ArunaDataProviderDefinition {
                        id: DataProviderId::from_str("86a7f7ce-1bab-4ce9-a32b-172c0f958ee0")
                            .unwrap(),
                        name: "NFDI".to_string(),
                        description: "NFDI".to_string(),
                        priority: Some(33),
                        api_url: "http://test".to_string(),
                        project_id: "project".to_string(),
                        api_token: "api_token".to_string(),
                        filter_label: "filter".to_string(),
                        cache_ttl: CacheTtlSeconds::new(0),
                    },
                ),
                TypedDataProviderDefinition::GbifDataProviderDefinition(
                    GbifDataProviderDefinition {
                        name: "GBIF".to_string(),
                        description: "GFBio".to_string(),
                        priority: None,
                        db_config: DatabaseConnectionConfig {
                            host: "testhost".to_string(),
                            port: 1234,
                            database: "testdb".to_string(),
                            schema: "testschema".to_string(),
                            user: "testuser".to_string(),
                            password: "testpass".to_string(),
                        },
                        cache_ttl: CacheTtlSeconds::new(0),
                        autocomplete_timeout: 3,
                        columns: GbifDataProvider::all_columns(),
                    },
                ),
                TypedDataProviderDefinition::GfbioAbcdDataProviderDefinition(
                    GfbioAbcdDataProviderDefinition {
                        name: "GFbio".to_string(),
                        description: "GFBio".to_string(),
                        priority: None,
                        db_config: DatabaseConnectionConfig {
                            host: "testhost".to_string(),
                            port: 1234,
                            database: "testdb".to_string(),
                            schema: "testschema".to_string(),
                            user: "testuser".to_string(),
                            password: "testpass".to_string(),
                        },
                        cache_ttl: CacheTtlSeconds::new(0),
                    },
                ),
                TypedDataProviderDefinition::GfbioCollectionsDataProviderDefinition(
                    GfbioCollectionsDataProviderDefinition {
                        name: "GFbio".to_string(),
                        description: "GFBio".to_string(),
                        priority: None,
                        collection_api_url: "http://testhost".try_into().unwrap(),
                        collection_api_auth_token: "token".to_string(),
                        abcd_db_config: DatabaseConnectionConfig {
                            host: "testhost".to_string(),
                            port: 1234,
                            database: "testdb".to_string(),
                            schema: "testschema".to_string(),
                            user: "testuser".to_string(),
                            password: "testpass".to_string(),
                        },
                        pangaea_url: "http://panaea".try_into().unwrap(),
                        cache_ttl: CacheTtlSeconds::new(0),
                    },
                ),
                TypedDataProviderDefinition::EbvPortalDataProviderDefinition(
                    EbvPortalDataProviderDefinition {
                        name: "ebv".to_string(),
                        description: "ebv".to_string(),
                        priority: Some(33),
                        data: "a_path".into(),
                        base_url: "http://base".try_into().unwrap(),
                        overviews: "another_path".into(),
                        cache_ttl: CacheTtlSeconds::new(0),
                    },
                ),
                TypedDataProviderDefinition::NetCdfCfDataProviderDefinition(
                    NetCdfCfDataProviderDefinition {
                        name: "netcdfcf".to_string(),
                        description: "netcdfcf".to_string(),
                        priority: Some(33),
                        data: "a_path".into(),
                        overviews: "another_path".into(),
                        cache_ttl: CacheTtlSeconds::new(0),
                    },
                ),
                TypedDataProviderDefinition::PangaeaDataProviderDefinition(
                    PangaeaDataProviderDefinition {
                        name: "pangaea".to_string(),
                        description: "pangaea".to_string(),
                        priority: None,
                        base_url: "http://base".try_into().unwrap(),
                        cache_ttl: CacheTtlSeconds::new(0),
                    },
                ),
            ],
        )
        .await;
    }
}
