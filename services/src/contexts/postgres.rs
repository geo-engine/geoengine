use self::migrations::all_migrations;
use crate::api::model::services::Volume;
use crate::config::{get_config_element, Cache, Oidc, Quota};
use crate::contexts::{
    initialize_database, migrations, ApplicationContext, CurrentSchemaMigration, MigrationResult,
    QueryContextImpl, SessionId,
};
use crate::contexts::{ExecutionContextImpl, QuotaCheckerImpl};
use crate::contexts::{GeoEngineDb, SessionContext};
use crate::datasets::upload::Volumes;
use crate::datasets::DatasetName;
use crate::error::{self, Error, Result};
use crate::layers::add_from_directory::{
    add_datasets_from_directory, add_layer_collections_from_directory, add_layers_from_directory,
    add_providers_from_directory,
};
use crate::machine_learning::error::MachineLearningError;
use crate::machine_learning::name::MlModelName;
use crate::quota::{initialize_quota_tracking, QuotaTrackingFactory};
use crate::tasks::SimpleTaskManagerContext;
use crate::tasks::{TypedTaskManagerBackend, UserTaskManager};
use crate::users::OidcManager;
use crate::users::{UserAuth, UserSession};
use async_trait::async_trait;
use bb8_postgres::{
    bb8::Pool,
    bb8::PooledConnection,
    tokio_postgres::{tls::MakeTlsConnect, tls::TlsConnect, Config, Socket},
    PostgresConnectionManager,
};
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::util::test::TestDefault;
use geoengine_operators::cache::shared_cache::SharedCache;
use geoengine_operators::engine::ChunkByteSize;
use geoengine_operators::meta::quota::QuotaChecker;
use geoengine_operators::util::create_rayon_thread_pool;
use log::info;
use rayon::ThreadPool;
use snafu::ResultExt;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_postgres::error::SqlState;
use uuid::Uuid;

// TODO: do not report postgres error details to user

/// A contex with references to Postgres backends of the dbs. Automatically migrates schema on instantiation
#[derive(Clone)]
pub struct PostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    thread_pool: Arc<ThreadPool>,
    exe_ctx_tiling_spec: TilingSpecification,
    query_ctx_chunk_size: ChunkByteSize,
    task_manager: Arc<TypedTaskManagerBackend>,
    oidc_manager: OidcManager,
    quota: QuotaTrackingFactory,
    pub(crate) pool: Pool<PostgresConnectionManager<Tls>>,
    volumes: Volumes,
    tile_cache: Arc<SharedCache>,
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
        quota_config: Quota,
        oidc_db: OidcManager,
    ) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(config, tls);

        let pool = Pool::builder().build(pg_mgr).await?;

        Self::create_pro_database(pool.get().await?).await?;

        let db = PostgresDb::new(pool.clone(), UserSession::admin_session());
        let quota = initialize_quota_tracking(
            quota_config.mode,
            db,
            quota_config.increment_quota_buffer_size,
            quota_config.increment_quota_buffer_timeout_seconds,
        );

        Ok(PostgresContext {
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            oidc_manager: oidc_db,
            quota,
            pool,
            volumes: Default::default(),
            tile_cache: Arc::new(SharedCache::test_default()),
        })
    }

    #[allow(clippy::missing_panics_doc)]
    pub async fn new_with_oidc(
        config: Config,
        tls: Tls,
        oidc_db: OidcManager,
        cache_config: Cache,
        quota_config: Quota,
    ) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(config, tls);

        let pool = Pool::builder().build(pg_mgr).await?;

        Self::create_pro_database(pool.get().await?).await?;

        let db = PostgresDb::new(pool.clone(), UserSession::admin_session());
        let quota = initialize_quota_tracking(
            quota_config.mode,
            db,
            quota_config.increment_quota_buffer_size,
            quota_config.increment_quota_buffer_timeout_seconds,
        );

        Ok(PostgresContext {
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec: TestDefault::test_default(),
            query_ctx_chunk_size: TestDefault::test_default(),
            oidc_manager: oidc_db,
            quota,
            pool,
            volumes: Default::default(),
            tile_cache: Arc::new(
                SharedCache::new(cache_config.size_in_mb, cache_config.landing_zone_ratio)
                    .expect("tile cache creation should work because the config is valid"),
            ),
        })
    }

    // TODO: check if the datasets exist already and don't output warnings when skipping them
    #[allow(clippy::too_many_arguments, clippy::missing_panics_doc)]
    pub async fn new_with_data(
        config: Config,
        tls: Tls,
        dataset_defs_path: PathBuf,
        provider_defs_path: PathBuf,
        layer_defs_path: PathBuf,
        layer_collection_defs_path: PathBuf,
        exe_ctx_tiling_spec: TilingSpecification,
        query_ctx_chunk_size: ChunkByteSize,
        oidc_config: Oidc,
        cache_config: Cache,
        quota_config: Quota,
    ) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(config, tls);

        let pool = Pool::builder().build(pg_mgr).await?;

        let created_schema = Self::create_pro_database(pool.get().await?).await?;

        let db = PostgresDb::new(pool.clone(), UserSession::admin_session());
        let quota = initialize_quota_tracking(
            quota_config.mode,
            db,
            quota_config.increment_quota_buffer_size,
            quota_config.increment_quota_buffer_timeout_seconds,
        );

        let app_ctx = PostgresContext {
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            oidc_manager: OidcManager::from(oidc_config),
            quota,
            pool,
            volumes: Default::default(),
            tile_cache: Arc::new(
                SharedCache::new(cache_config.size_in_mb, cache_config.landing_zone_ratio)
                    .expect("tile cache creation should work because the config is valid"),
            ),
        };

        if created_schema {
            info!("Populating database with initial data...");

            let mut db = app_ctx.session_context(UserSession::admin_session()).db();

            add_layers_from_directory(&mut db, layer_defs_path).await;
            add_layer_collections_from_directory(&mut db, layer_collection_defs_path).await;

            add_datasets_from_directory(&mut db, dataset_defs_path).await;

            add_providers_from_directory(&mut db, provider_defs_path.clone()).await;
        }

        Ok(app_ctx)
    }

    #[allow(clippy::too_many_lines)]
    /// Creates the database schema. Returns true if the schema was created, false if it already existed.
    pub(crate) async fn create_pro_database(
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

    /// Clears the database if the Settings demand and the database properties allows it.
    pub(crate) async fn maybe_clear_database(
        conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
    ) -> Result<()> {
        let postgres_config = get_config_element::<crate::config::Postgres>()?;
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
}

enum DatabaseStatus {
    Unitialized,
    InitializedClearDatabase,
    InitializedKeepDatabase,
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
    type Session = UserSession;

    fn session_context(&self, session: Self::Session) -> Self::SessionContext {
        PostgresSessionContext {
            session,
            context: self.clone(),
        }
    }

    async fn session_by_id(&self, session_id: SessionId) -> Result<Self::Session> {
        self.user_session_by_id(session_id)
            .await
            .map_err(Box::new)
            .context(error::Unauthorized)
    }

    fn oidc_manager(&self) -> &OidcManager {
        &self.oidc_manager
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
    session: UserSession,
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
    type Session = UserSession;
    type GeoEngineDB = PostgresDb<Tls>;

    type TaskContext = SimpleTaskManagerContext;
    type TaskManager = UserTaskManager; // this does not persist across restarts
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<Self::GeoEngineDB>;

    fn db(&self) -> Self::GeoEngineDB {
        PostgresDb::new(self.context.pool.clone(), self.session.clone())
    }

    fn tasks(&self) -> Self::TaskManager {
        UserTaskManager::new(self.context.task_manager.clone(), self.session.clone())
    }

    fn query_context(&self, workflow: Uuid, computation: Uuid) -> Result<Self::QueryContext> {
        // TODO: load config only once

        Ok(QueryContextImpl::new_with_extensions(
            self.context.query_ctx_chunk_size,
            self.context.thread_pool.clone(),
            Some(self.context.tile_cache.clone()),
            Some(
                self.context
                    .quota
                    .create_quota_tracking(&self.session, workflow, computation),
            ),
            Some(Box::new(QuotaCheckerImpl { user_db: self.db() }) as QuotaChecker),
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
                path: if self.session.is_admin() {
                    Some(v.path.to_string_lossy().to_string())
                } else {
                    None
                },
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
    pub(crate) session: UserSession,
}

impl<Tls> PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(conn_pool: Pool<PostgresConnectionManager<Tls>>, session: UserSession) -> Self {
        Self { conn_pool, session }
    }

    /// Check whether the namepsace of the given dataset is allowed for insertion
    pub(crate) fn check_dataset_namespace(&self, id: &DatasetName) -> Result<()> {
        let is_ok = match &id.namespace {
            Some(namespace) => namespace.as_str() == self.session.user.id.to_string(),
            None => self.session.is_admin(),
        };

        if is_ok {
            Ok(())
        } else {
            Err(Error::InvalidDatasetIdNamespace)
        }
    }

    /// Check whether the namepsace of the given model is allowed for insertion
    pub(crate) fn check_ml_model_namespace(
        &self,
        name: &MlModelName,
    ) -> Result<(), MachineLearningError> {
        let is_ok = match &name.namespace {
            Some(namespace) => namespace.as_str() == self.session.user.id.to_string(),
            None => self.session.is_admin(),
        };

        if is_ok {
            Ok(())
        } else {
            Err(MachineLearningError::InvalidModelNamespace { name: name.clone() })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::QuotaTrackingMode;
    use crate::datasets::external::netcdfcf::NetCdfCfDataProviderDefinition;
    use crate::datasets::listing::{DatasetListOptions, DatasetListing, ProvenanceOutput};
    use crate::datasets::listing::{DatasetProvider, Provenance};
    use crate::datasets::storage::{DatasetStore, MetaDataDefinition};
    use crate::datasets::upload::{FileId, UploadId};
    use crate::datasets::upload::{FileUpload, Upload, UploadDb};
    use crate::datasets::{AddDataset, DatasetIdAndName};
    use crate::ge_context;
    use crate::layers::add_from_directory::UNSORTED_COLLECTION_ID;
    use crate::layers::layer::{
        AddLayer, AddLayerCollection, CollectionItem, LayerCollection, LayerCollectionListOptions,
        LayerCollectionListing, LayerListing, ProviderLayerCollectionId, ProviderLayerId,
    };
    use crate::layers::listing::{
        LayerCollectionId, LayerCollectionProvider, SearchParameters, SearchType,
    };
    use crate::layers::storage::{
        LayerDb, LayerProviderDb, LayerProviderListing, LayerProviderListingOptions,
        INTERNAL_PROVIDER_ID,
    };
    use crate::permissions::{Permission, PermissionDb, Role, RoleDescription, RoleId};
    use crate::projects::{
        CreateProject, LayerUpdate, LoadVersion, OrderBy, Plot, PlotUpdate, PointSymbology,
        ProjectDb, ProjectId, ProjectLayer, ProjectListOptions, ProjectListing, STRectangle,
        UpdateProject,
    };
    use crate::users::{OidcTokens, SessionTokenStore};
    use crate::users::{RoleDb, UserClaims, UserCredentials, UserDb, UserId, UserRegistration};
    use crate::util::tests::mock_oidc::{mock_refresh_server, MockRefreshServerConfig};
    use crate::util::tests::{admin_login, register_ndvi_workflow_helper, MockQuotaTracking};
    use crate::workflows::registry::WorkflowRegistry;
    use crate::workflows::workflow::Workflow;
    use bb8_postgres::tokio_postgres::NoTls;
    use futures::join;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::dataset::{DataProviderId, LayerId};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, Coordinate2D, DateTime, Duration, FeatureDataType, Measurement,
        RasterQueryRectangle, SpatialResolution, TimeGranularity, TimeInstance, TimeInterval,
        TimeStep, VectorQueryRectangle,
    };
    use geoengine_datatypes::primitives::{CacheTtlSeconds, ColumnSelection};
    use geoengine_datatypes::raster::RasterDataType;
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::Identifier;
    use geoengine_operators::engine::{
        MetaData, MetaDataProvider, MultipleRasterOrSingleVectorSource, PlotOperator,
        RasterBandDescriptors, RasterResultDescriptor, StaticMetaData, TypedOperator,
        TypedResultDescriptor, VectorColumnInfo, VectorOperator, VectorResultDescriptor,
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
    use httptest::Server;
    use oauth2::{AccessToken, RefreshToken};
    use openidconnect::SubjectIdentifier;
    use serde_json::json;
    use std::str::FromStr;

    #[ge_context::test]
    async fn test(app_ctx: PostgresContext<NoTls>) {
        anonymous(&app_ctx).await;

        let _user_id = user_reg_login(&app_ctx).await;

        let session = app_ctx
            .login(UserCredentials {
                email: "foo@example.com".into(),
                password: "secret123".into(),
            })
            .await
            .unwrap();

        create_projects(&app_ctx, &session).await;

        let projects = list_projects(&app_ctx, &session).await;

        set_session(&app_ctx, &projects).await;

        let project_id = projects[0].id;

        update_projects(&app_ctx, &session, project_id).await;

        add_permission(&app_ctx, &session, project_id).await;

        delete_project(&app_ctx, &session, project_id).await;
    }

    #[ge_context::test]
    async fn test_external(app_ctx: PostgresContext<NoTls>) {
        anonymous(&app_ctx).await;

        let session = external_user_login_twice(&app_ctx).await;

        create_projects(&app_ctx, &session).await;

        let projects = list_projects(&app_ctx, &session).await;

        set_session_external(&app_ctx, &projects).await;

        let project_id = projects[0].id;

        update_projects(&app_ctx, &session, project_id).await;

        add_permission(&app_ctx, &session, project_id).await;

        delete_project(&app_ctx, &session, project_id).await;
    }

    fn tokens_from_duration(duration: Duration) -> OidcTokens {
        OidcTokens {
            access: AccessToken::new("AccessToken".to_string()),
            refresh: None,
            expires_in: duration,
        }
    }

    async fn set_session(app_ctx: &PostgresContext<NoTls>, projects: &[ProjectListing]) {
        let credentials = UserCredentials {
            email: "foo@example.com".into(),
            password: "secret123".into(),
        };

        let session = app_ctx.login(credentials).await.unwrap();

        set_session_in_database(app_ctx, projects, session).await;
    }

    async fn set_session_external(app_ctx: &PostgresContext<NoTls>, projects: &[ProjectListing]) {
        let external_user_claims = UserClaims {
            external_id: SubjectIdentifier::new("Foo bar Id".into()),
            email: "foo@bar.de".into(),
            real_name: "Foo Bar".into(),
        };

        let session = app_ctx
            .login_external(
                external_user_claims,
                tokens_from_duration(Duration::minutes(10)),
            )
            .await
            .unwrap();

        set_session_in_database(app_ctx, projects, session).await;
    }

    async fn set_session_in_database(
        app_ctx: &PostgresContext<NoTls>,
        projects: &[ProjectListing],
        session: UserSession,
    ) {
        let db = app_ctx.session_context(session.clone()).db();

        db.set_session_project(projects[0].id).await.unwrap();

        assert_eq!(
            app_ctx.session_by_id(session.id).await.unwrap().project,
            Some(projects[0].id)
        );

        let rect = STRectangle::new_unchecked(SpatialReference::epsg_4326(), 0., 1., 2., 3., 1, 2);
        db.set_session_view(rect.clone()).await.unwrap();
        assert_eq!(
            app_ctx.session_by_id(session.id).await.unwrap().view,
            Some(rect)
        );
    }

    async fn delete_project(
        app_ctx: &PostgresContext<NoTls>,
        session: &UserSession,
        project_id: ProjectId,
    ) {
        let db = app_ctx.session_context(session.clone()).db();

        db.delete_project(project_id).await.unwrap();

        assert!(db.load_project(project_id).await.is_err());
    }

    async fn add_permission(
        app_ctx: &PostgresContext<NoTls>,
        session: &UserSession,
        project_id: ProjectId,
    ) {
        let db = app_ctx.session_context(session.clone()).db();

        assert!(db
            .has_permission(project_id, Permission::Owner)
            .await
            .unwrap());

        let user2 = app_ctx
            .register_user(UserRegistration {
                email: "user2@example.com".into(),
                password: "12345678".into(),
                real_name: "User2".into(),
            })
            .await
            .unwrap();

        let session2 = app_ctx
            .login(UserCredentials {
                email: "user2@example.com".into(),
                password: "12345678".into(),
            })
            .await
            .unwrap();

        let db2 = app_ctx.session_context(session2.clone()).db();
        assert!(!db2
            .has_permission(project_id, Permission::Owner)
            .await
            .unwrap());

        db.add_permission(user2.into(), project_id, Permission::Read)
            .await
            .unwrap();

        assert!(db2
            .has_permission(project_id, Permission::Read)
            .await
            .unwrap());
    }

    #[allow(clippy::too_many_lines)]
    async fn update_projects(
        app_ctx: &PostgresContext<NoTls>,
        session: &UserSession,
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
        session: &UserSession,
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

    async fn create_projects(app_ctx: &PostgresContext<NoTls>, session: &UserSession) {
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

    async fn user_reg_login(app_ctx: &PostgresContext<NoTls>) -> UserId {
        let user_registration = UserRegistration {
            email: "foo@example.com".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        };

        let user_id = app_ctx.register_user(user_registration).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@example.com".into(),
            password: "secret123".into(),
        };

        let session = app_ctx.login(credentials).await.unwrap();

        let db = app_ctx.session_context(session.clone()).db();

        app_ctx.session_by_id(session.id).await.unwrap();

        db.logout().await.unwrap();

        assert!(app_ctx.session_by_id(session.id).await.is_err());

        user_id
    }

    async fn external_user_login_twice(app_ctx: &PostgresContext<NoTls>) -> UserSession {
        let external_user_claims = UserClaims {
            external_id: SubjectIdentifier::new("Foo bar Id".into()),
            email: "foo@bar.de".into(),
            real_name: "Foo Bar".into(),
        };
        let duration = Duration::minutes(30);

        let login_result = app_ctx
            .login_external(external_user_claims.clone(), tokens_from_duration(duration))
            .await;
        assert!(login_result.is_ok());

        let session_1 = login_result.unwrap();
        let user_id = session_1.user.id; //TODO: Not a deterministic test.

        let db1 = app_ctx.session_context(session_1.clone()).db();

        assert!(session_1.user.email.is_some());
        assert_eq!(session_1.user.email.unwrap(), "foo@bar.de");
        assert!(session_1.user.real_name.is_some());
        assert_eq!(session_1.user.real_name.unwrap(), "Foo Bar");

        let expected_duration = session_1.created + duration;
        assert_eq!(session_1.valid_until, expected_duration);

        assert!(app_ctx.session_by_id(session_1.id).await.is_ok());

        assert!(db1.logout().await.is_ok());

        assert!(app_ctx.session_by_id(session_1.id).await.is_err());

        let duration = Duration::minutes(10);
        let login_result = app_ctx
            .login_external(external_user_claims.clone(), tokens_from_duration(duration))
            .await;
        assert!(login_result.is_ok());

        let session_2 = login_result.unwrap();
        let result = session_2.clone();

        assert!(session_2.user.email.is_some()); //TODO: Technically, user details could change for each login. For simplicity, this is not covered yet.
        assert_eq!(session_2.user.email.unwrap(), "foo@bar.de");
        assert!(session_2.user.real_name.is_some());
        assert_eq!(session_2.user.real_name.unwrap(), "Foo Bar");
        assert_eq!(session_2.user.id, user_id);

        let expected_duration = session_2.created + duration;
        assert_eq!(session_2.valid_until, expected_duration);

        assert!(app_ctx.session_by_id(session_2.id).await.is_ok());

        result
    }

    async fn anonymous(app_ctx: &PostgresContext<NoTls>) {
        let now: DateTime = chrono::offset::Utc::now().into();
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let then: DateTime = chrono::offset::Utc::now().into();

        assert!(session.created >= now && session.created <= then);
        assert!(session.valid_until > session.created);

        let session = app_ctx.session_by_id(session.id).await.unwrap();

        let db = app_ctx.session_context(session.clone()).db();

        db.logout().await.unwrap();

        assert!(app_ctx.session_by_id(session.id).await.is_err());
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

        let session = app_ctx.create_anonymous_session().await.unwrap();
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

        let session = app_ctx.create_anonymous_session().await.unwrap();

        let dataset_name = DatasetName::new(Some(session.user.id.to_string()), "my_dataset");

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

        let session = app_ctx.create_anonymous_session().await.unwrap();

        let db = app_ctx.session_context(session.clone()).db();

        db.create_upload(input.clone()).await.unwrap();

        let upload = db.load_upload(id).await.unwrap();

        assert_eq!(upload, input);
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_persists_layer_providers(app_ctx: PostgresContext<NoTls>) {
        let db = app_ctx.session_context(UserSession::admin_session()).db();

        let provider = NetCdfCfDataProviderDefinition {
            name: "netcdfcf".to_string(),
            description: "NetCdfCfProviderDefinition".to_string(),
            priority: Some(33),
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
                priority: 33,
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

        assert_eq!(datasets.items.len(), 5);
    }

    #[ge_context::test]
    async fn it_lists_only_permitted_datasets(app_ctx: PostgresContext<NoTls>) {
        let session1 = app_ctx.create_anonymous_session().await.unwrap();
        let session2 = app_ctx.create_anonymous_session().await.unwrap();

        let db1 = app_ctx.session_context(session1.clone()).db();
        let db2 = app_ctx.session_context(session2.clone()).db();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
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
            result_descriptor: descriptor.clone(),
            phantom: Default::default(),
        };

        let _id = db1.add_dataset(ds, meta.into()).await.unwrap();

        let list1 = db1
            .list_datasets(DatasetListOptions {
                filter: None,
                order: crate::datasets::listing::OrderBy::NameAsc,
                offset: 0,
                limit: 1,
                tags: None,
            })
            .await
            .unwrap();

        assert_eq!(list1.len(), 1);

        let list2 = db2
            .list_datasets(DatasetListOptions {
                filter: None,
                order: crate::datasets::listing::OrderBy::NameAsc,
                offset: 0,
                limit: 1,
                tags: None,
            })
            .await
            .unwrap();

        assert_eq!(list2.len(), 0);
    }

    #[ge_context::test]
    async fn it_shows_only_permitted_provenance(app_ctx: PostgresContext<NoTls>) {
        let session1 = app_ctx.create_anonymous_session().await.unwrap();
        let session2 = app_ctx.create_anonymous_session().await.unwrap();

        let db1 = app_ctx.session_context(session1.clone()).db();
        let db2 = app_ctx.session_context(session2.clone()).db();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
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
            result_descriptor: descriptor.clone(),
            phantom: Default::default(),
        };

        let id = db1.add_dataset(ds, meta.into()).await.unwrap().id;

        assert!(db1.load_provenance(&id).await.is_ok());

        assert!(db2.load_provenance(&id).await.is_err());
    }

    #[ge_context::test]
    async fn it_updates_permissions(app_ctx: PostgresContext<NoTls>) {
        let session1 = app_ctx.create_anonymous_session().await.unwrap();
        let session2 = app_ctx.create_anonymous_session().await.unwrap();

        let db1 = app_ctx.session_context(session1.clone()).db();
        let db2 = app_ctx.session_context(session2.clone()).db();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
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
            result_descriptor: descriptor.clone(),
            phantom: Default::default(),
        };

        let id = db1.add_dataset(ds, meta.into()).await.unwrap().id;

        assert!(db1.load_dataset(&id).await.is_ok());

        assert!(db2.load_dataset(&id).await.is_err());

        db1.add_permission(session2.user.id.into(), id, Permission::Read)
            .await
            .unwrap();

        assert!(db2.load_dataset(&id).await.is_ok());
    }

    #[ge_context::test]
    async fn it_uses_roles_for_permissions(app_ctx: PostgresContext<NoTls>) {
        let session1 = app_ctx.create_anonymous_session().await.unwrap();
        let session2 = app_ctx.create_anonymous_session().await.unwrap();

        let db1 = app_ctx.session_context(session1.clone()).db();
        let db2 = app_ctx.session_context(session2.clone()).db();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
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
            result_descriptor: descriptor.clone(),
            phantom: Default::default(),
        };

        let id = db1.add_dataset(ds, meta.into()).await.unwrap().id;

        assert!(db1.load_dataset(&id).await.is_ok());

        assert!(db2.load_dataset(&id).await.is_err());

        db1.add_permission(session2.user.id.into(), id, Permission::Read)
            .await
            .unwrap();

        assert!(db2.load_dataset(&id).await.is_ok());
    }

    #[ge_context::test]
    async fn it_secures_meta_data(app_ctx: PostgresContext<NoTls>) {
        let session1 = app_ctx.create_anonymous_session().await.unwrap();
        let session2 = app_ctx.create_anonymous_session().await.unwrap();

        let db1 = app_ctx.session_context(session1.clone()).db();
        let db2 = app_ctx.session_context(session2.clone()).db();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
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
            result_descriptor: descriptor.clone(),
            phantom: Default::default(),
        };

        let id = db1.add_dataset(ds, meta.into()).await.unwrap().id;

        let meta: geoengine_operators::util::Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = db1.meta_data(&id.into()).await;

        assert!(meta.is_ok());

        let meta: geoengine_operators::util::Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = db2.meta_data(&id.into()).await;

        assert!(meta.is_err());

        db1.add_permission(session2.user.id.into(), id, Permission::Read)
            .await
            .unwrap();

        let meta: geoengine_operators::util::Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = db2.meta_data(&id.into()).await;

        assert!(meta.is_ok());
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_loads_all_meta_data_types(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

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

    #[ge_context::test]
    async fn it_secures_uploads(app_ctx: PostgresContext<NoTls>) {
        let session1 = app_ctx.create_anonymous_session().await.unwrap();
        let session2 = app_ctx.create_anonymous_session().await.unwrap();

        let db1 = app_ctx.session_context(session1.clone()).db();
        let db2 = app_ctx.session_context(session2.clone()).db();

        let upload_id = UploadId::new();

        let upload = Upload {
            id: upload_id,
            files: vec![FileUpload {
                id: FileId::new(),
                name: "test.bin".to_owned(),
                byte_size: 1024,
            }],
        };

        db1.create_upload(upload).await.unwrap();

        assert!(db1.load_upload(upload_id).await.is_ok());

        assert!(db2.load_upload(upload_id).await.is_err());
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_collects_layers(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;

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
        let session = admin_login(&app_ctx).await;

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
    async fn it_searches_layers_with_permissions(app_ctx: PostgresContext<NoTls>) {
        let admin_session = admin_login(&app_ctx).await;
        let admin_layer_db = app_ctx.session_context(admin_session).db();

        let user_session = app_ctx.create_anonymous_session().await.unwrap();
        let user_layer_db = app_ctx.session_context(user_session.clone()).db();

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

        let root_collection_id = admin_layer_db.get_root_layer_collection_id().await.unwrap();

        let layer1 = admin_layer_db
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

        let collection1_id = admin_layer_db
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

        let layer2 = admin_layer_db
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

        let collection2_id = admin_layer_db
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

        let collection3_id = admin_layer_db
            .add_layer_collection(
                AddLayerCollection {
                    name: "Collection3".to_string(),
                    description: "Collection 3".to_string(),
                    properties: Default::default(),
                },
                &collection1_id,
            )
            .await
            .unwrap();

        let layer3 = admin_layer_db
            .add_layer(
                AddLayer {
                    name: "Layer3".to_string(),
                    description: "Layer 3".to_string(),
                    symbology: None,
                    workflow: workflow.clone(),
                    metadata: Default::default(),
                    properties: Default::default(),
                },
                &collection2_id,
            )
            .await
            .unwrap();

        // Grant user permissions for collection1, collection2, layer1 and layer2
        admin_layer_db
            .add_permission(
                user_session.user.id.into(),
                collection1_id.clone(),
                Permission::Read,
            )
            .await
            .unwrap();

        admin_layer_db
            .add_permission(
                user_session.user.id.into(),
                collection2_id.clone(),
                Permission::Read,
            )
            .await
            .unwrap();

        admin_layer_db
            .add_permission(
                user_session.user.id.into(),
                layer1.clone(),
                Permission::Read,
            )
            .await
            .unwrap();

        admin_layer_db
            .add_permission(
                user_session.user.id.into(),
                layer2.clone(),
                Permission::Read,
            )
            .await
            .unwrap();

        // Ensure admin sees everything we added
        let admin_root_collection_all = admin_layer_db
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
            admin_root_collection_all,
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
                            collection_id: collection3_id.clone(),
                        },
                        name: "Collection3".to_string(),
                        description: "Collection 3".to_string(),
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
                    CollectionItem::Layer(LayerListing {
                        id: ProviderLayerId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            layer_id: layer3.clone(),
                        },
                        name: "Layer3".to_string(),
                        description: "Layer 3".to_string(),
                        properties: vec![],
                    }),
                ],
                entry_label: None,
                properties: vec![],
            }
        );

        let root_collection_all = user_layer_db
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

        let root_collection_filtered = user_layer_db
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

        let collection1_all = user_layer_db
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

        let collection1_filtered_fulltext = user_layer_db
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

        let collection1_filtered_prefix = user_layer_db
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

        let collection1_filtered_prefix2 = user_layer_db
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
        let session = admin_login(&app_ctx).await;

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
    async fn it_autocompletes_layers_with_permissions(app_ctx: PostgresContext<NoTls>) {
        let admin_session = admin_login(&app_ctx).await;
        let admin_layer_db = app_ctx.session_context(admin_session).db();

        let user_session = app_ctx.create_anonymous_session().await.unwrap();
        let user_layer_db = app_ctx.session_context(user_session.clone()).db();

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

        let root_collection_id = admin_layer_db.get_root_layer_collection_id().await.unwrap();

        let layer1 = admin_layer_db
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

        let collection1_id = admin_layer_db
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

        let layer2 = admin_layer_db
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

        let collection2_id = admin_layer_db
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

        let _collection3_id = admin_layer_db
            .add_layer_collection(
                AddLayerCollection {
                    name: "Collection3".to_string(),
                    description: "Collection 3".to_string(),
                    properties: Default::default(),
                },
                &collection1_id,
            )
            .await
            .unwrap();

        let _layer3 = admin_layer_db
            .add_layer(
                AddLayer {
                    name: "Layer3".to_string(),
                    description: "Layer 3".to_string(),
                    symbology: None,
                    workflow: workflow.clone(),
                    metadata: Default::default(),
                    properties: Default::default(),
                },
                &collection2_id,
            )
            .await
            .unwrap();

        // Grant user permissions for collection1, collection2, layer1 and layer2
        admin_layer_db
            .add_permission(
                user_session.user.id.into(),
                collection1_id.clone(),
                Permission::Read,
            )
            .await
            .unwrap();

        admin_layer_db
            .add_permission(
                user_session.user.id.into(),
                collection2_id.clone(),
                Permission::Read,
            )
            .await
            .unwrap();

        admin_layer_db
            .add_permission(
                user_session.user.id.into(),
                layer1.clone(),
                Permission::Read,
            )
            .await
            .unwrap();

        admin_layer_db
            .add_permission(
                user_session.user.id.into(),
                layer2.clone(),
                Permission::Read,
            )
            .await
            .unwrap();

        // Ensure admin sees everything we added
        let admin_root_collection_all = admin_layer_db
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
            admin_root_collection_all,
            vec![
                "Collection1".to_string(),
                "Collection2".to_string(),
                "Collection3".to_string(),
                "Layer1".to_string(),
                "Layer2".to_string(),
                "Layer3".to_string(),
                "Unsorted".to_string(),
            ]
        );

        let root_collection_all = user_layer_db
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

        let root_collection_filtered = user_layer_db
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

        let collection1_all = user_layer_db
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

        let collection1_filtered_fulltext = user_layer_db
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

        let collection1_filtered_prefix = user_layer_db
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

        let collection1_filtered_prefix2 = user_layer_db
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
        let session = admin_login(&app_ctx).await;

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

    #[ge_context::test]
    async fn it_tracks_used_quota_in_postgres(app_ctx: PostgresContext<NoTls>) {
        let _user = app_ctx
            .register_user(UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret1234".to_string(),
                real_name: "Foo Bar".to_string(),
            })
            .await
            .unwrap();

        let session = app_ctx
            .login(UserCredentials {
                email: "foo@example.com".to_string(),
                password: "secret1234".to_string(),
            })
            .await
            .unwrap();

        let admin_session = admin_login(&app_ctx).await;

        let quota = initialize_quota_tracking(
            QuotaTrackingMode::Check,
            app_ctx.session_context(admin_session).db(),
            0,
            60,
        );

        let tracking = quota.create_quota_tracking(&session, Uuid::new_v4(), Uuid::new_v4());

        tracking.mock_work_unit_done();
        tracking.mock_work_unit_done();

        let db = app_ctx.session_context(session).db();

        // wait for quota to be recorded
        let mut success = false;
        for _ in 0..10 {
            let used = db.quota_used().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            if used == 2 {
                success = true;
                break;
            }
        }

        assert!(success);
    }

    #[ge_context::test]
    async fn it_tracks_available_quota(app_ctx: PostgresContext<NoTls>) {
        let user = app_ctx
            .register_user(UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret1234".to_string(),
                real_name: "Foo Bar".to_string(),
            })
            .await
            .unwrap();

        let session = app_ctx
            .login(UserCredentials {
                email: "foo@example.com".to_string(),
                password: "secret1234".to_string(),
            })
            .await
            .unwrap();

        let admin_session = admin_login(&app_ctx).await;

        app_ctx
            .session_context(admin_session.clone())
            .db()
            .update_quota_available_by_user(&user, 1)
            .await
            .unwrap();

        let quota = initialize_quota_tracking(
            QuotaTrackingMode::Check,
            app_ctx.session_context(admin_session).db(),
            0,
            60,
        );

        let tracking = quota.create_quota_tracking(&session, Uuid::new_v4(), Uuid::new_v4());

        tracking.mock_work_unit_done();
        tracking.mock_work_unit_done();

        let db = app_ctx.session_context(session).db();

        // wait for quota to be recorded
        let mut success = false;
        for _ in 0..10 {
            let available = db.quota_available().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            if available == -1 {
                success = true;
                break;
            }
        }

        assert!(success);
    }

    #[ge_context::test]
    async fn it_updates_quota_in_postgres(app_ctx: PostgresContext<NoTls>) {
        let user = app_ctx
            .register_user(UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret1234".to_string(),
                real_name: "Foo Bar".to_string(),
            })
            .await
            .unwrap();

        let session = app_ctx
            .login(UserCredentials {
                email: "foo@example.com".to_string(),
                password: "secret1234".to_string(),
            })
            .await
            .unwrap();

        let db = app_ctx.session_context(session.clone()).db();
        let admin_db = app_ctx.session_context(UserSession::admin_session()).db();

        assert_eq!(
            db.quota_available().await.unwrap(),
            crate::config::get_config_element::<crate::config::Quota>()
                .unwrap()
                .initial_credits
        );

        assert_eq!(
            admin_db.quota_available_by_user(&user).await.unwrap(),
            crate::config::get_config_element::<crate::config::Quota>()
                .unwrap()
                .initial_credits
        );

        admin_db
            .update_quota_available_by_user(&user, 123)
            .await
            .unwrap();

        assert_eq!(db.quota_available().await.unwrap(), 123);

        assert_eq!(admin_db.quota_available_by_user(&user).await.unwrap(), 123);
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_removes_layer_collections(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;

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
        let session = admin_login(&app_ctx).await;

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
        let session = admin_login(&app_ctx).await;

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

        db.load_layer(&layer_in_two_collections).await.unwrap();

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

        let session = app_ctx.create_anonymous_session().await.unwrap();

        let dataset_name = DatasetName::new(Some(session.user.id.to_string()), "my_dataset");

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

        let session = admin_login(&app_ctx).await;

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
        let session = admin_login(&app_ctx).await;
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
    async fn it_restricts_layer_permissions(app_ctx: PostgresContext<NoTls>) {
        let admin_session = admin_login(&app_ctx).await;
        let session1 = app_ctx.create_anonymous_session().await.unwrap();

        let admin_db = app_ctx.session_context(admin_session.clone()).db();
        let db1 = app_ctx.session_context(session1.clone()).db();

        let root = admin_db.get_root_layer_collection_id().await.unwrap();

        // add new collection as admin
        let new_collection_id = admin_db
            .add_layer_collection(
                AddLayerCollection {
                    name: "admin collection".to_string(),
                    description: String::new(),
                    properties: Default::default(),
                },
                &root,
            )
            .await
            .unwrap();

        // load as regular user, not visible
        let collection = db1
            .load_layer_collection(
                &root,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                },
            )
            .await
            .unwrap();
        assert!(!collection.items.iter().any(|c| match c {
            CollectionItem::Collection(c) => c.id.collection_id == new_collection_id,
            CollectionItem::Layer(_) => false,
        }));

        // give user read permission
        admin_db
            .add_permission(
                session1.user.id.into(),
                new_collection_id.clone(),
                Permission::Read,
            )
            .await
            .unwrap();

        // now visible
        let collection = db1
            .load_layer_collection(
                &root,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                },
            )
            .await
            .unwrap();

        assert!(collection.items.iter().any(|c| match c {
            CollectionItem::Collection(c) => c.id.collection_id == new_collection_id,
            CollectionItem::Layer(_) => false,
        }));
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_handles_user_roles(app_ctx: PostgresContext<NoTls>) {
        let admin_session = admin_login(&app_ctx).await;
        let user_id = app_ctx
            .register_user(UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
                real_name: "Foo Bar".to_string(),
            })
            .await
            .unwrap();

        let admin_db = app_ctx.session_context(admin_session.clone()).db();

        // create a new role
        let role_id = admin_db.add_role("foo").await.unwrap();

        let user_session = app_ctx
            .login(UserCredentials {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        // user does not have the role yet

        assert!(!user_session.roles.contains(&role_id));

        //user can query their role descriptions (user role and registered user)
        assert_eq!(user_session.roles.len(), 2);

        let expected_user_role_description = RoleDescription {
            role: Role {
                id: RoleId::from(user_id),
                name: "foo@example.com".to_string(),
            },
            individual: true,
        };
        let expected_registered_role_description = RoleDescription {
            role: Role {
                id: Role::registered_user_role_id(),
                name: "user".to_string(),
            },
            individual: false,
        };

        let user_role_descriptions = app_ctx
            .session_context(user_session.clone())
            .db()
            .get_role_descriptions(&user_id)
            .await
            .unwrap();
        assert_eq!(
            vec![
                expected_user_role_description.clone(),
                expected_registered_role_description.clone(),
            ],
            user_role_descriptions
        );

        // we assign the role to the user
        admin_db.assign_role(&role_id, &user_id).await.unwrap();

        let user_session = app_ctx
            .login(UserCredentials {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        // should be present now
        assert!(user_session.roles.contains(&role_id));

        //user can query their role descriptions (now an additional foo role)
        let expected_foo_role_description = RoleDescription {
            role: Role {
                id: role_id,
                name: "foo".to_string(),
            },
            individual: false,
        };

        let user_role_descriptions = app_ctx
            .session_context(user_session.clone())
            .db()
            .get_role_descriptions(&user_id)
            .await
            .unwrap();
        assert_eq!(
            vec![
                expected_foo_role_description,
                expected_user_role_description.clone(),
                expected_registered_role_description.clone(),
            ],
            user_role_descriptions
        );

        // we revoke it
        admin_db.revoke_role(&role_id, &user_id).await.unwrap();

        let user_session = app_ctx
            .login(UserCredentials {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        // the role is gone now
        assert!(!user_session.roles.contains(&role_id));

        //user can query their role descriptions (user role and registered user)
        let user_role_descriptions = app_ctx
            .session_context(user_session.clone())
            .db()
            .get_role_descriptions(&user_id)
            .await
            .unwrap();
        assert_eq!(
            vec![
                expected_user_role_description.clone(),
                expected_registered_role_description.clone(),
            ],
            user_role_descriptions
        );

        // assign it again and then delete the whole role, should not be present at user

        admin_db.assign_role(&role_id, &user_id).await.unwrap();

        admin_db.remove_role(&role_id).await.unwrap();

        let user_session = app_ctx
            .login(UserCredentials {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        assert!(!user_session.roles.contains(&role_id));

        //user can query their role descriptions (user role and registered user)
        let user_role_descriptions = app_ctx
            .session_context(user_session.clone())
            .db()
            .get_role_descriptions(&user_id)
            .await
            .unwrap();
        assert_eq!(
            vec![
                expected_user_role_description,
                expected_registered_role_description.clone(),
            ],
            user_role_descriptions
        );
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_updates_project_layer_symbology(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

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
        let admin_session = UserSession::admin_session();
        let db = app_ctx.session_context(admin_session.clone()).db();

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

        let DatasetIdAndName {
            id: dataset_id2,
            name: dataset_name2,
        } = db
            .add_dataset(
                AddDataset {
                    name: Some(DatasetName::new(
                        Some(admin_session.user.id.to_string()),
                        "my_dataset".to_owned(),
                    )),
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

        assert_eq!(
            db.resolve_dataset_name_to_id(&dataset_name1)
                .await
                .unwrap()
                .unwrap(),
            dataset_id1
        );
        assert_eq!(
            db.resolve_dataset_name_to_id(&dataset_name2)
                .await
                .unwrap()
                .unwrap(),
            dataset_id2
        );
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_bulk_updates_quota(app_ctx: PostgresContext<NoTls>) {
        let admin_session = UserSession::admin_session();
        let db = app_ctx.session_context(admin_session.clone()).db();

        let user1 = app_ctx
            .register_user(UserRegistration {
                email: "user1@example.com".into(),
                password: "12345678".into(),
                real_name: "User1".into(),
            })
            .await
            .unwrap();

        let user2 = app_ctx
            .register_user(UserRegistration {
                email: "user2@example.com".into(),
                password: "12345678".into(),
                real_name: "User2".into(),
            })
            .await
            .unwrap();

        // single item in bulk
        db.bulk_increment_quota_used([(user1, 1)]).await.unwrap();

        assert_eq!(db.quota_used_by_user(&user1).await.unwrap(), 1);

        // multiple items in bulk
        db.bulk_increment_quota_used([(user1, 1), (user2, 3)])
            .await
            .unwrap();

        assert_eq!(db.quota_used_by_user(&user1).await.unwrap(), 2);
        assert_eq!(db.quota_used_by_user(&user2).await.unwrap(), 3);
    }

    async fn it_handles_oidc_tokens(app_ctx: PostgresContext<NoTls>) {
        let external_user_claims = UserClaims {
            external_id: SubjectIdentifier::new("Foo bar Id".into()),
            email: "foo@bar.de".into(),
            real_name: "Foo Bar".into(),
        };
        let tokens = OidcTokens {
            access: AccessToken::new("FIRST_ACCESS_TOKEN".into()),
            refresh: Some(RefreshToken::new("FIRST_REFRESH_TOKEN".into())),
            expires_in: Duration::seconds(2),
        };

        let login_result = app_ctx
            .login_external(external_user_claims.clone(), tokens)
            .await;
        assert!(login_result.is_ok());

        let session_id = login_result.unwrap().id;

        let access_token = app_ctx.get_access_token(session_id).await.unwrap();

        assert_eq!(
            "FIRST_ACCESS_TOKEN".to_string(),
            access_token.secret().to_owned()
        );

        //Access token duration oidc_login_refresh is 2 sec, i.e., session times out after 2 sec.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let access_token = app_ctx.get_access_token(session_id).await.unwrap();

        assert_eq!(
            "SECOND_ACCESS_TOKEN".to_string(),
            access_token.secret().to_owned()
        );
    }

    pub fn oidc_only_refresh() -> (Server, impl Fn() -> OidcManager) {
        let mock_refresh_server_config = MockRefreshServerConfig {
            expected_discoveries: 1,
            token_duration: std::time::Duration::from_secs(2),
            creates_first_token: false,
            first_access_token: "FIRST_ACCESS_TOKEN".to_string(),
            first_refresh_token: "FIRST_REFRESH_TOKEN".to_string(),
            second_access_token: "SECOND_ACCESS_TOKEN".to_string(),
            second_refresh_token: "SECOND_REFRESH_TOKEN".to_string(),
            client_side_password: None,
        };

        let (server, oidc_manager) = mock_refresh_server(mock_refresh_server_config);

        (server, move || {
            OidcManager::from_oidc_with_static_tokens(oidc_manager.clone())
        })
    }

    #[ge_context::test(oidc_db = "oidc_only_refresh")]
    async fn it_handles_oidc_tokens_without_encryption(app_ctx: PostgresContext<NoTls>) {
        it_handles_oidc_tokens(app_ctx).await;
    }

    pub fn oidc_only_refresh_with_encryption() -> (Server, impl Fn() -> OidcManager) {
        let mock_refresh_server_config = MockRefreshServerConfig {
            expected_discoveries: 1,
            token_duration: std::time::Duration::from_secs(2),
            creates_first_token: false,
            first_access_token: "FIRST_ACCESS_TOKEN".to_string(),
            first_refresh_token: "FIRST_REFRESH_TOKEN".to_string(),
            second_access_token: "SECOND_ACCESS_TOKEN".to_string(),
            second_refresh_token: "SECOND_REFRESH_TOKEN".to_string(),
            client_side_password: Some("password123".to_string()),
        };

        let (server, oidc_manager) = mock_refresh_server(mock_refresh_server_config);

        (server, move || {
            OidcManager::from_oidc_with_static_tokens(oidc_manager.clone())
        })
    }

    #[ge_context::test(oidc_db = "oidc_only_refresh_with_encryption")]
    async fn it_handles_oidc_tokens_with_encryption(app_ctx: PostgresContext<NoTls>) {
        it_handles_oidc_tokens(app_ctx).await;
    }
}
