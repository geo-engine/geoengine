use super::{ExecutionContextImpl, ProApplicationContext, ProGeoEngineDb, QuotaCheckerImpl};
use crate::api::model::datatypes::DatasetName;
use crate::contexts::{ApplicationContext, PostgresContext, QueryContextImpl, SessionId};
use crate::contexts::{GeoEngineDb, SessionContext};
use crate::datasets::add_from_directory::add_providers_from_directory;
use crate::datasets::upload::{Volume, Volumes};
use crate::error::{self, Error, Result};
use crate::layers::add_from_directory::UNSORTED_COLLECTION_ID;
use crate::layers::storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID;
use crate::pro::datasets::add_datasets_from_directory;
use crate::pro::layers::add_from_directory::{
    add_layer_collections_from_directory, add_layers_from_directory,
    add_pro_providers_from_directory,
};
use crate::pro::permissions::Role;
use crate::pro::quota::{initialize_quota_tracking, QuotaTrackingFactory};
use crate::pro::tasks::{ProTaskManager, ProTaskManagerBackend};
use crate::pro::users::{OidcRequestDb, UserAuth, UserSession};
use crate::pro::util::config::{Cache, Oidc, Quota};
use crate::tasks::SimpleTaskManagerContext;
use crate::util::config::get_config_element;
use async_trait::async_trait;
use bb8_postgres::{
    bb8::Pool,
    bb8::PooledConnection,
    tokio_postgres::{tls::MakeTlsConnect, tls::TlsConnect, Config, Socket},
    PostgresConnectionManager,
};
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::util::test::TestDefault;
use geoengine_datatypes::util::Identifier;
use geoengine_operators::engine::{ChunkByteSize, QueryContextExtensions};
use geoengine_operators::pro::cache::shared_cache::SharedCache;
use geoengine_operators::pro::meta::quota::{ComputationContext, QuotaChecker};
use geoengine_operators::util::create_rayon_thread_pool;
use log::{debug, info};
use pwhash::bcrypt;
use rayon::ThreadPool;
use snafu::{ensure, ResultExt};
use std::path::PathBuf;
use std::sync::Arc;

// TODO: do not report postgres error details to user

/// A contex with references to Postgres backends of the dbs. Automatically migrates schema on instantiation
#[derive(Clone)]
pub struct ProPostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    thread_pool: Arc<ThreadPool>,
    exe_ctx_tiling_spec: TilingSpecification,
    query_ctx_chunk_size: ChunkByteSize,
    task_manager: Arc<ProTaskManagerBackend>,
    oidc_request_db: Arc<Option<OidcRequestDb>>,
    quota: QuotaTrackingFactory,
    pub(crate) pool: Pool<PostgresConnectionManager<Tls>>,
    volumes: Volumes,
    tile_cache: Arc<SharedCache>,
}

impl<Tls> ProPostgresContext<Tls>
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
        quota_config: Quota,
    ) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(config, tls);

        let pool = Pool::builder().build(pg_mgr).await?;

        let created_schema = PostgresContext::create_schema(pool.get().await?).await?;
        if created_schema {
            Self::create_pro_schema(pool.get().await?).await?;
        }

        let db = ProPostgresDb::new(pool.clone(), UserSession::admin_session());
        let quota = initialize_quota_tracking(
            quota_config.mode,
            db,
            quota_config.increment_quota_buffer_size,
            quota_config.increment_quota_buffer_timeout_seconds,
        );

        Ok(ProPostgresContext {
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            oidc_request_db: Arc::new(None),
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
        oidc_db: OidcRequestDb,
        cache_config: Cache,
        quota_config: Quota,
    ) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(config, tls);

        let pool = Pool::builder().build(pg_mgr).await?;

        let created_schema = PostgresContext::create_schema(pool.get().await?).await?;
        if created_schema {
            Self::create_pro_schema(pool.get().await?).await?;
        }

        let db = ProPostgresDb::new(pool.clone(), UserSession::admin_session());
        let quota = initialize_quota_tracking(
            quota_config.mode,
            db,
            quota_config.increment_quota_buffer_size,
            quota_config.increment_quota_buffer_timeout_seconds,
        );

        Ok(ProPostgresContext {
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec: TestDefault::test_default(),
            query_ctx_chunk_size: TestDefault::test_default(),
            oidc_request_db: Arc::new(Some(oidc_db)),
            quota,
            pool,
            volumes: Default::default(),
            tile_cache: Arc::new(
                SharedCache::new(
                    cache_config.cache_size_in_mb,
                    cache_config.landing_zone_ratio,
                )
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

        let created_schema = PostgresContext::create_schema(pool.get().await?).await?;
        if created_schema {
            Self::create_pro_schema(pool.get().await?).await?;
        }

        let db = ProPostgresDb::new(pool.clone(), UserSession::admin_session());
        let quota = initialize_quota_tracking(
            quota_config.mode,
            db,
            quota_config.increment_quota_buffer_size,
            quota_config.increment_quota_buffer_timeout_seconds,
        );

        let app_ctx = ProPostgresContext {
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            oidc_request_db: Arc::new(OidcRequestDb::try_from(oidc_config).ok()),
            quota,
            pool,
            volumes: Default::default(),
            tile_cache: Arc::new(
                SharedCache::new(
                    cache_config.cache_size_in_mb,
                    cache_config.landing_zone_ratio,
                )
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

            add_pro_providers_from_directory(&mut db, provider_defs_path.join("pro")).await;
        }

        Ok(app_ctx)
    }

    #[allow(clippy::too_many_lines)]
    /// Creates the database schema. Returns true if the schema was created, false if it already existed.
    pub(crate) async fn create_pro_schema(
        mut conn: PooledConnection<'_, PostgresConnectionManager<Tls>>,
    ) -> Result<()> {
        let user_config = get_config_element::<crate::pro::util::config::User>()?;

        let tx = conn.build_transaction().start().await?;

        tx.batch_execute(include_str!("schema.sql")).await?;

        let stmt = tx
            .prepare(
                r#"
            INSERT INTO roles (id, name) VALUES
                ($1, 'admin'),
                ($2, 'user'),
                ($3, 'anonymous');"#,
            )
            .await?;

        tx.execute(
            &stmt,
            &[
                &Role::admin_role_id(),
                &Role::registered_user_role_id(),
                &Role::anonymous_role_id(),
            ],
        )
        .await?;

        let stmt = tx
            .prepare(
                r#"
            INSERT INTO users (
                id, 
                email,
                password_hash,
                real_name,
                active)
            VALUES (
                $1, 
                $2,
                $3,
                'admin',
                true
            );"#,
            )
            .await?;

        tx.execute(
            &stmt,
            &[
                &Role::admin_role_id(),
                &user_config.admin_email,
                &bcrypt::hash(user_config.admin_password)
                    .expect("Admin password hash should be valid"),
            ],
        )
        .await?;

        let stmt = tx
            .prepare(
                r#"
            INSERT INTO user_roles 
                (user_id, role_id)
            VALUES 
                ($1, $1);"#,
            )
            .await?;

        tx.execute(&stmt, &[&Role::admin_role_id()]).await?;

        let stmt = tx
            .prepare(
                r#"
            INSERT INTO permissions
             (role_id, layer_collection_id, permission)  
            VALUES 
                ($1, $4, 'Owner'),
                ($2, $4, 'Read'),
                ($3, $4, 'Read'),
                ($1, $5, 'Owner'),
                ($2, $5, 'Read'),
                ($3, $5, 'Read');"#,
            )
            .await?;

        tx.execute(
            &stmt,
            &[
                &Role::admin_role_id(),
                &Role::registered_user_role_id(),
                &Role::anonymous_role_id(),
                &INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
                &UNSORTED_COLLECTION_ID,
            ],
        )
        .await?;

        tx.commit().await?;

        debug!("Created pro database schema");

        Ok(())
    }

    pub fn oidc_request_db(&self) -> Arc<Option<OidcRequestDb>> {
        self.oidc_request_db.clone()
    }
}

#[async_trait]
impl<Tls> ApplicationContext for ProPostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
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
}

#[async_trait]
impl<Tls> ProApplicationContext for ProPostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn oidc_request_db(&self) -> Option<&OidcRequestDb> {
        self.oidc_request_db.as_ref().as_ref()
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
    context: ProPostgresContext<Tls>,
}

#[async_trait]
impl<Tls> SessionContext for PostgresSessionContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Session = UserSession;
    type GeoEngineDB = ProPostgresDb<Tls>;

    type TaskContext = SimpleTaskManagerContext;
    type TaskManager = ProTaskManager; // this does not persist across restarts
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<Self::GeoEngineDB>;

    fn db(&self) -> Self::GeoEngineDB {
        ProPostgresDb::new(self.context.pool.clone(), self.session.clone())
    }

    fn tasks(&self) -> Self::TaskManager {
        ProTaskManager::new(self.context.task_manager.clone(), self.session.clone())
    }

    fn query_context(&self) -> Result<Self::QueryContext> {
        // TODO: load config only once

        let mut extensions = QueryContextExtensions::default();
        extensions.insert(
            self.context
                .quota
                .create_quota_tracking(&self.session, ComputationContext::new()),
        );
        extensions.insert(Box::new(QuotaCheckerImpl { user_db: self.db() }) as QuotaChecker);
        extensions.insert(self.context.tile_cache.clone());

        Ok(QueryContextImpl::new_with_extensions(
            self.context.query_ctx_chunk_size,
            self.context.thread_pool.clone(),
            extensions,
        ))
    }

    fn execution_context(&self) -> Result<Self::ExecutionContext> {
        Ok(ExecutionContextImpl::<ProPostgresDb<Tls>>::new(
            self.db(),
            self.context.thread_pool.clone(),
            self.context.exe_ctx_tiling_spec,
        ))
    }

    fn volumes(&self) -> Result<Vec<Volume>> {
        ensure!(self.session.is_admin(), error::PermissionDenied);

        Ok(self.context.volumes.volumes.clone())
    }

    fn session(&self) -> &Self::Session {
        &self.session
    }
}

pub struct ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub(crate) conn_pool: Pool<PostgresConnectionManager<Tls>>,
    pub(crate) session: UserSession,
}

impl<Tls> ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(conn_pool: Pool<PostgresConnectionManager<Tls>>, session: UserSession) -> Self {
        Self { conn_pool, session }
    }

    /// Check whether the namepsace of the given dataset is allowed for insertion
    pub(crate) fn check_namespace(&self, id: &DatasetName) -> Result<()> {
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
}

impl<Tls> GeoEngineDb for ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
}

impl<Tls> ProGeoEngineDb for ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::api::model::datatypes::{DataProviderId, DatasetName, LayerId};
    use crate::api::model::responses::datasets::DatasetIdAndName;
    use crate::api::model::services::AddDataset;
    use crate::datasets::external::netcdfcf::NetCdfCfDataProviderDefinition;
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
    use crate::pro::permissions::{Permission, PermissionDb, RoleDescription, RoleId};
    use crate::pro::users::{
        ExternalUserClaims, RoleDb, UserCredentials, UserDb, UserId, UserRegistration,
    };
    use crate::pro::util::config::QuotaTrackingMode;
    use crate::pro::util::tests::with_pro_temp_context;
    use crate::pro::util::tests::{admin_login, register_ndvi_workflow_helper};
    use crate::projects::{
        CreateProject, LayerUpdate, LoadVersion, OrderBy, Plot, PlotUpdate, PointSymbology,
        ProjectDb, ProjectFilter, ProjectId, ProjectLayer, ProjectListOptions, ProjectListing,
        STRectangle, UpdateProject,
    };

    use crate::workflows::registry::WorkflowRegistry;
    use crate::workflows::workflow::Workflow;

    use bb8_postgres::tokio_postgres::NoTls;
    use futures::join;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::primitives::CacheTtlSeconds;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, Coordinate2D, DateTime, Duration, FeatureDataType, Measurement,
        RasterQueryRectangle, SpatialResolution, TimeGranularity, TimeInstance, TimeInterval,
        TimeStep, VectorQueryRectangle,
    };
    use geoengine_datatypes::raster::RasterDataType;
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::Identifier;
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
    use openidconnect::SubjectIdentifier;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test() {
        with_pro_temp_context(|app_ctx, _| async move {
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
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_external() {
        with_pro_temp_context(|app_ctx, _| async move {
            anonymous(&app_ctx).await;

            let session = external_user_login_twice(&app_ctx).await;

            create_projects(&app_ctx, &session).await;

            let projects = list_projects(&app_ctx, &session).await;

            set_session_external(&app_ctx, &projects).await;

            let project_id = projects[0].id;

            update_projects(&app_ctx, &session, project_id).await;

            add_permission(&app_ctx, &session, project_id).await;

            delete_project(&app_ctx, &session, project_id).await;
        })
        .await;
    }

    async fn set_session(app_ctx: &ProPostgresContext<NoTls>, projects: &[ProjectListing]) {
        let credentials = UserCredentials {
            email: "foo@example.com".into(),
            password: "secret123".into(),
        };

        let session = app_ctx.login(credentials).await.unwrap();

        set_session_in_database(app_ctx, projects, session).await;
    }

    async fn set_session_external(
        app_ctx: &ProPostgresContext<NoTls>,
        projects: &[ProjectListing],
    ) {
        let external_user_claims = ExternalUserClaims {
            external_id: SubjectIdentifier::new("Foo bar Id".into()),
            email: "foo@bar.de".into(),
            real_name: "Foo Bar".into(),
        };

        let session = app_ctx
            .login_external(external_user_claims, Duration::minutes(10))
            .await
            .unwrap();

        set_session_in_database(app_ctx, projects, session).await;
    }

    async fn set_session_in_database(
        app_ctx: &ProPostgresContext<NoTls>,
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
        app_ctx: &ProPostgresContext<NoTls>,
        session: &UserSession,
        project_id: ProjectId,
    ) {
        let db = app_ctx.session_context(session.clone()).db();

        db.delete_project(project_id).await.unwrap();

        assert!(db.load_project(project_id).await.is_err());
    }

    async fn add_permission(
        app_ctx: &ProPostgresContext<NoTls>,
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
        app_ctx: &ProPostgresContext<NoTls>,
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
        app_ctx: &ProPostgresContext<NoTls>,
        session: &UserSession,
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

    async fn create_projects(app_ctx: &ProPostgresContext<NoTls>, session: &UserSession) {
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

    async fn user_reg_login(app_ctx: &ProPostgresContext<NoTls>) -> UserId {
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

    //TODO: No duplicate tests for postgres and hashmap implementation possible?
    async fn external_user_login_twice(app_ctx: &ProPostgresContext<NoTls>) -> UserSession {
        let external_user_claims = ExternalUserClaims {
            external_id: SubjectIdentifier::new("Foo bar Id".into()),
            email: "foo@bar.de".into(),
            real_name: "Foo Bar".into(),
        };
        let duration = Duration::minutes(30);

        //NEW
        let login_result = app_ctx
            .login_external(external_user_claims.clone(), duration)
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
            .login_external(external_user_claims.clone(), duration)
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

    async fn anonymous(app_ctx: &ProPostgresContext<NoTls>) {
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_persists_workflows() {
        with_pro_temp_context(|app_ctx, _pg_config| async move {
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
        with_pro_temp_context(|app_ctx, _| async move {
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
                                measurement: Measurement::Unitless
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
        with_pro_temp_context(|app_ctx, _| async move {
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
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_persists_layer_providers() {
        with_pro_temp_context(|app_ctx, _| async move {
            let db = app_ctx.session_context(UserSession::admin_session()).db();

            let provider = NetCdfCfDataProviderDefinition {
                name: "netcdfcf".to_string(),
                path: test_data!("netcdf4d/").into(),
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
                    description: "NetCdfCfProviderDefinition".to_owned(),
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

            assert_eq!(datasets.items.len(), 3);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_lists_only_permitted_datasets() {
        with_pro_temp_context(|app_ctx, _| async move {
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

            let meta = db1.wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let _id = db1.add_dataset(ds, meta).await.unwrap();

            let list1 = db1
                .list_datasets(DatasetListOptions {
                    filter: None,
                    order: crate::datasets::listing::OrderBy::NameAsc,
                    offset: 0,
                    limit: 1,
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
                })
                .await
                .unwrap();

            assert_eq!(list2.len(), 0);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_shows_only_permitted_provenance() {
        with_pro_temp_context(|app_ctx, _| async move {
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

            let meta = db1.wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let id = db1.add_dataset(ds, meta).await.unwrap().id;

            assert!(db1.load_provenance(&id).await.is_ok());

            assert!(db2.load_provenance(&id).await.is_err());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_updates_permissions() {
        with_pro_temp_context(|app_ctx, _| async move {
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

            let meta = db1.wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let id = db1.add_dataset(ds, meta).await.unwrap().id;

            assert!(db1.load_dataset(&id).await.is_ok());

            assert!(db2.load_dataset(&id).await.is_err());

            db1.add_permission(session2.user.id.into(), id, Permission::Read)
                .await
                .unwrap();

            assert!(db2.load_dataset(&id).await.is_ok());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_uses_roles_for_permissions() {
        with_pro_temp_context(|app_ctx, _| async move {
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

            let meta = db1.wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let id = db1.add_dataset(ds, meta).await.unwrap().id;

            assert!(db1.load_dataset(&id).await.is_ok());

            assert!(db2.load_dataset(&id).await.is_err());

            db1.add_permission(session2.user.id.into(), id, Permission::Read)
                .await
                .unwrap();

            assert!(db2.load_dataset(&id).await.is_ok());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_secures_meta_data() {
        with_pro_temp_context(|app_ctx, _| async move {
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

            let meta = db1.wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let id = db1.add_dataset(ds, meta).await.unwrap().id;

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
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_loads_all_meta_data_types() {
        with_pro_temp_context(|app_ctx, _| async move {
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_secures_uploads() {
        with_pro_temp_context(|app_ctx, _| async move {
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
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_collects_layers() {
        with_pro_temp_context(|app_ctx, _| async move {
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_tracks_used_quota_in_postgres() {
        with_pro_temp_context(|app_ctx, _| async move {
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

            let tracking = quota.create_quota_tracking(&session, ComputationContext::new());

            tracking.work_unit_done();
            tracking.work_unit_done();

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
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_tracks_available_quota() {
        with_pro_temp_context(|app_ctx, _| async move {
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

            let tracking = quota.create_quota_tracking(&session, ComputationContext::new());

            tracking.work_unit_done();
            tracking.work_unit_done();

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
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_updates_quota_in_postgres() {
        with_pro_temp_context(|app_ctx, _| async move {
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
                crate::util::config::get_config_element::<crate::pro::util::config::Quota>()
                    .unwrap()
                    .default_available_quota
            );

            assert_eq!(
                admin_db.quota_available_by_user(&user).await.unwrap(),
                crate::util::config::get_config_element::<crate::pro::util::config::Quota>()
                    .unwrap()
                    .default_available_quota
            );

            admin_db
                .update_quota_available_by_user(&user, 123)
                .await
                .unwrap();

            assert_eq!(db.quota_available().await.unwrap(), 123);

            assert_eq!(admin_db.quota_available_by_user(&user).await.unwrap(), 123);
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_removes_layer_collections() {
        with_pro_temp_context(|app_ctx, _| async move {
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
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn it_removes_collections_from_collections() {
        with_pro_temp_context(|app_ctx, _| async move {
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
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn it_removes_layers_from_collections() {
        with_pro_temp_context(|app_ctx, _| async move {
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
        with_pro_temp_context(|app_ctx, _| async move {
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
        with_pro_temp_context(|app_ctx, _| async move {
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
        with_pro_temp_context(|app_ctx, _| async move {
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
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_restricts_layer_permissions() {
        with_pro_temp_context(|app_ctx, _| async move {
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

            // add new layer in the collection as user, fails because only read permission
            let result = db1
                .add_layer_collection(
                    AddLayerCollection {
                        name: "user layer".to_string(),
                        description: String::new(),
                        properties: Default::default(),
                    },
                    &new_collection_id,
                )
                .await;

            assert!(result.is_err());

            // give user owner permission
            admin_db
                .add_permission(
                    session1.user.id.into(),
                    new_collection_id.clone(),
                    Permission::Owner,
                )
                .await
                .unwrap();

            // add now works
            db1.add_layer_collection(
                AddLayerCollection {
                    name: "user layer".to_string(),
                    description: String::new(),
                    properties: Default::default(),
                },
                &new_collection_id,
            )
            .await
            .unwrap();

            // remove permissions again
            admin_db
                .remove_permission(
                    session1.user.id.into(),
                    new_collection_id.clone(),
                    Permission::Read,
                )
                .await
                .unwrap();
            admin_db
                .remove_permission(
                    session1.user.id.into(),
                    new_collection_id.clone(),
                    Permission::Owner,
                )
                .await
                .unwrap();

            // access is gone now
            let result = db1
                .add_layer_collection(
                    AddLayerCollection {
                        name: "user layer".to_string(),
                        description: String::new(),
                        properties: Default::default(),
                    },
                    &root,
                )
                .await;

            assert!(result.is_err());

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
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_handles_user_roles() {
        with_pro_temp_context(|app_ctx, _| async move {
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
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_updates_project_layer_symbology() {
        with_pro_temp_context(|app_ctx, _| async move {
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
        with_pro_temp_context(|app_ctx, _| async move {
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
                    },
                    db.wrap_meta_data(meta_data.clone()),
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
                    },
                    db.wrap_meta_data(meta_data),
                )
                .await
                .unwrap();

            assert_eq!(
                db.resolve_dataset_name_to_id(&dataset_name1).await.unwrap(),
                dataset_id1
            );
            assert_eq!(
                db.resolve_dataset_name_to_id(&dataset_name2).await.unwrap(),
                dataset_id2
            );
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn it_bulk_updates_quota() {
        with_pro_temp_context(|app_ctx, _| async move {
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
        })
        .await;
    }
}
