use crate::contexts::{ApplicationContext, QueryContextImpl, SessionId};
use crate::contexts::{GeoEngineDb, SessionContext};
use crate::datasets::add_from_directory::add_providers_from_directory;
use crate::datasets::upload::{Volume, Volumes};
use crate::error::{self, Result};
use crate::layers::add_from_directory::UNSORTED_COLLECTION_ID;
use crate::layers::storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID;
use crate::pro::datasets::add_datasets_from_directory;
use crate::pro::layers::add_from_directory::{
    add_layer_collections_from_directory, add_layers_from_directory,
};
use crate::pro::permissions::Role;
use crate::pro::quota::{initialize_quota_tracking, QuotaTrackingFactory};
use crate::pro::tasks::{ProTaskManager, ProTaskManagerBackend};
use crate::pro::users::{OidcRequestDb, UserAuth, UserSession};
use crate::pro::util::config::Oidc;

use crate::tasks::SimpleTaskManagerContext;
use crate::util::config::get_config_element;
use async_trait::async_trait;
use bb8_postgres::{
    bb8::Pool,
    bb8::PooledConnection,
    tokio_postgres::{error::SqlState, tls::MakeTlsConnect, tls::TlsConnect, Config, Socket},
    PostgresConnectionManager,
};
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::util::Identifier;
use geoengine_operators::engine::{ChunkByteSize, QueryContextExtensions};
use geoengine_operators::pro::meta::quota::{ComputationContext, QuotaChecker};
use geoengine_operators::util::create_rayon_thread_pool;
use log::{debug, warn};
use postgres_protocol::escape::escape_literal;
use pwhash::bcrypt;
use rayon::ThreadPool;
use snafu::{ensure, ResultExt};
use std::path::PathBuf;
use std::sync::Arc;

use super::{ExecutionContextImpl, ProApplicationContext, ProGeoEngineDb, QuotaCheckerImpl};

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
    task_manager: Arc<ProTaskManagerBackend>,
    oidc_request_db: Arc<Option<OidcRequestDb>>,
    quota: QuotaTrackingFactory,
    pub(crate) pool: Pool<PostgresConnectionManager<Tls>>,
    volumes: Volumes,
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
        Self::update_schema(pool.get().await?).await?;

        let db = PostgresDb::new(pool.clone(), UserSession::admin_session());
        let quota = initialize_quota_tracking(db);

        Ok(PostgresContext {
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            oidc_request_db: Arc::new(None),
            quota,
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
        oidc_config: Oidc,
    ) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(config, tls);

        let pool = Pool::builder().build(pg_mgr).await?;
        Self::update_schema(pool.get().await?).await?;

        let db = PostgresDb::new(pool.clone(), UserSession::admin_session());
        let quota = initialize_quota_tracking(db);

        let app_ctx = PostgresContext {
            task_manager: Default::default(),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            oidc_request_db: Arc::new(OidcRequestDb::try_from(oidc_config).ok()),
            quota,
            pool,
            volumes: Default::default(),
        };

        let mut db = app_ctx.session_context(UserSession::admin_session()).db();

        add_layers_from_directory(&mut db, layer_defs_path).await;
        add_layer_collections_from_directory(&mut db, layer_collection_defs_path).await;

        add_datasets_from_directory(&mut db, dataset_defs_path).await;

        add_providers_from_directory(
            &mut db,
            provider_defs_path.clone(),
            &[provider_defs_path.join("pro")],
        )
        .await;

        Ok(app_ctx)
    }

    async fn schema_version(
        conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
    ) -> Result<i32> {
        let stmt = match conn.prepare("SELECT version from version").await {
            Ok(stmt) => stmt,
            Err(e) => {
                if let Some(code) = e.code() {
                    if *code == SqlState::UNDEFINED_TABLE {
                        warn!("UserDB: Uninitialized schema");
                        return Ok(0);
                    }
                }
                return Err(error::Error::TokioPostgres { source: e });
            }
        };

        let row = conn.query_one(&stmt, &[]).await?;

        Ok(row.get(0))
    }

    #[allow(clippy::too_many_lines)]
    async fn update_schema(
        conn: PooledConnection<'_, PostgresConnectionManager<Tls>>,
    ) -> Result<()> {
        let mut version = Self::schema_version(&conn).await?;

        loop {
            match version {
                0 => {
                    let user_config = get_config_element::<crate::pro::util::config::User>()?;

                    conn.batch_execute(
                        &format!(r#"
                        CREATE TABLE version (
                            version INT
                        );
                        INSERT INTO version VALUES (1);

                        -- TODO: distinguish between roles that are (correspond to) users and roles that are not
                        -- TODO: integrity constraint for roles that correspond to users + DELETE CASCADE
                        CREATE TABLE roles (
                            id UUID PRIMARY KEY,
                            name text UNIQUE NOT NULL
                        );

                        INSERT INTO roles (id, name) VALUES
                            ({admin_role_id}, 'admin'),
                            ({user_role_id}, 'user'),
                            ({anonymous_role_id}, 'anonymous');

                        CREATE TABLE users (
                            id UUID PRIMARY KEY REFERENCES roles(id),
                            email character varying (256) UNIQUE,
                            password_hash character varying (256),
                            real_name character varying (256),
                            active boolean NOT NULL,
                            quota_available bigint NOT NULL DEFAULT 0,
                            quota_used bigint NOT NULL DEFAULT 0, -- TODO: rename to total_quota_used?
                            CONSTRAINT users_anonymous_ck CHECK (
                               (email IS NULL AND password_hash IS NULL AND real_name IS NULL) OR 
                               (email IS NOT NULL AND password_hash IS NOT NULL AND 
                                real_name IS NOT NULL) 
                            ),
                            CONSTRAINT users_quota_used_ck CHECK (quota_used >= 0)
                        );

                        INSERT INTO users (
                            id, 
                            email,
                            password_hash,
                            real_name,
                            active)
                        VALUES (
                            {admin_role_id}, 
                            {admin_email},
                            {admin_password},
                            'admin',
                            true
                        );

                        -- relation between users and roles
                        -- all users have a default role where role_id = user_id
                        CREATE TABLE user_roles (
                            user_id UUID REFERENCES users(id) ON DELETE CASCADE NOT NULL,
                            role_id UUID REFERENCES roles(id) ON DELETE CASCADE NOT NULL,
                            PRIMARY KEY (user_id, role_id)
                        );

                        -- admin user role
                        INSERT INTO user_roles 
                            (user_id, role_id)
                        VALUES 
                            ({admin_role_id}, 
                            {admin_role_id});

                        CREATE TYPE "SpatialReferenceAuthority" AS ENUM (
                            'Epsg', 'SrOrg', 'Iau2000', 'Esri'
                        );

                        CREATE TYPE "SpatialReference" AS (
                            authority "SpatialReferenceAuthority", 
                            code OID
                        );

                        CREATE TYPE "Coordinate2D" AS (
                            x double precision, 
                            y double precision
                        );

                        CREATE TYPE "BoundingBox2D" AS (
                            lower_left_coordinate "Coordinate2D", 
                            upper_right_coordinate "Coordinate2D"
                        );

                        CREATE TYPE "TimeInterval" AS (                                                      
                            start timestamp with time zone,
                            "end" timestamp with time zone
                        );

                        CREATE TYPE "STRectangle" AS (
                            spatial_reference "SpatialReference",
                            bounding_box "BoundingBox2D",
                            time_interval "TimeInterval"
                        );
                        
                        CREATE TYPE "TimeGranularity" AS ENUM (
                            'Millis', 'Seconds', 'Minutes', 'Hours',
                            'Days',  'Months', 'Years'
                        );
                        
                        CREATE TYPE "TimeStep" AS (
                            granularity "TimeGranularity",
                            step OID
                        );

                        CREATE TABLE projects (
                            id UUID PRIMARY KEY
                        );        
                        
                        CREATE TABLE sessions (
                            id UUID PRIMARY KEY,
                            user_id UUID REFERENCES users(id),
                            created timestamp with time zone NOT NULL,
                            valid_until timestamp with time zone NOT NULL,
                            project_id UUID REFERENCES projects(id) ON DELETE SET NULL,
                            view "STRectangle"
                        );                

                        CREATE TABLE project_versions (
                            id UUID PRIMARY KEY,
                            project_id UUID REFERENCES projects(id) ON DELETE CASCADE NOT NULL,
                            name character varying (256) NOT NULL,
                            description text NOT NULL,
                            bounds "STRectangle" NOT NULL,
                            time_step "TimeStep" NOT NULL,
                            changed timestamp with time zone,
                            author_user_id UUID REFERENCES users(id) NOT NULL,
                            latest boolean
                        );

                        CREATE INDEX project_version_latest_idx 
                        ON project_versions (project_id, latest DESC, changed DESC, author_user_id DESC);

                        CREATE TYPE "LayerType" AS ENUM ('Raster', 'Vector');
                        
                        CREATE TYPE "LayerVisibility" AS (
                            data BOOLEAN,
                            legend BOOLEAN
                        );

                        CREATE TABLE project_version_layers (
                            layer_index integer NOT NULL,
                            project_id UUID REFERENCES projects(id) ON DELETE CASCADE NOT NULL,
                            project_version_id UUID REFERENCES project_versions(id) ON DELETE CASCADE NOT NULL,                            
                            name character varying (256) NOT NULL,
                            workflow_id UUID NOT NULL, -- TODO: REFERENCES workflows(id)
                            symbology json,
                            visibility "LayerVisibility" NOT NULL,
                            PRIMARY KEY (project_id, project_version_id, layer_index)            
                        );
                        
                        CREATE TABLE project_version_plots (
                            plot_index integer NOT NULL,
                            project_id UUID REFERENCES projects(id) ON DELETE CASCADE NOT NULL,
                            project_version_id UUID REFERENCES project_versions(id) ON DELETE CASCADE NOT NULL,                            
                            name character varying (256) NOT NULL,
                            workflow_id UUID NOT NULL, -- TODO: REFERENCES workflows(id)
                            PRIMARY KEY (project_id, project_version_id, plot_index)            
                        );

                        CREATE TABLE workflows (
                            id UUID PRIMARY KEY,
                            workflow json NOT NULL
                        );

                        CREATE TABLE datasets (
                            id UUID PRIMARY KEY,
                            name text NOT NULL,
                            description text NOT NULL, 
                            tags text[], 
                            source_operator text NOT NULL,

                            result_descriptor json NOT NULL,
                            meta_data json NOT NULL,

                            symbology json,
                            provenance json
                        );

                        -- TODO: add constraint not null
                        -- TODO: add constaint byte_size >= 0
                        CREATE TYPE "FileUpload" AS (
                            id UUID,
                            name text,
                            byte_size bigint
                        );

                        -- TODO: store user
                        -- TODO: time of creation and last update
                        -- TODO: upload directory that is not directly derived from id
                        CREATE TABLE uploads (
                            id UUID PRIMARY KEY,
                            user_id UUID REFERENCES users(id) ON DELETE CASCADE NOT NULL,
                            files "FileUpload"[] NOT NULL
                        );

                        CREATE TYPE "Permission" AS ENUM (
                            'Read', 'Owner'
                        );  

                        CREATE TABLE layer_collections (
                            id UUID PRIMARY KEY,
                            name text NOT NULL,
                            description text NOT NULL
                        );

                        -- insert the root layer collection
                        INSERT INTO layer_collections (
                            id,
                            name,
                            description
                        ) VALUES (
                            {root_layer_collection_id},
                            'Layers',
                            'All available Geo Engine layers'
                        );

                        -- insert the unsorted layer collection
                        INSERT INTO layer_collections (
                            id,
                            name,
                            description
                        ) VALUES (
                            {unsorted_layer_collection_id},
                            'Unsorted',
                            'Unsorted Layers'
                        );
    

                        CREATE TABLE layers (
                            id UUID PRIMARY KEY,
                            name text NOT NULL,
                            description text NOT NULL,
                            workflow json NOT NULL,
                            symbology json 
                        );

                        CREATE TABLE collection_layers (
                            collection UUID REFERENCES layer_collections(id) ON DELETE CASCADE NOT NULL,
                            layer UUID REFERENCES layers(id) ON DELETE CASCADE NOT NULL,
                            PRIMARY KEY (collection, layer)
                        );

                        CREATE TABLE collection_children (
                            parent UUID REFERENCES layer_collections(id) ON DELETE CASCADE NOT NULL,
                            child UUID REFERENCES layer_collections(id) ON DELETE CASCADE NOT NULL,
                            PRIMARY KEY (parent, child)
                        );

                        -- add unsorted layers to root layer collection
                        INSERT INTO collection_children (parent, child) VALUES
                        ({root_layer_collection_id}, {unsorted_layer_collection_id});

                        -- TODO: should name be unique (per user)?
                        CREATE TABLE layer_providers (
                            id UUID PRIMARY KEY,
                            type_name text NOT NULL,
                            name text NOT NULL,

                            definition json NOT NULL
                        );

                        -- TODO: uploads, providers permissions

                        -- TODO: relationship between uploads and datasets?

                        CREATE TABLE external_users (
                            id UUID PRIMARY KEY REFERENCES users(id),
                            external_id character varying (256) UNIQUE,
                            email character varying (256),
                            real_name character varying (256),
                            active boolean NOT NULL
                        );

                        CREATE TABLE permissions (
                            -- resource_type "ResourceType" NOT NULL,
                            role_id UUID REFERENCES roles(id) ON DELETE CASCADE NOT NULL,
                            permission "Permission" NOT NULL,
                            dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE,
                            layer_id UUID REFERENCES layers(id) ON DELETE CASCADE,
                            layer_collection_id UUID REFERENCES layer_collections(id) ON DELETE CASCADE,
                            project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
                            check(
                                (
                                    (dataset_id is not null)::integer +
                                    (layer_id is not null)::integer +
                                    (layer_collection_id is not null)::integer +
                                    (project_id is not null)::integer 
                                ) = 1
                            )
                        );

                        CREATE UNIQUE INDEX ON permissions (role_id, permission, dataset_id);
                        CREATE UNIQUE INDEX ON permissions (role_id, permission, layer_id);
                        CREATE UNIQUE INDEX ON permissions (role_id, permission, layer_collection_id);
                        CREATE UNIQUE INDEX ON permissions (role_id, permission, project_id);   

                        CREATE VIEW user_permitted_datasets AS
                            SELECT 
                                r.user_id,
                                p.dataset_id,
                                p.permission
                            FROM 
                                user_roles r JOIN permissions p ON (r.role_id = p.role_id AND dataset_id IS NOT NULL);

                        CREATE VIEW user_permitted_projects AS
                            SELECT 
                                r.user_id,
                                p.project_id,
                                p.permission
                            FROM 
                                user_roles r JOIN permissions p ON (r.role_id = p.role_id AND project_id IS NOT NULL); 

                        CREATE VIEW user_permitted_layer_collections AS
                            SELECT 
                                r.user_id,
                                p.layer_collection_id,
                                p.permission
                            FROM 
                                user_roles r JOIN permissions p ON (r.role_id = p.role_id AND layer_collection_id IS NOT NULL); 

                        CREATE VIEW user_permitted_layers AS
                            SELECT 
                                r.user_id,
                                p.layer_id,
                                p.permission
                            FROM 
                                user_roles r JOIN permissions p ON (r.role_id = p.role_id AND layer_id IS NOT NULL); 

                        --- permission for unsorted layers and root layer collection
                        INSERT INTO permissions
                            (role_id, layer_collection_id, permission)  
                        VALUES 
                            ({admin_role_id}, {root_layer_collection_id}, 'Owner'),
                            ({admin_role_id}, {unsorted_layer_collection_id}, 'Owner'),
                            ({user_role_id}, {root_layer_collection_id}, 'Read'),
                            ({user_role_id}, {unsorted_layer_collection_id}, 'Read'),
                            ({anonymous_role_id}, {root_layer_collection_id}, 'Read'),
                            ({anonymous_role_id}, {unsorted_layer_collection_id}, 'Read');
                        "#
                    ,
                    admin_role_id = escape_literal(&Role::admin_role_id().to_string()),
                    admin_email = escape_literal(&user_config.admin_email),
                    admin_password = escape_literal(&bcrypt::hash(user_config.admin_password).expect("Admin password hash should be valid")),
                    user_role_id = escape_literal(&Role::registered_user_role_id().to_string()),
                    anonymous_role_id = escape_literal(&Role::anonymous_role_id().to_string()),
                    root_layer_collection_id = escape_literal(&INTERNAL_LAYER_DB_ROOT_COLLECTION_ID.to_string()),
                    unsorted_layer_collection_id = escape_literal(&UNSORTED_COLLECTION_ID.to_string())))
                    .await?;
                    debug!("Updated user database to schema version {}", version + 1);
                }
                // 1 => {
                // next version
                // conn.batch_execute(
                //     "\
                //     ALTER TABLE users ...
                //
                //     UPDATE version SET version = 2;\
                //     ",
                // )
                // .await?;
                // eprintln!("Updated user database to schema version {}", version + 1);
                // }
                _ => return Ok(()),
            }
            version += 1;
        }
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
            .context(error::Authorization)
    }
}

#[async_trait]
impl<Tls> ProApplicationContext for PostgresContext<Tls>
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
    type Session = UserSession;
    type GeoEngineDB = PostgresDb<Tls>;

    type TaskContext = SimpleTaskManagerContext;
    type TaskManager = ProTaskManager; // this does not persist across restarts
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<Self::GeoEngineDB>;

    fn db(&self) -> Self::GeoEngineDB {
        PostgresDb::new(self.context.pool.clone(), self.session.clone())
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

        Ok(QueryContextImpl::new_with_extensions(
            self.context.query_ctx_chunk_size,
            self.context.thread_pool.clone(),
            extensions,
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
        ensure!(self.session.is_admin(), error::PermissionDenied);

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
    pub(crate) session: UserSession,
}

impl<Tls> PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(conn_pool: Pool<PostgresConnectionManager<Tls>>, session: UserSession) -> Self {
        Self { conn_pool, session }
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

impl<Tls> ProGeoEngineDb for PostgresDb<Tls>
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
    use std::str::FromStr;

    use super::*;
    use crate::api::model::datatypes::{DataProviderId, DatasetId, LayerId};
    use crate::api::model::services::AddDataset;
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
    use crate::pro::permissions::{Permission, PermissionDb};
    use crate::pro::projects::{LoadVersion, ProProjectDb};
    use crate::pro::users::{
        ExternalUserClaims, RoleDb, UserCredentials, UserDb, UserId, UserRegistration,
    };
    use crate::pro::util::tests::admin_login;
    use crate::projects::{
        CreateProject, LayerUpdate, OrderBy, Plot, PlotUpdate, PointSymbology, ProjectDb,
        ProjectFilter, ProjectId, ProjectLayer, ProjectListOptions, ProjectListing, STRectangle,
        UpdateProject,
    };
    use crate::util::config::{get_config_element, Postgres};
    use crate::util::user_input::UserInput;
    use crate::workflows::registry::WorkflowRegistry;
    use crate::workflows::workflow::Workflow;
    use bb8_postgres::bb8::ManageConnection;
    use bb8_postgres::tokio_postgres::{self, NoTls};
    use futures::Future;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, Coordinate2D, DateTime, Duration, FeatureDataType, Measurement,
        SpatialResolution, TimeInterval, VectorQueryRectangle,
    };
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_datatypes::util::Identifier;
    use geoengine_operators::engine::{
        MetaData, MetaDataProvider, MultipleRasterOrSingleVectorSource, PlotOperator,
        StaticMetaData, TypedOperator, TypedResultDescriptor, VectorColumnInfo, VectorOperator,
        VectorResultDescriptor,
    };
    use geoengine_operators::mock::{MockPointSource, MockPointSourceParams};
    use geoengine_operators::plot::{Statistics, StatisticsParams};
    use geoengine_operators::source::{
        CsvHeader, FormatSpecifics, OgrSourceColumnSpec, OgrSourceDataset,
        OgrSourceDatasetTimeType, OgrSourceDurationSpec, OgrSourceErrorSpec, OgrSourceTimeFormat,
    };
    use geoengine_operators::util::input::MultiRasterOrVectorOperator::Raster;
    use openidconnect::SubjectIdentifier;
    use rand::RngCore;
    use tokio::runtime::Handle;

    /// Setup database schema and return its name.
    async fn setup_db() -> (tokio_postgres::Config, String) {
        let mut db_config = get_config_element::<Postgres>().unwrap();
        db_config.schema = format!("geoengine_test_{}", rand::thread_rng().next_u64()); // generate random temp schema

        let mut pg_config = tokio_postgres::Config::new();
        pg_config
            .user(&db_config.user)
            .password(&db_config.password)
            .host(&db_config.host)
            .dbname(&db_config.database);

        // generate schema with prior connection
        PostgresConnectionManager::new(pg_config.clone(), NoTls)
            .connect()
            .await
            .unwrap()
            .batch_execute(&format!("CREATE SCHEMA {};", &db_config.schema))
            .await
            .unwrap();

        // fix schema by providing `search_path` option
        pg_config.options(&format!("-c search_path={}", db_config.schema));

        (pg_config, db_config.schema)
    }

    /// Tear down database schema.
    async fn tear_down_db(pg_config: tokio_postgres::Config, schema: &str) {
        // generate schema with prior connection
        PostgresConnectionManager::new(pg_config, NoTls)
            .connect()
            .await
            .unwrap()
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE;"))
            .await
            .unwrap();
    }

    async fn with_temp_context<F, Fut>(f: F)
    where
        F: FnOnce(PostgresContext<NoTls>, tokio_postgres::Config) -> Fut
            + std::panic::UnwindSafe
            + Send
            + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let (pg_config, schema) = setup_db().await;

        // catch all panics and clean up first…
        let executed_fn = {
            let pg_config = pg_config.clone();
            std::panic::catch_unwind(move || {
                tokio::task::block_in_place(move || {
                    Handle::current().block_on(async move {
                        let ctx = PostgresContext::new_with_context_spec(
                            pg_config.clone(),
                            tokio_postgres::NoTls,
                            TestDefault::test_default(),
                            TestDefault::test_default(),
                        )
                        .await
                        .unwrap();
                        f(ctx, pg_config.clone()).await;
                    });
                });
            })
        };

        tear_down_db(pg_config, &schema).await;

        // then throw errors afterwards
        if let Err(err) = executed_fn {
            std::panic::resume_unwind(err);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test() {
        with_temp_context(|app_ctx, _| async move {
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
        with_temp_context(|app_ctx, _| async move {
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

    async fn set_session(app_ctx: &PostgresContext<NoTls>, projects: &[ProjectListing]) {
        let credentials = UserCredentials {
            email: "foo@example.com".into(),
            password: "secret123".into(),
        };

        let session = app_ctx.login(credentials).await.unwrap();

        set_session_in_database(app_ctx, projects, session).await;
    }

    async fn set_session_external(app_ctx: &PostgresContext<NoTls>, projects: &[ProjectListing]) {
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
            .register_user(
                UserRegistration {
                    email: "user2@example.com".into(),
                    password: "12345678".into(),
                    real_name: "User2".into(),
                }
                .validated()
                .unwrap(),
            )
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
        db.update_project(update.validated().unwrap())
            .await
            .unwrap();

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
        db.update_project(update.validated().unwrap())
            .await
            .unwrap();

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
        db.update_project(update.validated().unwrap())
            .await
            .unwrap();

        let versions = db.list_project_versions(project_id).await.unwrap();
        assert_eq!(versions.len(), 4);
    }

    async fn list_projects(
        app_ctx: &PostgresContext<NoTls>,
        session: &UserSession,
    ) -> Vec<ProjectListing> {
        let options = ProjectListOptions {
            filter: ProjectFilter::None,
            order: OrderBy::NameDesc,
            offset: 0,
            limit: 2,
        }
        .validated()
        .unwrap();

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
            }
            .validated()
            .unwrap();
            db.create_project(create).await.unwrap();
        }
    }

    async fn user_reg_login(app_ctx: &PostgresContext<NoTls>) -> UserId {
        let user_registration = UserRegistration {
            email: "foo@example.com".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

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
    async fn external_user_login_twice(app_ctx: &PostgresContext<NoTls>) -> UserSession {
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
        with_temp_context(|app_ctx, _| async move {
            let dataset_id = DatasetId::from_str("2e8af98d-3b98-4e2c-a35b-e487bffad7b6").unwrap();

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

            let db = app_ctx.session_context(session.clone()).db();
            let wrap = db.wrap_meta_data(meta_data);
            db.add_dataset(
                AddDataset {
                    id: Some(dataset_id),
                    name: "Ogr Test".to_owned(),
                    description: "desc".to_owned(),
                    source_operator: "OgrSource".to_owned(),
                    symbology: None,
                    provenance: Some(vec![Provenance {
                        citation: "citation".to_owned(),
                        license: "license".to_owned(),
                        uri: "uri".to_owned(),
                    }]),
                }
                .validated()
                .unwrap(),
                wrap,
            )
            .await
            .unwrap();

            let datasets = db
                .list_datasets(
                    DatasetListOptions {
                        filter: None,
                        order: crate::datasets::listing::OrderBy::NameAsc,
                        offset: 0,
                        limit: 10,
                    }
                    .validated()
                    .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(datasets.len(), 1);

            assert_eq!(
                datasets[0],
                DatasetListing {
                    id: dataset_id,
                    name: "Ogr Test".to_owned(),
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
        with_temp_context(|app_ctx, _| async move {
            let db = app_ctx.session_context(UserSession::admin_session()).db();

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
                .list_layer_providers(
                    LayerProviderListingOptions {
                        offset: 0,
                        limit: 10,
                    }
                    .validated()
                    .unwrap(),
                )
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
                    }
                    .validated()
                    .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(datasets.items.len(), 1);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_lists_only_permitted_datasets() {
        with_temp_context(|app_ctx, _| async move {
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
                id: None,
                name: "OgrDataset".to_string(),
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
                },
                result_descriptor: descriptor.clone(),
                phantom: Default::default(),
            };

            let meta = db1.wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let _id = db1
                .add_dataset(ds.validated().unwrap(), meta)
                .await
                .unwrap();

            let list1 = db1
                .list_datasets(
                    DatasetListOptions {
                        filter: None,
                        order: crate::datasets::listing::OrderBy::NameAsc,
                        offset: 0,
                        limit: 1,
                    }
                    .validated()
                    .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(list1.len(), 1);

            let list2 = db2
                .list_datasets(
                    DatasetListOptions {
                        filter: None,
                        order: crate::datasets::listing::OrderBy::NameAsc,
                        offset: 0,
                        limit: 1,
                    }
                    .validated()
                    .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(list2.len(), 0);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_shows_only_permitted_provenance() {
        with_temp_context(|app_ctx, _| async move {
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
                id: None,
                name: "OgrDataset".to_string(),
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
                },
                result_descriptor: descriptor.clone(),
                phantom: Default::default(),
            };

            let meta = db1.wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let id = db1
                .add_dataset(ds.validated().unwrap(), meta)
                .await
                .unwrap();

            assert!(db1.load_provenance(&id).await.is_ok());

            assert!(db2.load_provenance(&id).await.is_err());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_updates_permissions() {
        with_temp_context(|app_ctx, _| async move {
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
                id: None,
                name: "OgrDataset".to_string(),
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
                },
                result_descriptor: descriptor.clone(),
                phantom: Default::default(),
            };

            let meta = db1.wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let id = db1
                .add_dataset(ds.validated().unwrap(), meta)
                .await
                .unwrap();

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
        with_temp_context(|app_ctx, _| async move {
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
                id: None,
                name: "OgrDataset".to_string(),
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
                },
                result_descriptor: descriptor.clone(),
                phantom: Default::default(),
            };

            let meta = db1.wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let id = db1
                .add_dataset(ds.validated().unwrap(), meta)
                .await
                .unwrap();

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
        with_temp_context(|app_ctx, _| async move {
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
                id: None,
                name: "OgrDataset".to_string(),
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
                },
                result_descriptor: descriptor.clone(),
                phantom: Default::default(),
            };

            let meta = db1.wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let id = db1
                .add_dataset(ds.validated().unwrap(), meta)
                .await
                .unwrap();

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_secures_uploads() {
        with_temp_context(|app_ctx, _| async move {
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
        with_temp_context(|app_ctx, _| async move {
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
                    }
                    .validated()
                    .unwrap(),
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
                    properties: vec![],
                    metadata: HashMap::new(),
                }
            );

            let collection1_id = layer_db
                .add_layer_collection(
                    AddLayerCollection {
                        name: "Collection1".to_string(),
                        description: "Collection 1".to_string(),
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
                    &collection1_id,
                )
                .await
                .unwrap();

            let collection2_id = layer_db
                .add_layer_collection(
                    AddLayerCollection {
                        name: "Collection2".to_string(),
                        description: "Collection 2".to_string(),
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
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
                        }),
                        CollectionItem::Layer(LayerListing {
                            id: ProviderLayerId {
                                provider_id: INTERNAL_PROVIDER_ID,
                                layer_id: layer1,
                            },
                            name: "Layer1".to_string(),
                            description: "Layer 1".to_string(),
                            properties: vec![],
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
                    }
                    .validated()
                    .unwrap(),
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
        with_temp_context(|app_ctx, _| async move {
            let _user = app_ctx
                .register_user(
                    UserRegistration {
                        email: "foo@example.com".to_string(),
                        password: "secret1234".to_string(),
                        real_name: "Foo Bar".to_string(),
                    }
                    .validated()
                    .unwrap(),
                )
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

            let quota = initialize_quota_tracking(app_ctx.session_context(admin_session).db());

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
        with_temp_context(|app_ctx, _| async move {
            let user = app_ctx
                .register_user(
                    UserRegistration {
                        email: "foo@example.com".to_string(),
                        password: "secret1234".to_string(),
                        real_name: "Foo Bar".to_string(),
                    }
                    .validated()
                    .unwrap(),
                )
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

            let quota = initialize_quota_tracking(app_ctx.session_context(admin_session).db());

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
        with_temp_context(|app_ctx, _| async move {
            let user = app_ctx
                .register_user(
                    UserRegistration {
                        email: "foo@example.com".to_string(),
                        password: "secret1234".to_string(),
                        real_name: "Foo Bar".to_string(),
                    }
                    .validated()
                    .unwrap(),
                )
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
                crate::util::config::get_config_element::<crate::pro::util::config::User>()
                    .unwrap()
                    .default_available_quota
            );

            assert_eq!(
                admin_db.quota_available_by_user(&user).await.unwrap(),
                crate::util::config::get_config_element::<crate::pro::util::config::User>()
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
        with_temp_context(|app_ctx, _| async move {
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
            }
            .validated()
            .unwrap();

            let root_collection = &layer_db.get_root_layer_collection_id().await.unwrap();

            let collection = AddLayerCollection {
                name: "top collection".to_string(),
                description: "description".to_string(),
            }
            .validated()
            .unwrap();

            let top_c_id = layer_db
                .add_layer_collection(collection, root_collection)
                .await
                .unwrap();

            let l_id = layer_db.add_layer(layer, &top_c_id).await.unwrap();

            let collection = AddLayerCollection {
                name: "empty collection".to_string(),
                description: "description".to_string(),
            }
            .validated()
            .unwrap();

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
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
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
            let session = admin_login(&app_ctx).await;

            let db = app_ctx.session_context(session).db();

            let root_collection_id = &db.get_root_layer_collection_id().await.unwrap();

            let mid_collection_id = db
                .add_layer_collection(
                    AddLayerCollection {
                        name: "mid collection".to_string(),
                        description: "description".to_string(),
                    }
                    .validated()
                    .unwrap(),
                    root_collection_id,
                )
                .await
                .unwrap();

            let bottom_collection_id = db
                .add_layer_collection(
                    AddLayerCollection {
                        name: "bottom collection".to_string(),
                        description: "description".to_string(),
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
                    &mid_collection_id,
                )
                .await
                .unwrap();

            // removing the mid collection…
            db.remove_layer_collection_from_parent(&mid_collection_id, root_collection_id)
                .await
                .unwrap();

            // …should remove itself
            db.load_layer_collection(
                &mid_collection_id,
                LayerCollectionListOptions::default().validated().unwrap(),
            )
            .await
            .unwrap_err();

            // …should remove the bottom collection
            db.load_layer_collection(
                &bottom_collection_id,
                LayerCollectionListOptions::default().validated().unwrap(),
            )
            .await
            .unwrap_err();

            // … and should remove the layer of the bottom collection
            db.load_layer(&layer_id).await.unwrap_err();

            // the root collection is still there
            db.load_layer_collection(
                root_collection_id,
                LayerCollectionListOptions::default().validated().unwrap(),
            )
            .await
            .unwrap();
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn it_removes_layers_from_collections() {
        with_temp_context(|app_ctx, _| async move {
            let session = admin_login(&app_ctx).await;

            let db = app_ctx.session_context(session).db();

            let root_collection = &db.get_root_layer_collection_id().await.unwrap();

            let another_collection = db
                .add_layer_collection(
                    AddLayerCollection {
                        name: "top collection".to_string(),
                        description: "description".to_string(),
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
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
            let dataset_id = DatasetId::from_str("2e8af98d-3b98-4e2c-a35b-e487bffad7b6").unwrap();

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

            let db = app_ctx.session_context(session.clone()).db();
            let wrap = db.wrap_meta_data(meta_data);
            db.add_dataset(
                AddDataset {
                    id: Some(dataset_id),
                    name: "Ogr Test".to_owned(),
                    description: "desc".to_owned(),
                    source_operator: "OgrSource".to_owned(),
                    symbology: None,
                    provenance: Some(vec![Provenance {
                        citation: "citation".to_owned(),
                        license: "license".to_owned(),
                        uri: "uri".to_owned(),
                    }]),
                }
                .validated()
                .unwrap(),
                wrap,
            )
            .await
            .unwrap();

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
            let dataset_id = DatasetId::from_str("2e8af98d-3b98-4e2c-a35b-e487bffad7b6").unwrap();

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
            db.add_dataset(
                AddDataset {
                    id: Some(dataset_id),
                    name: "Ogr Test".to_owned(),
                    description: "desc".to_owned(),
                    source_operator: "OgrSource".to_owned(),
                    symbology: None,
                    provenance: Some(vec![Provenance {
                        citation: "citation".to_owned(),
                        license: "license".to_owned(),
                        uri: "uri".to_owned(),
                    }]),
                }
                .validated()
                .unwrap(),
                wrap,
            )
            .await
            .unwrap();

            assert!(db.load_dataset(&dataset_id).await.is_ok());

            db.delete_dataset(dataset_id).await.unwrap();

            assert!(db.load_dataset(&dataset_id).await.is_err());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_missing_layer_dataset_in_collection_listing() {
        with_temp_context(|app_ctx, _| async move {
            let session = admin_login(&app_ctx).await;
            let db = app_ctx.session_context(session).db();

            let root_collection_id = &db.get_root_layer_collection_id().await.unwrap();

            let top_collection_id = db
                .add_layer_collection(
                    AddLayerCollection {
                        name: "top collection".to_string(),
                        description: "description".to_string(),
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
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
        with_temp_context(|app_ctx, _| async move {
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
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
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
                }
                .validated()
                .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
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
                    }
                    .validated()
                    .unwrap(),
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_handles_user_roles() {
        with_temp_context(|app_ctx, _| async move {
            let admin_session = admin_login(&app_ctx).await;
            let user_id = app_ctx
                .register_user(
                    UserRegistration {
                        email: "foo@example.com".to_string(),
                        password: "secret123".to_string(),
                        real_name: "Foo Bar".to_string(),
                    }
                    .validated()
                    .unwrap(),
                )
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
        })
        .await;
    }
}
