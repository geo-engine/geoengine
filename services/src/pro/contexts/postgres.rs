use crate::datasets::add_from_directory::add_providers_from_directory;
use crate::error::{self, Result};
use crate::layers::add_from_directory::{
    add_layer_collections_from_directory, add_layers_from_directory, UNSORTED_COLLECTION_ID,
};
use crate::layers::storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID;
use crate::pro::datasets::{add_datasets_from_directory, PostgresDatasetDb, Role};
use crate::pro::layers::postgres_layer_db::{PostgresLayerDb, PostgresLayerProviderDb};
use crate::pro::projects::ProjectPermission;
use crate::pro::users::{OidcRequestDb, UserDb, UserId, UserSession};
use crate::pro::util::config::Oidc;
use crate::pro::workflows::postgres_workflow_registry::PostgresWorkflowRegistry;
use crate::projects::ProjectId;
use crate::tasks::{SimpleTaskManager, SimpleTaskManagerContext};
use crate::{contexts::Context, pro::users::PostgresUserDb};
use crate::{contexts::QueryContextImpl, pro::projects::PostgresProjectDb};
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
use log::{debug, warn};
use rayon::ThreadPool;
use snafu::ResultExt;
use std::path::PathBuf;
use std::sync::Arc;

use super::{ExecutionContextImpl, ProContext};

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
    user_db: Arc<PostgresUserDb<Tls>>,
    project_db: Arc<PostgresProjectDb<Tls>>,
    workflow_registry: Arc<PostgresWorkflowRegistry<Tls>>,
    dataset_db: Arc<PostgresDatasetDb<Tls>>,
    layer_db: Arc<PostgresLayerDb<Tls>>,
    layer_provider_db: Arc<PostgresLayerProviderDb<Tls>>,
    thread_pool: Arc<ThreadPool>,
    exe_ctx_tiling_spec: TilingSpecification,
    query_ctx_chunk_size: ChunkByteSize,
    task_manager: Arc<SimpleTaskManager>,
    oidc_request_db: Arc<Option<OidcRequestDb>>,
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

        Ok(Self {
            user_db: Arc::new(PostgresUserDb::new(pool.clone())),
            project_db: Arc::new(PostgresProjectDb::new(pool.clone())),
            workflow_registry: Arc::new(PostgresWorkflowRegistry::new(pool.clone())),
            dataset_db: Arc::new(PostgresDatasetDb::new(pool.clone())),
            layer_db: Arc::new(PostgresLayerDb::new(pool.clone())),
            layer_provider_db: Arc::new(PostgresLayerProviderDb::new(pool.clone())),
            task_manager: Arc::new(SimpleTaskManager::default()),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            oidc_request_db: Arc::new(None),
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

        let workflow_db = PostgresWorkflowRegistry::new(pool.clone());
        let mut layer_db = PostgresLayerDb::new(pool.clone());

        add_layers_from_directory(&mut layer_db, layer_defs_path).await;
        add_layer_collections_from_directory(&mut layer_db, layer_collection_defs_path).await;

        let mut dataset_db = PostgresDatasetDb::new(pool.clone());

        add_datasets_from_directory(&mut dataset_db, dataset_defs_path).await;

        let mut layer_provider_db = PostgresLayerProviderDb::new(pool.clone());

        add_providers_from_directory(&mut layer_provider_db, provider_defs_path.clone()).await;
        add_providers_from_directory(&mut layer_provider_db, provider_defs_path.join("pro")).await;

        Ok(Self {
            user_db: Arc::new(PostgresUserDb::new(pool.clone())),
            project_db: Arc::new(PostgresProjectDb::new(pool.clone())),
            workflow_registry: Arc::new(workflow_db),
            dataset_db: Arc::new(dataset_db),
            layer_db: Arc::new(layer_db),
            layer_provider_db: Arc::new(PostgresLayerProviderDb::new(pool.clone())),
            task_manager: Arc::new(SimpleTaskManager::default()),
            thread_pool: create_rayon_thread_pool(0),
            exe_ctx_tiling_spec,
            query_ctx_chunk_size,
            oidc_request_db: Arc::new(OidcRequestDb::try_from(oidc_config).ok()),
        })
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
                    conn.batch_execute(
                        &format!(r#"
                        CREATE TABLE version (
                            version INT
                        );
                        INSERT INTO version VALUES (1);

                        CREATE TABLE roles (
                            id UUID PRIMARY KEY,
                            name text NOT NULL
                        );

                        INSERT INTO roles (id, name) VALUES
                            ('{system_role_id}', 'system'),
                            ('{user_role_id}', 'user'),
                            ('{anonymous_role_id}', 'anonymous');

                        CREATE TABLE users (
                            id UUID PRIMARY KEY REFERENCES roles(id),
                            email character varying (256) UNIQUE,
                            password_hash character varying (256),
                            real_name character varying (256),
                            active boolean NOT NULL
                            CONSTRAINT users_anonymous_ck CHECK (
                               (email IS NULL AND password_hash IS NULL AND real_name IS NULL) OR 
                               (email IS NOT NULL AND password_hash IS NOT NULL AND 
                                real_name IS NOT NULL) 
                            )
                        );

                        INSERT INTO users (
                            id, 
                            email,
                            password_hash,
                            real_name,
                            active)
                        VALUES (
                            '{system_role_id}', 
                            'system@geoengine.io',
                            '',
                            'system',
                            true
                        );

                        -- relation between users and roles
                        -- all users have a default role where role_id = user_id
                        CREATE TABLE user_roles (
                            user_id UUID REFERENCES users(id) ON DELETE CASCADE NOT NULL,
                            role_id UUID REFERENCES roles(id) ON DELETE CASCADE NOT NULL,
                            PRIMARY KEY (user_id, role_id)
                        );

                        -- system user role
                        INSERT INTO user_roles 
                            (user_id, role_id)
                        VALUES 
                            ('{system_role_id}', 
                            '{system_role_id}');

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

                        CREATE TYPE "ProjectPermission" AS ENUM ('Read', 'Write', 'Owner');

                        CREATE TABLE user_project_permissions (
                            user_id UUID REFERENCES users(id) NOT NULL,
                            project_id UUID REFERENCES projects(id) ON DELETE CASCADE NOT NULL,
                            permission "ProjectPermission" NOT NULL,
                            PRIMARY KEY (user_id, project_id)
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
                            'Read', 'Write', 'Owner'
                        );

                        -- TODO: add indexes
                        CREATE TABLE dataset_permissions (
                            role_id UUID REFERENCES roles(id) ON DELETE CASCADE NOT NULL,
                            dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE NOT NULL,
                            permission "Permission" NOT NULL,
                            PRIMARY KEY (role_id, dataset_id)
                        );

                        CREATE VIEW user_permitted_datasets AS
                            SELECT 
                                r.user_id,
                                p.dataset_id,
                                p.permission
                            FROM 
                                user_roles r JOIN dataset_permissions p ON (r.role_id = p.role_id);


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
                            '{root_layer_collection_id}',
                            'Layers',
                            'All available Geo Engine layers'
                        );

                        -- insert the unsorted layer collection
                        INSERT INTO layer_collections (
                            id,
                            name,
                            description
                        ) VALUES (
                            '{unsorted_layer_collection_id}',
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
                        ('{root_layer_collection_id}', '{unsorted_layer_collection_id}');

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
                        "#
                    ,
                    system_role_id = Role::system_role_id(),
                    user_role_id = Role::user_role_id(),
                    anonymous_role_id = Role::anonymous_role_id(),
                    root_layer_collection_id = INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
                    unsorted_layer_collection_id = UNSORTED_COLLECTION_ID))
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

    pub(crate) async fn check_user_project_permission(
        conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
        user: UserId,
        project: ProjectId,
        permissions: &[ProjectPermission],
    ) -> Result<()> {
        let stmt = conn
            .prepare(
                "
                SELECT TRUE
                FROM user_project_permissions
                WHERE user_id = $1 AND project_id = $2 AND permission = ANY ($3);",
            )
            .await?;

        conn.query_one(&stmt, &[&user, &project, &permissions])
            .await
            .map_err(|_error| error::Error::ProjectDbUnauthorized)?;

        Ok(())
    }
}

#[async_trait]
impl<Tls> ProContext for PostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type UserDB = PostgresUserDb<Tls>;

    fn user_db(&self) -> Arc<Self::UserDB> {
        self.user_db.clone()
    }
    fn user_db_ref(&self) -> &Self::UserDB {
        &self.user_db
    }
    fn oidc_request_db(&self) -> Option<&OidcRequestDb> {
        self.oidc_request_db.as_ref().as_ref()
    }
}

#[async_trait]
impl<Tls> Context for PostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Session = UserSession;
    type ProjectDB = PostgresProjectDb<Tls>;
    type WorkflowRegistry = PostgresWorkflowRegistry<Tls>;
    type DatasetDB = PostgresDatasetDb<Tls>;
    type LayerDB = PostgresLayerDb<Tls>;
    type LayerProviderDB = PostgresLayerProviderDb<Tls>;
    type TaskContext = SimpleTaskManagerContext;
    type TaskManager = SimpleTaskManager; // this does not persist across restarts
    type QueryContext = QueryContextImpl;
    type ExecutionContext =
        ExecutionContextImpl<UserSession, PostgresDatasetDb<Tls>, PostgresLayerProviderDb<Tls>>;

    fn project_db(&self) -> Arc<Self::ProjectDB> {
        self.project_db.clone()
    }
    fn project_db_ref(&self) -> &Self::ProjectDB {
        &self.project_db
    }

    fn workflow_registry(&self) -> Arc<Self::WorkflowRegistry> {
        self.workflow_registry.clone()
    }
    fn workflow_registry_ref(&self) -> &Self::WorkflowRegistry {
        &self.workflow_registry
    }

    fn dataset_db(&self) -> Arc<Self::DatasetDB> {
        self.dataset_db.clone()
    }
    fn dataset_db_ref(&self) -> &Self::DatasetDB {
        &self.dataset_db
    }

    fn layer_db(&self) -> Arc<Self::LayerDB> {
        self.layer_db.clone()
    }
    fn layer_db_ref(&self) -> &Self::LayerDB {
        &self.layer_db
    }

    fn layer_provider_db(&self) -> Arc<Self::LayerProviderDB> {
        self.layer_provider_db.clone()
    }
    fn layer_provider_db_ref(&self) -> &Self::LayerProviderDB {
        &self.layer_provider_db
    }

    fn tasks(&self) -> Arc<Self::TaskManager> {
        self.task_manager.clone()
    }
    fn tasks_ref(&self) -> &Self::TaskManager {
        &self.task_manager
    }

    fn query_context(&self) -> Result<Self::QueryContext> {
        // TODO: load config only once
        Ok(QueryContextImpl::new(
            self.query_ctx_chunk_size,
            self.thread_pool.clone(),
        ))
    }

    fn execution_context(&self, session: UserSession) -> Result<Self::ExecutionContext> {
        Ok(ExecutionContextImpl::<
            UserSession,
            PostgresDatasetDb<Tls>,
            PostgresLayerProviderDb<Tls>,
        >::new(
            self.dataset_db.clone(),
            self.layer_provider_db.clone(),
            self.thread_pool.clone(),
            session,
            self.exe_ctx_tiling_spec,
        ))
    }

    async fn session_by_id(&self, session_id: crate::contexts::SessionId) -> Result<Self::Session> {
        self.user_db_ref()
            .session(session_id)
            .await
            .map_err(Box::new)
            .context(error::Authorization)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;

    use super::*;
    use crate::api::model::datatypes::{DataProviderId, DatasetId};
    use crate::datasets::external::mock::MockExternalLayerProviderDefinition;
    use crate::datasets::listing::SessionMetaDataProvider;
    use crate::datasets::listing::{DatasetListOptions, DatasetListing, ProvenanceOutput};
    use crate::datasets::listing::{DatasetProvider, Provenance};
    use crate::datasets::storage::{
        AddDataset, DatasetDefinition, DatasetStore, MetaDataDefinition,
    };
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
    use crate::pro::datasets::{DatasetPermission, Permission, UpdateDatasetPermissions};
    use crate::pro::projects::{LoadVersion, ProProjectDb, UserProjectPermission};
    use crate::pro::users::{ExternalUserClaims, UserCredentials, UserDb, UserRegistration};
    use crate::projects::{
        CreateProject, Layer, LayerUpdate, OrderBy, Plot, PlotUpdate, PointSymbology, ProjectDb,
        ProjectFilter, ProjectId, ProjectListOptions, ProjectListing, STRectangle, UpdateProject,
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
        MetaData, MultipleRasterOrSingleVectorSource, PlotOperator, StaticMetaData, TypedOperator,
        TypedResultDescriptor, VectorColumnInfo, VectorOperator, VectorResultDescriptor,
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
            .batch_execute(&format!("DROP SCHEMA {} CASCADE;", schema))
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
        with_temp_context(|ctx, _| async move {
            anonymous(&ctx).await;

            let _user_id = user_reg_login(&ctx).await;

            let session = ctx
                .user_db_ref()
                .login(UserCredentials {
                    email: "foo@example.com".into(),
                    password: "secret123".into(),
                })
                .await
                .unwrap();

            create_projects(&ctx, &session).await;

            let projects = list_projects(&ctx, &session).await;

            set_session(&ctx, &projects).await;

            let project_id = projects[0].id;

            update_projects(&ctx, &session, project_id).await;

            add_permission(&ctx, &session, project_id).await;

            delete_project(&ctx, &session, project_id).await;
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_external() {
        with_temp_context(|ctx, _| async move {
            anonymous(&ctx).await;

            let session = external_user_login_twice(&ctx).await;

            create_projects(&ctx, &session).await;

            let projects = list_projects(&ctx, &session).await;

            set_session_external(&ctx, &projects).await;

            let project_id = projects[0].id;

            update_projects(&ctx, &session, project_id).await;

            add_permission(&ctx, &session, project_id).await;

            delete_project(&ctx, &session, project_id).await;
        })
        .await;
    }

    async fn set_session(ctx: &PostgresContext<NoTls>, projects: &[ProjectListing]) {
        let credentials = UserCredentials {
            email: "foo@example.com".into(),
            password: "secret123".into(),
        };

        let user_db = ctx.user_db_ref();

        let session = user_db.login(credentials).await.unwrap();

        set_session_in_database(user_db, projects, session).await;
    }

    async fn set_session_external(ctx: &PostgresContext<NoTls>, projects: &[ProjectListing]) {
        let external_user_claims = ExternalUserClaims {
            external_id: SubjectIdentifier::new("Foo bar Id".into()),
            email: "foo@bar.de".into(),
            real_name: "Foo Bar".into(),
        };

        let user_db = ctx.user_db_ref();

        let session = user_db
            .login_external(external_user_claims, Duration::minutes(10))
            .await
            .unwrap();

        set_session_in_database(user_db, projects, session).await;
    }

    async fn set_session_in_database(
        user_db: &PostgresUserDb<NoTls>,
        projects: &[ProjectListing],
        session: UserSession,
    ) {
        user_db
            .set_session_project(&session, projects[0].id)
            .await
            .unwrap();

        assert_eq!(
            user_db.session(session.id).await.unwrap().project,
            Some(projects[0].id)
        );

        let rect = STRectangle::new_unchecked(SpatialReference::epsg_4326(), 0., 1., 2., 3., 1, 2);
        user_db
            .set_session_view(&session, rect.clone())
            .await
            .unwrap();
        assert_eq!(user_db.session(session.id).await.unwrap().view, Some(rect));
    }

    async fn delete_project(
        ctx: &PostgresContext<NoTls>,
        session: &UserSession,
        project_id: ProjectId,
    ) {
        ctx.project_db_ref()
            .delete(session, project_id)
            .await
            .unwrap();

        assert!(ctx
            .project_db_ref()
            .load(session, project_id)
            .await
            .is_err());
    }

    async fn add_permission(
        ctx: &PostgresContext<NoTls>,
        session: &UserSession,
        project_id: ProjectId,
    ) {
        assert_eq!(
            ctx.project_db_ref()
                .list_permissions(session, project_id)
                .await
                .unwrap()
                .len(),
            1
        );

        let user2 = ctx
            .user_db_ref()
            .register(
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

        ctx.project_db_ref()
            .add_permission(
                session,
                UserProjectPermission {
                    project: project_id,
                    permission: ProjectPermission::Read,
                    user: user2,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            ctx.project_db_ref()
                .list_permissions(session, project_id)
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[allow(clippy::too_many_lines)]
    async fn update_projects(
        ctx: &PostgresContext<NoTls>,
        session: &UserSession,
        project_id: ProjectId,
    ) {
        let project = ctx
            .project_db_ref()
            .load_version(session, project_id, LoadVersion::Latest)
            .await
            .unwrap();

        let layer_workflow_id = ctx
            .workflow_registry_ref()
            .register(Workflow {
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

        assert!(ctx
            .workflow_registry_ref()
            .load(&layer_workflow_id)
            .await
            .is_ok());

        let plot_workflow_id = ctx
            .workflow_registry_ref()
            .register(Workflow {
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

        assert!(ctx
            .workflow_registry_ref()
            .load(&plot_workflow_id)
            .await
            .is_ok());

        // add a plot
        let update = UpdateProject {
            id: project.id,
            name: Some("Test9 Updated".into()),
            description: None,
            layers: Some(vec![LayerUpdate::UpdateOrInsert(Layer {
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
        ctx.project_db_ref()
            .update(session, update.validated().unwrap())
            .await
            .unwrap();

        let versions = ctx
            .project_db_ref()
            .versions(session, project_id)
            .await
            .unwrap();
        assert_eq!(versions.len(), 2);

        // add second plot
        let update = UpdateProject {
            id: project.id,
            name: Some("Test9 Updated".into()),
            description: None,
            layers: Some(vec![LayerUpdate::UpdateOrInsert(Layer {
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
        ctx.project_db_ref()
            .update(session, update.validated().unwrap())
            .await
            .unwrap();

        let versions = ctx
            .project_db_ref()
            .versions(session, project_id)
            .await
            .unwrap();
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
        ctx.project_db_ref()
            .update(session, update.validated().unwrap())
            .await
            .unwrap();

        let versions = ctx
            .project_db_ref()
            .versions(session, project_id)
            .await
            .unwrap();
        assert_eq!(versions.len(), 4);
    }

    async fn list_projects(
        ctx: &PostgresContext<NoTls>,
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
        let projects = ctx.project_db_ref().list(session, options).await.unwrap();

        assert_eq!(projects.len(), 2);
        assert_eq!(projects[0].name, "Test9");
        assert_eq!(projects[1].name, "Test8");
        projects
    }

    async fn create_projects(ctx: &PostgresContext<NoTls>, session: &UserSession) {
        for i in 0..10 {
            let create = CreateProject {
                name: format!("Test{}", i),
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
            ctx.project_db_ref().create(session, create).await.unwrap();
        }
    }

    async fn user_reg_login(ctx: &PostgresContext<NoTls>) -> UserId {
        let db = ctx.user_db_ref();

        let user_registration = UserRegistration {
            email: "foo@example.com".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        let user_id = db.register(user_registration).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@example.com".into(),
            password: "secret123".into(),
        };

        let session = db.login(credentials).await.unwrap();

        db.session(session.id).await.unwrap();

        db.logout(session.id).await.unwrap();

        assert!(db.session(session.id).await.is_err());

        user_id
    }

    //TODO: No duplicate tests for postgres and hashmap implementation possible?
    async fn external_user_login_twice(ctx: &PostgresContext<NoTls>) -> UserSession {
        let db = ctx.user_db_ref();

        let external_user_claims = ExternalUserClaims {
            external_id: SubjectIdentifier::new("Foo bar Id".into()),
            email: "foo@bar.de".into(),
            real_name: "Foo Bar".into(),
        };
        let duration = Duration::minutes(30);

        //NEW
        let login_result = db
            .login_external(external_user_claims.clone(), duration)
            .await;
        assert!(login_result.is_ok());

        let session_1 = login_result.unwrap();
        let user_id = session_1.user.id; //TODO: Not a deterministic test.

        assert!(session_1.user.email.is_some());
        assert_eq!(session_1.user.email.unwrap(), "foo@bar.de");
        assert!(session_1.user.real_name.is_some());
        assert_eq!(session_1.user.real_name.unwrap(), "Foo Bar");

        let expected_duration = session_1.created + duration;
        assert_eq!(session_1.valid_until, expected_duration);

        assert!(db.session(session_1.id).await.is_ok());

        assert!(db.logout(session_1.id).await.is_ok());

        assert!(db.session(session_1.id).await.is_err());

        let duration = Duration::minutes(10);
        let login_result = db
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

        assert!(db.session(session_2.id).await.is_ok());

        result
    }

    async fn anonymous(ctx: &PostgresContext<NoTls>) {
        let db = ctx.user_db_ref();

        let now: DateTime = chrono::offset::Utc::now().into();
        let session = db.anonymous().await.unwrap();
        let then: DateTime = chrono::offset::Utc::now().into();

        assert!(session.created >= now && session.created <= then);
        assert!(session.valid_until > session.created);

        let session = db.session(session.id).await.unwrap();

        db.logout(session.id).await.unwrap();

        assert!(db.session(session.id).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_persists_workflows() {
        with_temp_context(|ctx, pg_config| async move {
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

            let id = ctx
                .workflow_registry_ref()
                .register(workflow)
                .await
                .unwrap();

            drop(ctx);

            let ctx = PostgresContext::new_with_context_spec(pg_config.clone(), tokio_postgres::NoTls, TestDefault::test_default(), TestDefault::test_default())
                .await
                .unwrap();

            let workflow = ctx.workflow_registry_ref().load(&id).await.unwrap();

            let json = serde_json::to_string(&workflow).unwrap();
            assert_eq!(json, r#"{"type":"Vector","operator":{"type":"MockPointSource","params":{"points":[{"x":1.0,"y":2.0},{"x":1.0,"y":2.0},{"x":1.0,"y":2.0}]}}}"#);
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_persists_datasets() {
        with_temp_context(|ctx, _| async move {
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

            let session = ctx.user_db_ref().anonymous().await.unwrap();

            let db = ctx.dataset_db_ref();
            let wrap = db.wrap_meta_data(meta_data);
            db.add_dataset(
                &session,
                AddDataset {
                    id: Some(dataset_id),
                    name: "Ogr Test".to_owned(),
                    description: "desc".to_owned(),
                    source_operator: "OgrSource".to_owned(),
                    symbology: None,
                    provenance: Some(Provenance {
                        citation: "citation".to_owned(),
                        license: "license".to_owned(),
                        uri: "uri".to_owned(),
                    }),
                }
                .validated()
                .unwrap(),
                wrap,
            )
            .await
            .unwrap();

            let datasets = db
                .list(
                    &session,
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
                    }),
                },
            );

            let provenance = db.provenance(&session, &dataset_id).await.unwrap();

            assert_eq!(
                provenance,
                ProvenanceOutput {
                    data: dataset_id.into(),
                    provenance: Some(Provenance {
                        citation: "citation".to_owned(),
                        license: "license".to_owned(),
                        uri: "uri".to_owned(),
                    })
                }
            );

            let meta_data: Box<dyn MetaData<OgrSourceDataset, _, _>> = db
                .session_meta_data(&session, &dataset_id.into())
                .await
                .unwrap();

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
        with_temp_context(|ctx, _| async move {
            let db = ctx.dataset_db_ref();

            let id = UploadId::from_str("2de18cd8-4a38-4111-a445-e3734bc18a80").unwrap();
            let input = Upload {
                id,
                files: vec![FileUpload {
                    id: FileId::from_str("e80afab0-831d-4d40-95d6-1e4dfd277e72").unwrap(),
                    name: "test.csv".to_owned(),
                    byte_size: 1337,
                }],
            };

            let session = ctx.user_db_ref().anonymous().await.unwrap();
            db.create_upload(&session, input.clone()).await.unwrap();

            let upload = db.get_upload(&session, id).await.unwrap();

            assert_eq!(upload, input);
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_persists_layer_providers() {
        with_temp_context(|ctx, _| async move {
            let db = ctx.layer_provider_db_ref();

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
                datasets: vec![DatasetDefinition {
                    properties: AddDataset {
                        id: Some(DatasetId::new()),
                        name: "test".to_owned(),
                        description: "desc".to_owned(),
                        source_operator: "MockPointSource".to_owned(),
                        symbology: None,
                        provenance: None,
                    },
                    meta_data,
                }],
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

            let provider = db.layer_provider(provider_id).await.unwrap();

            let datasets = provider
                .collection(
                    &provider.root_collection_id().await.unwrap(),
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
        with_temp_context(|ctx, _| async move {
            let session1 = ctx.user_db_ref().anonymous().await.unwrap();
            let session2 = ctx.user_db_ref().anonymous().await.unwrap();

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

            let meta = ctx
                .dataset_db_ref()
                .wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let _id = ctx
                .dataset_db_ref()
                .add_dataset(&session1, ds.validated().unwrap(), meta)
                .await
                .unwrap();

            let list1 = ctx
                .dataset_db_ref()
                .list(
                    &session1,
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

            let list2 = ctx
                .dataset_db_ref()
                .list(
                    &session2,
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
        with_temp_context(|ctx, _| async move {
            let session1 = ctx.user_db_ref().anonymous().await.unwrap();
            let session2 = ctx.user_db_ref().anonymous().await.unwrap();

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

            let meta = ctx
                .dataset_db_ref()
                .wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let id = ctx
                .dataset_db_ref()
                .add_dataset(&session1, ds.validated().unwrap(), meta)
                .await
                .unwrap();

            assert!(ctx
                .dataset_db_ref()
                .provenance(&session1, &id)
                .await
                .is_ok());

            assert!(ctx
                .dataset_db_ref()
                .provenance(&session2, &id)
                .await
                .is_err());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_updates_permissions() {
        with_temp_context(|ctx, _| async move {
            let session1 = ctx.user_db_ref().anonymous().await.unwrap();
            let session2 = ctx.user_db_ref().anonymous().await.unwrap();

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

            let meta = ctx
                .dataset_db_ref()
                .wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let id = ctx
                .dataset_db_ref()
                .add_dataset(&session1, ds.validated().unwrap(), meta)
                .await
                .unwrap();

            assert!(ctx.dataset_db_ref().load(&session1, &id).await.is_ok());

            assert!(ctx.dataset_db_ref().load(&session2, &id).await.is_err());

            ctx.dataset_db_ref()
                .add_dataset_permission(
                    &session1,
                    DatasetPermission {
                        role: session2.user.id.into(),
                        dataset: id,
                        permission: Permission::Read,
                    },
                )
                .await
                .unwrap();

            assert!(ctx.dataset_db_ref().load(&session2, &id).await.is_ok());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_uses_roles_for_permissions() {
        with_temp_context(|ctx, _| async move {
            let session1 = ctx.user_db_ref().anonymous().await.unwrap();
            let session2 = ctx.user_db_ref().anonymous().await.unwrap();

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

            let meta = ctx
                .dataset_db_ref()
                .wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let id = ctx
                .dataset_db_ref()
                .add_dataset(&session1, ds.validated().unwrap(), meta)
                .await
                .unwrap();

            assert!(ctx.dataset_db_ref().load(&session1, &id).await.is_ok());

            assert!(ctx.dataset_db_ref().load(&session2, &id).await.is_err());

            ctx.dataset_db_ref()
                .add_dataset_permission(
                    &session1,
                    DatasetPermission {
                        role: session2.user.id.into(),
                        dataset: id,
                        permission: Permission::Read,
                    },
                )
                .await
                .unwrap();

            assert!(ctx.dataset_db_ref().load(&session2, &id).await.is_ok());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_secures_meta_data() {
        with_temp_context(|ctx, _| async move {
            let session1 = ctx.user_db_ref().anonymous().await.unwrap();
            let session2 = ctx.user_db_ref().anonymous().await.unwrap();

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

            let meta = ctx
                .dataset_db_ref()
                .wrap_meta_data(MetaDataDefinition::OgrMetaData(meta));

            let id = ctx
                .dataset_db_ref()
                .add_dataset(&session1, ds.validated().unwrap(), meta)
                .await
                .unwrap();

            let meta: Result<
                Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
            > = ctx
                .dataset_db_ref()
                .session_meta_data(&session1, &id.into())
                .await;

            assert!(meta.is_ok());

            let meta: Result<
                Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
            > = ctx
                .dataset_db_ref()
                .session_meta_data(&session2, &id.into())
                .await;

            assert!(meta.is_err());

            ctx.dataset_db_ref()
                .add_dataset_permission(
                    &session1,
                    DatasetPermission {
                        role: session2.user.id.into(),
                        dataset: id,
                        permission: Permission::Read,
                    },
                )
                .await
                .unwrap();

            let meta: Result<
                Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
            > = ctx
                .dataset_db_ref()
                .session_meta_data(&session2, &id.into())
                .await;

            assert!(meta.is_ok());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_secures_uploads() {
        with_temp_context(|ctx, _| async move {
            let session1 = ctx.user_db_ref().anonymous().await.unwrap();
            let session2 = ctx.user_db_ref().anonymous().await.unwrap();

            let upload_id = UploadId::new();

            let upload = Upload {
                id: upload_id,
                files: vec![FileUpload {
                    id: FileId::new(),
                    name: "test.bin".to_owned(),
                    byte_size: 1024,
                }],
            };

            ctx.dataset_db_ref()
                .create_upload(&session1, upload)
                .await
                .unwrap();

            assert!(ctx
                .dataset_db_ref()
                .get_upload(&session1, upload_id)
                .await
                .is_ok());

            assert!(ctx
                .dataset_db_ref()
                .get_upload(&session2, upload_id)
                .await
                .is_err());
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_collects_layers() {
        with_temp_context(|ctx, _| async move {
            let layer_db = ctx.layer_db_ref();

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

            let root_collection_id = layer_db.root_collection_id().await.unwrap();

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
                layer_db.get_layer(&layer1).await.unwrap(),
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
                .add_collection(
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
                .add_collection(
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
                .collection(
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
                        })
                    ],
                    entry_label: None,
                    properties: vec![],
                }
            );

            let collection1 = layer_db
                .collection(
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
                        })
                    ],
                    entry_label: None,
                    properties: vec![],
                }
            );
        })
        .await;
    }
}
