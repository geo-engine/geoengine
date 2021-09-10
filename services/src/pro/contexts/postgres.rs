use crate::error::{self, Result};
use crate::pro::datasets::PostgresDatasetDb;
use crate::pro::projects::ProjectPermission;
use crate::pro::users::{UserDb, UserId, UserSession};
use crate::projects::ProjectId;
use crate::workflows::postgres_workflow_registry::PostgresWorkflowRegistry;
use crate::{
    contexts::{Context, Db},
    pro::users::PostgresUserDb,
};
use crate::{
    contexts::{ExecutionContextImpl, QueryContextImpl},
    pro::projects::PostgresProjectDb,
};
use async_trait::async_trait;
use bb8_postgres::{
    bb8::Pool,
    bb8::PooledConnection,
    tokio_postgres::{error::SqlState, tls::MakeTlsConnect, tls::TlsConnect, Config, Socket},
    PostgresConnectionManager,
};
use log::{debug, warn};
use snafu::ResultExt;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::ProContext;

/// A contex with references to Postgres backends of the dbs. Automatically migrates schema on instantiation
#[derive(Clone)]
pub struct PostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    user_db: Db<PostgresUserDb<Tls>>,
    project_db: Db<PostgresProjectDb<Tls>>,
    workflow_registry: Db<PostgresWorkflowRegistry<Tls>>,
    session: Option<UserSession>,
}

impl<Tls> PostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub async fn new(config: Config, tls: Tls) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(config, tls);

        let pool = Pool::builder().build(pg_mgr).await?;

        Self::update_schema(pool.get().await?).await?;

        Ok(Self {
            user_db: Arc::new(RwLock::new(PostgresUserDb::new(pool.clone()))),
            project_db: Arc::new(RwLock::new(PostgresProjectDb::new(pool.clone()))),
            workflow_registry: Arc::new(RwLock::new(PostgresWorkflowRegistry::new(pool.clone()))),
            session: None,
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
                        r#"
                        CREATE TABLE version (
                            version INT
                        );
                        INSERT INTO version VALUES (1);

                        CREATE TABLE users (
                            id UUID PRIMARY KEY,
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
                            PRIMARY KEY (project_id, layer_index)            
                        );
                        
                        CREATE TABLE project_version_plots (
                            plot_index integer NOT NULL,
                            project_id UUID REFERENCES projects(id) ON DELETE CASCADE NOT NULL,
                            project_version_id UUID REFERENCES project_versions(id) ON DELETE CASCADE NOT NULL,                            
                            name character varying (256) NOT NULL,
                            workflow_id UUID NOT NULL, -- TODO: REFERENCES workflows(id)
                            PRIMARY KEY (project_id, plot_index)            
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
                        "#,
                    )
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

    fn user_db(&self) -> Db<Self::UserDB> {
        self.user_db.clone()
    }
    async fn user_db_ref(&self) -> RwLockReadGuard<'_, Self::UserDB> {
        self.user_db.read().await
    }
    async fn user_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::UserDB> {
        self.user_db.write().await
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
    type DatasetDB = PostgresDatasetDb;
    type QueryContext = QueryContextImpl;
    type ExecutionContext = ExecutionContextImpl<UserSession, PostgresDatasetDb>;

    fn project_db(&self) -> Db<Self::ProjectDB> {
        self.project_db.clone()
    }
    async fn project_db_ref(&self) -> RwLockReadGuard<'_, Self::ProjectDB> {
        self.project_db.read().await
    }
    async fn project_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::ProjectDB> {
        self.project_db.write().await
    }

    fn workflow_registry(&self) -> Db<Self::WorkflowRegistry> {
        self.workflow_registry.clone()
    }
    async fn workflow_registry_ref(&self) -> RwLockReadGuard<'_, Self::WorkflowRegistry> {
        self.workflow_registry.read().await
    }
    async fn workflow_registry_ref_mut(&self) -> RwLockWriteGuard<'_, Self::WorkflowRegistry> {
        self.workflow_registry.write().await
    }

    fn dataset_db(&self) -> Db<Self::DatasetDB> {
        todo!()
    }

    async fn dataset_db_ref(&self) -> RwLockReadGuard<'_, Self::DatasetDB> {
        todo!()
    }

    async fn dataset_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::DatasetDB> {
        todo!()
    }

    fn query_context(&self) -> Result<Self::QueryContext> {
        todo!()
    }

    fn execution_context(&self, _session: UserSession) -> Result<Self::ExecutionContext> {
        todo!()
    }

    async fn session_by_id(&self, session_id: crate::contexts::SessionId) -> Result<Self::Session> {
        self.user_db_ref()
            .await
            .session(session_id)
            .await
            .map_err(Box::new)
            .context(error::Authorization)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pro::projects::{LoadVersion, ProProjectDb, UserProjectPermission};
    use crate::pro::users::{UserCredentials, UserDb, UserRegistration};
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
    use geoengine_datatypes::primitives::Coordinate2D;
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
    use geoengine_operators::engine::{
        MultipleRasterSources, PlotOperator, TypedOperator, VectorOperator,
    };
    use geoengine_operators::mock::{MockPointSource, MockPointSourceParams};
    use geoengine_operators::plot::{Statistics, StatisticsParams};
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
        F: FnOnce(PostgresContext<NoTls>) -> Fut + std::panic::UnwindSafe + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let (pg_config, schema) = setup_db().await;

        // catch all panics and clean up first…
        let executed_fn = {
            let pg_config = pg_config.clone();
            std::panic::catch_unwind(move || {
                tokio::task::block_in_place(move || {
                    Handle::current().block_on(async move {
                        let ctx = PostgresContext::new(pg_config, tokio_postgres::NoTls)
                            .await
                            .unwrap();
                        f(ctx).await
                    })
                })
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
        with_temp_context(|ctx| async move {
            anonymous(&ctx).await;

            let _user_id = user_reg_login(&ctx).await;

            let session = ctx
                .user_db()
                .write()
                .await
                .login(UserCredentials {
                    email: "foo@bar.de".into(),
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

    async fn set_session(ctx: &PostgresContext<NoTls>, projects: &[ProjectListing]) {
        let credentials = UserCredentials {
            email: "foo@bar.de".into(),
            password: "secret123".into(),
        };

        let mut user_db = ctx.user_db_ref_mut().await;

        let session = user_db.login(credentials).await.unwrap();

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
        ctx.project_db_ref_mut()
            .await
            .delete(session, project_id)
            .await
            .unwrap();

        assert!(ctx
            .project_db_ref()
            .await
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
                .await
                .list_permissions(session, project_id)
                .await
                .unwrap()
                .len(),
            1
        );

        let user2 = ctx
            .user_db_ref_mut()
            .await
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

        ctx.project_db_ref_mut()
            .await
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
                .await
                .list_permissions(session, project_id)
                .await
                .unwrap()
                .len(),
            2
        );
    }

    async fn update_projects(
        ctx: &PostgresContext<NoTls>,
        session: &UserSession,
        project_id: ProjectId,
    ) {
        let project = ctx
            .project_db_ref_mut()
            .await
            .load_version(session, project_id, LoadVersion::Latest)
            .await
            .unwrap();

        let layer_workflow_id = ctx
            .workflow_registry_ref_mut()
            .await
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
            .await
            .load(&layer_workflow_id)
            .await
            .is_ok());

        let plot_workflow_id = ctx
            .workflow_registry_ref_mut()
            .await
            .register(Workflow {
                operator: Statistics {
                    params: StatisticsParams {},
                    sources: MultipleRasterSources { rasters: vec![] },
                }
                .boxed()
                .into(),
            })
            .await
            .unwrap();

        assert!(ctx
            .workflow_registry_ref()
            .await
            .load(&plot_workflow_id)
            .await
            .is_ok());

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
        ctx.project_db_ref_mut()
            .await
            .update(session, update.validated().unwrap())
            .await
            .unwrap();

        let versions = ctx
            .project_db_ref()
            .await
            .versions(session, project_id)
            .await
            .unwrap();
        assert_eq!(versions.len(), 2);
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
        let projects = ctx
            .project_db_ref_mut()
            .await
            .list(session, options)
            .await
            .unwrap();

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
            ctx.project_db_ref_mut()
                .await
                .create(session, create)
                .await
                .unwrap();
        }
    }

    async fn user_reg_login(ctx: &PostgresContext<NoTls>) -> UserId {
        let user_db = ctx.user_db();
        let mut db = user_db.write().await;

        let user_registration = UserRegistration {
            email: "foo@bar.de".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        let user_id = db.register(user_registration).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".into(),
            password: "secret123".into(),
        };

        let session = db.login(credentials).await.unwrap();

        db.session(session.id).await.unwrap();

        db.logout(session.id).await.unwrap();

        assert!(db.session(session.id).await.is_err());

        user_id
    }

    async fn anonymous(ctx: &PostgresContext<NoTls>) {
        let user_db = ctx.user_db();
        let mut db = user_db.write().await;

        let session = db.anonymous().await.unwrap();

        let session = db.session(session.id).await.unwrap();

        db.logout(session.id).await.unwrap();

        assert!(db.session(session.id).await.is_err());
    }
}
