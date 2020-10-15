use crate::error::{self, Result};
use crate::{
    projects::postgres_projectdb::PostgresProjectDB, users::postgres_userdb::PostgresUserDB,
    users::session::Session, workflows::postgres_workflow_registry::PostgresWorkflowRegistry,
};
use async_trait::async_trait;
use bb8_postgres::{
    bb8::Pool,
    bb8::PooledConnection,
    tokio_postgres::{error::SqlState, tls::MakeTlsConnect, tls::TlsConnect, Config, Socket},
    PostgresConnectionManager,
};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::{Context, DB};

/// A contex with references to Postgres backends of the dbs. Automatically migrates schema on instantiation
#[derive(Clone)]
pub struct PostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    user_db: DB<PostgresUserDB<Tls>>,
    project_db: DB<PostgresProjectDB<Tls>>,
    workflow_registry: DB<PostgresWorkflowRegistry<Tls>>,
    session: Option<Session>,
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
            user_db: Arc::new(RwLock::new(PostgresUserDB::new(pool.clone()))),
            project_db: Arc::new(RwLock::new(PostgresProjectDB::new(pool.clone()))),
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
                        // TODO: log
                        eprintln!("UserDB: Uninitialized schema");
                        return Ok(0);
                    }
                }
                return Err(error::Error::TokioPostgres { source: e });
            }
        };

        let row = conn.query_one(&stmt, &[]).await?;

        Ok(row.get(0))
    }

    async fn update_schema(
        conn: PooledConnection<'_, PostgresConnectionManager<Tls>>,
    ) -> Result<()> {
        let mut version = Self::schema_version(&conn).await?;

        loop {
            match version {
                0 => {
                    conn.batch_execute(
                        "\
                        -- CREATE EXTENSION postgis;

                        CREATE TABLE version (
                            version INT
                        );
                        INSERT INTO version VALUES (1);

                        CREATE TABLE users (
                            id UUID PRIMARY KEY,
                            email character varying (256) UNIQUE NOT NULL,
                            password_hash character varying (256) NOT NULL,
                            real_name character varying (256) NOT NULL,
                            active boolean NOT NULL
                        );

                        CREATE TABLE sessions (
                            id UUID PRIMARY KEY,
                            user_id UUID REFERENCES users(id)
                        );

                        CREATE TABLE projects (
                            id UUID PRIMARY KEY
                        );

                        CREATE TABLE project_versions (
                            id UUID PRIMARY KEY,
                            project_id UUID REFERENCES projects(id) ON DELETE CASCADE NOT NULL,
                            name character varying (256) NOT NULL,
                            description text NOT NULL,
                            view_ll_x double precision NOT NULL,
                            view_ll_y double precision NOT NULL,
                            view_ur_x double precision NOT NULL,
                            view_ur_y double precision NOT NULL,
                            view_t1 timestamp without time zone NOT NULL,
                            view_t2 timestamp without time zone  NOT NULL,
                            bounds_ll_x double precision NOT NULL,
                            bounds_ll_y double precision NOT NULL,
                            bounds_ur_x double precision NOT NULL,
                            bounds_ur_y double precision NOT NULL,
                            bounds_t1 timestamp without time zone NOT NULL,
                            bounds_t2 timestamp without time zone  NOT NULL,
                            time timestamp without time zone,
                            author_user_id UUID REFERENCES users(id) NOT NULL,
                            latest boolean
                        );

                        CREATE INDEX project_version_latest_idx 
                        ON project_versions (project_id, latest DESC, time DESC, author_user_id DESC);

                        CREATE TYPE layer_type AS ENUM ('raster', 'vector'); -- TODO: distinguish points/lines/polygons

                        CREATE TABLE project_version_layers (
                            layer_index integer NOT NULL,
                            project_id UUID REFERENCES projects(id) ON DELETE CASCADE NOT NULL,
                            project_version_id UUID REFERENCES project_versions(id) ON DELETE CASCADE NOT NULL,                            
                            layer_type layer_type NOT NULL,
                            name character varying (256) NOT NULL,
                            workflow_id UUID NOT NULL, -- TODO: REFERENCES workflows(id)
                            raster_colorizer json,
                            PRIMARY KEY (project_id, layer_index)            
                        );

                        CREATE TYPE project_permission AS ENUM ('read', 'write', 'owner');

                        CREATE TABLE user_project_permissions (
                            user_id UUID REFERENCES users(id) NOT NULL,
                            project_id UUID REFERENCES projects(id) ON DELETE CASCADE NOT NULL,
                            permission project_permission NOT NULL,
                            PRIMARY KEY (user_id, project_id)
                        );

                        CREATE TABLE workflows (
                            id UUID PRIMARY KEY,
                            workflow json NOT NULL
                        );
                        ",
                    )
                    .await?;
                    // TODO log
                    eprintln!("Updated user database to schema version {}", version + 1);
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
impl<Tls> Context for PostgresContext<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type UserDB = PostgresUserDB<Tls>;
    type ProjectDB = PostgresProjectDB<Tls>;
    type WorkflowRegistry = PostgresWorkflowRegistry<Tls>;

    fn user_db(&self) -> DB<Self::UserDB> {
        self.user_db.clone()
    }
    async fn user_db_ref(&self) -> RwLockReadGuard<'_, Self::UserDB> {
        self.user_db.read().await
    }
    async fn user_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::UserDB> {
        self.user_db.write().await
    }

    fn project_db(&self) -> DB<Self::ProjectDB> {
        self.project_db.clone()
    }
    async fn project_db_ref(&self) -> RwLockReadGuard<'_, Self::ProjectDB> {
        self.project_db.read().await
    }
    async fn project_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::ProjectDB> {
        self.project_db.write().await
    }

    fn workflow_registry(&self) -> DB<Self::WorkflowRegistry> {
        self.workflow_registry.clone()
    }
    async fn workflow_registry_ref(&self) -> RwLockReadGuard<'_, Self::WorkflowRegistry> {
        self.workflow_registry.read().await
    }
    async fn workflow_registry_ref_mut(&self) -> RwLockWriteGuard<'_, Self::WorkflowRegistry> {
        self.workflow_registry.write().await
    }

    fn session(&self) -> Result<&Session> {
        self.session
            .as_ref()
            .ok_or(error::Error::SessionNotInitialized)
    }

    fn set_session(&mut self, session: Session) {
        self.session = Some(session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projects::project::{
        CreateProject, Layer, LayerInfo, LoadVersion, OrderBy, ProjectFilter, ProjectId,
        ProjectListOptions, ProjectListing, ProjectPermission, STRectangle, UpdateProject,
        UserProjectPermission, VectorInfo,
    };
    use crate::projects::projectdb::ProjectDB;
    use crate::users::user::{UserCredentials, UserId, UserRegistration};
    use crate::users::userdb::UserDB;
    use crate::util::user_input::UserInput;
    use crate::workflows::registry::WorkflowRegistry;
    use crate::workflows::workflow::Workflow;
    use bb8_postgres::tokio_postgres;
    use bb8_postgres::tokio_postgres::NoTls;
    use geoengine_datatypes::primitives::Coordinate2D;
    use geoengine_operators::engine::{TypedOperator, VectorOperator};
    use geoengine_operators::mock::{MockPointSource, MockPointSourceParams};
    use std::str::FromStr;

    #[ignore]
    #[tokio::test]
    async fn test() {
        // TODO: load from test config
        let config = tokio_postgres::config::Config::from_str(
            "postgresql://geoengine:geoengine@localhost:5432",
        )
        .unwrap();

        let ctx = PostgresContext::new(config, tokio_postgres::NoTls)
            .await
            .unwrap();

        let user_id = user_reg_login(&ctx).await;

        create_projects(&ctx, user_id).await;

        let projects = list_projects(&ctx, user_id).await;

        let project_id = projects[0].id;

        update_projects(&ctx, user_id, project_id).await;

        add_permission(&ctx, user_id, project_id).await;

        delete_project(ctx, user_id, project_id).await;
    }

    async fn delete_project(ctx: PostgresContext<NoTls>, user_id: UserId, project_id: ProjectId) {
        ctx.project_db_ref_mut()
            .await
            .delete(user_id, project_id)
            .await
            .unwrap();

        assert!(ctx
            .project_db_ref()
            .await
            .load_latest(user_id, project_id)
            .await
            .is_err());
    }

    async fn add_permission(ctx: &PostgresContext<NoTls>, user_id: UserId, project_id: ProjectId) {
        assert_eq!(
            ctx.project_db_ref()
                .await
                .list_permissions(user_id, project_id)
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
                user_id,
                UserProjectPermission {
                    user: user2,
                    project: project_id,
                    permission: ProjectPermission::Read,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            ctx.project_db_ref()
                .await
                .list_permissions(user_id, project_id)
                .await
                .unwrap()
                .len(),
            2
        );
    }

    async fn update_projects(ctx: &PostgresContext<NoTls>, user_id: UserId, project_id: ProjectId) {
        let project = ctx
            .project_db_ref_mut()
            .await
            .load(user_id, project_id, LoadVersion::Latest)
            .await
            .unwrap();

        let workflow_id = ctx
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
            .load(&workflow_id)
            .await
            .is_ok());

        let update = UpdateProject {
            id: project.id,
            name: Some("Test9 Updated".into()),
            description: None,
            layers: Some(vec![Some(Layer {
                workflow: workflow_id,
                name: "TestLayer".into(),
                info: LayerInfo::Vector(VectorInfo {}),
            })]),
            view: None,
            bounds: None,
        };
        ctx.project_db_ref_mut()
            .await
            .update(user_id, update.validated().unwrap())
            .await
            .unwrap();

        let versions = ctx
            .project_db_ref()
            .await
            .versions(user_id, project_id)
            .await
            .unwrap();
        assert_eq!(versions.len(), 2);
    }

    async fn list_projects(ctx: &PostgresContext<NoTls>, user_id: UserId) -> Vec<ProjectListing> {
        let options = ProjectListOptions {
            permissions: vec![
                ProjectPermission::Owner,
                ProjectPermission::Write,
                ProjectPermission::Read,
            ],
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
            .list(user_id, options)
            .await
            .unwrap();

        assert_eq!(projects.len(), 2);
        assert_eq!(projects[0].name, "Test9");
        assert_eq!(projects[1].name, "Test8");
        projects
    }

    async fn create_projects(ctx: &PostgresContext<NoTls>, user_id: UserId) {
        for i in 0..10 {
            let create = CreateProject {
                name: format!("Test{}", i),
                description: format!("Test{}", 10 - i),
                view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
                bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
            }
            .validated()
            .unwrap();
            ctx.project_db_ref_mut()
                .await
                .create(user_id, create)
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

        let result = db.login(credentials).await;
        assert!(result.is_ok());

        let session = result.unwrap();

        assert!(db.session(session.id).await.is_ok());

        assert!(db.logout(session.id).await.is_ok());

        assert!(db.session(session.id).await.is_err());

        user_id
    }
}
