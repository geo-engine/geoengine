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
                        println!("UserDB: Uninitialized schema");
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

                        -- TODO: unique constraint poject_id, lates = true?
                        -- TODO: index on latest


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

                        -- TODO: indexes
                        ",
                    )
                    .await?;
                    // TODO log
                    println!("Updated user database to schema version {}", version + 1);
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
                // println!("Updated user database to schema version {}", version + 1);
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
