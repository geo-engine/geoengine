use crate::pro::contexts::PostgresContext;
use crate::pro::users::UserId;
use crate::pro::users::UserSession;
use crate::projects::Layer;
use crate::projects::Plot;
use crate::projects::{
    CreateProject, Project, ProjectDb, ProjectId, ProjectListOptions, ProjectListing,
    ProjectVersion, ProjectVersionId, UpdateProject,
};
use crate::util::user_input::Validated;
use crate::util::Identifier;
use crate::workflows::workflow::WorkflowId;
use crate::{
    error::{self, Result},
    pro::projects::projectdb::ProjectPermission,
};
use async_trait::async_trait;
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::{
    bb8::Pool, tokio_postgres::tls::MakeTlsConnect, tokio_postgres::tls::TlsConnect,
    tokio_postgres::Socket,
};
use snafu::ResultExt;

use bb8_postgres::bb8::PooledConnection;
use bb8_postgres::tokio_postgres::Transaction;

use super::LoadVersion;
use super::ProProjectDb;
use super::UserProjectPermission;

pub struct PostgresProjectDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    conn_pool: Pool<PostgresConnectionManager<Tls>>,
}

impl<Tls> PostgresProjectDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(conn_pool: Pool<PostgresConnectionManager<Tls>>) -> Self {
        Self { conn_pool }
    }

    async fn list_plots(
        &self,
        conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
        project_version_id: &ProjectVersionId,
    ) -> Result<Vec<String>> {
        let stmt = conn
            .prepare(
                "
                    SELECT name
                    FROM project_version_plots
                    WHERE project_version_id = $1;
                ",
            )
            .await?;

        let plot_rows = conn.query(&stmt, &[project_version_id]).await?;
        let plot_names = plot_rows.iter().map(|row| row.get(0)).collect();

        Ok(plot_names)
    }

    async fn load_plots(
        &self,
        conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
        project_version_id: &ProjectVersionId,
    ) -> Result<Vec<Plot>> {
        let stmt = conn
            .prepare(
                "
                SELECT  
                    name, workflow_id
                FROM project_version_plots
                WHERE project_version_id = $1
                ORDER BY plot_index ASC
                ",
            )
            .await?;

        let rows = conn.query(&stmt, &[project_version_id]).await?;

        let plots = rows
            .into_iter()
            .map(|row| Plot {
                workflow: WorkflowId(row.get(1)),
                name: row.get(0),
            })
            .collect();

        Ok(plots)
    }

    async fn update_plots(
        &self,
        trans: &Transaction<'_>,
        project_id: &ProjectId,
        project_version_id: &ProjectVersionId,
        plots: &[Plot],
    ) -> Result<()> {
        for (idx, plot) in plots.iter().enumerate() {
            let stmt = trans
                .prepare(
                    "
                    INSERT INTO project_version_plots (
                        project_id,
                        project_version_id,
                        plot_index,
                        name,
                        workflow_id)
                    VALUES ($1, $2, $3, $4, $5);
                    ",
                )
                .await?;

            trans
                .execute(
                    &stmt,
                    &[
                        project_id,
                        project_version_id,
                        &(idx as i32),
                        &plot.name,
                        &plot.workflow,
                    ],
                )
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl<Tls> ProjectDb<UserSession> for PostgresProjectDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn list(
        &self,
        session: &UserSession,
        options: Validated<ProjectListOptions>,
    ) -> Result<Vec<ProjectListing>> {
        // TODO: project filters
        let options = options.user_input;

        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(&format!(
                "
        SELECT p.id, p.project_id, p.name, p.description, p.changed 
        FROM user_project_permissions u JOIN project_versions p ON (u.project_id = p.project_id)
        WHERE
            u.user_id = $1
            AND latest IS TRUE
        ORDER BY p.{}
        LIMIT $2
        OFFSET $3;",
                options.order.to_sql_string()
            ))
            .await?;

        let project_rows = conn
            .query(
                &stmt,
                &[
                    &session.user.id,
                    &i64::from(options.limit),
                    &i64::from(options.offset),
                ],
            )
            .await?;

        let mut project_listings = vec![];
        for project_row in project_rows {
            let project_version_id = ProjectVersionId(project_row.get(0));
            let project_id = ProjectId(project_row.get(1));
            let name = project_row.get(2);
            let description = project_row.get(3);
            let changed = project_row.get(4);

            let stmt = conn
                .prepare(
                    "
                    SELECT name
                    FROM project_version_layers
                    WHERE project_version_id = $1;",
                )
                .await?;

            let layer_rows = conn.query(&stmt, &[&project_version_id]).await?;
            let layer_names = layer_rows.iter().map(|row| row.get(0)).collect();

            project_listings.push(ProjectListing {
                id: project_id,
                name,
                description,
                layer_names,
                plot_names: self.list_plots(&conn, &project_version_id).await?,
                changed,
            });
        }
        Ok(project_listings)
    }

    async fn create(
        &self,
        session: &UserSession,
        create: Validated<CreateProject>,
    ) -> Result<ProjectId> {
        let mut conn = self.conn_pool.get().await?;

        let project: Project = Project::from_create_project(create.user_input);

        let trans = conn.build_transaction().start().await?;

        let stmt = trans
            .prepare("INSERT INTO projects (id) VALUES ($1);")
            .await?;

        trans.execute(&stmt, &[&project.id]).await?;

        let stmt = trans
            .prepare(
                "INSERT INTO project_versions (
                    id,
                    project_id,
                    name,
                    description,
                    bounds,
                    time_step,
                    author_user_id,
                    changed,
                    latest)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, TRUE);",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[
                    &ProjectVersionId::new(),
                    &project.id,
                    &project.name,
                    &project.description,
                    &project.bounds,
                    &project.time_step,
                    &session.user.id,
                ],
            )
            .await?;

        let stmt = trans
            .prepare(
                "INSERT INTO user_project_permissions (user_id, project_id, permission) VALUES ($1, $2, $3);",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[&session.user.id, &project.id, &ProjectPermission::Owner],
            )
            .await?;

        trans.commit().await?;

        Ok(project.id)
    }

    async fn load(&self, session: &UserSession, project: ProjectId) -> Result<Project> {
        self.load_version(session, project, LoadVersion::Latest)
            .await
    }

    #[allow(clippy::too_many_lines)]
    async fn update(&self, session: &UserSession, update: Validated<UpdateProject>) -> Result<()> {
        let update = update.user_input;

        let mut conn = self.conn_pool.get().await?;

        PostgresContext::check_user_project_permission(
            &conn,
            session.user.id,
            update.id,
            &[ProjectPermission::Write, ProjectPermission::Owner],
        )
        .await?;

        let trans = conn.build_transaction().start().await?;

        let project = self.load(session, update.id).await?; // TODO: move inside transaction?

        let stmt = trans
            .prepare("UPDATE project_versions SET latest = FALSE WHERE project_id = $1 AND latest IS TRUE;")
            .await?;
        trans.execute(&stmt, &[&project.id]).await?;

        let project = project.update_project(update)?;

        let stmt = trans
            .prepare(
                "
                INSERT INTO project_versions (
                    id,
                    project_id,
                    name,
                    description,
                    bounds,
                    time_step,
                    author_user_id,
                    changed,
                    latest)
                VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, TRUE);",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[
                    &project.version.id,
                    &project.id,
                    &project.name,
                    &project.description,
                    &project.bounds,
                    &project.time_step,
                    &session.user.id,
                ],
            )
            .await?;

        for (idx, layer) in project.layers.iter().enumerate() {
            let stmt = trans
                .prepare(
                    "
                INSERT INTO project_version_layers (
                    project_id,
                    project_version_id,
                    layer_index,
                    name,
                    workflow_id,
                    symbology,
                    visibility)
                VALUES ($1, $2, $3, $4, $5, $6, $7);",
                )
                .await?;

            let symbology = serde_json::to_value(&layer.symbology).context(error::SerdeJson)?;

            trans
                .execute(
                    &stmt,
                    &[
                        &project.id,
                        &project.version.id,
                        &(idx as i32),
                        &layer.name,
                        &layer.workflow,
                        &symbology,
                        &layer.visibility,
                    ],
                )
                .await?;
        }

        self.update_plots(&trans, &project.id, &project.version.id, &project.plots)
            .await?;

        trans.commit().await?;

        Ok(())
    }

    async fn delete(&self, session: &UserSession, project: ProjectId) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        PostgresContext::check_user_project_permission(
            &conn,
            session.user.id,
            project,
            &[ProjectPermission::Owner],
        )
        .await?;

        let stmt = conn.prepare("DELETE FROM projects WHERE id = $1;").await?;

        conn.execute(&stmt, &[&project]).await?;

        Ok(())
    }
}

#[async_trait]
impl<Tls> ProProjectDb for PostgresProjectDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    #[allow(clippy::too_many_lines)]
    async fn load_version(
        &self,
        session: &UserSession,
        project: ProjectId,
        version: LoadVersion,
    ) -> Result<Project> {
        let conn = self.conn_pool.get().await?;

        PostgresContext::check_user_project_permission(
            &conn,
            session.user.id,
            project,
            &[
                ProjectPermission::Read,
                ProjectPermission::Write,
                ProjectPermission::Owner,
            ],
        )
        .await?;

        let row = if let LoadVersion::Version(version) = version {
            let stmt = conn
                .prepare(
                    "
        SELECT 
            p.project_id, 
            p.id, 
            p.name, 
            p.description,
            p.bounds,
            p.time_step,
            p.changed,
            p.author_user_id
        FROM user_project_permissions u JOIN project_versions p ON (u.project_id = p.project_id)
        WHERE u.user_id = $1 AND u.project_id = $2 AND p.project_version = $3",
                )
                .await?;

            conn.query_one(&stmt, &[&session.user.id, &project, &version])
                .await?
        } else {
            let stmt = conn
                .prepare(
                    "
        SELECT  
            p.project_id, 
            p.id, 
            p.name, 
            p.description,
            p.bounds,
            p.time_step,
            p.changed,
            p.author_user_id
        FROM user_project_permissions u JOIN project_versions p ON (u.project_id = p.project_id)
        WHERE u.user_id = $1 AND u.project_id = $2 AND latest IS TRUE",
                )
                .await?;

            conn.query_one(&stmt, &[&session.user.id, &project]).await?
        };

        let project_id = ProjectId(row.get(0));
        let version_id = ProjectVersionId(row.get(1));
        let name = row.get(2);
        let description = row.get(3);
        let bounds = row.get(4);
        let time_step = row.get(5);
        let changed = row.get(6);
        let _author_id = UserId(row.get(7));

        let stmt = conn
            .prepare(
                "
        SELECT  
            name, workflow_id, symbology, visibility
        FROM project_version_layers
        WHERE project_version_id = $1
        ORDER BY layer_index ASC",
            )
            .await?;

        let rows = conn.query(&stmt, &[&version_id]).await?;

        let mut layers = vec![];
        for row in rows {
            layers.push(Layer {
                workflow: WorkflowId(row.get(1)),
                name: row.get(0),
                symbology: serde_json::from_value(row.get(2)).context(error::SerdeJson)?,
                visibility: row.get(3),
            });
        }

        Ok(Project {
            id: project_id,
            version: ProjectVersion {
                id: version_id,
                changed,
            },
            name,
            description,
            layers,
            plots: self.load_plots(&conn, &version_id).await?,
            bounds,
            time_step,
        })
    }

    async fn versions(
        &self,
        session: &UserSession,
        project: ProjectId,
    ) -> Result<Vec<ProjectVersion>> {
        let conn = self.conn_pool.get().await?;

        PostgresContext::check_user_project_permission(
            &conn,
            session.user.id,
            project,
            &[
                ProjectPermission::Read,
                ProjectPermission::Write,
                ProjectPermission::Owner,
            ],
        )
        .await?;

        let stmt = conn
            .prepare(
                "
                SELECT id, changed, author_user_id
                FROM project_versions WHERE project_id = $1 
                ORDER BY latest DESC, changed DESC, author_user_id DESC",
            )
            .await?;

        let rows = conn.query(&stmt, &[&project]).await?;

        Ok(rows
            .iter()
            .map(|row| ProjectVersion {
                id: ProjectVersionId(row.get(0)),
                changed: row.get(1),
            })
            .collect())
    }

    async fn list_permissions(
        &self,
        session: &UserSession,
        project: ProjectId,
    ) -> Result<Vec<UserProjectPermission>> {
        let conn = self.conn_pool.get().await?;

        PostgresContext::check_user_project_permission(
            &conn,
            session.user.id,
            project,
            &[
                ProjectPermission::Read,
                ProjectPermission::Write,
                ProjectPermission::Owner,
            ],
        )
        .await?;

        let stmt = conn
            .prepare(
                "
        SELECT user_id, project_id, permission FROM user_project_permissions WHERE project_id = $1;",
            )
            .await?;

        let rows = conn.query(&stmt, &[&project]).await?;

        Ok(rows
            .into_iter()
            .map(|row| UserProjectPermission {
                project: ProjectId(row.get(1)),
                permission: row.get(2),
                user: session.user.id,
            })
            .collect())
    }

    async fn add_permission(
        &self,
        session: &UserSession,
        permission: UserProjectPermission,
    ) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        PostgresContext::check_user_project_permission(
            &conn,
            session.user.id,
            permission.project,
            &[ProjectPermission::Owner],
        )
        .await?;

        let stmt = conn
            .prepare(
                "
        INSERT INTO user_project_permissions (user_id, project_id, permission)
        VALUES ($1, $2, $3);",
            )
            .await?;

        conn.execute(
            &stmt,
            &[
                &permission.user,
                &permission.project,
                &permission.permission,
            ],
        )
        .await?;

        Ok(())
    }

    async fn remove_permission(
        &self,
        session: &UserSession,
        permission: UserProjectPermission,
    ) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        PostgresContext::check_user_project_permission(
            &conn,
            session.user.id,
            permission.project,
            &[ProjectPermission::Owner],
        )
        .await?;

        let stmt = conn
            .prepare(
                "
            DELETE FROM user_project_permissions 
            WHERE user_id = $1 AND project_id = $2 AND permission = $3;",
            )
            .await?;

        conn.execute(
            &stmt,
            &[
                &session.user.id,
                &permission.project,
                &permission.permission,
            ],
        )
        .await?;

        Ok(())
    }
}
