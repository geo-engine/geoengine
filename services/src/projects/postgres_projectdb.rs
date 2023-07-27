use crate::contexts::PostgresDb;
use crate::error::{self, Result};

use crate::projects::Plot;
use crate::projects::ProjectLayer;
use crate::projects::{
    CreateProject, Project, ProjectDb, ProjectId, ProjectListOptions, ProjectListing,
    ProjectVersion, ProjectVersionId, UpdateProject,
};

use crate::util::Identifier;
use crate::workflows::workflow::WorkflowId;
use async_trait::async_trait;
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::{
    tokio_postgres::tls::MakeTlsConnect, tokio_postgres::tls::TlsConnect, tokio_postgres::Socket,
};
use snafu::{ensure, ResultExt};

use bb8_postgres::bb8::PooledConnection;
use bb8_postgres::tokio_postgres::Transaction;

use super::LoadVersion;

async fn list_plots<Tls>(
    conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
    project_version_id: &ProjectVersionId,
) -> Result<Vec<String>>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
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

async fn load_plots<Tls>(
    conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
    project_version_id: &ProjectVersionId,
) -> Result<Vec<Plot>>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
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

#[async_trait]
impl<Tls> ProjectDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn list_projects(&self, options: ProjectListOptions) -> Result<Vec<ProjectListing>> {
        // TODO: project filters

        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(&format!(
                "
        SELECT p.id, p.project_id, p.name, p.description, p.changed
        FROM project_versions p
        WHERE
            p.changed >= ALL (SELECT changed FROM project_versions WHERE project_id = p.project_id)
        ORDER BY p.{}
        LIMIT $1
        OFFSET $2;",
                options.order.to_sql_string()
            ))
            .await?;

        let project_rows = conn
            .query(
                &stmt,
                &[&i64::from(options.limit), &i64::from(options.offset)],
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
                plot_names: list_plots(&conn, &project_version_id).await?,
                changed,
            });
        }
        Ok(project_listings)
    }

    async fn create_project(&self, create: CreateProject) -> Result<ProjectId> {
        let mut conn = self.conn_pool.get().await?;

        let project: Project = Project::from_create_project(create);

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
                    changed)
                    VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP);",
            )
            .await?;

        let version_id = ProjectVersionId::new();

        trans
            .execute(
                &stmt,
                &[
                    &version_id,
                    &project.id,
                    &project.name,
                    &project.description,
                    &project.bounds,
                    &project.time_step,
                ],
            )
            .await?;

        trans.commit().await?;

        Ok(project.id)
    }

    async fn load_project(&self, project: ProjectId) -> Result<Project> {
        self.load_project_version(project, LoadVersion::Latest)
            .await
    }

    #[allow(clippy::too_many_lines)]
    async fn update_project(&self, update: UpdateProject) -> Result<()> {
        let update = update;

        let mut conn = self.conn_pool.get().await?;

        let trans = conn.build_transaction().start().await?;

        let project = self.load_project(update.id).await?; // TODO: move inside transaction?

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
                    changed)
                VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP);",
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

        update_plots(&trans, &project.id, &project.version.id, &project.plots).await?;

        trans.commit().await?;

        Ok(())
    }

    async fn delete_project(&self, project: ProjectId) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn.prepare("DELETE FROM projects WHERE id = $1;").await?;

        let rows_affected = conn.execute(&stmt, &[&project]).await?;

        ensure!(rows_affected == 1, error::ProjectDeleteFailed);

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn load_project_version(
        &self,
        project: ProjectId,
        version: LoadVersion,
    ) -> Result<Project> {
        let conn = self.conn_pool.get().await?;

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
                p.changed
            FROM 
                project_versions p
            WHERE p.project_id = $1 AND p.id = $2",
                )
                .await?;

            conn.query_one(&stmt, &[&project, &version]).await?
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
                p.changed
            FROM 
                project_versions p
            WHERE project_id = $1 AND p.changed >= ALL(
                SELECT changed FROM project_versions WHERE project_id = $1
            )",
                )
                .await?;

            conn.query_one(&stmt, &[&project]).await?
        };

        let project_id = ProjectId(row.get(0));
        let version_id = ProjectVersionId(row.get(1));
        let name = row.get(2);
        let description = row.get(3);
        let bounds = row.get(4);
        let time_step = row.get(5);
        let changed = row.get(6);

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
            layers.push(ProjectLayer {
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
            plots: load_plots(&conn, &version_id).await?,
            bounds,
            time_step,
        })
    }

    async fn list_project_versions(&self, project: ProjectId) -> Result<Vec<ProjectVersion>> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
                SELECT 
                    id, changed
                FROM 
                    project_versions
                WHERE 
                    project_id = $1 
                ORDER BY 
                    changed DESC",
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
}
