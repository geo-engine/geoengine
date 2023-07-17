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
use geoengine_datatypes::primitives::DateTime;
use snafu::ResultExt;

use bb8_postgres::bb8::PooledConnection;
use bb8_postgres::tokio_postgres::Transaction;

async fn list_plots<Tls>(
    conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
    project_id: &ProjectId,
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
                    FROM project_plots
                    WHERE project_id = $1;
                ",
        )
        .await?;

    let plot_rows = conn.query(&stmt, &[project_id]).await?;
    let plot_names = plot_rows.iter().map(|row| row.get(0)).collect();

    Ok(plot_names)
}

async fn load_plots<Tls>(
    conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
    project_id: &ProjectId,
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
                FROM project_plots
                WHERE project_id = $1
                ORDER BY plot_index ASC
                ",
        )
        .await?;

    let rows = conn.query(&stmt, &[project_id]).await?;

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
    plots: &[Plot],
) -> Result<()> {
    trans
        .execute(
            "DELETE FROM project_plots WHERE project_id = $1;",
            &[&project_id],
        )
        .await?;

    for (idx, plot) in plots.iter().enumerate() {
        let stmt = trans
            .prepare(
                "
                INSERT INTO project_plots (
                    project_id,
                    plot_index,
                    name,
                    workflow_id)
                VALUES ($1, $2, $3, $);
                ",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[project_id, &(idx as i32), &plot.name, &plot.workflow],
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
        SELECT id, name, description
        FROM projects
        ORDER BY {}
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
            let project_id = ProjectId(project_row.get(0));
            let name = project_row.get(1);
            let description = project_row.get(2);
            let changed = project_row.get(3);

            let stmt = conn
                .prepare(
                    "
                    SELECT name
                    FROM project_layers
                    WHERE project_id = $1;",
                )
                .await?;

            let layer_rows = conn.query(&stmt, &[&project_id]).await?;
            let layer_names = layer_rows.iter().map(|row| row.get(0)).collect();

            project_listings.push(ProjectListing {
                id: project_id,
                name,
                description,
                layer_names,
                plot_names: list_plots(&conn, &project_id).await?,
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
            .prepare(
                "INSERT INTO projects (
                    id,
                    name,
                    description,
                    bounds,
                    time_step,
                    changed)
                    VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP);",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[
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
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            SELECT 
                id, 
                name, 
                description,
                bounds,
                time_step
            FROM projects
            WHERE id = $1",
            )
            .await?;

        let row = conn.query_one(&stmt, &[&project]).await?;

        let project_id = ProjectId(row.get(0));
        let name = row.get(1);
        let description = row.get(2);
        let bounds = row.get(3);
        let time_step = row.get(4);

        let stmt = conn
            .prepare(
                "
        SELECT  
            name, workflow_id, symbology, visibility
        FROM project_layers
        WHERE project_id = $1
        ORDER BY layer_index ASC",
            )
            .await?;

        let rows = conn.query(&stmt, &[&project_id]).await?;

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
                id: ProjectVersionId::new(),
                changed: DateTime::now(),
            }, // TODO: remove
            name,
            description,
            layers,
            plots: load_plots(&conn, &project_id).await?,
            bounds,
            time_step,
        })
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
                UPDATE 
                    projects 
                SET 
                    name = $1,
                    description = $2,
                    bounds = $3,
                    time_step = $4,
                    changed = CURRENT_TIMESTAMP
                WHERE
                    id = $5;",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[
                    &project.name,
                    &project.description,
                    &project.bounds,
                    &project.time_step,
                    &project.id,
                ],
            )
            .await?;

        trans
            .execute(
                "DELETE FROM project_layers WHERE project_id = $1;",
                &[&project.id],
            )
            .await?;

        for (idx, layer) in project.layers.iter().enumerate() {
            let stmt = trans
                .prepare(
                    "
            INSERT INTO project_layers (
                project_id,
                layer_index,
                name,
                workflow_id,
                symbology,
                visibility)
            VALUES ($1, $2, $3, $4, $5, $6);",
                )
                .await?;

            let symbology = serde_json::to_value(&layer.symbology).context(error::SerdeJson)?;

            trans
                .execute(
                    &stmt,
                    &[
                        &project.id,
                        &(idx as i32),
                        &layer.name,
                        &layer.workflow,
                        &symbology,
                        &layer.visibility,
                    ],
                )
                .await?;
        }

        update_plots(&trans, &project.id, &project.plots).await?;

        trans.commit().await?;

        Ok(())
    }

    async fn delete_project(&self, project: ProjectId) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn.prepare("DELETE FROM projects WHERE id = $1;").await?;

        conn.execute(&stmt, &[&project]).await?;

        Ok(())
    }
}
