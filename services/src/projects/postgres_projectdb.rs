use crate::contexts::PostgresDb;
use crate::projects::error::ProjectNotFoundProjectDbError;
use crate::projects::Plot;
use crate::projects::ProjectLayer;
use crate::projects::{
    CreateProject, Project, ProjectDb, ProjectId, ProjectListOptions, ProjectListing,
    ProjectVersion, ProjectVersionId, UpdateProject,
};

use super::error::Bb8ProjectDbError;
use super::error::PostgresProjectDbError;
use super::error::ProjectDbError;
use super::LoadVersion;
use crate::util::Identifier;
use crate::workflows::workflow::WorkflowId;
use async_trait::async_trait;
use bb8_postgres::tokio_postgres::Transaction;
use bb8_postgres::{
    tokio_postgres::tls::MakeTlsConnect, tokio_postgres::tls::TlsConnect, tokio_postgres::Socket,
};
use snafu::ensure;
use snafu::ResultExt;
use tokio_postgres::Row;

pub async fn list_plots(
    trans: &Transaction<'_>,
    project_version_id: &ProjectVersionId,
) -> Result<Vec<String>, ProjectDbError> {
    let stmt: tokio_postgres::Statement = trans
        .prepare(
            "
                    SELECT name
                    FROM project_version_plots
                    WHERE project_version_id = $1;
                ",
        )
        .await
        .context(PostgresProjectDbError)?;

    let plot_rows = trans
        .query(&stmt, &[project_version_id])
        .await
        .context(PostgresProjectDbError)?;
    let plot_names = plot_rows.iter().map(|row| row.get(0)).collect();

    Ok(plot_names)
}

pub async fn load_plots(
    trans: &Transaction<'_>,
    project_version_id: &ProjectVersionId,
) -> Result<Vec<Plot>, ProjectDbError> {
    let stmt = trans
        .prepare(
            "
                SELECT  
                    name, workflow_id
                FROM project_version_plots
                WHERE project_version_id = $1
                ORDER BY plot_index ASC
                ",
        )
        .await
        .context(PostgresProjectDbError)?;

    let rows = trans
        .query(&stmt, &[project_version_id])
        .await
        .context(PostgresProjectDbError)?;

    let plots = rows
        .into_iter()
        .map(|row| Plot {
            workflow: WorkflowId(row.get(1)),
            name: row.get(0),
        })
        .collect();

    Ok(plots)
}

pub async fn update_plots(
    trans: &Transaction<'_>,
    project_id: &ProjectId,
    project_version_id: &ProjectVersionId,
    plots: &[Plot],
) -> Result<(), ProjectDbError> {
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
            .await
            .context(PostgresProjectDbError)?;

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
            .await
            .context(PostgresProjectDbError)?;
    }

    Ok(())
}

pub async fn project_listings_from_rows(
    tx: &tokio_postgres::Transaction<'_>,
    project_rows: Vec<Row>,
) -> Result<Vec<ProjectListing>, ProjectDbError> {
    let mut project_listings = vec![];
    for project_row in project_rows {
        let project_version_id = ProjectVersionId(project_row.get(0));
        let project_id = ProjectId(project_row.get(1));
        let name = project_row.get(2);
        let description = project_row.get(3);
        let changed = project_row.get(4);

        let stmt = tx
            .prepare(
                "
                SELECT name
                FROM project_version_layers
                WHERE project_version_id = $1;",
            )
            .await
            .context(PostgresProjectDbError)?;

        let layer_rows = tx
            .query(&stmt, &[&project_version_id])
            .await
            .context(PostgresProjectDbError)?;
        let layer_names = layer_rows.iter().map(|row| row.get(0)).collect();

        project_listings.push(ProjectListing {
            id: project_id,
            name,
            description,
            layer_names,
            plot_names: list_plots(tx, &project_version_id).await?,
            changed,
        });
    }
    Ok(project_listings)
}

pub async fn insert_project(
    trans: &Transaction<'_>,
    project: &Project,
) -> Result<ProjectVersionId, ProjectDbError> {
    let stmt = trans
        .prepare("INSERT INTO projects (id) VALUES ($1);")
        .await
        .context(PostgresProjectDbError)?;

    trans
        .execute(&stmt, &[&project.id])
        .await
        .context(PostgresProjectDbError)?;

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
        .await
        .context(PostgresProjectDbError)?;

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
        .await
        .context(PostgresProjectDbError)?;

    Ok(version_id)
}

pub async fn update_project(
    trans: &Transaction<'_>,
    project: &Project,
    update: UpdateProject,
) -> Result<Project, ProjectDbError> {
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
        .await
        .context(PostgresProjectDbError)?;

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
        .await
        .context(PostgresProjectDbError)?;

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
            .await
            .context(PostgresProjectDbError)?;

        trans
            .execute(
                &stmt,
                &[
                    &project.id,
                    &project.version.id,
                    &(idx as i32),
                    &layer.name,
                    &layer.workflow,
                    &layer.symbology,
                    &layer.visibility,
                ],
            )
            .await
            .context(PostgresProjectDbError)?;
    }

    update_plots(trans, &project.id, &project.version.id, &project.plots).await?;

    Ok(project)
}

#[async_trait]
impl<Tls> ProjectDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn list_projects(
        &self,
        options: ProjectListOptions,
    ) -> Result<Vec<ProjectListing>, ProjectDbError> {
        // TODO: project filters

        let mut conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;
        let trans = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresProjectDbError)?;

        let stmt = trans
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
            .await
            .context(PostgresProjectDbError)?;

        let project_rows = trans
            .query(
                &stmt,
                &[&i64::from(options.limit), &i64::from(options.offset)],
            )
            .await
            .context(PostgresProjectDbError)?;

        let project_listings = project_listings_from_rows(&trans, project_rows).await?;

        trans.commit().await.context(PostgresProjectDbError)?;

        Ok(project_listings)
    }

    async fn create_project(&self, create: CreateProject) -> Result<ProjectId, ProjectDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;

        let project: Project = Project::from_create_project(create);

        let trans = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresProjectDbError)?;

        insert_project(&trans, &project).await?;

        trans.commit().await.context(PostgresProjectDbError)?;

        Ok(project.id)
    }

    async fn load_project(&self, project: ProjectId) -> Result<Project, ProjectDbError> {
        self.load_project_version(project, LoadVersion::Latest)
            .await
    }

    #[allow(clippy::too_many_lines)]
    async fn update_project(&self, update: UpdateProject) -> Result<(), ProjectDbError> {
        let update = update;

        let mut conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;

        let trans = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresProjectDbError)?;

        let project = self.load_project(update.id).await?; // TODO: move inside transaction?

        update_project(&trans, &project, update).await?;

        trans.commit().await.context(PostgresProjectDbError)?;

        Ok(())
    }

    async fn delete_project(&self, project: ProjectId) -> Result<(), ProjectDbError> {
        let conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;

        let stmt = conn
            .prepare("DELETE FROM projects WHERE id = $1;")
            .await
            .context(PostgresProjectDbError)?;

        let rows_affected = conn
            .execute(&stmt, &[&project])
            .await
            .context(PostgresProjectDbError)?;

        ensure!(
            rows_affected == 1,
            ProjectNotFoundProjectDbError { project }
        );

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn load_project_version(
        &self,
        project: ProjectId,
        version: LoadVersion,
    ) -> Result<Project, ProjectDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;

        let trans = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresProjectDbError)?;

        let rows = if let LoadVersion::Version(version) = version {
            let stmt = trans
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
                .await
                .context(PostgresProjectDbError)?;

            let rows = trans
                .query(&stmt, &[&project, &version])
                .await
                .context(PostgresProjectDbError)?;

            if rows.is_empty() {
                return Err(ProjectDbError::ProjectVersionNotFound { project, version });
            }

            rows
        } else {
            let stmt = trans
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
                .await
                .context(PostgresProjectDbError)?;

            let rows = trans
                .query(&stmt, &[&project])
                .await
                .context(PostgresProjectDbError)?;

            if rows.is_empty() {
                return Err(ProjectDbError::ProjectNotFound { project });
            }

            rows
        };

        let row = &rows[0];

        let project_id = ProjectId(row.get(0));
        let version_id = ProjectVersionId(row.get(1));
        let name = row.get(2);
        let description = row.get(3);
        let bounds = row.get(4);
        let time_step = row.get(5);
        let changed = row.get(6);

        let stmt = trans
            .prepare(
                "
        SELECT  
            name, workflow_id, symbology, visibility
        FROM project_version_layers
        WHERE project_version_id = $1
        ORDER BY layer_index ASC",
            )
            .await
            .context(PostgresProjectDbError)?;

        let rows = trans
            .query(&stmt, &[&version_id])
            .await
            .context(PostgresProjectDbError)?;

        let mut layers = vec![];
        for row in rows {
            layers.push(ProjectLayer {
                workflow: WorkflowId(row.get(1)),
                name: row.get(0),
                symbology: row.get(2),
                visibility: row.get(3),
            });
        }

        let project = Project {
            id: project_id,
            version: ProjectVersion {
                id: version_id,
                changed,
            },
            name,
            description,
            layers,
            plots: load_plots(&trans, &version_id).await?,
            bounds,
            time_step,
        };

        trans.commit().await.context(PostgresProjectDbError)?;

        Ok(project)
    }

    async fn list_project_versions(
        &self,
        project: ProjectId,
    ) -> Result<Vec<ProjectVersion>, ProjectDbError> {
        let conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;

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
            .await
            .context(PostgresProjectDbError)?;

        let rows = conn
            .query(&stmt, &[&project])
            .await
            .context(PostgresProjectDbError)?;

        Ok(rows
            .iter()
            .map(|row| ProjectVersion {
                id: ProjectVersionId(row.get(0)),
                changed: row.get(1),
            })
            .collect())
    }
}
