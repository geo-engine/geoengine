use super::error::PostgresProjectDbError;
use super::error::ProjectDbError;
use crate::projects::Plot;
use crate::projects::{Project, ProjectId, ProjectListing, ProjectVersionId, UpdateProject};
use crate::util::Identifier;
use crate::workflows::workflow::WorkflowId;
use bb8_postgres::tokio_postgres::Transaction;
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
