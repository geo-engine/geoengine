use crate::error::Result;
use crate::permissions::Permission;
use crate::permissions::TxPermissionDb;
use crate::pro::contexts::PostgresDb;
use crate::pro::users::UserId;
use crate::projects::error::ProjectNotFoundProjectDbError;
use crate::projects::error::{
    AccessFailedProjectDbError, Bb8ProjectDbError, PostgresProjectDbError, ProjectDbError,
};
use crate::projects::postgres_projectdb::{
    insert_project, load_plots, project_listings_from_rows, update_project,
};
use crate::projects::LoadVersion;
use crate::projects::ProjectLayer;
use crate::projects::{
    CreateProject, Project, ProjectDb, ProjectId, ProjectListOptions, ProjectListing,
    ProjectVersion, ProjectVersionId, UpdateProject,
};
use crate::workflows::workflow::WorkflowId;
use async_trait::async_trait;
use bb8_postgres::{
    tokio_postgres::tls::MakeTlsConnect, tokio_postgres::tls::TlsConnect, tokio_postgres::Socket,
};
use geoengine_datatypes::error::BoxedResultExt;
use snafu::{ensure, ResultExt};

#[async_trait]
impl<Tls> ProjectDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn list_projects(
        &self,
        options: ProjectListOptions,
    ) -> Result<Vec<ProjectListing>, ProjectDbError> {
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
        FROM user_permitted_projects u JOIN project_versions p ON (u.project_id = p.project_id)
        WHERE
            u.user_id = $1
            AND p.changed >= ALL (SELECT changed FROM project_versions WHERE project_id = p.project_id)
        ORDER BY p.{}
        LIMIT $2
        OFFSET $3;",
                options.order.to_sql_string()
            ))
            .await.context(PostgresProjectDbError)?;

        let project_rows = trans
            .query(
                &stmt,
                &[
                    &self.session.user.id,
                    &i64::from(options.limit),
                    &i64::from(options.offset),
                ],
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

        let version_id = insert_project(&trans, &project).await?;

        let stmt = trans
            .prepare(
                "INSERT INTO 
                    project_version_authors (project_version_id, user_id) 
                VALUES 
                    ($1, $2);",
            )
            .await
            .context(PostgresProjectDbError)?;

        trans
            .execute(&stmt, &[&version_id, &self.session.user.id])
            .await
            .context(PostgresProjectDbError)?;

        let stmt = trans
            .prepare(
                "INSERT INTO permissions (role_id, permission, project_id) VALUES ($1, $2, $3);",
            )
            .await
            .context(PostgresProjectDbError)?;

        trans
            .execute(
                &stmt,
                &[&self.session.user.id, &Permission::Owner, &project.id],
            )
            .await
            .context(PostgresProjectDbError)?;

        trans.commit().await.context(PostgresProjectDbError)?;

        Ok(project.id)
    }

    async fn load_project(&self, project: ProjectId) -> Result<Project, ProjectDbError> {
        self.load_project_version(project, LoadVersion::Latest)
            .await
    }

    #[allow(clippy::too_many_lines)]
    async fn update_project(&self, update: UpdateProject) -> Result<(), ProjectDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;

        let trans = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresProjectDbError)?;

        self.ensure_permission_in_tx(update.id.into(), Permission::Owner, &trans)
            .await
            .boxed_context(AccessFailedProjectDbError { project: update.id })?;

        let project = self.load_project(update.id).await?; // TODO: move inside transaction?

        let project = update_project(&trans, &project, update).await?;

        let stmt = trans
            .prepare(
                "INSERT INTO 
                    project_version_authors (project_version_id, user_id) 
                VALUES 
                    ($1, $2);",
            )
            .await
            .context(PostgresProjectDbError)?;

        trans
            .execute(&stmt, &[&project.version.id, &self.session.user.id])
            .await
            .context(PostgresProjectDbError)?;

        trans.commit().await.context(PostgresProjectDbError)?;

        Ok(())
    }

    async fn delete_project(&self, project: ProjectId) -> Result<(), ProjectDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;
        let trans = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresProjectDbError)?;

        self.ensure_permission_in_tx(project.into(), Permission::Owner, &trans)
            .await
            .boxed_context(AccessFailedProjectDbError { project })?;

        let stmt = trans
            .prepare("DELETE FROM projects WHERE id = $1;")
            .await
            .context(PostgresProjectDbError)?;

        let rows_affected = trans
            .execute(&stmt, &[&project])
            .await
            .context(PostgresProjectDbError)?;

        trans.commit().await.context(PostgresProjectDbError)?;

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

        self.ensure_permission_in_tx(project.into(), Permission::Owner, &trans)
            .await
            .boxed_context(AccessFailedProjectDbError { project })?;

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
                p.changed,
                a.user_id
            FROM 
                project_versions p JOIN project_version_authors a ON (p.id = a.project_version_id)
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
                p.changed,
                a.user_id
            FROM 
                project_versions p JOIN project_version_authors a ON (p.id = a.project_version_id)
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
        let _author_id = UserId(row.get(7));

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
        let mut conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;
        let trans = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresProjectDbError)?;

        self.ensure_permission_in_tx(project.into(), Permission::Read, &trans)
            .await
            .boxed_context(AccessFailedProjectDbError { project })?;

        let stmt = trans
            .prepare(
                "
                SELECT 
                    p.id, p.changed, a.user_id
                FROM 
                    project_versions p JOIN project_version_authors a ON (p.id = a.project_version_id)
                WHERE 
                    project_id = $1 
                ORDER BY 
                    p.changed DESC, a.user_id DESC",
            )
            .await.context(PostgresProjectDbError)?;

        let rows = trans
            .query(&stmt, &[&project])
            .await
            .context(PostgresProjectDbError)?;

        trans.commit().await.context(PostgresProjectDbError)?;

        Ok(rows
            .iter()
            .map(|row| ProjectVersion {
                id: ProjectVersionId(row.get(0)),
                changed: row.get(1),
            })
            .collect())
    }
}
