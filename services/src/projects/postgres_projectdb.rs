use crate::error;
use crate::error::Result;
use crate::projects::project::{
    CreateProject, LoadVersion, Project, ProjectId, ProjectListOptions, ProjectListing,
    ProjectVersion, ProjectVersionId, UpdateProject, UserProjectPermission,
};
use crate::projects::projectdb::ProjectDB;
use crate::users::user::UserId;
use crate::util::identifiers::Identifier;
use crate::util::user_input::Validated;
use crate::workflows::workflow::WorkflowId;
use async_trait::async_trait;
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::{
    bb8::Pool, tokio_postgres::tls::MakeTlsConnect, tokio_postgres::tls::TlsConnect,
    tokio_postgres::Socket,
};
use snafu::ResultExt;

use super::project::{Layer, LayerInfo, LayerType, ProjectPermission, RasterInfo, VectorInfo};
use crate::contexts::PostgresContext;

pub struct PostgresProjectDB<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    conn_pool: Pool<PostgresConnectionManager<Tls>>,
}

impl<Tls> PostgresProjectDB<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(conn_pool: Pool<PostgresConnectionManager<Tls>>) -> Self {
        Self { conn_pool }
    }
}

#[async_trait]
impl<Tls> ProjectDB for PostgresProjectDB<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn list(
        &self,
        user: UserId,
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
        WHERE u.user_id = $1 AND u.permission = ANY ($2) AND latest IS TRUE
        ORDER BY p.{}
        LIMIT $3
        OFFSET $4;",
                options.order.to_sql_string()
            ))
            .await?;

        let project_rows = conn
            .query(
                &stmt,
                &[
                    &user,
                    &options.permissions,
                    &i64::from(options.limit),
                    &i64::from(options.offset),
                ],
            )
            .await?;

        let mut project_listings = vec![];
        for project_row in project_rows {
            let project_version_id = ProjectVersionId::from_uuid(project_row.get(0));
            let project_id = ProjectId::from_uuid(project_row.get(1));
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
                changed,
            });
        }
        Ok(project_listings)
    }

    #[allow(clippy::too_many_lines)]
    async fn load(
        &self,
        user: UserId,
        project: ProjectId,
        version: LoadVersion,
    ) -> Result<Project> {
        let conn = self.conn_pool.get().await?;

        PostgresContext::check_user_project_permission(
            &conn,
            user,
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
            p.changed,
            p.author_user_id
        FROM user_project_permissions u JOIN project_versions p ON (u.project_id = p.project_id)
        WHERE u.user_id = $1 AND u.project_id = $2 AND p.project_version = $3",
                )
                .await?;

            conn.query_one(&stmt, &[&user, &project, &version]).await?
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
            p.changed,
            p.author_user_id
        FROM user_project_permissions u JOIN project_versions p ON (u.project_id = p.project_id)
        WHERE u.user_id = $1 AND u.project_id = $2 AND latest IS TRUE",
                )
                .await?;

            conn.query_one(&stmt, &[&user, &project]).await?
        };

        let project_id = ProjectId::from_uuid(row.get(0));
        let version_id = ProjectVersionId::from_uuid(row.get(1));
        let name = row.get(2);
        let description = row.get(3);
        let bounds = row.get(4);
        let changed = row.get(5);
        let author_id = UserId::from_uuid(row.get(6));

        let stmt = conn
            .prepare(
                "
        SELECT  
            layer_type, name, workflow_id, raster_colorizer
        FROM project_version_layers
        WHERE project_version_id = $1
        ORDER BY layer_index ASC",
            )
            .await?;

        let rows = conn.query(&stmt, &[&version_id]).await?;

        let mut layers = vec![];
        for row in rows {
            let layer_type = row.get(1);
            let info = match layer_type {
                LayerType::Raster => LayerInfo::Raster(RasterInfo {
                    colorizer: serde_json::from_value(row.get(4)).context(error::SerdeJson)?, // TODO: default serializer on error?
                }),
                LayerType::Vector => LayerInfo::Vector(VectorInfo {}),
            };

            layers.push(Layer {
                workflow: WorkflowId::from_uuid(row.get(3)),
                name: row.get(1),
                info,
            });
        }

        Ok(Project {
            id: project_id,
            version: ProjectVersion {
                id: version_id,
                changed,
                author: author_id,
            },
            name,
            description,
            layers,
            bounds,
        })
    }

    async fn create(
        &mut self,
        user: UserId,
        create: Validated<CreateProject>,
    ) -> Result<ProjectId> {
        let mut conn = self.conn_pool.get().await?;

        let project: Project = Project::from_create_project(create.user_input, user);

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
                    author_user_id,
                    changed,
                    latest)
                    VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, TRUE);",
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
                    &user,
                ],
            )
            .await?;

        let stmt = trans
            .prepare(
                "INSERT INTO user_project_permissions (user_id, project_id, permission) VALUES ($1, $2, $3);",
            )
            .await?;

        trans
            .execute(&stmt, &[&user, &project.id, &ProjectPermission::Owner])
            .await?;

        trans.commit().await?;

        Ok(project.id)
    }

    #[allow(clippy::too_many_lines)]
    async fn update(&mut self, user: UserId, update: Validated<UpdateProject>) -> Result<()> {
        let update = update.user_input;

        let mut conn = self.conn_pool.get().await?;

        PostgresContext::check_user_project_permission(
            &conn,
            user,
            update.id,
            &[ProjectPermission::Write, ProjectPermission::Owner],
        )
        .await?;

        let trans = conn.build_transaction().start().await?;

        let project = self.load_latest(user, update.id).await?; // TODO: move inside transaction?

        let stmt = trans
            .prepare("UPDATE project_versions SET latest = FALSE WHERE project_id = $1 AND latest IS TRUE;")
            .await?;
        trans.execute(&stmt, &[&project.id]).await?;

        let project = project.update_project(update, user);

        let stmt = trans
            .prepare(
                "
                INSERT INTO project_versions (
                    id,
                    project_id,
                    name,
                    description,
                    bounds,
                    author_user_id,
                    changed,
                    latest)
                VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, TRUE);",
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
                    &user,
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
                    layer_type,
                    name,
                    workflow_id,
                    raster_colorizer)
                VALUES ($1, $2, $3, $4, $5, $6, $7);",
                )
                .await?;

            let raster_colorizer = if let LayerInfo::Raster(info) = &layer.info {
                Some(serde_json::to_value(&info.colorizer).context(error::SerdeJson)?)
            } else {
                None
            };

            trans
                .execute(
                    &stmt,
                    &[
                        &project.id,
                        &project.version.id,
                        &(idx as i32),
                        &layer.layer_type(),
                        &layer.name,
                        &layer.workflow,
                        &raster_colorizer,
                    ],
                )
                .await?;
        }

        trans.commit().await?;

        Ok(())
    }

    async fn delete(&mut self, user: UserId, project: ProjectId) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        PostgresContext::check_user_project_permission(
            &conn,
            user,
            project,
            &[ProjectPermission::Owner],
        )
        .await?;

        let stmt = conn.prepare("DELETE FROM projects WHERE id = $1;").await?;

        conn.execute(&stmt, &[&project]).await?;

        Ok(())
    }

    async fn versions(&self, user: UserId, project: ProjectId) -> Result<Vec<ProjectVersion>> {
        let conn = self.conn_pool.get().await?;

        PostgresContext::check_user_project_permission(
            &conn,
            user,
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
                id: ProjectVersionId::from_uuid(row.get(0)),
                changed: row.get(1),
                author: UserId::from_uuid(row.get(2)),
            })
            .collect())
    }

    async fn list_permissions(
        &self,
        user: UserId,
        project: ProjectId,
    ) -> Result<Vec<UserProjectPermission>> {
        let conn = self.conn_pool.get().await?;

        PostgresContext::check_user_project_permission(
            &conn,
            user,
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
                user: UserId::from_uuid(row.get(0)),
                project: ProjectId::from_uuid(row.get(1)),
                permission: row.get(2),
            })
            .collect())
    }

    async fn add_permission(
        &mut self,
        user: UserId,
        permission: UserProjectPermission,
    ) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        PostgresContext::check_user_project_permission(
            &conn,
            user,
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
        &mut self,
        user: UserId,
        permission: UserProjectPermission,
    ) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        PostgresContext::check_user_project_permission(
            &conn,
            user,
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

        conn.execute(&stmt, &[&user, &permission.project, &permission.permission])
            .await?;

        Ok(())
    }
}
