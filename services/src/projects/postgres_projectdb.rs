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
use chrono::{TimeZone, Utc};
use snafu::ResultExt;

use super::project::{
    Layer, LayerInfo, LayerType, ProjectPermission, RasterInfo, STRectangle, VectorInfo,
};

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

    async fn check_user_project_permission(
        &self,
        user: UserId,
        project: ProjectId,
        permissions: &[ProjectPermission],
    ) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
                SELECT TRUE
                FROM user_project_permissions
                WHERE user_id = $1 AND project_id = $2 AND permission = ANY ($3);",
            )
            .await?;

        conn.query_one(&stmt, &[&user.uuid(), &project.uuid(), &permissions])
            .await
            .map_err(|_| error::Error::ProjectDBUnauthorized)?;

        Ok(())
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
        SELECT p.id, p.project_id, p.name, p.description, p.time 
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
                    &user.uuid(),
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
            let time = Utc.from_local_datetime(&project_row.get(4)).unwrap();

            let stmt = conn
                .prepare(
                    "
                    SELECT name
                    FROM project_version_layers
                    WHERE project_version_id = $1;",
                )
                .await?;

            let layer_rows = conn.query(&stmt, &[&project_version_id.uuid()]).await?;
            let layer_names = layer_rows.iter().map(|row| row.get(0)).collect();

            project_listings.push(ProjectListing {
                id: project_id,
                name,
                description,
                layer_names,
                changed: time,
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
        self.check_user_project_permission(
            user,
            project,
            &[
                ProjectPermission::Read,
                ProjectPermission::Write,
                ProjectPermission::Owner,
            ],
        )
        .await?;

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
            p.view_ll_x,
            p.view_ll_y,
            p.view_ur_x,
            p.view_ur_y,
            p.view_t1,
            p.view_t2,
            p.bounds_ll_x,
            p.bounds_ll_y,
            p.bounds_ur_x,
            p.bounds_ur_y,
            p.bounds_t1,
            p.bounds_t2,
            p.time,
            p.author_user_id
        FROM user_project_permissions u JOIN project_versions p ON (u.project_id = p.project_id)
        WHERE u.user_id = $1 AND u.project_id = $2 AND p.project_version = $3",
                )
                .await?;

            conn.query_one(&stmt, &[&user.uuid(), &project.uuid(), &version.uuid()])
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
            p.view_ll_x,
            p.view_ll_y,
            p.view_ur_x,
            p.view_ur_y,
            p.view_t1,
            p.view_t2,
            p.bounds_ll_x,
            p.bounds_ll_y,
            p.bounds_ur_x,
            p.bounds_ur_y,
            p.bounds_t1,
            p.bounds_t2,
            p.time,
            p.author_user_id
        FROM user_project_permissions u JOIN project_versions p ON (u.project_id = p.project_id)
        WHERE u.user_id = $1 AND u.project_id = $2 AND latest IS TRUE",
                )
                .await?;

            conn.query_one(&stmt, &[&user.uuid(), &project.uuid()])
                .await?
        };

        let project_id = ProjectId::from_uuid(row.get(0));
        let version_id = ProjectVersionId::from_uuid(row.get(1));
        let name = row.get(2);
        let description = row.get(3);
        let view = STRectangle::new_unchecked(
            row.get(4),
            row.get(5),
            row.get(6),
            row.get(7),
            Utc.from_local_datetime(&row.get(8)).unwrap(),
            Utc.from_local_datetime(&row.get(9)).unwrap(),
        );
        let bounds = STRectangle::new_unchecked(
            row.get(10),
            row.get(11),
            row.get(12),
            row.get(13),
            Utc.from_local_datetime(&row.get(14)).unwrap(),
            Utc.from_local_datetime(&row.get(15)).unwrap(),
        );
        let time = Utc.from_local_datetime(&row.get(16)).unwrap();
        let author_id = UserId::from_uuid(row.get(17));

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

        let rows = conn.query(&stmt, &[&version_id.uuid()]).await?;

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
                changed: time,
                author: author_id,
            },
            name,
            description,
            layers,
            view,
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

        trans.execute(&stmt, &[&project.id.uuid()]).await?;

        let stmt = trans
            .prepare(
                "INSERT INTO project_versions (
                    id,
                    project_id,
                    name,
                    description,
                    view_ll_x,
                    view_ll_y,
                    view_ur_x,
                    view_ur_y,
                    view_t1,
                    view_t2,
                    bounds_ll_x,
                    bounds_ll_y,
                    bounds_ur_x,
                    bounds_ur_y,
                    bounds_t1,
                    bounds_t2,
                    author_user_id,
                    time,
                    latest)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                            $11, $12, $13, $14, $15, $16, $17, CURRENT_TIMESTAMP, TRUE);",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[
                    &ProjectVersionId::new().uuid(),
                    &project.id.uuid(),
                    &project.name,
                    &project.description,
                    &project.view.bounding_box.lower_left().x,
                    &project.view.bounding_box.lower_left().y,
                    &project.view.bounding_box.upper_right().x,
                    &project.view.bounding_box.upper_right().x,
                    &project.view.time_interval.start().as_naive_date_time(),
                    &project.view.time_interval.end().as_naive_date_time(),
                    &project.bounds.bounding_box.lower_left().x,
                    &project.bounds.bounding_box.lower_left().y,
                    &project.bounds.bounding_box.upper_right().x,
                    &project.bounds.bounding_box.upper_right().x,
                    &project.bounds.time_interval.start().as_naive_date_time(),
                    &project.bounds.time_interval.end().as_naive_date_time(),
                    &user.uuid(),
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
                &[&user.uuid(), &project.id.uuid(), &ProjectPermission::Owner],
            )
            .await?;

        trans.commit().await?;

        Ok(project.id)
    }

    #[allow(clippy::too_many_lines)]
    async fn update(&mut self, user: UserId, update: Validated<UpdateProject>) -> Result<()> {
        let update = update.user_input;

        self.check_user_project_permission(
            user,
            update.id,
            &[ProjectPermission::Write, ProjectPermission::Owner],
        )
        .await?;

        let mut conn = self.conn_pool.get().await?;

        let trans = conn.build_transaction().start().await?;

        let project = self.load_latest(user, update.id).await?; // TODO: move inside transaction?

        let stmt = trans
            .prepare("UPDATE project_versions SET latest = FALSE WHERE project_id = $1 AND latest IS TRUE;")
            .await?;
        trans.execute(&stmt, &[&project.id.uuid()]).await?;

        let project = project.update_project(update, user);

        let stmt = trans
            .prepare(
                "
                INSERT INTO project_versions (
                    id,
                    project_id,
                    name,
                    description,
                    view_ll_x,
                    view_ll_y,
                    view_ur_x,
                    view_ur_y,
                    view_t1,
                    view_t2,
                    bounds_ll_x,
                    bounds_ll_y,
                    bounds_ur_x,
                    bounds_ur_y,
                    bounds_t1,
                    bounds_t2,
                    author_user_id,
                    time,
                    latest)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15, $16, $17, CURRENT_TIMESTAMP, TRUE);",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[
                    &project.version.id.uuid(),
                    &project.id.uuid(),
                    &project.name,
                    &project.description,
                    &project.view.bounding_box.lower_left().x,
                    &project.view.bounding_box.lower_left().y,
                    &project.view.bounding_box.upper_right().x,
                    &project.view.bounding_box.upper_right().x,
                    &project.view.time_interval.start().as_naive_date_time(),
                    &project.view.time_interval.end().as_naive_date_time(),
                    &project.bounds.bounding_box.lower_left().x,
                    &project.bounds.bounding_box.lower_left().y,
                    &project.bounds.bounding_box.upper_right().x,
                    &project.bounds.bounding_box.upper_right().x,
                    &project.bounds.time_interval.start().as_naive_date_time(),
                    &project.bounds.time_interval.end().as_naive_date_time(),
                    &user.uuid(),
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
                        &project.id.uuid(),
                        &project.version.id.uuid(),
                        &(idx as i32),
                        &layer.layer_type(),
                        &layer.name,
                        &layer.workflow.uuid(),
                        &raster_colorizer,
                    ],
                )
                .await?;
        }

        trans.commit().await?;

        Ok(())
    }

    async fn delete(&mut self, user: UserId, project: ProjectId) -> Result<()> {
        self.check_user_project_permission(user, project, &[ProjectPermission::Owner])
            .await?;

        let conn = self.conn_pool.get().await?;

        let stmt = conn.prepare("DELETE FROM projects WHERE id = $1;").await?;

        conn.execute(&stmt, &[&project.uuid()]).await?;

        Ok(())
    }

    async fn versions(&self, user: UserId, project: ProjectId) -> Result<Vec<ProjectVersion>> {
        self.check_user_project_permission(
            user,
            project,
            &[
                ProjectPermission::Read,
                ProjectPermission::Write,
                ProjectPermission::Owner,
            ],
        )
        .await?;

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
                SELECT id, time, author_user_id
                FROM project_versions WHERE project_id = $1 
                ORDER BY latest DESC, time DESC, author_user_id DESC",
            )
            .await?;

        let rows = conn.query(&stmt, &[&project.uuid()]).await?;

        Ok(rows
            .iter()
            .map(|row| ProjectVersion {
                id: ProjectVersionId::from_uuid(row.get(0)),
                changed: Utc.from_local_datetime(&row.get(1)).unwrap(),
                author: UserId::from_uuid(row.get(2)),
            })
            .collect())
    }

    async fn list_permissions(
        &self,
        user: UserId,
        project: ProjectId,
    ) -> Result<Vec<UserProjectPermission>> {
        self.check_user_project_permission(
            user,
            project,
            &[
                ProjectPermission::Read,
                ProjectPermission::Write,
                ProjectPermission::Owner,
            ],
        )
        .await?;

        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
        SELECT user_id, project_id, permission FROM user_project_permissions WHERE project_id = $1;",
            )
            .await?;

        let rows = conn.query(&stmt, &[&project.uuid()]).await?;

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
        self.check_user_project_permission(user, permission.project, &[ProjectPermission::Owner])
            .await?;

        let conn = self.conn_pool.get().await?;

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
                &permission.user.uuid(),
                &permission.project.uuid(),
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
        self.check_user_project_permission(user, permission.project, &[ProjectPermission::Owner])
            .await?;

        let conn = self.conn_pool.get().await?;

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
                &user.uuid(),
                &permission.project.uuid(),
                &permission.permission,
            ],
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::Context;
    use crate::projects::project::{OrderBy, ProjectFilter, ProjectPermission, STRectangle};
    use crate::util::user_input::UserInput;
    use crate::workflows::workflow::Workflow;
    use crate::{contexts::PostgresContext, users::userdb::UserDB};
    use crate::{users::user::UserRegistration, workflows::registry::WorkflowRegistry};
    use bb8_postgres::tokio_postgres;
    use geoengine_datatypes::primitives::Coordinate2D;
    use geoengine_operators::{
        engine::TypedOperator, engine::VectorOperator, mock::MockPointSource,
        mock::MockPointSourceParams,
    };
    use std::str::FromStr;

    #[ignore]
    #[tokio::test]
    async fn test() {
        // TODO: load from test config
        // TODO: add postgres to ci
        // TODO: clear database
        let config = tokio_postgres::config::Config::from_str(
            "postgresql://geoengine:geoengine@localhost:5432",
        )
        .unwrap();

        let ctx = PostgresContext::new(config, tokio_postgres::NoTls)
            .await
            .unwrap();

        let user_registration = UserRegistration {
            email: "foo@bar.de".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        let result = ctx
            .user_db_ref_mut()
            .await
            .register(user_registration)
            .await;
        let user_id = result.unwrap();

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

        let project_id = projects[0].id;

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
}
