use crate::error::Result;
use crate::projects::project::{
    CreateProject, LoadVersion, Project, ProjectId, ProjectListOptions, ProjectListing,
    ProjectVersion, ProjectVersionId, UpdateProject, UserProjectPermission,
};
use crate::projects::projectdb::ProjectDB;
use crate::users::user::UserId;
use crate::util::identifiers::Identifier;
use crate::util::user_input::Validated;
use async_trait::async_trait;
use bb8_postgres::bb8::Pool;
use bb8_postgres::tokio_postgres::NoTls;
use bb8_postgres::PostgresConnectionManager;

pub struct PostgresProjectDB {
    conn_pool: Pool<PostgresConnectionManager<NoTls>>, // TODO: support Tls connection as well
}

impl PostgresProjectDB {
    pub fn new(conn_pool: Pool<PostgresConnectionManager<NoTls>>) -> Self {
        Self { conn_pool }
    }
}

#[async_trait]
impl ProjectDB for PostgresProjectDB {
    async fn list(
        &self,
        user: UserId,
        options: Validated<ProjectListOptions>,
    ) -> Vec<ProjectListing> {
        todo!()
    }

    async fn load(
        &self,
        user: UserId,
        project: ProjectId,
        version: LoadVersion,
    ) -> Result<Project> {
        todo!()
    }

    async fn create(
        &mut self,
        user: UserId,
        create: Validated<CreateProject>,
    ) -> Result<ProjectId> {
        let conn = self.conn_pool.get().await?;

        let project: Project = Project::from_create_project(create.user_input, user);

        let stmt = conn
            .prepare("INSERT INTO projects (id) VALUES ($1);")
            .await?;

        conn.execute(&stmt, &[&project.id.uuid()]).await?;

        let stmt = conn
            .prepare(
                "INSERT INTO project_versions (\
                    id, \
                    project_id, \
                    name, \
                    description, \
                    view_ll_x, \
                    view_ll_y, \
                    view_ur_x, \
                    view_ur_y, \
                    view_t1, \
                    view_t2, \
                    bounds_ll_x, \
                    bounds_ll_y, \
                    bounds_ur_x, \
                    bounds_ur_y, \
                    bounds_t1, \
                    bounds_t2, \
                    author_user_id, \
                    time) \
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, \
                            $11, $12, $13, $14, $15, $16, $17, CURRENT_TIMESTAMP);",
            )
            .await?;

        conn.execute(
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

        Ok(project.id)
    }

    async fn update(&mut self, user: UserId, project: Validated<UpdateProject>) -> Result<()> {
        todo!()
    }

    async fn delete(&mut self, user: UserId, project: ProjectId) -> Result<()> {
        todo!()
    }

    async fn versions(&self, user: UserId, project: ProjectId) -> Result<Vec<ProjectVersion>> {
        todo!()
    }

    async fn list_permissions(
        &mut self,
        user: UserId,
        project: ProjectId,
    ) -> Result<Vec<UserProjectPermission>> {
        todo!()
    }

    async fn add_permission(
        &mut self,
        user: UserId,
        permission: UserProjectPermission,
    ) -> Result<()> {
        todo!()
    }

    async fn remove_permission(
        &mut self,
        user: UserId,
        permission: UserProjectPermission,
    ) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projects::project::STRectangle;
    use crate::users::postgres_userdb::PostgresUserDB;
    use crate::users::user::UserRegistration;
    use crate::users::userdb::UserDB;
    use crate::util::user_input::UserInput;
    use bb8_postgres::tokio_postgres;
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
        let pg_mgr = PostgresConnectionManager::new(config, tokio_postgres::NoTls);

        let pool = Pool::builder().build(pg_mgr).await.unwrap();

        let mut user_db = PostgresUserDB::new(pool.clone()).await.unwrap();

        let user_registration = UserRegistration {
            email: "foo@bar.de".into(),
            password: "secret123".into(),
            real_name: "Foo Bar".into(),
        }
        .validated()
        .unwrap();

        let result = user_db.register(user_registration).await;
        assert!(result.is_ok());
        let user_id = result.unwrap();

        let mut project_db = PostgresProjectDB::new(pool.clone());

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
            bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
        }
        .validated()
        .unwrap();

        project_db.create(user_id, create).await.unwrap();
    }
}
