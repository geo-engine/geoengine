use crate::contexts::Db;
use crate::error::Result;
use crate::projects::{
    CreateProject, OrderBy, Project, ProjectDb, ProjectFilter, ProjectId, ProjectListOptions,
    ProjectListing, UpdateProject,
};
use crate::util::user_input::Validated;
use crate::{contexts::SimpleSession, error};
use async_trait::async_trait;
use std::collections::HashMap;

#[derive(Default)]
pub struct HashMapProjectDb {
    projects: Db<HashMap<ProjectId, Project>>,
}

#[async_trait]
impl ProjectDb<SimpleSession> for HashMapProjectDb {
    /// List projects
    async fn list(
        &self,
        _session: &SimpleSession,
        options: Validated<ProjectListOptions>,
    ) -> Result<Vec<ProjectListing>> {
        let ProjectListOptions {
            filter,
            order,
            offset,
            limit,
        } = options.user_input;

        let mut projects = self
            .projects
            .read()
            .await
            .values()
            .map(ProjectListing::from)
            .filter(|p| match &filter {
                ProjectFilter::Name { term } => p.name == *term,
                ProjectFilter::Description { term } => p.description == *term,
                ProjectFilter::None => true,
            })
            .collect::<Vec<_>>();

        match order {
            OrderBy::DateAsc => projects.sort_by(|a, b| a.changed.cmp(&b.changed)),
            OrderBy::DateDesc => projects.sort_by(|a, b| b.changed.cmp(&a.changed)),
            OrderBy::NameAsc => projects.sort_by(|a, b| a.name.cmp(&b.name)),
            OrderBy::NameDesc => projects.sort_by(|a, b| b.name.cmp(&a.name)),
        }

        Ok(projects
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .collect())
    }

    /// Load a project
    async fn load(&self, _session: &SimpleSession, project: ProjectId) -> Result<Project> {
        self.projects
            .read()
            .await
            .get(&project)
            .cloned()
            .ok_or(error::Error::ProjectLoadFailed)
    }

    /// Create a project
    async fn create(
        &self,
        _session: &SimpleSession,
        create: Validated<CreateProject>,
    ) -> Result<ProjectId> {
        let project: Project = Project::from_create_project(create.user_input);
        let id = project.id;
        self.projects.write().await.insert(id, project);
        Ok(id)
    }

    /// Update a project
    async fn update(
        &self,
        _session: &SimpleSession,
        update: Validated<UpdateProject>,
    ) -> Result<()> {
        let update = update.user_input;

        let mut projects = self.projects.write().await;
        let project = projects
            .get_mut(&update.id)
            .ok_or(error::Error::ProjectUpdateFailed)?;

        let project_update = project.update_project(update)?;

        *project = project_update;

        Ok(())
    }

    /// Delete a project
    async fn delete(&self, _session: &SimpleSession, project: ProjectId) -> Result<()> {
        self.projects
            .write()
            .await
            .remove(&project)
            .map(|_| ())
            .ok_or(error::Error::ProjectDeleteFailed)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::contexts::MockableSession;
    use crate::projects::project::STRectangle;
    use crate::util::user_input::UserInput;
    use crate::util::Identifier;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;

    #[tokio::test]
    async fn list() {
        let project_db = HashMapProjectDb::default();
        let session = SimpleSession::mock();

        for i in 0..10 {
            let create = CreateProject {
                name: format!("Test{i}"),
                description: format!("Test{}", 10 - i),
                bounds: STRectangle::new(
                    SpatialReferenceOption::Unreferenced,
                    0.,
                    0.,
                    1.,
                    1.,
                    0,
                    1,
                )
                .unwrap(),
                time_step: None,
            }
            .validated()
            .unwrap();
            project_db.create(&session, create).await.unwrap();
        }
        let options = ProjectListOptions {
            filter: ProjectFilter::None,
            order: OrderBy::NameDesc,
            offset: 0,
            limit: 2,
        }
        .validated()
        .unwrap();
        let projects = project_db.list(&session, options).await.unwrap();

        assert_eq!(projects.len(), 2);
        assert_eq!(projects[0].name, "Test9");
        assert_eq!(projects[1].name, "Test8");
    }

    #[tokio::test]
    async fn load() {
        let project_db = HashMapProjectDb::default();
        let session = SimpleSession::default();

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let id = project_db.create(&session, create.clone()).await.unwrap();
        assert!(project_db.load(&session, id).await.is_ok());

        assert!(project_db.load(&session, ProjectId::new()).await.is_err());
    }

    #[tokio::test]
    async fn create() {
        let project_db = HashMapProjectDb::default();
        let session = SimpleSession::default();

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let id = project_db.create(&session, create).await.unwrap();

        assert!(project_db.load(&session, id).await.is_ok());
    }

    #[tokio::test]
    async fn update() {
        let project_db = HashMapProjectDb::default();
        let session = SimpleSession::default();

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let id = project_db.create(&session, create).await.unwrap();

        let update = UpdateProject {
            id,
            name: Some("Foo".into()),
            description: None,
            layers: None,
            plots: None,
            bounds: None,
            time_step: None,
        }
        .validated()
        .unwrap();

        project_db.update(&session, update).await.unwrap();

        assert_eq!(project_db.load(&session, id).await.unwrap().name, "Foo");
    }

    #[tokio::test]
    async fn delete() {
        let project_db = HashMapProjectDb::default();
        let session = SimpleSession::default();

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let id = project_db.create(&session, create).await.unwrap();

        assert!(project_db.delete(&session, id).await.is_ok());
    }
}
