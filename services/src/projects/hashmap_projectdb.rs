use crate::contexts::InMemoryDb;
use crate::error;
use crate::error::Result;
use crate::projects::{
    CreateProject, OrderBy, Project, ProjectDb, ProjectFilter, ProjectId, ProjectListOptions,
    ProjectListing, UpdateProject,
};
use async_trait::async_trait;
use std::collections::HashMap;

#[derive(Default)]
pub struct HashMapProjectDbBackend {
    projects: HashMap<ProjectId, Project>,
}

#[async_trait]
impl ProjectDb for InMemoryDb {
    /// List projects
    async fn list_projects(&self, options: ProjectListOptions) -> Result<Vec<ProjectListing>> {
        let ProjectListOptions {
            filter,
            order,
            offset,
            limit,
        } = options;

        let mut projects = self
            .backend
            .project_db
            .read()
            .await
            .projects
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
    async fn load_project(&self, project: ProjectId) -> Result<Project> {
        self.backend
            .project_db
            .read()
            .await
            .projects
            .get(&project)
            .cloned()
            .ok_or(error::Error::ProjectLoadFailed)
    }

    /// Create a project
    async fn create_project(&self, create: CreateProject) -> Result<ProjectId> {
        let project: Project = Project::from_create_project(create);
        let id = project.id;
        self.backend
            .project_db
            .write()
            .await
            .projects
            .insert(id, project);
        Ok(id)
    }

    /// Update a project
    async fn update_project(&self, update: UpdateProject) -> Result<()> {
        let update = update;

        let mut backend = self.backend.project_db.write().await;

        let project = backend
            .projects
            .get_mut(&update.id)
            .ok_or(error::Error::ProjectUpdateFailed)?;

        let project_update = project.update_project(update)?;

        *project = project_update;

        Ok(())
    }

    /// Delete a project
    async fn delete_project(&self, project: ProjectId) -> Result<()> {
        self.backend
            .project_db
            .write()
            .await
            .projects
            .remove(&project)
            .map(|_| ())
            .ok_or(error::Error::ProjectDeleteFailed)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::projects::project::STRectangle;
    use crate::util::Identifier;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;

    #[tokio::test]
    async fn list() {
        let project_db = InMemoryDb::default();

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
            };
            project_db.create_project(create).await.unwrap();
        }
        let options = ProjectListOptions {
            filter: ProjectFilter::None,
            order: OrderBy::NameDesc,
            offset: 0,
            limit: 2,
        };
        let projects = project_db.list_projects(options).await.unwrap();

        assert_eq!(projects.len(), 2);
        assert_eq!(projects[0].name, "Test9");
        assert_eq!(projects[1].name, "Test8");
    }

    #[tokio::test]
    async fn load() {
        let project_db = InMemoryDb::default();

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        };

        let id = project_db.create_project(create.clone()).await.unwrap();
        assert!(project_db.load_project(id).await.is_ok());

        assert!(project_db.load_project(ProjectId::new()).await.is_err());
    }

    #[tokio::test]
    async fn create() {
        let project_db = InMemoryDb::default();

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        };

        let id = project_db.create_project(create).await.unwrap();

        assert!(project_db.load_project(id).await.is_ok());
    }

    #[tokio::test]
    async fn update() {
        let project_db = InMemoryDb::default();

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        };

        let id = project_db.create_project(create).await.unwrap();

        let update = UpdateProject {
            id,
            name: Some("Foo".into()),
            description: None,
            layers: None,
            plots: None,
            bounds: None,
            time_step: None,
        };

        project_db.update_project(update).await.unwrap();

        assert_eq!(project_db.load_project(id).await.unwrap().name, "Foo");
    }

    #[tokio::test]
    async fn delete() {
        let project_db = InMemoryDb::default();

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        };

        let id = project_db.create_project(create).await.unwrap();

        assert!(project_db.delete_project(id).await.is_ok());
    }
}
