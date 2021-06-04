use crate::error::Result;
use crate::projects::{
    CreateProject, LoadVersion, OrderBy, Project, ProjectDb, ProjectFilter, ProjectId,
    ProjectListOptions, ProjectListing, ProjectVersion, UpdateProject,
};
use crate::util::user_input::Validated;
use crate::{contexts::SimpleSession, error};
use async_trait::async_trait;
use std::collections::HashMap;

#[derive(Default)]
pub struct HashMapProjectDb {
    projects: HashMap<ProjectId, Vec<Project>>,
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
            .values()
            .filter_map(|projects| projects.last())
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
    async fn load(
        &self,
        _session: &SimpleSession,
        project: ProjectId,
        version: LoadVersion,
    ) -> Result<Project> {
        let project_versions = self
            .projects
            .get(&project)
            .ok_or(error::Error::ProjectLoadFailed)?;
        if let LoadVersion::Version(version) = version {
            Ok(project_versions
                .iter()
                .find(|p| p.version.id == version)
                .ok_or(error::Error::ProjectLoadFailed)?
                .clone())
        } else {
            Ok(project_versions
                .last()
                .ok_or(error::Error::ProjectLoadFailed)?
                .clone())
        }
    }

    /// Create a project
    async fn create(
        &mut self,
        _session: &SimpleSession,
        create: Validated<CreateProject>,
    ) -> Result<ProjectId> {
        let project: Project = Project::from_create_project(create.user_input);
        let id = project.id;
        self.projects.insert(id, vec![project]);
        Ok(id)
    }

    /// Update a project
    async fn update(
        &mut self,
        _session: &SimpleSession,
        update: Validated<UpdateProject>,
    ) -> Result<()> {
        let update = update.user_input;

        let project_versions = self
            .projects
            .get_mut(&update.id)
            .ok_or(error::Error::ProjectUpdateFailed)?;
        let project = project_versions
            .last()
            .ok_or(error::Error::ProjectUpdateFailed)?;

        let project_update = project.update_project(update)?;

        project_versions.push(project_update);

        Ok(())
    }

    /// Delete a project
    async fn delete(&mut self, _session: &SimpleSession, project: ProjectId) -> Result<()> {
        self.projects
            .remove(&project)
            .map(|_| ())
            .ok_or(error::Error::ProjectDeleteFailed)
    }

    /// Get the versions of a project
    async fn versions(
        &self,
        _session: &SimpleSession,
        project: ProjectId,
    ) -> Result<Vec<ProjectVersion>> {
        // TODO: pagination?

        Ok(self
            .projects
            .get(&project)
            .ok_or(error::Error::ProjectLoadFailed)?
            .iter()
            .map(|p| p.version)
            .collect())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::projects::project::STRectangle;
    use crate::util::user_input::UserInput;
    use crate::util::Identifier;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use std::{thread, time};

    #[tokio::test]
    async fn list() {
        let mut project_db = HashMapProjectDb::default();
        let session = SimpleSession::default();

        for i in 0..10 {
            let create = CreateProject {
                name: format!("Test{}", i),
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
        let mut project_db = HashMapProjectDb::default();
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
        assert!(project_db.load_latest(&session, id).await.is_ok());

        assert!(project_db
            .load_latest(&session, ProjectId::new())
            .await
            .is_err())
    }

    #[tokio::test]
    async fn create() {
        let mut project_db = HashMapProjectDb::default();
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

        assert!(project_db.load_latest(&session, id).await.is_ok())
    }

    #[tokio::test]
    async fn update() {
        let mut project_db = HashMapProjectDb::default();
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

        assert_eq!(
            project_db.load_latest(&session, id).await.unwrap().name,
            "Foo"
        );
    }

    #[tokio::test]
    async fn delete() {
        let mut project_db = HashMapProjectDb::default();
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

    #[tokio::test]
    async fn versions() {
        let mut project_db = HashMapProjectDb::default();
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

        thread::sleep(time::Duration::from_millis(10));

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

        let versions = project_db.versions(&session, id).await.unwrap();

        assert_eq!(versions.len(), 2);
        assert!(versions[0].changed < versions[1].changed);
    }
}
