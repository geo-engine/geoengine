use crate::error;
use crate::error::Result;
use crate::pro::contexts::ProInMemoryDb;
use crate::pro::permissions::{Permission, PermissionDb};
use crate::pro::projects::ProProjectDb;

use crate::projects::{
    CreateProject, OrderBy, Project, ProjectDb, ProjectFilter, ProjectId, ProjectListOptions,
    ProjectListing, ProjectVersion, UpdateProject,
};
use crate::util::user_input::Validated;
use async_trait::async_trait;
use futures::{stream, StreamExt};
use snafu::ensure;
use std::collections::HashMap;

use super::LoadVersion;

#[derive(Default)]
pub struct ProHashMapProjectDbBackend {
    projects: HashMap<ProjectId, Vec<Project>>,
}

#[async_trait]
impl ProjectDb for ProInMemoryDb {
    /// List projects
    async fn list_projects(
        &self,
        options: Validated<ProjectListOptions>,
    ) -> Result<Vec<ProjectListing>> {
        let ProjectListOptions {
            filter,
            order,
            offset,
            limit,
        } = options.user_input;

        let backend = self.backend.project_db.read().await;

        let mut projects = stream::iter(&backend.projects)
            .filter(|(id, _)| async {
                self.has_permission(**id, Permission::Read)
                    .await
                    .unwrap_or(false)
            })
            .filter_map(|(_, v)| async { v.last() })
            .map(ProjectListing::from)
            .filter_map(|p| async {
                let m = match &filter {
                    ProjectFilter::Name { term } => &p.name == term,
                    ProjectFilter::Description { term } => &p.description == term,
                    ProjectFilter::None => true,
                };

                if m {
                    Some(p)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .await;

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

    /// Create a project
    async fn create_project(&self, create: Validated<CreateProject>) -> Result<ProjectId> {
        let project: Project = Project::from_create_project(create.user_input);
        let id = project.id;
        let mut backend = self.backend.project_db.write().await;

        backend.projects.insert(id, vec![project]);

        // TODO: delete project if this fails:
        self.create_resource(id).await?;

        Ok(id)
    }

    /// Update a project
    async fn update_project(&self, update: Validated<UpdateProject>) -> Result<()> {
        let update = update.user_input;

        ensure!(
            self.has_permission(update.id, Permission::Owner).await?,
            error::PermissionDenied
        );

        let mut backend = self.backend.project_db.write().await;
        let projects = &mut backend.projects;

        let project_versions = projects
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
    async fn delete_project(&self, project: ProjectId) -> Result<()> {
        ensure!(
            self.has_permission(project, Permission::Owner).await?,
            error::PermissionDenied
        );

        self.backend
            .project_db
            .write()
            .await
            .projects
            .remove(&project)
            .map(|_| ())
            .ok_or(error::Error::ProjectDeleteFailed)
    }

    async fn load_project(&self, project: ProjectId) -> Result<Project> {
        self.load_project_version(project, LoadVersion::Latest)
            .await
    }
}

#[async_trait]
impl ProProjectDb for ProInMemoryDb {
    /// Load a project
    async fn load_project_version(
        &self,
        project: ProjectId,
        version: LoadVersion,
    ) -> Result<Project> {
        ensure!(
            self.has_permission(project, Permission::Read).await?,
            error::PermissionDenied
        );

        let backend = self.backend.project_db.read().await;
        let projects = &backend.projects;

        let project_versions = projects
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

    /// Get the versions of a project
    async fn list_project_versions(&self, project: ProjectId) -> Result<Vec<ProjectVersion>> {
        // TODO: pagination?
        ensure!(
            self.has_permission(project, Permission::Read).await?,
            error::PermissionDenied
        );

        Ok(self
            .backend
            .project_db
            .read()
            .await
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
    use crate::contexts::Context;
    use crate::pro::contexts::ProInMemoryContext;
    use crate::pro::util::tests::create_random_user_session_helper;
    use crate::projects::STRectangle;
    use crate::util::user_input::UserInput;
    use crate::util::Identifier;
    use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D, TimeInterval};
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_datatypes::util::test::TestDefault;
    use std::{thread, time};

    fn strect() -> STRectangle {
        STRectangle {
            spatial_reference: SpatialReferenceOption::Unreferenced,
            bounding_box: BoundingBox2D::new(Coordinate2D::new(0., 0.), Coordinate2D::new(1., 1.))
                .unwrap(),
            time_interval: TimeInterval::new(0, 1).unwrap(),
        }
    }

    #[tokio::test]
    async fn list_permitted() {
        let ctx = ProInMemoryContext::test_default();

        let session1 = create_random_user_session_helper();
        let session2 = create_random_user_session_helper();
        let session3 = create_random_user_session_helper();

        let db1 = ctx.db(session1.clone());
        let db2 = ctx.db(session2);
        let db3 = ctx.db(session3);

        let create = CreateProject {
            name: "Own".into(),
            description: "Text".into(),
            bounds: strect(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let _ = db1.create_project(create).await.unwrap();

        let create = CreateProject {
            name: "User2's".into(),
            description: "Text".into(),
            bounds: strect(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let project2 = db2.create_project(create).await.unwrap();

        let create = CreateProject {
            name: "User3's".into(),
            description: "Text".into(),
            bounds: strect(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let project3 = db3.create_project(create).await.unwrap();

        db2.add_permission(session1.user.id.into(), project2, Permission::Read)
            .await
            .unwrap();
        db3.add_permission(session1.user.id.into(), project3, Permission::Read)
            .await
            .unwrap();

        let options = ProjectListOptions {
            filter: ProjectFilter::None,
            order: OrderBy::NameDesc,
            offset: 0,
            limit: 3,
        }
        .validated()
        .unwrap();

        let projects = db1.list_projects(options).await.unwrap();

        assert!(projects.iter().any(|p| p.name == "Own"));
        assert!(projects.iter().any(|p| p.name == "User2's"));
        assert!(projects.iter().any(|p| p.name == "User3's"));

        let options = ProjectListOptions {
            filter: ProjectFilter::None,
            order: OrderBy::NameAsc,
            offset: 0,
            limit: 1,
        }
        .validated()
        .unwrap();

        let projects = db1.list_projects(options).await.unwrap();
        assert!(projects[0].name == "Own");
        assert_eq!(projects.len(), 1);
    }

    #[tokio::test]
    async fn list() {
        let ctx = ProInMemoryContext::test_default();
        let session = create_random_user_session_helper();

        let db = ctx.db(session.clone());

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
            db.create_project(create).await.unwrap();
        }
        let options = ProjectListOptions {
            filter: ProjectFilter::None,
            order: OrderBy::NameDesc,
            offset: 0,
            limit: 2,
        }
        .validated()
        .unwrap();
        let projects = db.list_projects(options).await.unwrap();

        assert_eq!(projects.len(), 2);
        assert_eq!(projects[0].name, "Test9");
        assert_eq!(projects[1].name, "Test8");
    }

    #[tokio::test]
    async fn load() {
        let ctx = ProInMemoryContext::test_default();
        let session = create_random_user_session_helper();

        let db = ctx.db(session.clone());

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let id = db.create_project(create.clone()).await.unwrap();
        assert!(db.load_project(id).await.is_ok());

        let session2 = create_random_user_session_helper();
        let db2 = ctx.db(session2);
        let id = db2.create_project(create).await.unwrap();
        assert!(db.load_project(id).await.is_err());

        assert!(db.load_project(ProjectId::new()).await.is_err());
    }

    #[tokio::test]
    async fn create() {
        let ctx = ProInMemoryContext::test_default();
        let session = create_random_user_session_helper();

        let db = ctx.db(session.clone());

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let id = db.create_project(create).await.unwrap();

        assert!(db.load_project(id).await.is_ok());
    }

    #[tokio::test]
    async fn update() {
        let ctx = ProInMemoryContext::test_default();
        let session = create_random_user_session_helper();

        let db = ctx.db(session.clone());

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let id = db.create_project(create).await.unwrap();

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

        db.update_project(update).await.unwrap();

        assert_eq!(db.load_project(id).await.unwrap().name, "Foo");
    }

    #[tokio::test]
    async fn delete() {
        let ctx = ProInMemoryContext::test_default();
        let session = create_random_user_session_helper();

        let db = ctx.db(session.clone());

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let id = db.create_project(create).await.unwrap();

        assert!(db.delete_project(id).await.is_ok());
    }

    #[tokio::test]
    async fn versions() {
        let ctx = ProInMemoryContext::test_default();
        let session = create_random_user_session_helper();

        let db = ctx.db(session.clone());

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let id = db.create_project(create).await.unwrap();

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

        db.update_project(update).await.unwrap();

        let versions = db.list_project_versions(id).await.unwrap();

        assert_eq!(versions.len(), 2);
        assert!(versions[0].changed < versions[1].changed);
    }

    #[tokio::test]
    async fn permissions() {
        let ctx = ProInMemoryContext::test_default();

        let session1 = create_random_user_session_helper();
        let session2 = create_random_user_session_helper();
        let session3 = create_random_user_session_helper();

        let db1 = ctx.db(session1.clone());
        let db2 = ctx.db(session2.clone());
        let db3 = ctx.db(session3.clone());

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let project = db1.create_project(create).await.unwrap();

        assert!(!db2.has_permission(project, Permission::Read).await.unwrap());
        assert!(!db3.has_permission(project, Permission::Read).await.unwrap());

        db1.add_permission(session2.user.id.into(), project, Permission::Read)
            .await
            .unwrap();
        db1.add_permission(session3.user.id.into(), project, Permission::Read)
            .await
            .unwrap();

        assert!(db2.has_permission(project, Permission::Read).await.unwrap());
        assert!(db3.has_permission(project, Permission::Read).await.unwrap());

        db1.remove_permission(session2.user.id.into(), project, Permission::Read)
            .await
            .unwrap();

        assert!(!db2.has_permission(project, Permission::Read).await.unwrap());
    }
}
