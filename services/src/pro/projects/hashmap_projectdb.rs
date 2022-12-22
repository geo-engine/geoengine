use crate::contexts::Db;
use crate::error;
use crate::error::Result;
use crate::pro::projects::{ProProjectDb, ProjectPermission, UserProjectPermission};
use crate::pro::users::UserSession;
use crate::projects::{
    CreateProject, OrderBy, Project, ProjectDb, ProjectFilter, ProjectId, ProjectListOptions,
    ProjectListing, ProjectVersion, UpdateProject,
};
use crate::util::user_input::Validated;
use async_trait::async_trait;
use snafu::ensure;
use std::collections::HashMap;

use super::LoadVersion;

#[derive(Default)]
pub struct ProHashMapProjectDb {
    projects: Db<HashMap<ProjectId, Vec<Project>>>,
    permissions: Db<Vec<UserProjectPermission>>,
}

#[async_trait]
impl ProjectDb<UserSession> for ProHashMapProjectDb {
    /// List projects
    async fn list(
        &self,
        session: &UserSession,
        options: Validated<ProjectListOptions>,
    ) -> Result<Vec<ProjectListing>> {
        let ProjectListOptions {
            filter,
            order,
            offset,
            limit,
        } = options.user_input;

        let all_projects = self.projects.read().await;

        #[allow(clippy::flat_map_option)]
        let mut projects = self
            .permissions
            .read()
            .await
            .iter()
            .filter(|p| p.user == session.user.id) // && permissions.contains(&p.permission))
            .flat_map(|p| all_projects.get(&p.project).and_then(|p| p.last()))
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

    /// Create a project
    async fn create(
        &self,
        session: &UserSession,
        create: Validated<CreateProject>,
    ) -> Result<ProjectId> {
        let project: Project = Project::from_create_project(create.user_input);
        let id = project.id;
        self.projects.write().await.insert(id, vec![project]);
        self.permissions.write().await.push(UserProjectPermission {
            project: id,
            permission: ProjectPermission::Owner,
            user: session.user.id,
        });
        Ok(id)
    }

    /// Update a project
    async fn update(&self, session: &UserSession, update: Validated<UpdateProject>) -> Result<()> {
        let update = update.user_input;

        ensure!(
            self.permissions
                .read()
                .await
                .iter()
                .any(|p| p.project == update.id
                    && p.user == session.user.id
                    && (p.permission == ProjectPermission::Write
                        || p.permission == ProjectPermission::Owner)),
            error::ProjectUpdateFailed
        );

        let mut projects = self.projects.write().await;

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
    async fn delete(&self, session: &UserSession, project: ProjectId) -> Result<()> {
        ensure!(
            self.permissions
                .read()
                .await
                .iter()
                .any(|p| p.project == project
                    && p.user == session.user.id
                    && p.permission == ProjectPermission::Owner),
            error::ProjectUpdateFailed
        );

        self.projects
            .write()
            .await
            .remove(&project)
            .map(|_| ())
            .ok_or(error::Error::ProjectDeleteFailed)
    }

    async fn load(&self, session: &UserSession, project: ProjectId) -> Result<Project> {
        self.load_version(session, project, LoadVersion::Latest)
            .await
    }
}

#[async_trait]
impl ProProjectDb for ProHashMapProjectDb {
    /// Load a project
    async fn load_version(
        &self,
        session: &UserSession,
        project: ProjectId,
        version: LoadVersion,
    ) -> Result<Project> {
        ensure!(
            self.permissions
                .read()
                .await
                .iter()
                .any(|p| p.project == project && p.user == session.user.id),
            error::ProjectLoadFailed
        );

        let projects = self.projects.read().await;

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
    async fn versions(
        &self,
        session: &UserSession,
        project: ProjectId,
    ) -> Result<Vec<ProjectVersion>> {
        // TODO: pagination?
        ensure!(
            self.permissions
                .read()
                .await
                .iter()
                .any(|p| p.project == project && p.user == session.user.id),
            error::ProjectLoadFailed
        );

        Ok(self
            .projects
            .read()
            .await
            .get(&project)
            .ok_or(error::Error::ProjectLoadFailed)?
            .iter()
            .map(|p| p.version)
            .collect())
    }

    /// List all permissions on a project
    async fn list_permissions(
        &self,
        session: &UserSession,
        project: ProjectId,
    ) -> Result<Vec<UserProjectPermission>> {
        ensure!(
            self.permissions
                .read()
                .await
                .iter()
                .any(|p| p.project == project && p.user == session.user.id),
            error::ProjectLoadFailed
        );

        Ok(self
            .permissions
            .read()
            .await
            .iter()
            .filter(|p| p.project == project)
            .cloned()
            .collect())
    }

    /// Add a permissions on a project
    async fn add_permission(
        &self,
        session: &UserSession,
        permission: UserProjectPermission,
    ) -> Result<()> {
        let mut permissions = self.permissions.write().await;
        ensure!(
            permissions.iter().any(|p| p.project == permission.project
                && p.user == session.user.id
                && p.permission == ProjectPermission::Owner),
            error::ProjectUpdateFailed
        );

        if !permissions.contains(&permission) {
            permissions.push(permission);
        }
        Ok(())
    }

    /// Remove a permissions from a project
    async fn remove_permission(
        &self,
        session: &UserSession,
        permission: UserProjectPermission,
    ) -> Result<()> {
        let mut permissions = self.permissions.write().await;

        ensure!(
            permissions.iter().any(|p| p.project == permission.project
                && p.user == session.user.id
                && p.permission == ProjectPermission::Owner),
            error::ProjectUpdateFailed
        );

        permissions.iter().position(|p| p == &permission).map_or(
            Err(error::Error::PermissionFailed),
            |i| {
                permissions.remove(i);
                Ok(())
            },
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pro::users::UserId;
    use crate::pro::util::tests::create_random_user_session_helper;
    use crate::projects::STRectangle;
    use crate::util::user_input::UserInput;
    use crate::util::Identifier;
    use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D, TimeInterval};
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
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
        let project_db = ProHashMapProjectDb::default();

        let session1 = create_random_user_session_helper();
        let session2 = create_random_user_session_helper();
        let session3 = create_random_user_session_helper();

        let create = CreateProject {
            name: "Own".into(),
            description: "Text".into(),
            bounds: strect(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let _ = project_db.create(&session1, create).await.unwrap();

        let create = CreateProject {
            name: "User2's".into(),
            description: "Text".into(),
            bounds: strect(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let project2 = project_db.create(&session2, create).await.unwrap();

        let create = CreateProject {
            name: "User3's".into(),
            description: "Text".into(),
            bounds: strect(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let project3 = project_db.create(&session3, create).await.unwrap();

        let permission1 = UserProjectPermission {
            user: session1.user.id,
            project: project2,
            permission: ProjectPermission::Read,
        };
        let permission2 = UserProjectPermission {
            user: session1.user.id,
            project: project3,
            permission: ProjectPermission::Write,
        };

        project_db
            .add_permission(&session2, permission1)
            .await
            .unwrap();
        project_db
            .add_permission(&session3, permission2)
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

        let projects = project_db.list(&session1, options).await.unwrap();

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

        let projects = project_db.list(&session1, options).await.unwrap();
        assert!(projects[0].name == "Own");
        assert_eq!(projects.len(), 1);
    }

    #[tokio::test]
    async fn list() {
        let project_db = ProHashMapProjectDb::default();
        let session = create_random_user_session_helper();

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
        let project_db = ProHashMapProjectDb::default();
        let session = create_random_user_session_helper();

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

        let session2 = create_random_user_session_helper();
        let id = project_db.create(&session2, create).await.unwrap();
        assert!(project_db.load(&session, id).await.is_err());

        assert!(project_db.load(&session, ProjectId::new()).await.is_err());
    }

    #[tokio::test]
    async fn create() {
        let project_db = ProHashMapProjectDb::default();
        let session = create_random_user_session_helper();

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
        let project_db = ProHashMapProjectDb::default();
        let session = create_random_user_session_helper();

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
        let project_db = ProHashMapProjectDb::default();
        let session = create_random_user_session_helper();

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
        let project_db = ProHashMapProjectDb::default();
        let session = create_random_user_session_helper();

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

    #[tokio::test]
    async fn permissions() {
        let project_db = ProHashMapProjectDb::default();
        let session = create_random_user_session_helper();

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let project = project_db.create(&session, create).await.unwrap();

        let user2 = UserId::new();
        let user3 = UserId::new();

        let permission1 = UserProjectPermission {
            user: user2,
            project,
            permission: ProjectPermission::Read,
        };
        let permission2 = UserProjectPermission {
            user: user3,
            project,
            permission: ProjectPermission::Write,
        };

        project_db
            .add_permission(&session, permission1.clone())
            .await
            .unwrap();
        project_db
            .add_permission(&session, permission2.clone())
            .await
            .unwrap();

        let permissions = project_db
            .list_permissions(&session, project)
            .await
            .unwrap();
        assert!(permissions.contains(&permission1));
        assert!(permissions.contains(&permission2));
    }

    #[tokio::test]
    async fn add_permission() {
        let project_db = ProHashMapProjectDb::default();
        let session = create_random_user_session_helper();

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let project = project_db.create(&session, create).await.unwrap();

        let session2 = create_random_user_session_helper();
        let session3 = create_random_user_session_helper();

        let permission1 = UserProjectPermission {
            user: session2.user.id,
            project,
            permission: ProjectPermission::Read,
        };
        let permission2 = UserProjectPermission {
            user: session3.user.id,
            project,
            permission: ProjectPermission::Write,
        };

        project_db
            .add_permission(&session, permission1.clone())
            .await
            .unwrap();
        project_db
            .add_permission(&session, permission2.clone())
            .await
            .unwrap();

        let permissions = project_db
            .list_permissions(&session, project)
            .await
            .unwrap();
        assert!(permissions.contains(&permission1));
        assert!(permissions.contains(&permission2));
    }

    #[tokio::test]
    async fn remove_permission() {
        let project_db = ProHashMapProjectDb::default();
        let session = create_random_user_session_helper();

        let create = CreateProject {
            name: "Test".into(),
            description: "Text".into(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        }
        .validated()
        .unwrap();

        let project = project_db.create(&session, create).await.unwrap();

        let session2 = create_random_user_session_helper();
        let session3 = create_random_user_session_helper();

        let permission1 = UserProjectPermission {
            user: session2.user.id,
            project,
            permission: ProjectPermission::Read,
        };
        let permission2 = UserProjectPermission {
            user: session3.user.id,
            project,
            permission: ProjectPermission::Write,
        };

        project_db
            .add_permission(&session, permission1.clone())
            .await
            .unwrap();
        project_db
            .add_permission(&session, permission2.clone())
            .await
            .unwrap();

        project_db
            .remove_permission(&session, permission2.clone())
            .await
            .unwrap();

        let permissions = project_db
            .list_permissions(&session, project)
            .await
            .unwrap();
        assert!(permissions.contains(&permission1));
        assert!(!permissions.contains(&permission2));
    }
}
