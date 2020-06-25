use crate::projects::project::{OrderBy, ProjectFilter, ProjectListing, ProjectId, Project, CreateProject, UpdateProject, UserProjectPermission, ProjectPermission, ProjectListOptions, ProjectVersion, LoadVersion};
use std::collections::HashMap;
use crate::error::Result;
use crate::error;
use snafu::ensure;
use crate::projects::projectdb::ProjectDB;
use crate::util::user_input::Validated;
use crate::users::user::UserId;

#[derive(Default)]
pub struct HashMapProjectDB {
    projects: HashMap<ProjectId, Vec<Project>>,
    permissions: Vec<UserProjectPermission>,
}

impl ProjectDB for HashMapProjectDB {
    /// List projects
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::projects::project::{CreateProject, ProjectId, ProjectFilter, OrderBy, ProjectListOptions, STRectangle, ProjectPermission};
    /// use geoengine_services::projects::projectdb::ProjectDB;
    /// use geoengine_services::projects::hashmap_projectdb::HashMapProjectDB;
    /// use geoengine_services::util::user_input::UserInput;
    /// use geoengine_services::util::identifiers::Identifier;
    /// use geoengine_services::users::user::UserId;
    ///
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserId::new();
    ///
    /// for i in 0..10 {
    ///     let create = CreateProject {
    ///         name: format!("Test{}", i),
    ///         description: format!("Test{}", 10 - i),
    ///         view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    ///         bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    ///     }.validated().unwrap();
    ///     project_db.create(user, create);
    /// }
    /// let options = ProjectListOptions {
    ///     permissions: vec![ProjectPermission::Owner, ProjectPermission::Write, ProjectPermission::Read],
    ///     filter: ProjectFilter::None,
    ///     order: OrderBy::NameDesc,
    ///     offset: 0,
    ///     limit: 2,
    /// }.validated().unwrap();
    /// let projects = project_db.list(user, options);
    ///
    /// assert_eq!(projects.len(), 2);
    /// assert_eq!(projects[0].name, "Test9");
    /// assert_eq!(projects[1].name, "Test8");
    /// ```
    fn list(&self, user: UserId, options: Validated<ProjectListOptions>) -> Vec<ProjectListing> {
        let ProjectListOptions { permissions, filter, order, offset, limit } = options.user_input;
        #[allow(clippy::filter_map)]
            let mut projects = self.permissions.iter()
            .filter(|p| p.user == user && permissions.contains(&p.permission))
            .flat_map(|p| self.projects.get(&p.project).and_then(|p| p.last()))
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

        projects.into_iter().skip(offset).take(limit).collect()
    }

    /// Load a project
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::projects::project::{CreateProject, ProjectId, STRectangle, LoadVersion};
    /// use geoengine_services::projects::projectdb::ProjectDB;
    /// use geoengine_services::projects::hashmap_projectdb::HashMapProjectDB;
    /// use geoengine_services::util::user_input::UserInput;
    /// use geoengine_services::util::identifiers::Identifier;
    /// use geoengine_services::users::user::UserId;
    ///
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserId::new();
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into(),
    ///     view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    ///     bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    /// }.validated().unwrap();
    ///
    /// let id = project_db.create(user, create.clone());
    /// assert!(project_db.load_latest(user, id).is_ok());
    ///
    /// let user2 = UserId::new();
    /// let id = project_db.create(user2, create);
    /// assert!(project_db.load_latest(user, id).is_err());
    ///
    /// assert!(project_db.load_latest(user, ProjectId::new()).is_err())
    /// ```
    fn load(&self, user: UserId, project: ProjectId, version: LoadVersion) -> Result<Project> {
        ensure!(
            self.permissions.iter().any(|p| p.project == project && p.user == user),
            error::ProjectLoadFailed
        );
        let project_versions = self.projects.get(&project).ok_or(error::Error::ProjectLoadFailed)?;
        if let LoadVersion::Version(version) = version {
            Ok(
                project_versions.iter()
                    .find(|p| p.version.id == version)
                    .ok_or(error::Error::ProjectLoadFailed)?.clone()
            )
        } else {
            Ok(project_versions.last().ok_or(error::Error::ProjectLoadFailed)?.clone())
        }
    }

    /// Create a project
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::projects::project::{CreateProject, STRectangle};
    /// use geoengine_services::projects::projectdb::ProjectDB;
    /// use geoengine_services::projects::hashmap_projectdb::HashMapProjectDB;
    /// use geoengine_services::util::user_input::UserInput;
    /// use geoengine_services::util::identifiers::Identifier;
    /// use geoengine_services::users::user::UserId;
    ///
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserId::new();
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into(),
    ///     view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    ///     bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    /// }.validated().unwrap();
    ///
    /// let id = project_db.create(user, create);
    ///
    /// assert!(project_db.load_latest(user, id).is_ok())
    /// ```
    fn create(&mut self, user: UserId, create: Validated<CreateProject>) -> ProjectId {
        let project: Project = Project::from_create_project(create.user_input, user);
        let id = project.id;
        self.projects.insert(id, vec![project]);
        self.permissions.push(UserProjectPermission { user, project: id, permission: ProjectPermission::Owner });
        id
    }

    /// Update a project
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::projects::project::{CreateProject, UpdateProject, STRectangle};
    /// use geoengine_services::projects::projectdb::ProjectDB;
    /// use geoengine_services::projects::hashmap_projectdb::HashMapProjectDB;
    /// use geoengine_services::util::user_input::UserInput;
    /// use geoengine_services::util::identifiers::Identifier;
    /// use geoengine_services::users::user::UserId;
    ///
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserId::new();
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into(),
    ///     view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    ///     bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    /// }.validated().unwrap();
    ///
    /// let id = project_db.create(user, create);
    ///
    /// let update = UpdateProject {
    ///    id,
    ///    name: Some("Foo".into()),
    ///    description: None,
    ///    layers: None,
    ///    view: None,
    ///    bounds:None
    /// }.validated().unwrap();
    ///
    /// project_db.update(user, update).unwrap();
    ///
    /// assert_eq!(project_db.load_latest(user, id).unwrap().name, "Foo");
    /// ```
    fn update(&mut self, user: UserId, update: Validated<UpdateProject>) -> Result<()> {
        let update = update.user_input;

        ensure!(
            self.permissions.iter().any(|p| p.project == update.id && p.user == user &&
            (p.permission == ProjectPermission::Write || p.permission == ProjectPermission::Owner)),
            error::ProjectUpdateFailed
        );

        let project_versions = self.projects.get_mut(&update.id).ok_or(error::Error::ProjectUpdateFailed)?;
        let project = project_versions.last().ok_or(error::Error::ProjectUpdateFailed)?;

        let project_update = project.update_project(update, user);

        ensure!(
            project_update.version > project.version,
            error::ProjectUpdateFailed
        );

        project_versions.push(project_update);

        Ok(())
    }

    /// Delete a project
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::projects::project::{CreateProject, UpdateProject, STRectangle};
    /// use geoengine_services::projects::projectdb::ProjectDB;
    /// use geoengine_services::projects::hashmap_projectdb::HashMapProjectDB;
    /// use geoengine_services::util::user_input::UserInput;
    /// use geoengine_services::util::identifiers::Identifier;
    /// use geoengine_services::users::user::UserId;
    ///
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserId::new();
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into(),
    ///     view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    ///     bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    /// }.validated().unwrap();
    ///
    /// let id = project_db.create(user, create);
    ///
    /// assert!(project_db.delete(user, id).is_ok());
    /// ```
    fn delete(&mut self, user: UserId, project: ProjectId) -> Result<()> {
        ensure!(
            self.permissions.iter().any(|p| p.project == project && p.user == user &&
            p.permission == ProjectPermission::Owner),
            error::ProjectUpdateFailed
        );

        self.projects.remove(&project).map(|_| ()).ok_or(error::Error::ProjectDeleteFailed)
    }

    /// Get the versions of a project
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::projects::project::{CreateProject, UpdateProject, STRectangle};
    /// use geoengine_services::projects::projectdb::ProjectDB;
    /// use geoengine_services::projects::hashmap_projectdb::HashMapProjectDB;
    /// use geoengine_services::util::user_input::UserInput;
    /// use geoengine_services::util::identifiers::Identifier;
    /// use geoengine_services::users::user::UserId;
    /// use std::{thread, time};
    ///
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserId::new();
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into(),
    ///     view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    ///     bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    /// }.validated().unwrap();
    ///
    /// let id = project_db.create(user, create);
    ///
    /// thread::sleep(time::Duration::from_millis(10));
    ///
    /// let update = UpdateProject {
    ///    id,
    ///    name: Some("Foo".into()),
    ///    description: None,
    ///    layers: None,
    ///    view: None,
    ///    bounds:None
    /// }.validated().unwrap();
    ///
    /// project_db.update(user, update).unwrap();
    ///
    /// let versions = project_db.versions(user, id).unwrap();
    ///
    /// assert_eq!(versions.len(), 2);
    /// assert!(versions[0].changed < versions[1].changed);
    /// ```
    fn versions(&self, user: UserId, project: ProjectId) -> Result<Vec<ProjectVersion>> {
        // TODO: pagination?
        ensure!(
            self.permissions.iter().any(|p| p.project == project && p.user == user),
            error::ProjectLoadFailed
        );

        Ok(self.projects.get(&project).ok_or(error::Error::ProjectLoadFailed)?.iter()
            .map(|p| p.version).collect())
    }

    /// List all permissions on a project
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::projects::project::{CreateProject, UpdateProject, ProjectPermission, UserProjectPermission, STRectangle};
    /// use geoengine_services::projects::projectdb::ProjectDB;
    /// use geoengine_services::projects::hashmap_projectdb::HashMapProjectDB;
    /// use geoengine_services::util::user_input::UserInput;
    /// use geoengine_services::util::identifiers::Identifier;
    /// use geoengine_services::users::user::UserId;
    ///
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserId::new();
    ///
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into(),
    ///     view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    ///     bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    /// }.validated().unwrap();
    ///
    /// let project = project_db.create(user, create);
    ///
    /// let user2 = UserId::new();
    /// let user3 = UserId::new();
    ///
    /// let permission1 = UserProjectPermission { user: user2, project, permission: ProjectPermission::Read};
    /// let permission2 = UserProjectPermission { user: user3, project, permission: ProjectPermission::Write};
    ///
    /// project_db.add_permission(user, permission1.clone());
    /// project_db.add_permission(user, permission2.clone());
    ///
    /// let permissions = project_db.list_permissions(user, project).unwrap();
    /// assert!(permissions.contains(&permission1));
    /// assert!(permissions.contains(&permission2));
    /// ```
    fn list_permissions(&mut self, user: UserId, project: ProjectId) -> Result<Vec<UserProjectPermission>> {
        ensure!(
            self.permissions.iter().any(|p| p.project == project && p.user == user),
            error::ProjectLoadFailed
        );

        Ok(self.permissions.iter()
            .filter(|p| p.project == project)
            .cloned()
            .collect())
    }

    /// Add a permissions on a project
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::projects::project::{CreateProject, UpdateProject, ProjectPermission, UserProjectPermission, STRectangle};
    /// use geoengine_services::projects::projectdb::ProjectDB;
    /// use geoengine_services::projects::hashmap_projectdb::HashMapProjectDB;
    /// use geoengine_services::util::user_input::UserInput;
    /// use geoengine_services::util::identifiers::Identifier;
    /// use geoengine_services::users::user::UserId;
    ///
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserId::new();
    ///
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into(),
    ///     view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    ///     bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    /// }.validated().unwrap();
    ///
    /// let project = project_db.create(user, create);
    ///
    /// let user2 = UserId::new();
    /// let user3 = UserId::new();
    ///
    /// let permission1 = UserProjectPermission { user: user2, project, permission: ProjectPermission::Read};
    /// let permission2 = UserProjectPermission { user: user3, project, permission: ProjectPermission::Write};
    ///
    /// project_db.add_permission(user, permission1.clone());
    /// project_db.add_permission(user, permission2.clone());
    ///
    /// let permissions = project_db.list_permissions(user, project).unwrap();
    /// assert!(permissions.contains(&permission1));
    /// assert!(permissions.contains(&permission2));
    /// ```
    fn add_permission(&mut self, user: UserId, permission: UserProjectPermission) -> Result<()> {
        ensure!(
            self.permissions.iter().any(|p| p.project == permission.project && p.user == user &&
            p.permission == ProjectPermission::Owner),
            error::ProjectUpdateFailed
        );

        if !self.permissions.contains(&permission) {
            self.permissions.push(permission);
        }
        Ok(())
    }

    /// Remove a permissions from a project
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::projects::project::{CreateProject, UpdateProject, ProjectPermission, UserProjectPermission, STRectangle};
    /// use geoengine_services::projects::projectdb::ProjectDB;
    /// use geoengine_services::projects::hashmap_projectdb::HashMapProjectDB;
    /// use geoengine_services::util::user_input::UserInput;
    /// use geoengine_services::util::identifiers::Identifier;
    /// use geoengine_services::users::user::UserId;
    ///
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserId::new();
    ///
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into(),
    ///     view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    ///     bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
    /// }.validated().unwrap();
    ///
    /// let project = project_db.create(user, create);
    ///
    /// let user2 = UserId::new();
    /// let user3 = UserId::new();
    ///
    /// let permission1 = UserProjectPermission { user: user2, project, permission: ProjectPermission::Read};
    /// let permission2 = UserProjectPermission { user: user3, project, permission: ProjectPermission::Write};
    ///
    /// project_db.add_permission(user, permission1.clone());
    /// project_db.add_permission(user, permission2.clone());
    ///
    /// project_db.remove_permission(user, permission2.clone());
    ///
    /// let permissions = project_db.list_permissions(user, project).unwrap();
    /// assert!(permissions.contains(&permission1));
    /// assert!(!permissions.contains(&permission2));
    /// ```
    fn remove_permission(&mut self, user: UserId, permission: UserProjectPermission) -> Result<()> {
        ensure!(
            self.permissions.iter().any(|p| p.project == permission.project && p.user == user &&
            p.permission == ProjectPermission::Owner),
            error::ProjectUpdateFailed
        );

        if let Some(i) = self.permissions.iter().position(|p| p == &permission) {
            self.permissions.remove(i);
            Ok(())
        } else {
            Err(error::Error::PermissionFailed)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::projects::project::STRectangle;
    use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D, TimeInterval};
    use crate::util::user_input::UserInput;
    use crate::util::identifiers::Identifier;

    fn strect() -> STRectangle {
        STRectangle {
            bounding_box: BoundingBox2D::new(Coordinate2D::new(0., 0.), Coordinate2D::new(1., 1.)).unwrap(),
            time_interval: TimeInterval::new(0, 1).unwrap(),
        }
    }

    #[test]
    fn list_permitted() {
        let mut project_db = HashMapProjectDB::default();
        let user = UserId::new();
        let user2 = UserId::new();
        let user3 = UserId::new();

        let create = CreateProject {
            name: "Own".into(),
            description: "Text".into(),
            view: strect(),
            bounds: strect(),

        }.validated().unwrap();

        let _ = project_db.create(user, create);

        let create = CreateProject {
            name: "User2's".into(),
            description: "Text".into(),
            view: strect(),
            bounds: strect(),
        }.validated().unwrap();

        let project2 = project_db.create(user2, create);

        let create = CreateProject {
            name: "User3's".into(),
            description: "Text".into(),
            view: strect(),
            bounds: strect(),
        }.validated().unwrap();

        let project3 = project_db.create(user3, create);

        let permission1 = UserProjectPermission { user, project: project2, permission: ProjectPermission::Read };
        let permission2 = UserProjectPermission { user, project: project3, permission: ProjectPermission::Write };

        project_db.add_permission(user2, permission1).unwrap();
        project_db.add_permission(user3, permission2).unwrap();

        let options = ProjectListOptions {
            permissions: vec![ProjectPermission::Owner, ProjectPermission::Write, ProjectPermission::Read],
            filter: ProjectFilter::None,
            order: OrderBy::NameDesc,
            offset: 0,
            limit: 3,
        }.validated().unwrap();

        let projects = project_db.list(user, options);

        assert!(projects.iter().any(|p| p.name == "Own"));
        assert!(projects.iter().any(|p| p.name == "User2's"));
        assert!(projects.iter().any(|p| p.name == "User3's"));

        let options = ProjectListOptions {
            permissions: vec![ProjectPermission::Owner],
            filter: ProjectFilter::None,
            order: OrderBy::NameDesc,
            offset: 0,
            limit: 3,
        }.validated().unwrap();

        let projects = project_db.list(user, options);
        assert!(projects[0].name == "Own");
        assert_eq!(projects.len(), 1);
    }
}
