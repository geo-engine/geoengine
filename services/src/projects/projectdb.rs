use crate::projects::project::{OrderBy, ProjectFilter, ProjectListing, ProjectOwner, ProjectId, Project, CreateProject, UpdateProject, UserProjectPermission, ProjectPermission, ProjectListOptions};
use crate::users::user::{UserIdentification, Validated};
use std::collections::HashMap;
use crate::error::Result;
use crate::error;
use snafu::ensure;

pub trait ProjectDB: Send + Sync {
    // TODO: pass session instead of UserIdentification?
    fn list(&self, user: UserIdentification, options: Validated<ProjectListOptions>) -> Vec<ProjectListing>;
    fn load(&self, user: UserIdentification, project: ProjectId) -> Result<Project>;
    fn create(&mut self, user: UserIdentification, project: Validated<CreateProject>) -> ProjectId;
    fn update(&mut self, user: UserIdentification, project: Validated<UpdateProject>) -> Result<()>;
    fn delete(&mut self, user: UserIdentification, project: ProjectId) -> Result<()>;

    fn list_permissions(&mut self, user: UserIdentification, project: ProjectId) -> Result<Vec<UserProjectPermission>>;
    fn add_permission(&mut self, user: UserIdentification, permission: UserProjectPermission) -> Result<()>;
    fn remove_permission(&mut self, user: UserIdentification, permission: UserProjectPermission) -> Result<()>;
}

#[derive(Default)]
pub struct HashMapProjectDB {
    projects: HashMap<ProjectId, Project>,
    permissions: Vec<UserProjectPermission>,
}

// TODO: versioning?
impl ProjectDB for HashMapProjectDB {
    /// List projects
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::users::user::{UserIdentification, UserInput};
    /// use geoengine_services::projects::project::{CreateProject, ProjectId, ProjectOwner, ProjectFilter, OrderBy, ProjectListOptions};
    /// use geoengine_services::projects::projectdb::{ProjectDB, HashMapProjectDB};
    ///
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserIdentification::new();
    ///
    /// for i in 0..10 {
    ///     let create = CreateProject {
    ///         name: format!("Test{}", i),
    ///         description: format!("Test{}", 10 - i)
    ///     }.validated().unwrap();
    ///     project_db.create(user, create);
    /// }
    /// let options = ProjectListOptions {
    ///     owner: ProjectOwner::Any,
    ///     filter: ProjectFilter::None,
    ///     order: OrderBy::NameDesc,
    ///     offset: 0,
    ///     limit: 2,
    /// }.validated().unwrap();
    /// let projects = project_db.list(user, options);
    ///
    /// assert_eq!(projects.len(), 2)
    /// ```
    fn list(&self, user: UserIdentification, options: Validated<ProjectListOptions>) -> Vec<ProjectListing> {
        let ProjectListOptions {  owner, filter, order, offset, limit } = options.user_input;
        let mut projects = self.permissions.iter()
            .filter(|p| p.user == user && (owner == ProjectOwner::Any || owner == ProjectOwner::User { user }))
            .map(|p| self.projects.get(&p.project).cloned())
            .flatten()
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
    /// use geoengine_services::users::user::{UserIdentification, UserInput};
    /// use geoengine_services::projects::project::{CreateProject, ProjectId};
    /// use geoengine_services::projects::projectdb::{ProjectDB, HashMapProjectDB};
    ///
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserIdentification::new();
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into()
    /// }.validated().unwrap();
    ///
    /// let id = project_db.create(user, create.clone());
    /// assert!(project_db.load(user, id).is_ok());
    ///
    /// let user2 = UserIdentification::new();
    /// let id = project_db.create(user2, create);
    /// assert!(project_db.load(user, id).is_err());
    ///
    /// assert!(project_db.load(user, ProjectId::new()).is_err())
    /// ```
    fn load(&self, user: UserIdentification, project: ProjectId) -> Result<Project> {
        ensure!(
            self.permissions.iter().any(|p| p.project == project && p.user == user),
            error::ProjectLoadFailed
        );
        self.projects.get(&project).cloned().ok_or(error::Error::ProjectLoadFailed)
    }

    /// Create a project
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::users::user::{UserIdentification, UserInput};
    /// use geoengine_services::projects::project::CreateProject;
    /// use geoengine_services::projects::projectdb::{ProjectDB, HashMapProjectDB};
    ///
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserIdentification::new();
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into()
    /// }.validated().unwrap();
    ///
    /// let id = project_db.create(user, create);
    ///
    /// assert!(project_db.load(user, id).is_ok())
    /// ```
    fn create(&mut self, user: UserIdentification, create: Validated<CreateProject>) -> ProjectId {
        let project: Project = create.user_input.into();
        let id = project.id.clone();
        self.projects.insert(id.clone(), project);
        self.permissions.push(UserProjectPermission { user: user.clone(), project: id.clone(), permission: ProjectPermission::Owner });
        id
    }

    /// Update a project
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::users::user::{UserIdentification, UserInput};
    /// use geoengine_services::projects::project::{CreateProject, UpdateProject};
    ///use geoengine_services::projects::projectdb::{ProjectDB, HashMapProjectDB};
    /// 
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserIdentification::new();
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into()
    /// }.validated().unwrap();
    ///
    /// let id = project_db.create(user, create);
    ///
    /// let update = UpdateProject {
    ///    id,
    ///    name: Some("Foo".into()),
    ///    description: None,
    ///    layers: None,
    /// }.validated().unwrap();
    ///
    /// project_db.update(user, update).unwrap();
    ///
    /// assert_eq!(project_db.load(user, id).unwrap().name, "Foo");
    /// ```
    fn update(&mut self, user: UserIdentification, update: Validated<UpdateProject>) -> Result<()> {
        let update = update.user_input;

        ensure!(
            self.permissions.iter().any(|p| p.project == update.id && p.user == user &&
            (p.permission == ProjectPermission::Write || p.permission == ProjectPermission::Owner)),
            error::ProjectUpdateFailed
        );

        if let Some(project) = self.projects.get_mut(&update.id) {
            if let Some(name) = update.name {
                project.name = name;
            }

            if let Some(description) = update.description {
                project.description = description;
            }

            if let Some(layers) = update.layers {
                for (i, layer) in layers.into_iter().enumerate() {
                    if let Some(layer) = layer {
                        if i >= project.layers.len() {
                            project.layers.push(layer);
                        } else {
                            project.layers[i] = layer;
                        }
                    }
                }
            }

            Ok(())
        } else {
            Err(error::Error::ProjectUpdateFailed)
        }
    }

    /// Delete a project
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::users::user::{UserIdentification, UserInput};
    /// use geoengine_services::projects::project::{CreateProject, UpdateProject};
    /// use geoengine_services::projects::projectdb::{ProjectDB, HashMapProjectDB};
    /// 
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserIdentification::new();
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into()
    /// }.validated().unwrap();
    ///
    /// let id = project_db.create(user, create);
    ///
    /// assert!(project_db.delete(user, id).is_ok());
    /// ```
    fn delete(&mut self, user: UserIdentification, project: ProjectId) -> Result<()> {
        ensure!(
            self.permissions.iter().any(|p| p.project == project && p.user == user &&
            p.permission == ProjectPermission::Owner),
            error::ProjectUpdateFailed
        );

        self.projects.remove(&project).map(|_| ()).ok_or(error::Error::ProjectDeleteFailed)
    }

    /// List all permissions on a project
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_services::users::user::{UserIdentification, UserInput};
    /// use geoengine_services::projects::project::{CreateProject, UpdateProject, ProjectPermission, UserProjectPermission};
    /// use geoengine_services::projects::projectdb::{ProjectDB, HashMapProjectDB};
    /// 
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserIdentification::new();
    ///
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into()
    /// }.validated().unwrap();
    ///
    /// let project = project_db.create(user, create);
    ///
    /// let user2 = UserIdentification::new();
    /// let user3 = UserIdentification::new();
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
    fn list_permissions(&mut self, user: UserIdentification, project: ProjectId) -> Result<Vec<UserProjectPermission>> {
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
    /// use geoengine_services::users::user::{UserIdentification, UserInput};
    /// use geoengine_services::projects::project::{CreateProject, UpdateProject, ProjectPermission, UserProjectPermission};
    /// use geoengine_services::projects::projectdb::{ProjectDB, HashMapProjectDB};
    /// 
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserIdentification::new();
    ///
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into()
    /// }.validated().unwrap();
    ///
    /// let project = project_db.create(user, create);
    ///
    /// let user2 = UserIdentification::new();
    /// let user3 = UserIdentification::new();
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
    fn add_permission(&mut self, user: UserIdentification, permission: UserProjectPermission) -> Result<()> {
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
    /// use geoengine_services::users::user::{UserIdentification, UserInput};
    /// use geoengine_services::projects::project::{CreateProject, UpdateProject, ProjectPermission, UserProjectPermission};
    /// use geoengine_services::projects::projectdb::{ProjectDB, HashMapProjectDB};
    ///
    /// let mut project_db = HashMapProjectDB::default();
    /// let user = UserIdentification::new();
    ///
    ///
    /// let create = CreateProject {
    ///     name: "Test".into(),
    ///     description: "Text".into()
    /// }.validated().unwrap();
    ///
    /// let project = project_db.create(user, create);
    ///
    /// let user2 = UserIdentification::new();
    /// let user3 = UserIdentification::new();
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
    fn remove_permission(&mut self, user: UserIdentification, permission:UserProjectPermission) -> Result<()> {
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
