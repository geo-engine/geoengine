use crate::error::Result;
use crate::projects::project::{
    CreateProject, LoadVersion, Project, ProjectId, ProjectListOptions, ProjectListing,
    ProjectVersion, UpdateProject, UserProjectPermission,
};
use crate::users::user::UserId;
use crate::util::user_input::Validated;

/// Storage of user projects
pub trait ProjectDB: Send + Sync {
    // TODO: pass session instead of UserIdentification?

    /// List all data sets accessible to `user` that match the `options`
    fn list(&self, user: UserId, options: Validated<ProjectListOptions>) -> Vec<ProjectListing>;

    /// Load the the `version` of the `project` for the `user`
    fn load(&self, user: UserId, project: ProjectId, version: LoadVersion) -> Result<Project>;

    /// Load the the latest version of the `project` for the `user`
    fn load_latest(&self, user: UserId, project: ProjectId) -> Result<Project> {
        self.load(user, project, LoadVersion::Latest)
    }

    /// Create a new `project` for the `user`
    fn create(&mut self, user: UserId, project: Validated<CreateProject>) -> ProjectId;

    /// Update a `project` for the `user`. A new version is created
    fn update(&mut self, user: UserId, project: Validated<UpdateProject>) -> Result<()>;

    /// Delete the `project` if `user` is an owner
    fn delete(&mut self, user: UserId, project: ProjectId) -> Result<()>;

    /// List all versions of the `project` if given `user` has at lest read permission
    fn versions(&self, user: UserId, project: ProjectId) -> Result<Vec<ProjectVersion>>;

    /// List all permissions of users for the `project` if the `user` is an owner
    fn list_permissions(
        &mut self,
        user: UserId,
        project: ProjectId,
    ) -> Result<Vec<UserProjectPermission>>;

    /// Add a `permission` if the `user` is owner of the permission's target project
    fn add_permission(&mut self, user: UserId, permission: UserProjectPermission) -> Result<()>;

    /// Remove a `permission` if the `user` is owner of the target project
    fn remove_permission(&mut self, user: UserId, permission: UserProjectPermission) -> Result<()>;
}
