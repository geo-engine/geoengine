use crate::error::Result;
use crate::projects::project::{
    CreateProject, LoadVersion, Project, ProjectId, ProjectListOptions, ProjectListing,
    ProjectVersion, UpdateProject, UserProjectPermission,
};
use crate::users::user::UserId;
use crate::util::user_input::Validated;
use async_trait::async_trait;

/// Storage of user projects
#[async_trait]
pub trait ProjectDB: Send + Sync {
    // TODO: pass session instead of UserIdentification?

    /// List all data sets accessible to `user` that match the `options`
    async fn list(
        &self,
        user: UserId,
        options: Validated<ProjectListOptions>,
    ) -> Result<Vec<ProjectListing>>;

    /// Load the the `version` of the `project` for the `user`
    async fn load(&self, user: UserId, project: ProjectId, version: LoadVersion)
        -> Result<Project>;

    /// Load the the latest version of the `project` for the `user`
    async fn load_latest(&self, user: UserId, project: ProjectId) -> Result<Project> {
        self.load(user, project, LoadVersion::Latest).await
    }

    /// Create a new `project` for the `user`
    async fn create(
        &mut self,
        user: UserId,
        project: Validated<CreateProject>,
    ) -> Result<ProjectId>;

    /// Update a `project` for the `user`. A new version is created
    async fn update(&mut self, user: UserId, project: Validated<UpdateProject>) -> Result<()>;

    /// Delete the `project` if `user` is an owner
    async fn delete(&mut self, user: UserId, project: ProjectId) -> Result<()>;

    /// List all versions of the `project` if given `user` has at lest read permission
    async fn versions(&self, user: UserId, project: ProjectId) -> Result<Vec<ProjectVersion>>;

    /// List all permissions of users for the `project` if the `user` is an owner
    async fn list_permissions(
        &self,
        user: UserId,
        project: ProjectId,
    ) -> Result<Vec<UserProjectPermission>>;

    /// Add a `permission` if the `user` is owner of the permission's target project
    async fn add_permission(
        &mut self,
        user: UserId,
        permission: UserProjectPermission,
    ) -> Result<()>;

    /// Remove a `permission` if the `user` is owner of the target project
    async fn remove_permission(
        &mut self,
        user: UserId,
        permission: UserProjectPermission,
    ) -> Result<()>;
}
