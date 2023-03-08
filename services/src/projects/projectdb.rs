use crate::error::Result;
use crate::projects::project::{
    CreateProject, Project, ProjectId, ProjectListOptions, ProjectListing, UpdateProject,
};
use crate::util::user_input::Validated;
use async_trait::async_trait;

/// Storage of user projects
#[async_trait]
pub trait ProjectDb: Send + Sync {
    /// List all datasets accessible to `user` that match the `options`
    async fn list_projects(
        &self,
        options: Validated<ProjectListOptions>,
    ) -> Result<Vec<ProjectListing>>;

    /// Load the the latest version of the `project` for the `user`
    async fn load_project(&self, project: ProjectId) -> Result<Project>;

    /// Create a new `project` for the `user`
    async fn create_project(&self, project: Validated<CreateProject>) -> Result<ProjectId>;

    /// Update a `project` for the `user`. A new version is created
    async fn update_project(&self, project: Validated<UpdateProject>) -> Result<()>;

    /// Delete the `project` if `user` is an owner
    async fn delete_project(&self, project: ProjectId) -> Result<()>;
}
