use crate::projects::project::{
    CreateProject, Project, ProjectId, ProjectListOptions, ProjectListing, UpdateProject,
};
use crate::util::user_input::Validated;
use crate::{contexts::Session, error::Result};
use async_trait::async_trait;

/// Storage of user projects
#[async_trait]
pub trait ProjectDb<S: Session>: Send + Sync {
    /// List all datasets accessible to `user` that match the `options`
    async fn list(
        &self,
        session: &S,
        options: Validated<ProjectListOptions>,
    ) -> Result<Vec<ProjectListing>>;

    /// Load the the latest version of the `project` for the `user`
    async fn load(&self, session: &S, project: ProjectId) -> Result<Project>;

    /// Create a new `project` for the `user`
    async fn create(&mut self, session: &S, project: Validated<CreateProject>)
        -> Result<ProjectId>;

    /// Update a `project` for the `user`. A new version is created
    async fn update(&mut self, session: &S, project: Validated<UpdateProject>) -> Result<()>;

    /// Delete the `project` if `user` is an owner
    async fn delete(&mut self, session: &S, project: ProjectId) -> Result<()>;
}
