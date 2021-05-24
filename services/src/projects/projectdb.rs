use crate::projects::project::{
    CreateProject, LoadVersion, Project, ProjectId, ProjectListOptions, ProjectListing,
    ProjectVersion, UpdateProject,
};
use crate::util::user_input::Validated;
use crate::{contexts::Session, error::Result};
use async_trait::async_trait;

/// Storage of user projects
#[async_trait]
pub trait ProjectDb<S: Session>: Send + Sync {
    // TODO: pass session instead of UserIdentification?

    /// List all datasets accessible to `user` that match the `options`
    async fn list(
        &self,
        session: &S,
        options: Validated<ProjectListOptions>,
    ) -> Result<Vec<ProjectListing>>;

    /// Load the the `version` of the `project` for the `user`
    async fn load(&self, session: &S, project: ProjectId, version: LoadVersion) -> Result<Project>;

    /// Load the the latest version of the `project` for the `user`
    async fn load_latest(&self, session: &S, project: ProjectId) -> Result<Project> {
        self.load(session, project, LoadVersion::Latest).await
    }

    /// Create a new `project` for the `user`
    async fn create(&mut self, session: &S, project: Validated<CreateProject>)
        -> Result<ProjectId>;

    /// Update a `project` for the `user`. A new version is created
    async fn update(&mut self, session: &S, project: Validated<UpdateProject>) -> Result<()>;

    /// Delete the `project` if `user` is an owner
    async fn delete(&mut self, session: &S, project: ProjectId) -> Result<()>;

    /// List all versions of the `project` if given `user` has at lest read permission
    async fn versions(&self, session: &S, project: ProjectId) -> Result<Vec<ProjectVersion>>;
}
