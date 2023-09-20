use crate::projects::project::{
    CreateProject, Project, ProjectId, ProjectListOptions, ProjectListing, UpdateProject,
};

use async_trait::async_trait;

use super::{error::ProjectDbError, LoadVersion, ProjectVersion};

/// Storage of user projects
#[async_trait]
pub trait ProjectDb: Send + Sync {
    /// List all datasets accessible to `user` that match the `options`
    async fn list_projects(
        &self,
        options: ProjectListOptions,
    ) -> Result<Vec<ProjectListing>, ProjectDbError>;

    /// Load the the latest version of the `project` for the `user`
    async fn load_project(&self, project: ProjectId) -> Result<Project, ProjectDbError>;

    /// Create a new `project` for the `user`
    async fn create_project(&self, project: CreateProject) -> Result<ProjectId, ProjectDbError>;

    /// Update a `project` for the `user`. A new version is created
    async fn update_project(&self, project: UpdateProject) -> Result<(), ProjectDbError>;

    /// Delete the `project` if `user` is an owner
    async fn delete_project(&self, project: ProjectId) -> Result<(), ProjectDbError>;

    /// Load the the `version` of the `project` for the `user`
    async fn load_project_version(
        &self,
        project: ProjectId,
        version: LoadVersion,
    ) -> Result<Project, ProjectDbError>;

    /// List all versions of the `project` if given `user` has at least read permission
    async fn list_project_versions(
        &self,
        project: ProjectId,
    ) -> Result<Vec<ProjectVersion>, ProjectDbError>;
}
