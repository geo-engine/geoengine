use crate::error::Result;
use crate::projects::{Project, ProjectDb, ProjectId, ProjectVersion};
use crate::{
    projects::{OrderBy, ProjectFilter},
};
use async_trait::async_trait;

use serde::{Deserialize, Serialize};


use super::LoadVersion;

/// Storage of user projects
#[async_trait]
pub trait ProProjectDb: ProjectDb {
    /// Load the the `version` of the `project` for the `user`
    async fn load_project_version(
        &self,
        project: ProjectId,
        version: LoadVersion,
    ) -> Result<Project>;

    /// List all versions of the `project` if given `user` has at least read permission
    async fn list_project_versions(&self, project: ProjectId) -> Result<Vec<ProjectVersion>>;
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct ProjectListOptions {
    #[serde(default)]
    pub filter: ProjectFilter,
    pub order: OrderBy,
    pub offset: u32,
    pub limit: u32,
}
