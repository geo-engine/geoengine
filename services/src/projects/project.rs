use uuid::Uuid;
use crate::workflows::Workflow;
use crate::users::user::{UserIdentification, UserInput};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::error::Error;
use crate::error;
use snafu::ensure;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Copy, Hash)]
pub struct ProjectId {
    id: Uuid
}

impl ProjectId {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4()
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct Project {
    pub id: ProjectId,
    pub version: usize,
    pub changed: DateTime<Utc>,
    pub name: String,
    pub description: String,
    pub layers: Vec<Layer>,
    //TODO: stref
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct Layer {
    pub workflow: Workflow,
    pub name: String,
    //TODO: colorizer
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub enum OrderBy {
    DateAsc,
    DateDesc,
    NameAsc,
    NameDesc
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct ProjectListing {
    pub id: ProjectId,
    pub name: String,
    pub description: String,
    pub layer_name: Vec<String>,
    pub changed: DateTime<Utc>
}

impl From<Project> for ProjectListing {
    fn from(project: Project) -> Self {
        Self {
            id: project.id,
            name: project.name,
            description: project.description,
            layer_name: project.layers.into_iter().map(|l| l.name).collect(),
            changed: project.changed
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub enum ProjectFilter {
    Name { term: String },
    Description { term: String },
    None,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct CreateProject {
    pub name: String,
    pub description: String,
    // TODO: stref
}

impl UserInput for CreateProject {
    fn validate(&self) -> Result<(), Error> {
        ensure!(
            !(self.name.is_empty() || self.description.is_empty()),
            error::ProjectCreateFailed
        );

        Ok(())
    }
}

impl From<CreateProject> for Project {
    fn from(create: CreateProject) -> Self {
        Self {
            id: ProjectId::new(),
            version: 0,
            changed: Utc::now(),
            name: create.name,
            description: create.description,
            layers: vec![],
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct UpdateProject {
    pub id: ProjectId,
    pub name: Option<String>,
    pub description: Option<String>,
    pub layers: Option<Vec<Option<Layer>>>,
    // TODO: stref
}

impl UserInput for UpdateProject {
    fn validate(&self) -> Result<(), Error> {
        if let Some(name) = &self.name {
            ensure!(
                !name.is_empty(),
                error::ProjectUpdateFailed
            );
        }

        if let Some(description) = &self.description {
            ensure!(
                !description.is_empty(),
                error::ProjectUpdateFailed
            );
        }

        // TODO: layers

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub enum ProjectPermission {
    Read,
    Write,
    Owner,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserProjectPermission {
    pub user: UserIdentification,
    pub project: ProjectId,
    pub permission: ProjectPermission,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct ProjectListOptions {
    pub only_owned: bool,
    pub filter: ProjectFilter,
    pub order: OrderBy,
    pub offset: usize,
    pub limit: usize,
}

impl UserInput for ProjectListOptions {
    fn validate(&self) -> Result<(), Error> {
        ensure!(
            self.limit <= 20, // TODO: configuration
            error::ProjectListFailed
        );

        Ok(())
    }
}
