use crate::error;
use crate::error::Error;
use crate::error::Result;
use crate::users::user::UserId;
use crate::util::identifiers::Identifier;
use crate::util::user_input::UserInput;
use crate::workflows::workflow::WorkflowId;
use chrono::{DateTime, Utc};
use geoengine_datatypes::operations::image::Colorizer;
use geoengine_datatypes::primitives::{
    BoundingBox2D, Coordinate2D, SpatialBounded, TemporalBounded, TimeInterval,
};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use std::cmp::Ordering;
use uuid::Uuid;

identifier!(ProjectId);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Project {
    pub id: ProjectId,
    pub version: ProjectVersion,
    pub name: String,
    pub description: String,
    pub layers: Vec<Layer>,
    pub view: STRectangle,
    pub bounds: STRectangle,
    // TODO: spatial reference system, must be either stored in the rectangle/bbox or globally for project
}

impl Project {
    pub fn from_create_project(create: CreateProject, user: UserId) -> Self {
        Self {
            id: ProjectId::new(),
            version: ProjectVersion::new(user),
            name: create.name,
            description: create.description,
            layers: vec![],
            view: create.view,
            bounds: create.bounds,
        }
    }

    pub fn update_project(&self, update: UpdateProject, user: UserId) -> Project {
        let mut project = self.clone();
        project.version = ProjectVersion::new(user);

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

        if let Some(view) = update.view {
            project.view = view;
        }

        if let Some(bounds) = update.bounds {
            project.bounds = bounds;
        }

        project
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct STRectangle {
    pub bounding_box: BoundingBox2D,
    pub time_interval: TimeInterval,
}

impl STRectangle {
    pub fn new(
        lower_left_x: f64,
        lower_left_y: f64,
        upper_left_x: f64,
        upper_left_y: f64,
        time_start: i64,
        time_stop: i64,
    ) -> Result<Self> {
        Ok(Self {
            bounding_box: BoundingBox2D::new(
                Coordinate2D::new(lower_left_x, lower_left_y),
                Coordinate2D::new(upper_left_x, upper_left_y),
            )
            .context(error::DataType)?,
            time_interval: TimeInterval::new(time_start, time_stop).context(error::DataType {})?,
        })
    }
}

impl SpatialBounded for STRectangle {
    fn spatial_bounds(&self) -> BoundingBox2D {
        self.bounding_box
    }
}

impl TemporalBounded for STRectangle {
    fn temporal_bounds(&self) -> TimeInterval {
        self.time_interval
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Layer {
    // TODO: check that workflow/operator output type fits to the type of LayerInfo
    pub workflow: WorkflowId,
    pub name: String,
    pub info: LayerInfo,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum LayerInfo {
    Raster(RasterInfo),
    Vector(VectorInfo),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RasterInfo {
    pub colorizer: Colorizer,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct VectorInfo {
    // TODO add vector layer specific info
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub enum OrderBy {
    DateAsc,
    DateDesc,
    NameAsc,
    NameDesc,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct ProjectListing {
    pub id: ProjectId,
    pub name: String,
    pub description: String,
    pub layer_names: Vec<String>,
    pub changed: DateTime<Utc>,
}

impl From<&Project> for ProjectListing {
    fn from(project: &Project) -> Self {
        Self {
            id: project.id,
            name: project.name.clone(),
            description: project.description.clone(),
            layer_names: project.layers.iter().map(|l| l.name.clone()).collect(),
            changed: project.version.changed,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub enum ProjectFilter {
    Name { term: String },
    Description { term: String },
    None,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct CreateProject {
    pub name: String,
    pub description: String,
    pub view: STRectangle,
    pub bounds: STRectangle,
}

impl UserInput for CreateProject {
    fn validate(&self) -> Result<(), Error> {
        ensure!(
            !(self.name.is_empty() || self.name.len() > 256 || self.description.is_empty()),
            error::ProjectCreateFailed
        );

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UpdateProject {
    pub id: ProjectId,
    pub name: Option<String>,
    pub description: Option<String>,
    pub layers: Option<Vec<Option<Layer>>>,
    pub view: Option<STRectangle>,
    pub bounds: Option<STRectangle>,
}

impl UserInput for UpdateProject {
    fn validate(&self) -> Result<(), Error> {
        if let Some(name) = &self.name {
            ensure!(!name.is_empty(), error::ProjectUpdateFailed);
        }

        if let Some(description) = &self.description {
            ensure!(!description.is_empty(), error::ProjectUpdateFailed);
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
    pub user: UserId,
    pub project: ProjectId,
    pub permission: ProjectPermission,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct ProjectListOptions {
    pub permissions: Vec<ProjectPermission>,
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

identifier!(ProjectVersionId);

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Copy)]
pub struct ProjectVersion {
    pub id: ProjectVersionId,
    pub changed: DateTime<Utc>,
    pub author: UserId,
}

impl ProjectVersion {
    fn new(user: UserId) -> Self {
        Self {
            id: ProjectVersionId::new(),
            changed: chrono::offset::Utc::now(),
            author: user,
        }
    }
}

impl PartialOrd for ProjectVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.changed.partial_cmp(&other.changed)
    }
}

pub enum LoadVersion {
    Version(ProjectVersionId),
    Latest,
}

impl From<Option<Uuid>> for LoadVersion {
    fn from(id: Option<Uuid>) -> Self {
        id.map_or(LoadVersion::Latest, |id| {
            LoadVersion::Version(ProjectVersionId::from_uuid(id))
        })
    }
}
