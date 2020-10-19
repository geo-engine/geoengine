use crate::error::Error;
use crate::error::Result;
use crate::users::user::UserId;
use crate::util::config::ProjectService;
use crate::util::identifiers::Identifier;
use crate::util::user_input::UserInput;
use crate::workflows::workflow::WorkflowId;
use crate::{error, util::config::get_config_element};
use chrono::{DateTime, Utc};
use geoengine_datatypes::primitives::{
    BoundingBox2D, Coordinate2D, SpatialBounded, TemporalBounded, TimeInterval,
};
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use geoengine_datatypes::{operations::image::Colorizer, primitives::TimeInstance};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use uuid::Uuid;

identifier!(ProjectId);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Project {
    pub id: ProjectId,
    pub version: ProjectVersion,
    pub name: String,
    pub description: String,
    pub layers: Vec<Layer>,
    pub bounds: STRectangle,
}

impl Project {
    pub fn from_create_project(create: CreateProject, user: UserId) -> Self {
        Self {
            id: ProjectId::new(),
            version: ProjectVersion::new(user),
            name: create.name,
            description: create.description,
            layers: vec![],
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

        if let Some(bounds) = update.bounds {
            project.bounds = bounds;
        }

        project
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, ToSql, FromSql)]
pub struct STRectangle {
    pub spatial_reference: SpatialReferenceOption,
    pub bounding_box: BoundingBox2D,
    pub time_interval: TimeInterval,
}

impl STRectangle {
    pub fn new<A, B>(
        spatial_reference: SpatialReferenceOption,
        lower_left_x: f64,
        lower_left_y: f64,
        upper_left_x: f64,
        upper_left_y: f64,
        time_start: A,
        time_stop: B,
    ) -> Result<Self>
    where
        A: Into<TimeInstance>,
        B: Into<TimeInstance>,
    {
        Ok(Self {
            spatial_reference,
            bounding_box: BoundingBox2D::new(
                Coordinate2D::new(lower_left_x, lower_left_y),
                Coordinate2D::new(upper_left_x, upper_left_y),
            )
            .context(error::DataType)?,
            time_interval: TimeInterval::new(time_start, time_stop).context(error::DataType {})?,
        })
    }

    pub fn new_unchecked<A, B>(
        spatial_reference: SpatialReferenceOption,
        lower_left_x: f64,
        lower_left_y: f64,
        upper_left_x: f64,
        upper_left_y: f64,
        time_start: A,
        time_stop: B,
    ) -> Self
    where
        A: Into<TimeInstance>,
        B: Into<TimeInstance>,
    {
        Self {
            spatial_reference,
            bounding_box: BoundingBox2D::new_unchecked(
                Coordinate2D::new(lower_left_x, lower_left_y),
                Coordinate2D::new(upper_left_x, upper_left_y),
            ),
            time_interval: TimeInterval::new_unchecked(time_start, time_stop),
        }
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
    // TODO: LayerId?
    pub workflow: WorkflowId,
    pub name: String,
    pub info: LayerInfo,
}

impl Layer {
    pub fn layer_type(&self) -> LayerType {
        match self.info {
            LayerInfo::Raster(_) => LayerType::Raster,
            LayerInfo::Vector(_) => LayerType::Vector,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSql, FromSql)]
pub enum LayerType {
    Raster,
    Vector,
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

impl OrderBy {
    pub fn to_sql_string(&self) -> &'static str {
        match self {
            OrderBy::DateAsc => "time ASC",
            OrderBy::DateDesc => "time DESC",
            OrderBy::NameAsc => "name ASC",
            OrderBy::NameDesc => "name DESC",
        }
    }
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSql, FromSql)]
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
    pub offset: u32,
    pub limit: u32,
}

impl UserInput for ProjectListOptions {
    fn validate(&self) -> Result<(), Error> {
        ensure!(
            self.limit <= get_config_element::<ProjectService>()?.list_limit,
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
