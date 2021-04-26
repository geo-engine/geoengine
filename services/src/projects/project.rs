use std::{convert::TryInto, fmt::Debug};

use crate::error::{Error, Result};
use crate::users::user::UserId;
use crate::util::config::ProjectService;
use crate::util::user_input::UserInput;
use crate::workflows::workflow::WorkflowId;
use crate::{error, util::config::get_config_element};
use chrono::{DateTime, Utc};
use geoengine_datatypes::{identifier, operations::image::RgbaColor};
use geoengine_datatypes::{operations::image::Colorizer, primitives::TimeInstance};
use geoengine_datatypes::{
    primitives::{BoundingBox2D, Coordinate2D, TimeGranularity, TimeInterval, TimeStep},
    spatial_reference::SpatialReferenceOption,
};
use geoengine_datatypes::{
    primitives::{SpatialBounded, TemporalBounded},
    util::Identifier,
};
use geoengine_operators::string_token;
#[cfg(feature = "postgres")]
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use uuid::Uuid;

identifier!(ProjectId);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub id: ProjectId,
    pub version: ProjectVersion,
    pub name: String,
    pub description: String,
    pub layers: Vec<Layer>,
    pub plots: Vec<Plot>,
    pub bounds: STRectangle,
    pub time_step: TimeStep,
}

impl Project {
    pub fn from_create_project(create: CreateProject, user: UserId) -> Self {
        Self {
            id: ProjectId::new(),
            version: ProjectVersion::new(user),
            name: create.name,
            description: create.description,
            layers: vec![],
            plots: vec![],
            bounds: create.bounds,
            time_step: create.time_step.unwrap_or(TimeStep {
                // TODO: use config to store default time step
                granularity: TimeGranularity::Days,
                step: 1,
            }),
        }
    }

    /// Updates a project with partial fields.
    ///
    /// If the updates layer list is longer than the current list,
    /// it just inserts new layers to the end.
    ///
    pub fn update_project(&self, update: UpdateProject, user: UserId) -> Result<Project> {
        fn update_layer_or_plots<Content>(
            state: Vec<Content>,
            updates: Vec<VecUpdate<Content>>,
        ) -> Result<Vec<Content>> {
            let mut result = Vec::new();

            let mut updates = updates.into_iter();

            for (layer, layer_update) in state.into_iter().zip(&mut updates) {
                match layer_update {
                    VecUpdate::None(..) => result.push(layer),
                    VecUpdate::UpdateOrInsert(updated_content) => result.push(updated_content),
                    VecUpdate::Delete(..) => {}
                }
            }

            for update in updates {
                if let VecUpdate::UpdateOrInsert(new_content) = update {
                    result.push(new_content);
                } else {
                    return Err(Error::ProjectUpdateFailed);
                }
            }

            Ok(result)
        }

        let mut project = self.clone();
        project.version = ProjectVersion::new(user);

        if let Some(name) = update.name {
            project.name = name;
        }

        if let Some(description) = update.description {
            project.description = description;
        }

        if let Some(layer_updates) = update.layers {
            project.layers = update_layer_or_plots(project.layers, layer_updates)?;
        }

        if let Some(plot_updates) = update.plots {
            project.plots = update_layer_or_plots(project.plots, plot_updates)?;
        }

        if let Some(bounds) = update.bounds {
            project.bounds = bounds;
        }

        if let Some(time_step) = update.time_step {
            project.time_step = time_step;
        }

        Ok(project)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
#[allow(clippy::upper_case_acronyms)]
#[serde(rename_all = "camelCase")]
pub struct STRectangle {
    pub spatial_reference: SpatialReferenceOption,
    pub bounding_box: BoundingBox2D,
    pub time_interval: TimeInterval,
}

impl STRectangle {
    pub fn new<A, B, S>(
        spatial_reference: S,
        lower_left_x: f64,
        lower_left_y: f64,
        upper_left_x: f64,
        upper_left_y: f64,
        time_start: A,
        time_stop: B,
    ) -> Result<Self>
    where
        A: TryInto<TimeInstance>,
        B: TryInto<TimeInstance>,
        geoengine_datatypes::error::Error: From<A::Error>,
        geoengine_datatypes::error::Error: From<B::Error>,
        S: Into<SpatialReferenceOption>,
    {
        Ok(Self {
            spatial_reference: spatial_reference.into(),
            bounding_box: BoundingBox2D::new(
                Coordinate2D::new(lower_left_x, lower_left_y),
                Coordinate2D::new(upper_left_x, upper_left_y),
            )
            .context(error::DataType)?,
            time_interval: TimeInterval::new(time_start, time_stop).context(error::DataType {})?,
        })
    }

    pub fn new_unchecked<A, B, S>(
        spatial_reference: S,
        lower_left_x: f64,
        lower_left_y: f64,
        upper_left_x: f64,
        upper_left_y: f64,
        time_start: A,
        time_stop: B,
    ) -> Self
    where
        A: TryInto<TimeInstance>,
        B: TryInto<TimeInstance>,
        A::Error: Debug,
        B::Error: Debug,
        S: Into<SpatialReferenceOption>,
    {
        Self {
            spatial_reference: spatial_reference.into(),
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct Layer {
    // TODO: check that workflow/operator output type fits to the type of LayerInfo
    // TODO: LayerId?
    pub workflow: WorkflowId,
    pub name: String,
    pub visibility: LayerVisibility,
    pub symbology: Symbology,
}

impl Layer {
    pub fn layer_type(&self) -> LayerType {
        match self.symbology {
            Symbology::Raster(_) => LayerType::Raster,
            Symbology::Vector(_) => LayerType::Vector,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
#[serde(rename_all = "camelCase")]
pub enum LayerType {
    Raster,
    Vector,
}
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[allow(clippy::large_enum_variant)]
#[serde(rename_all = "camelCase")]
pub enum Symbology {
    Raster(RasterSymbology),
    Vector(VectorSymbology),
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct RasterSymbology {
    pub opacity: f64,
    pub colorizer: Colorizer,
}

impl Eq for RasterSymbology {}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum VectorSymbology {
    Point(PointSymbology),
    Line(LineSymbology),
    Polygon(PolygonSymbology),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TextSymbology {
    pub attribute: String,
    pub fill_color: ColorParam,
    pub stroke: StrokeParam,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PointSymbology {
    pub radius: NumberParam,
    pub fill_color: ColorParam,
    pub stroke: StrokeParam,
    pub text: Option<TextSymbology>,
}

impl Default for PointSymbology {
    fn default() -> Self {
        Self {
            radius: NumberParam::Static(10),
            fill_color: ColorParam::Static(RgbaColor::white()),
            stroke: StrokeParam {
                width: NumberParam::Static(1),
                color: ColorParam::Static(RgbaColor::black()),
            },
            text: None,
        }
    }
}

impl From<PointSymbology> for Symbology {
    fn from(value: PointSymbology) -> Self {
        Symbology::Vector(VectorSymbology::Point(value))
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct LineSymbology {
    pub stroke: StrokeParam,

    pub text: Option<TextSymbology>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PolygonSymbology {
    pub fill_color: ColorParam,

    pub stroke: StrokeParam,

    pub text: Option<TextSymbology>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum NumberParam {
    Static(usize),
    Derived(DerivedNumber),
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DerivedNumber {
    pub attribute: String,
    pub factor: f64,
    pub default_value: f64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct StrokeParam {
    pub width: NumberParam,
    pub color: ColorParam,
    // TODO: dash
}

impl Eq for DerivedNumber {}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum ColorParam {
    Static(RgbaColor),
    Derived(DerivedColor),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct DerivedColor {
    pub attribute: String,
    pub colorizer: Colorizer,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
pub struct LayerVisibility {
    pub data: bool,
    pub legend: bool,
}

impl Default for LayerVisibility {
    fn default() -> Self {
        LayerVisibility {
            data: true,
            legend: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Plot {
    pub workflow: WorkflowId,
    pub name: String,
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
#[serde(rename_all = "camelCase")]
pub struct ProjectListing {
    pub id: ProjectId,
    pub name: String,
    pub description: String,
    pub layer_names: Vec<String>,
    pub plot_names: Vec<String>,
    pub changed: DateTime<Utc>,
}

impl From<&Project> for ProjectListing {
    fn from(project: &Project) -> Self {
        Self {
            id: project.id,
            name: project.name.clone(),
            description: project.description.clone(),
            layer_names: project.layers.iter().map(|l| l.name.clone()).collect(),
            plot_names: project.layers.iter().map(|p| p.name.clone()).collect(),
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

impl Default for ProjectFilter {
    fn default() -> Self {
        ProjectFilter::None
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CreateProject {
    pub name: String,
    pub description: String,
    pub bounds: STRectangle,
    pub time_step: Option<TimeStep>,
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProject {
    pub id: ProjectId,
    pub name: Option<String>,
    pub description: Option<String>,
    pub layers: Option<Vec<LayerUpdate>>,
    pub plots: Option<Vec<PlotUpdate>>,
    pub bounds: Option<STRectangle>,
    pub time_step: Option<TimeStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum VecUpdate<Content> {
    None(NoUpdate),
    Delete(Delete),
    UpdateOrInsert(Content),
}

pub type LayerUpdate = VecUpdate<Layer>;
pub type PlotUpdate = VecUpdate<Plot>;

string_token!(NoUpdate, "none");
string_token!(Delete, "delete");

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
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
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
    // TODO: remove, once warp allows parsing list params
    #[serde(deserialize_with = "permissions_from_json_str")]
    pub permissions: Vec<ProjectPermission>,
    #[serde(default)]
    pub filter: ProjectFilter,
    pub order: OrderBy,
    pub offset: u32,
    pub limit: u32,
}

/// Instead of parsing list params, deserialize `ProjectPermission`s as JSON list.
pub fn permissions_from_json_str<'de, D>(
    deserializer: D,
) -> Result<Vec<ProjectPermission>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Visitor;

    struct PermissionsFromJsonStrVisitor;
    impl<'de> Visitor<'de> for PermissionsFromJsonStrVisitor {
        type Value = Vec<ProjectPermission>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a JSON array of type `ProjectPermission`")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            serde_json::from_str(v).map_err(|error| E::custom(error.to_string()))
        }
    }

    deserializer.deserialize_str(PermissionsFromJsonStrVisitor)
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
            LoadVersion::Version(ProjectVersionId(id))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn strectangle_serialization() {
        assert!(serde_json::from_str::<STRectangle>(
            &json!({
                "spatialReference": "EPSG:4326",
                "boundingBox": {
                  "lowerLeftCoordinate": {
                    "x": -180,
                    "y": -90
                  },
                  "upperRightCoordinate": {
                    "x": 180,
                    "y": 90
                  }
                },
                "timeInterval": {
                  "start": 0,
                  "end": 0
                }
              }
            )
            .to_string(),
        )
        .is_ok());
    }

    #[test]
    fn list_options_serialization() {
        serde_json::from_str::<ProjectListOptions>(
            &json!({
                "permissions": "[\"Owner\"]",
                "filter": "None",
                "order": "NameAsc",
                "offset": 0,
                "limit": 1
            })
            .to_string(),
        )
        .unwrap();
    }

    #[test]
    fn deserialize_layer_update() {
        assert_eq!(
            serde_json::from_str::<LayerUpdate>(&json!("none").to_string()).unwrap(),
            LayerUpdate::None(Default::default())
        );

        assert_eq!(
            serde_json::from_str::<LayerUpdate>(&json!("delete").to_string()).unwrap(),
            LayerUpdate::Delete(Default::default())
        );

        let workflow = WorkflowId::new();
        assert_eq!(
            serde_json::from_str::<LayerUpdate>(
                &json!({
                    "workflow": workflow.clone(),
                    "name": "L2",
                    "visibility": {
                        "data": true,
                        "legend": false,
                    },
                    "symbology": {
                        "raster": {
                            "opacity": 1.0,
                            "colorizer": "rgba"
                        }
                    }
                })
                .to_string()
            )
            .unwrap(),
            LayerUpdate::UpdateOrInsert(Layer {
                workflow,
                name: "L2".to_string(),
                visibility: LayerVisibility {
                    data: true,
                    legend: false,
                },
                symbology: Symbology::Raster(RasterSymbology {
                    opacity: 1.0,
                    colorizer: Colorizer::Rgba,
                })
            })
        );
    }

    #[test]
    fn serialize_update_project() {
        let update = UpdateProject {
            id: ProjectId::new(),
            name: Some("name".to_string()),
            description: Some("description".to_string()),
            layers: Some(vec![
                LayerUpdate::None(Default::default()),
                LayerUpdate::Delete(Default::default()),
                LayerUpdate::UpdateOrInsert(Layer {
                    workflow: WorkflowId::new(),
                    name: "vector layer".to_string(),
                    visibility: Default::default(),
                    symbology: Symbology::Raster(RasterSymbology {
                        opacity: 1.0,
                        colorizer: Colorizer::Rgba,
                    }),
                }),
                LayerUpdate::UpdateOrInsert(Layer {
                    workflow: WorkflowId::new(),
                    name: "raster layer".to_string(),
                    visibility: Default::default(),
                    symbology: Symbology::Raster(RasterSymbology {
                        opacity: 1.0,
                        colorizer: Colorizer::Rgba,
                    }),
                }),
            ]),
            plots: None,
            bounds: Some(STRectangle {
                spatial_reference: SpatialReferenceOption::Unreferenced,
                bounding_box: BoundingBox2D::new((0.0, 0.1).into(), (1.0, 1.1).into()).unwrap(),
                time_interval: Default::default(),
            }),
            time_step: Some(TimeStep {
                step: 1,
                granularity: TimeGranularity::Days,
            }),
        };

        let serialized = serde_json::to_string(&update).unwrap();

        let deserialized: UpdateProject = serde_json::from_str(&serialized).unwrap();

        assert_eq!(update, deserialized);

        let _update_project: UpdateProject =
            serde_json::from_reader(serialized.as_bytes()).unwrap();
    }

    #[test]
    fn serialize_symbology() {
        let symbology = Symbology::Vector(VectorSymbology::Point(PointSymbology {
            radius: NumberParam::Static(1),
            fill_color: ColorParam::Derived(DerivedColor {
                attribute: "foo".to_owned(),
                colorizer: Colorizer::Rgba,
            }),
            stroke: StrokeParam {
                width: NumberParam::Static(1),
                color: ColorParam::Static(RgbaColor::black()),
            },
            text: None,
        }));

        assert_eq!(
            serde_json::to_string(&symbology).unwrap(),
            json!({
                "vector": {
                    "point": {
                        "radius": {
                            "static": 1
                        },
                        "fillColor": {
                            "derived": {
                                "attribute": "foo",
                                "colorizer": "rgba"
                            }
                        },
                        "stroke": {
                            "width": {
                                "static": 1
                            },
                            "color": {
                                "static": [
                                    0,
                                    0,
                                    0,
                                    255
                                ]
                            }
                        },
                        "text": null
                    }
                }
            })
            .to_string(),
        );
    }
}
