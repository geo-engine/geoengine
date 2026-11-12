use crate::config::ProjectService;
use crate::config::get_config_element;
use crate::error::Result;
use crate::identifier;
use crate::layers::layer::{Layer, Property, ProviderLayerId};
use crate::layers::storage::PROJECTS_PROVIDER_ID;
use crate::processing_graphs::processing_graph::{ProcessingGraph, ProcessingGraphId};
use crate::projects::error::ProjectDbError;
use geoengine_datatypes::dataset::LayerId;
use geoengine_datatypes::operations::image::{Colorizer, RasterColorizer, RgbaColor};
use geoengine_datatypes::primitives::DateTime;
use geoengine_datatypes::primitives::TimeInstance;
use geoengine_datatypes::{
    primitives::{BoundingBox2D, Coordinate2D, TimeGranularity, TimeInterval, TimeStep},
    spatial_reference::SpatialReferenceOption,
};
use geoengine_datatypes::{
    primitives::{SpatialBounded, TemporalBounded},
    util::Identifier,
};
use geoengine_macros::type_tag;
use geoengine_operators::string_token;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::{convert::TryInto, fmt::Debug};
use utoipa::PartialSchema;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;
use validator::{Validate, ValidationError};

identifier!(ProjectId);

#[derive(Debug, Clone, PartialEq)]
pub struct Project {
    pub id: ProjectId,
    pub version: ProjectVersion, // TODO: remove, this exists only for pro
    pub name: String,
    pub description: String,
    pub layers: Vec<ProjectLayer>,
    pub plots: Vec<ProjectLayer>,
    pub bounds: STRectangle,
    pub time_step: TimeStep,
}

impl Project {
    pub fn from_create_project(create: CreateProject) -> Self {
        Self {
            id: ProjectId::new(),
            version: ProjectVersion::new(),
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
    /// New layers and plots get a new id.
    /// Updated layers and plots keep their id, which must belong to a layer or plot of this project.
    /// `processing_graphs` must contain the processing graphs of all updated layers and plots.
    ///
    pub fn update_project(
        &self,
        update: UpdateProject,
        processing_graphs: &HashMap<ProcessingGraphId, ProcessingGraph>,
    ) -> Result<Project, ProjectDbError> {
        fn update_layer_or_plots(
            project: ProjectId,
            state: &[ProjectLayer],
            updates: Vec<VecUpdate<ProjectLayerUpdate>>,
            processing_graphs: &HashMap<ProcessingGraphId, ProcessingGraph>,
        ) -> Result<Vec<ProjectLayer>, ProjectDbError> {
            let existing_ids: HashSet<LayerId> = state
                .iter()
                .map(|layer| layer.layer.id.layer_id.clone())
                .collect();

            let into_layer = |update: ProjectLayerUpdate| {
                if let Some(id) = &update.id
                    && !existing_ids.contains(id)
                {
                    return Err(ProjectDbError::UnknownLayer {
                        project,
                        layer: id.clone(),
                    });
                }
                update.into_project_layer(processing_graphs)
            };

            let mut result = Vec::new();

            let mut updates = updates.into_iter();

            for (layer, layer_update) in state.iter().zip(&mut updates) {
                match layer_update {
                    VecUpdate::None(..) => result.push(layer.clone()),
                    VecUpdate::UpdateOrInsert(updated_content) => {
                        result.push(into_layer(updated_content)?);
                    }
                    VecUpdate::Delete(..) => {}
                }
            }

            for update in updates {
                if let VecUpdate::UpdateOrInsert(new_content) = update {
                    result.push(into_layer(new_content)?);
                } else {
                    return Err(ProjectDbError::ProjectUpdateFailed { project });
                }
            }

            let mut ids = HashSet::new();
            for layer in &result {
                if !ids.insert(&layer.layer.id.layer_id) {
                    return Err(ProjectDbError::DuplicateLayer {
                        project,
                        layer: layer.layer.id.layer_id.clone(),
                    });
                }
            }

            Ok(result)
        }

        let mut project = self.clone();
        project.version = ProjectVersion::new();

        if let Some(name) = update.name {
            project.name = name;
        }

        if let Some(description) = update.description {
            project.description = description;
        }

        if let Some(layer_updates) = update.layers {
            project.layers = update_layer_or_plots(
                update.id,
                &project.layers,
                layer_updates,
                processing_graphs,
            )?;
        }

        if let Some(plot_updates) = update.plots {
            project.plots =
                update_layer_or_plots(update.id, &project.plots, plot_updates, processing_graphs)?;
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

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, ToSchema, ToSql, FromSql)]
#[allow(clippy::upper_case_acronyms)]
#[serde(rename_all = "camelCase")]
// TODO: add example, once utoipas schema macro can co-exist with postgres OR: split type into API and database model
// cf. utoipa issue: https://github.com/juhaku/utoipa/issues/266
// #[schema(example = json!({
//     "boundingBox": {
//         "lowerLeftCoordinate": {
//             "x": -180.0,
//             "y": -90.0
//         },
//         "upperRightCoordinate": {
//             "x": 180.0,
//             "y": 90.0
//         }
//     },
//     "spatialReference": "EPSG:4326",
//     "timeInterval": {
//         "end": 1_388_534_400_000_i64,
//         "start": 1_388_534_400_000_i64
//     }
// }))]
pub struct STRectangle {
    #[schema(value_type = String)]
    pub spatial_reference: SpatialReferenceOption,
    #[schema(value_type = crate::api::model::datatypes::BoundingBox2D)]
    pub bounding_box: BoundingBox2D,
    #[schema(value_type = crate::api::model::datatypes::TimeInterval)]
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
            )?,
            time_interval: TimeInterval::new(time_start, time_stop)?,
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

/// A layer or plot of a project.
///
/// It is a normal [`Layer`] that the projects data connector ([`PROJECTS_PROVIDER_ID`]) lists.
/// Only the visibility is specific to the project, and the order is the position in the project.
#[derive(Debug, PartialEq, Clone)]
pub struct ProjectLayer {
    /// The layer, with `id = (PROJECTS_PROVIDER_ID, <uuid>)`.
    /// Plots are layers without symbology.
    pub layer: Layer,
    pub visibility: LayerVisibility,
    /// The catalog layer this layer was copied from, if any.
    /// It is only a reference: the layer itself is always the stored copy.
    pub source: Option<ProviderLayerId>,
}

impl ProjectLayer {
    /// The id of a project layer as a layer of the projects data connector.
    pub fn provider_layer_id(id: Uuid) -> ProviderLayerId {
        ProviderLayerId {
            provider_id: PROJECTS_PROVIDER_ID,
            layer_id: LayerId(id.to_string()),
        }
    }

    /// The UUID of this layer, as stored in the database.
    pub fn layer_uuid(&self) -> Result<Uuid, ProjectDbError> {
        parse_layer_uuid(&self.layer.id.layer_id)
    }
}

/// Parses the id of a project layer, which is always a UUID.
pub fn parse_layer_uuid(id: &LayerId) -> Result<Uuid, ProjectDbError> {
    Uuid::parse_str(&id.0).map_err(|_| ProjectDbError::InvalidLayerId { layer: id.clone() })
}

/// A layer or plot as it is sent to update a project.
///
/// Without `id`, it is a new layer and gets a new id.
/// With `id`, it replaces the layer of the project with this id.
#[derive(Debug, PartialEq, Clone)]
pub struct ProjectLayerUpdate {
    pub id: Option<LayerId>,
    pub name: String,
    pub description: String,
    pub processing_graph: ProcessingGraphId,
    pub symbology: Option<Symbology>,
    pub properties: Vec<Property>,
    pub metadata: HashMap<String, String>,
    pub visibility: LayerVisibility,
    pub source: Option<ProviderLayerId>,
}

impl ProjectLayerUpdate {
    fn into_project_layer(
        self,
        processing_graphs: &HashMap<ProcessingGraphId, ProcessingGraph>,
    ) -> Result<ProjectLayer, ProjectDbError> {
        let processing_graph = processing_graphs
            .get(&self.processing_graph)
            .ok_or(ProjectDbError::ProcessingGraphNotFound {
                processing_graph: self.processing_graph,
            })?
            .clone();

        let id = match self.id {
            Some(id) => ProviderLayerId {
                provider_id: PROJECTS_PROVIDER_ID,
                layer_id: id,
            },
            None => ProjectLayer::provider_layer_id(Uuid::new_v4()),
        };

        Ok(ProjectLayer {
            layer: Layer {
                id,
                name: self.name,
                description: self.description,
                processing_graph,
                symbology: self.symbology,
                properties: self.properties,
                metadata: self.metadata,
            },
            visibility: self.visibility,
            source: self.source,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSql, FromSql)]
#[serde(rename_all = "camelCase")]
pub enum LayerType {
    Raster,
    Vector,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
#[allow(clippy::large_enum_variant)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum Symbology {
    Raster(RasterSymbology),
    Point(PointSymbology),
    Line(LineSymbology),
    Polygon(PolygonSymbology),
}

impl Symbology {
    pub fn into_raster_symbology(self) -> Option<RasterSymbology> {
        match self {
            Symbology::Raster(raster) => Some(raster),
            _ => None,
        }
    }

    pub fn as_raster_symbology(&self) -> Option<&RasterSymbology> {
        match self {
            Symbology::Raster(raster) => Some(raster),
            _ => None,
        }
    }
}

#[type_tag(value = "raster")]
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RasterSymbology {
    pub opacity: f64,
    #[schema(value_type = crate::api::model::datatypes::RasterColorizer)]
    pub raster_colorizer: RasterColorizer,
}

impl Eq for RasterSymbology {}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema, ToSql, FromSql)]
#[serde(rename_all = "camelCase")]
pub struct TextSymbology {
    pub attribute: String,
    pub fill_color: ColorParam,
    pub stroke: StrokeParam,
}

#[type_tag(value = "point")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
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
            r#type: Default::default(),
            radius: NumberParam::Static(StaticNumber {
                r#type: Default::default(),
                value: 10,
            }),
            fill_color: ColorParam::Static(StaticColor {
                r#type: Default::default(),
                color: RgbaColor::white(),
            }),
            stroke: StrokeParam {
                width: NumberParam::Static(StaticNumber {
                    r#type: Default::default(),
                    value: 1,
                }),
                color: ColorParam::Static(StaticColor {
                    r#type: Default::default(),
                    color: RgbaColor::black(),
                }),
            },
            text: None,
        }
    }
}

impl From<PointSymbology> for Symbology {
    fn from(value: PointSymbology) -> Self {
        Symbology::Point(value)
    }
}

#[type_tag(value = "line")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LineSymbology {
    pub stroke: StrokeParam,

    pub text: Option<TextSymbology>,

    pub auto_simplified: bool,
}

#[type_tag(value = "polygon")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PolygonSymbology {
    pub fill_color: ColorParam,

    pub stroke: StrokeParam,

    pub text: Option<TextSymbology>,

    pub auto_simplified: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum NumberParam {
    Static(StaticNumber),
    Derived(DerivedNumber),
}

#[type_tag(value = "static")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct StaticNumber {
    pub value: usize,
}

#[type_tag(value = "derived")]
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DerivedNumber {
    pub attribute: String,
    pub factor: f64,
    pub default_value: f64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema, ToSql, FromSql)]
pub struct StrokeParam {
    pub width: NumberParam,
    pub color: ColorParam,
    // TODO: dash
}

impl Eq for DerivedNumber {}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum ColorParam {
    Static(StaticColor),
    Derived(DerivedColor),
}

#[type_tag(value = "static")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
pub struct StaticColor {
    #[schema(value_type = crate::api::model::datatypes::RgbaColor)]
    pub color: RgbaColor,
}

#[type_tag(value = "derived")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
pub struct DerivedColor {
    pub attribute: String,
    #[schema(value_type = crate::api::model::datatypes::Colorizer)]
    pub colorizer: Colorizer,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSchema, ToSql, FromSql)]
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

/// The order of a project listing.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSchema)]
#[schema(as = ProjectOrderBy)]
pub enum OrderBy {
    DateAsc,
    DateDesc,
    NameAsc,
    NameDesc,
}

impl OrderBy {
    pub fn to_sql_string(&self) -> &'static str {
        match self {
            OrderBy::DateAsc => "changed ASC",
            OrderBy::DateDesc => "changed DESC",
            OrderBy::NameAsc => "name ASC",
            OrderBy::NameDesc => "name DESC",
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProjectListing {
    pub id: ProjectId,
    pub name: String,
    pub description: String,
    pub layer_names: Vec<String>,
    pub plot_names: Vec<String>,
    pub changed: DateTime,
}

impl From<&Project> for ProjectListing {
    fn from(project: &Project) -> Self {
        Self {
            id: project.id,
            name: project.name.clone(),
            description: project.description.clone(),
            layer_names: project
                .layers
                .iter()
                .map(|l| l.layer.name.clone())
                .collect(),
            plot_names: project.plots.iter().map(|p| p.layer.name.clone()).collect(),
            changed: project.version.changed,
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, ToSchema, Validate)]
#[serde(rename_all = "camelCase")]
#[schema(example = json!({
    "name": "Test",
    "description": "Foo",
    "bounds": {
        "spatialReference": "EPSG:4326",
        "boundingBox": {
            "lowerLeftCoordinate": { "x": 0, "y": 0 },
            "upperRightCoordinate": { "x": 1, "y": 1 }
        },
        "timeInterval": {
            "start": 0,
            "end": 1
        }
    },
    "timeStep": {
        "step": 1,
        "granularity": "months"
    }
}))]
pub struct CreateProject {
    #[validate(length(min = 1, max = 256))]
    pub name: String,
    #[validate(length(min = 1))]
    pub description: String,
    pub bounds: STRectangle,
    #[schema(value_type = Option<crate::api::model::datatypes::TimeStep>)]
    pub time_step: Option<TimeStep>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateProject {
    pub id: ProjectId,
    pub name: Option<String>,
    pub description: Option<String>,
    pub layers: Option<Vec<LayerUpdate>>,
    pub plots: Option<Vec<PlotUpdate>>,
    pub bounds: Option<STRectangle>,
    pub time_step: Option<TimeStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum VecUpdate<Content> {
    None(NoUpdate),
    Delete(Delete),
    UpdateOrInsert(Content),
}

pub type LayerUpdate = VecUpdate<ProjectLayerUpdate>;
pub type PlotUpdate = VecUpdate<ProjectLayerUpdate>;

string_token!(NoUpdate, "none");
string_token!(Delete, "delete");

// use common schema for `Delete` and `NoUpdate`
// TODO: maybe revert when https://github.com/OpenAPITools/openapi-generator/issues/14831 is fixed
impl ToSchema for Delete {
    fn name() -> Cow<'static, str> {
        "ProjectUpdateToken".into()
    }
}

impl PartialSchema for Delete {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        use utoipa::openapi::ObjectBuilder;
        use utoipa::openapi::schema::{SchemaType, Type};
        ObjectBuilder::new()
            .schema_type(SchemaType::Type(Type::String))
            .enum_values::<[&str; 2], &str>(Some(["none", "delete"]))
            .into()
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, IntoParams, Validate)]
#[into_params(parameter_in = Query)]
pub struct ProjectListOptions {
    #[param(example = "NameAsc")]
    pub order: OrderBy,
    #[param(example = 0)]
    pub offset: u32,
    #[param(example = 2)]
    #[validate(custom(function = "validate_list_limit"))]
    pub limit: u32,
}

fn validate_list_limit(value: u32) -> Result<(), ValidationError> {
    let limit = get_config_element::<ProjectService>()
        .expect("should exist because it is defined in the default config")
        .list_limit;
    if value <= limit {
        return Ok(());
    }

    let mut err = ValidationError::new("limit (too large)");
    err.add_param::<u32>(Cow::Borrowed("max limit"), &limit);
    Err(err)
}

identifier!(ProjectVersionId);

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Copy, ToSchema)]
pub struct ProjectVersion {
    pub id: ProjectVersionId,
    pub changed: DateTime,
}

impl ProjectVersion {
    fn new() -> Self {
        Self {
            id: ProjectVersionId::new(),
            changed: DateTime::now(),
        }
    }
}

#[derive(PartialEq)]
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
    use geoengine_datatypes::util::test::TestDefault;
    use serde_json::json;

    #[test]
    fn strectangle_serialization() {
        assert!(
            serde_json::from_str::<STRectangle>(
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
            .is_ok()
        );
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
    fn serialize_point_symbology() {
        let symbology = Symbology::Point(PointSymbology {
            r#type: Default::default(),
            radius: NumberParam::Static(StaticNumber {
                r#type: Default::default(),
                value: 1,
            }),
            fill_color: ColorParam::Derived(DerivedColor {
                r#type: Default::default(),
                attribute: "foo".to_owned(),
                colorizer: Colorizer::test_default(),
            }),
            stroke: StrokeParam {
                width: NumberParam::Static(StaticNumber {
                    r#type: Default::default(),
                    value: 1,
                }),
                color: ColorParam::Static(StaticColor {
                    r#type: Default::default(),
                    color: RgbaColor::black(),
                }),
            },
            text: None,
        });

        assert_eq!(
            serde_json::to_value(symbology).unwrap(),
            json!({
                "type": "point",
                "radius": {
                    "type": "static",
                    "value": 1
                },
                "fillColor": {
                    "type": "derived",
                    "attribute": "foo",
                    "colorizer": {
                        "type": "linearGradient",
                        "breakpoints": [
                            {
                                "value": 1.0,
                                "color": [255, 255, 255, 255],
                            },
                            {
                                "value": 2.0,
                                "color": [0, 0, 0, 255],
                            },
                        ],
                        "noDataColor": [0, 0, 0, 0],
                        "overColor": [255, 255, 255, 255],
                        "underColor": [0, 0, 0, 255],
                    }
                },
                "stroke": {
                    "width": {
                        "type": "static",
                        "value": 1
                    },
                    "color": {
                        "type": "static",
                        "color": [
                            0,
                            0,
                            0,
                            255
                        ]
                    }
                },
                "text": null
            }),
        );
    }

    #[test]
    fn serialize_linestring_symbology() {
        let symbology = Symbology::Line(LineSymbology {
            r#type: Default::default(),
            stroke: StrokeParam {
                width: NumberParam::Static(StaticNumber {
                    r#type: Default::default(),
                    value: 1,
                }),
                color: ColorParam::Static(StaticColor {
                    r#type: Default::default(),
                    color: RgbaColor::black(),
                }),
            },
            text: None,
            auto_simplified: true,
        });

        assert_eq!(
            serde_json::to_value(symbology).unwrap(),
            json!({
                "type": "line",
                "autoSimplified": true,
                "stroke": {
                    "width": {
                        "type": "static",
                        "value": 1
                    },
                    "color": {
                        "type": "static",
                        "color": [
                            0,
                            0,
                            0,
                            255
                        ]
                    }
                },
                "text": null
            }),
        );
    }

    #[test]
    fn serialize_derived_number_param() {
        assert_eq!(
            serde_json::to_value(NumberParam::Derived(DerivedNumber {
                r#type: Default::default(),
                attribute: "foo".to_owned(),
                factor: 1.,
                default_value: 0.
            }))
            .unwrap(),
            json!({
                "type": "derived",
                "attribute": "foo",
                "factor": 1.0,
                "defaultValue": 0.0
            })
        );
    }
}
