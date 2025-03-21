use crate::config::ProjectService;
use crate::config::get_config_element;
use crate::error::Result;
use crate::identifier;
use crate::projects::error::ProjectDbError;
use crate::workflows::workflow::WorkflowId;
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
use std::{convert::TryInto, fmt::Debug};
use utoipa::PartialSchema;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;
use validator::{Validate, ValidationError};

identifier!(ProjectId);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub id: ProjectId,
    pub version: ProjectVersion, // TODO: remove, this exists only for pro
    pub name: String,
    pub description: String,
    pub layers: Vec<ProjectLayer>,
    pub plots: Vec<Plot>,
    pub bounds: STRectangle,
    #[schema(value_type = crate::api::model::datatypes::TimeStep)]
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
    pub fn update_project(&self, update: UpdateProject) -> Result<Project, ProjectDbError> {
        fn update_layer_or_plots<Content>(
            project: ProjectId,
            state: Vec<Content>,
            updates: Vec<VecUpdate<Content>>,
        ) -> Result<Vec<Content>, ProjectDbError> {
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
                    return Err(ProjectDbError::ProjectUpdateFailed { project });
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
            project.layers = update_layer_or_plots(update.id, project.layers, layer_updates)?;
        }

        if let Some(plot_updates) = update.plots {
            project.plots = update_layer_or_plots(update.id, project.plots, plot_updates)?;
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

// TODO: split into Raster and VectorLayer like in frontend?
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
pub struct ProjectLayer {
    // TODO: check that workflow/operator output type fits to the type of LayerInfo
    // TODO: LayerId?
    pub workflow: WorkflowId,
    pub name: String,
    pub visibility: LayerVisibility,
    pub symbology: Symbology,
}

impl ProjectLayer {
    pub fn layer_type(&self) -> LayerType {
        match self.symbology {
            Symbology::Raster(_) => LayerType::Raster,
            _ => LayerType::Vector,
        }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Plot {
    pub workflow: WorkflowId,
    pub name: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSchema)]
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
            layer_names: project.layers.iter().map(|l| l.name.clone()).collect(),
            plot_names: project.layers.iter().map(|p| p.name.clone()).collect(),
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, Validate)]
#[serde(rename_all = "camelCase")]
#[schema(example = json!({
    "id": "df4ad02e-0d61-4e29-90eb-dc1259c1f5b9",
    "name": "TestUpdate",
    "layers": [
        {
            "workflow": "100ee39c-761c-4218-9d85-ec861a8f3097",
            "name": "L1",
            "visibility": {
                "data": true,
                "legend": false
            },
            "symbology": {
                "type": "raster",
                "opacity": 1.0,
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
            }
        }
    ]
}))]
pub struct UpdateProject {
    pub id: ProjectId,
    #[validate(length(min = 1))]
    pub name: Option<String>,
    #[validate(length(min = 1))]
    pub description: Option<String>,
    pub layers: Option<Vec<LayerUpdate>>,
    pub plots: Option<Vec<PlotUpdate>>,
    pub bounds: Option<STRectangle>,
    #[schema(value_type = Option<crate::api::model::datatypes::TimeStep>)]
    pub time_step: Option<TimeStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum VecUpdate<Content> {
    None(NoUpdate),
    Delete(Delete),
    UpdateOrInsert(Content),
}

pub type LayerUpdate = VecUpdate<ProjectLayer>;
pub type PlotUpdate = VecUpdate<Plot>;

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

impl ToSchema for LayerUpdate {}

impl PartialSchema for LayerUpdate {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        use utoipa::openapi::*;
        OneOfBuilder::new()
            .item(Ref::from_schema_name("ProjectUpdateToken"))
            .item(Ref::from_schema_name("ProjectLayer"))
            .into()
    }
}

impl ToSchema for PlotUpdate {}

impl PartialSchema for PlotUpdate {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        use utoipa::openapi::*;
        OneOfBuilder::new()
            .item(Ref::from_schema_name("ProjectUpdateToken"))
            .item(Ref::from_schema_name("Plot"))
            .into()
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, IntoParams, Validate)]
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
                        "type": "raster",
                        "opacity": 1.0,
                        "rasterColorizer": {
                            "type": "singleBand",
                            "band": 0,
                            "bandColorizer": {
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
                        }
                    }
                })
                .to_string()
            )
            .unwrap(),
            LayerUpdate::UpdateOrInsert(ProjectLayer {
                workflow,
                name: "L2".to_string(),
                visibility: LayerVisibility {
                    data: true,
                    legend: false,
                },
                symbology: Symbology::Raster(RasterSymbology {
                    r#type: Default::default(),
                    opacity: 1.0,
                    raster_colorizer: RasterColorizer::SingleBand {
                        band: 0,
                        band_colorizer: Colorizer::test_default(),
                    },
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
                LayerUpdate::UpdateOrInsert(ProjectLayer {
                    workflow: WorkflowId::new(),
                    name: "vector layer".to_string(),
                    visibility: Default::default(),
                    symbology: Symbology::Raster(RasterSymbology {
                        r#type: Default::default(),
                        opacity: 1.0,
                        raster_colorizer: RasterColorizer::SingleBand {
                            band: 0,
                            band_colorizer: Colorizer::test_default(),
                        },
                    }),
                }),
                LayerUpdate::UpdateOrInsert(ProjectLayer {
                    workflow: WorkflowId::new(),
                    name: "raster layer".to_string(),
                    visibility: Default::default(),
                    symbology: Symbology::Raster(RasterSymbology {
                        r#type: Default::default(),
                        opacity: 1.0,
                        raster_colorizer: RasterColorizer::SingleBand {
                            band: 0,
                            band_colorizer: Colorizer::test_default(),
                        },
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
