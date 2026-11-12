use crate::{
    api::model::{datatypes::LayerId, processing_graphs::ProcessingGraphId},
    layers::layer::{Property, ProviderLayerId},
    projects::{LayerVisibility, ProjectId, ProjectVersion, STRectangle, Symbology, VecUpdate},
};
use geoengine_datatypes::primitives::TimeStep;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use utoipa::{PartialSchema, ToSchema};
use validator::Validate;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub id: ProjectId,
    pub version: ProjectVersion,
    pub name: String,
    pub description: String,
    pub layers: Vec<ProjectLayer>,
    /// Plots are project layers without symbology.
    pub plots: Vec<ProjectLayer>,
    pub bounds: STRectangle,
    #[schema(value_type = crate::api::model::datatypes::TimeStep)]
    pub time_step: TimeStep,
}

impl From<crate::projects::Project> for Project {
    fn from(value: crate::projects::Project) -> Self {
        Self {
            id: value.id,
            version: value.version,
            name: value.name,
            description: value.description,
            layers: value.layers.into_iter().map(Into::into).collect(),
            plots: value.plots.into_iter().map(Into::into).collect(),
            bounds: value.bounds,
            time_step: value.time_step,
        }
    }
}

/// A layer or plot of a project.
///
/// It is also a layer of the projects data connector, with the same `id`.
/// Only the visibility is specific to the project.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProjectLayer {
    /// The id of the layer, stable across project versions.
    pub id: LayerId,
    pub name: String,
    pub description: String,
    /// The id of the processing graph of the layer.
    pub processing_graph: ProcessingGraphId,
    /// The symbology of the layer, which plots do not have.
    pub symbology: Option<Symbology>,
    /// properties, for instance, to be rendered in the UI
    pub properties: Vec<Property>,
    /// metadata used for loading the data
    pub metadata: HashMap<String, String>,
    pub visibility: LayerVisibility,
    /// The catalog layer this layer was copied from, if any.
    pub source: Option<ProviderLayerId>,
}

impl From<crate::projects::ProjectLayer> for ProjectLayer {
    fn from(value: crate::projects::ProjectLayer) -> Self {
        Self {
            id: LayerId(value.layer.id.layer_id.0),
            name: value.layer.name,
            description: value.layer.description,
            processing_graph:
                crate::processing_graphs::processing_graph::ProcessingGraphId::from_hash(
                    &value.layer.processing_graph,
                )
                .into(),
            symbology: value.layer.symbology,
            properties: value.layer.properties,
            metadata: value.layer.metadata,
            visibility: value.visibility,
            source: value.source,
        }
    }
}

/// A layer or plot that is added to or changed in a project.
///
/// Without `id`, it is a new layer and the server assigns an id.
/// With `id`, it replaces the layer of the project with this id, so layers can also be moved.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProjectLayerUpdate {
    /// The id of an existing layer of the project, or none for a new layer.
    #[serde(default)]
    pub id: Option<LayerId>,
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// The id of a registered processing graph.
    pub processing_graph: ProcessingGraphId,
    #[serde(default)]
    pub symbology: Option<Symbology>,
    #[serde(default)]
    pub properties: Vec<Property>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    #[serde(default)]
    pub visibility: LayerVisibility,
    /// The catalog layer this layer was copied from, if any.
    #[serde(default)]
    pub source: Option<ProviderLayerId>,
}

impl From<ProjectLayerUpdate> for crate::projects::ProjectLayerUpdate {
    fn from(value: ProjectLayerUpdate) -> Self {
        Self {
            id: value.id.map(Into::into),
            name: value.name,
            description: value.description,
            processing_graph: value.processing_graph.into(),
            symbology: value.symbology,
            properties: value.properties,
            metadata: value.metadata,
            visibility: value.visibility,
            source: value.source,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, Validate)]
#[serde(rename_all = "camelCase")]
#[schema(example = json!({
    "id": "df4ad02e-0d61-4e29-90eb-dc1259c1f5b9",
    "name": "TestUpdate",
    "layers": [
        {
            "processingGraph": "100ee39c-761c-4218-9d85-ec861a8f3097",
            "name": "L1",
            "description": "",
            "visibility": {
                "data": true,
                "legend": false
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
    pub plots: Option<Vec<LayerUpdate>>,
    pub bounds: Option<STRectangle>,
    #[schema(value_type = Option<crate::api::model::datatypes::TimeStep>)]
    pub time_step: Option<TimeStep>,
}

impl From<UpdateProject> for crate::projects::UpdateProject {
    fn from(value: UpdateProject) -> Self {
        Self {
            id: value.id,
            name: value.name,
            description: value.description,
            layers: value
                .layers
                .map(|layers| layers.into_iter().map(convert_vec_update).collect()),
            plots: value
                .plots
                .map(|plots| plots.into_iter().map(convert_vec_update).collect()),
            bounds: value.bounds,
            time_step: value.time_step,
        }
    }
}

fn convert_vec_update<A, B: From<A>>(update: VecUpdate<A>) -> VecUpdate<B> {
    match update {
        VecUpdate::None(none) => VecUpdate::None(none),
        VecUpdate::Delete(delete) => VecUpdate::Delete(delete),
        VecUpdate::UpdateOrInsert(content) => VecUpdate::UpdateOrInsert(content.into()),
    }
}

/// An update of one position in the layer or plot list of a project.
pub type LayerUpdate = VecUpdate<ProjectLayerUpdate>;

impl ToSchema for LayerUpdate {
    fn name() -> Cow<'static, str> {
        "LayerUpdate".into()
    }
}

impl PartialSchema for LayerUpdate {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        use utoipa::openapi::*;
        OneOfBuilder::new()
            .item(Ref::from_schema_name("ProjectUpdateToken"))
            .item(Ref::from_schema_name("ProjectLayerUpdate"))
            .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projects::RasterSymbology;
    use geoengine_datatypes::{
        operations::image::{Colorizer, RasterColorizer},
        primitives::{BoundingBox2D, TimeGranularity},
        spatial_reference::SpatialReferenceOption,
        util::{Identifier, test::TestDefault},
    };
    use serde_json::json;

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

        let processing_graph = ProcessingGraphId::new();
        assert_eq!(
            serde_json::from_str::<LayerUpdate>(
                &json!({
                    "processingGraph": processing_graph,
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
            LayerUpdate::UpdateOrInsert(ProjectLayerUpdate {
                id: None,
                processing_graph,
                name: "L2".to_string(),
                description: String::new(),
                visibility: LayerVisibility {
                    data: true,
                    legend: false,
                },
                symbology: Some(Symbology::Raster(RasterSymbology {
                    r#type: Default::default(),
                    opacity: 1.0,
                    raster_colorizer: RasterColorizer::SingleBand {
                        band: 0,
                        band_colorizer: Colorizer::test_default(),
                    },
                })),
                properties: vec![],
                metadata: HashMap::new(),
                source: None,
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
                LayerUpdate::UpdateOrInsert(ProjectLayerUpdate {
                    id: None,
                    processing_graph: ProcessingGraphId::new(),
                    name: "vector layer".to_string(),
                    description: "description".to_string(),
                    visibility: Default::default(),
                    symbology: Some(Symbology::Raster(RasterSymbology {
                        r#type: Default::default(),
                        opacity: 1.0,
                        raster_colorizer: RasterColorizer::SingleBand {
                            band: 0,
                            band_colorizer: Colorizer::test_default(),
                        },
                    })),
                    properties: vec![("key".to_string(), "value".to_string()).into()],
                    metadata: HashMap::from([("key".to_string(), "value".to_string())]),
                    source: None,
                }),
                LayerUpdate::UpdateOrInsert(ProjectLayerUpdate {
                    id: Some(LayerId("9f5a4f8c-25b6-4bd4-a2a3-b1c1ac2c2d0b".to_string())),
                    processing_graph: ProcessingGraphId::new(),
                    name: "raster layer".to_string(),
                    description: "description".to_string(),
                    visibility: Default::default(),
                    symbology: Some(Symbology::Raster(RasterSymbology {
                        r#type: Default::default(),
                        opacity: 1.0,
                        raster_colorizer: RasterColorizer::SingleBand {
                            band: 0,
                            band_colorizer: Colorizer::test_default(),
                        },
                    })),
                    properties: vec![("key".to_string(), "value".to_string()).into()],
                    metadata: HashMap::from([("key".to_string(), "value".to_string())]),
                    source: Some(ProviderLayerId {
                        provider_id: geoengine_datatypes::dataset::DataProviderId::new(),
                        layer_id: geoengine_datatypes::dataset::LayerId("layer".to_string()),
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
    }
}
