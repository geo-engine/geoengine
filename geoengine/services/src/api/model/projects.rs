use crate::{
    api::model::processing_graphs::ProcessingGraphId,
    projects::{LayerVisibility, ProjectId, ProjectVersion, STRectangle, Symbology, VecUpdate},
};
use geoengine_datatypes::primitives::TimeStep;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
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
    pub plots: Vec<Plot>,
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

impl From<Project> for crate::projects::Project {
    fn from(value: Project) -> Self {
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProjectLayer {
    pub processing_graph: ProcessingGraphId,
    pub name: String,
    pub visibility: LayerVisibility,
    pub symbology: Symbology,
}

impl From<crate::projects::ProjectLayer> for ProjectLayer {
    fn from(value: crate::projects::ProjectLayer) -> Self {
        Self {
            processing_graph: value.workflow.into(),
            name: value.name,
            visibility: value.visibility,
            symbology: value.symbology,
        }
    }
}

impl From<ProjectLayer> for crate::projects::ProjectLayer {
    fn from(value: ProjectLayer) -> Self {
        Self {
            workflow: value.processing_graph.into(),
            name: value.name,
            visibility: value.visibility,
            symbology: value.symbology,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Plot {
    pub processing_graph: ProcessingGraphId,
    pub name: String,
}

impl From<crate::projects::Plot> for Plot {
    fn from(value: crate::projects::Plot) -> Self {
        Self {
            processing_graph: value.workflow.into(),
            name: value.name,
        }
    }
}

impl From<Plot> for crate::projects::Plot {
    fn from(value: Plot) -> Self {
        Self {
            workflow: value.processing_graph.into(),
            name: value.name,
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

impl From<crate::projects::UpdateProject> for UpdateProject {
    fn from(value: crate::projects::UpdateProject) -> Self {
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

pub type LayerUpdate = VecUpdate<ProjectLayer>;
pub type PlotUpdate = VecUpdate<Plot>;

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
            .item(Ref::from_schema_name("ProjectLayer"))
            .into()
    }
}

impl ToSchema for PlotUpdate {
    fn name() -> Cow<'static, str> {
        "PlotUpdate".into()
    }
}

impl PartialSchema for PlotUpdate {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        use utoipa::openapi::*;
        OneOfBuilder::new()
            .item(Ref::from_schema_name("ProjectUpdateToken"))
            .item(Ref::from_schema_name("Plot"))
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
            LayerUpdate::UpdateOrInsert(ProjectLayer {
                processing_graph,
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
                    processing_graph: ProcessingGraphId::new(),
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
                    processing_graph: ProcessingGraphId::new(),
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
    }
}
