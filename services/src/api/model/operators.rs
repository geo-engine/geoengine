use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::datatypes::{
    BoundingBox2D, FeatureDataType, Measurement, RasterDataType, SpatialPartition2D,
    SpatialReferenceOption, TimeInterval, VectorDataType,
};

/// A `ResultDescriptor` for raster queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RasterResultDescriptor {
    pub data_type: RasterDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub measurement: Measurement,
    pub time: Option<TimeInterval>,
    pub bbox: Option<SpatialPartition2D>,
}

/// An enum to differentiate between `Operator` variants
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "operator")]
pub enum TypedOperator {
    Vector(Box<dyn geoengine_operators::engine::VectorOperator>),
    Raster(Box<dyn geoengine_operators::engine::RasterOperator>),
    Plot(Box<dyn geoengine_operators::engine::PlotOperator>),
}

impl ToSchema for TypedOperator {
    fn schema() -> utoipa::openapi::Schema {
        use utoipa::openapi::*;
        ObjectBuilder::new()
            .property(
                "type",
                ObjectBuilder::new()
                    .schema_type(SchemaType::String)
                    .enum_values(Some(vec!["Vector", "Raster", "Plot"]))
            )
            .required("type")
            .property(
                "operator",
                ObjectBuilder::new()
                    .property(
                        "type",
                        Object::with_type(SchemaType::String)
                    )
                    .required("type")
                    .property(
                        "params",
                        Object::with_type(SchemaType::Object)
                    )
                    .property(
                        "sources",
                        Object::with_type(SchemaType::Object)
                    )
            )
            .required("operator")
            .example(Some(serde_json::json!(
                {"type": "MockPointSource", "params": {"points": [{"x": 0.0, "y": 0.1}, {"x": 1.0, "y": 1.1}]}
            })))
            .description(Some("An enum to differentiate between `Operator` variants"))
            .into()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VectorResultDescriptor {
    pub data_type: VectorDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub columns: HashMap<String, VectorColumnInfo>,
    pub time: Option<TimeInterval>,
    pub bbox: Option<BoundingBox2D>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VectorColumnInfo {
    pub data_type: FeatureDataType,
    pub measurement: Measurement,
}

/// A `ResultDescriptor` for plot queries
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlotResultDescriptor {
    pub spatial_reference: SpatialReferenceOption,
    pub time: Option<TimeInterval>,
    pub bbox: Option<BoundingBox2D>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum TypedResultDescriptor {
    Plot(PlotResultDescriptor),
    Raster(RasterResultDescriptor),
    Vector(VectorResultDescriptor),
}
