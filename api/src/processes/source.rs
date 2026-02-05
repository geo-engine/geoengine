use crate::parameters::{Coordinate2D, SpatialBoundsDerive};
use geoengine_datatypes::dataset::NamedData;
use geoengine_macros::type_tag;
use geoengine_operators::{
    mock::{
        MockPointSource as OperatorsMockPointSource,
        MockPointSourceParams as OperatorsMockPointSourceParameters,
    },
    source::{
        GdalSource as OperatorsGdalSource, GdalSourceParameters as OperatorsGdalSourceParameters,
    },
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// The [`GdalSource`] is a source operator that reads raster data using GDAL.
/// The counterpart for vector data is the [`OgrSource`].
///
/// ## Errors
///
/// If the given dataset does not exist or is not readable, an error is thrown.
///
#[type_tag(value = "GdalSource")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(
    title = "GDAL Source",
    examples(json!({
        "type": "GdalSource",
        "params": {
            "data": "ndvi",
            "overviewLevel": null
        }
    }))
)]
pub struct GdalSource {
    pub params: GdalSourceParameters,
}

/// Parameters for the [`GdalSource`] operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalSourceParameters {
    /// Dataset name or identifier to be loaded.
    #[schema(examples("ndvi"))]
    pub data: String,

    /// *Optional*: overview level to use.
    ///
    /// If not provided, the data source will determine the resolution, i.e., uses its native resolution.
    #[schema(examples(3))]
    pub overview_level: Option<u32>,
}

impl TryFrom<GdalSource> for OperatorsGdalSource {
    type Error = anyhow::Error;
    fn try_from(value: GdalSource) -> Result<Self, Self::Error> {
        Ok(OperatorsGdalSource {
            params: OperatorsGdalSourceParameters {
                data: serde_json::from_str::<NamedData>(&serde_json::to_string(
                    &value.params.data,
                )?)?,
                overview_level: value.params.overview_level,
            },
        })
    }
}

/// The [`MockPointSource`] is a source operator that provides mock vector point data for testing and development purposes.
///
#[type_tag(value = "MockPointSource")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(
    title = "Mock Point Source",
    examples(json!({
        "type": "MockPointSource",
        "params": {
            "points": [ { "x": 1.0, "y": 2.0 }, { "x": 3.0, "y": 4.0 } ],
            "spatialBounds": { "type": "derive" }
        }
    }))
)]
pub struct MockPointSource {
    pub params: MockPointSourceParameters,
}

/// Parameters for the [`MockPointSource`] operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MockPointSourceParameters {
    /// Points to be output by the mock point source.
    ///
    #[schema(examples(json!([
        { "x": 1.0, "y": 2.0 },
        { "x": 3.0, "y": 4.0 }
    ])))]
    pub points: Vec<Coordinate2D>,

    /// Defines how the spatial bounds of the source are derived.
    ///
    /// Defaults to `None`.
    #[schema(examples(json!({ "type": "derive" })))]
    pub spatial_bounds: SpatialBoundsDerive,
}

impl TryFrom<MockPointSource> for OperatorsMockPointSource {
    type Error = anyhow::Error;
    fn try_from(value: MockPointSource) -> Result<Self, Self::Error> {
        Ok(OperatorsMockPointSource {
            params: OperatorsMockPointSourceParameters {
                points: value.params.points.into_iter().map(Into::into).collect(),
                spatial_bounds: value.params.spatial_bounds.try_into()?,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processes::{RasterOperator, TypedOperator, VectorOperator};
    use geoengine_operators::engine::TypedOperator as OperatorsTypedOperator;

    #[test]
    fn it_converts_into_gdal_source() {
        let api_operator = GdalSource {
            r#type: Default::default(),
            params: GdalSourceParameters {
                data: "example_dataset".to_string(),
                overview_level: None,
            },
        };

        let operators_operator: OperatorsGdalSource =
            api_operator.try_into().expect("it should convert");

        assert_eq!(
            operators_operator.params.data,
            NamedData::with_system_name("example_dataset")
        );

        let typed_operator = TypedOperator::Raster(RasterOperator::GdalSource(GdalSource {
            r#type: Default::default(),
            params: GdalSourceParameters {
                data: "example_dataset".to_string(),
                overview_level: None,
            },
        }));

        OperatorsTypedOperator::try_from(typed_operator).expect("it should convert");
    }

    #[test]
    fn it_converts_mock_point_source() {
        let api_operator = MockPointSource {
            r#type: Default::default(),
            params: MockPointSourceParameters {
                points: vec![
                    Coordinate2D { x: 1.0, y: 2.0 },
                    Coordinate2D { x: 3.0, y: 4.0 },
                ],
                spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
            },
        };

        let operators_operator: OperatorsMockPointSource =
            api_operator.try_into().expect("it should convert");

        assert_eq!(
            operators_operator.params.points,
            vec![
                geoengine_datatypes::primitives::Coordinate2D { x: 1.0, y: 2.0 },
                geoengine_datatypes::primitives::Coordinate2D { x: 3.0, y: 4.0 }
            ]
        );

        let typed_operator =
            TypedOperator::Vector(VectorOperator::MockPointSource(MockPointSource {
                r#type: Default::default(),
                params: MockPointSourceParameters {
                    points: vec![Coordinate2D { x: 1.0, y: 2.0 }],
                    spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                },
            }));

        OperatorsTypedOperator::try_from(typed_operator).expect("it should convert");
    }
}
