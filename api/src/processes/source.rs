use geoengine_datatypes::dataset::NamedData;
use geoengine_macros::type_tag;
use geoengine_operators::source::{
    GdalSource as OperatorsGdalSource, GdalSourceParameters as OperatorsGdalSourceParameters,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # GdalSource
///
/// The [`GdalSource`] is a source operator that reads raster data using GDAL.
/// The counterpart for vector data is the [`OgrSource`].
///
/// ## Errors
///
/// If the given dataset does not exist or is not readable, an error is thrown.
///
/// ## Example JSON
///
/// ```json
/// {
///   "type": "GdalSource",
///   "params": {
///     "data": "ndvi"
///   }
/// }
/// ```
#[type_tag(value = "GdalSource")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalSource {
    pub params: GdalSourceParameters,
}

/// Parameters for the [`GdalSource`] operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalSourceParameters {
    /// Dataset name or identifier to be loaded.
    ///
    /// ### Example
    /// `"ndvi"`
    pub data: String,
}

impl TryFrom<GdalSource> for OperatorsGdalSource {
    type Error = anyhow::Error;
    fn try_from(value: GdalSource) -> Result<Self, Self::Error> {
        Ok(OperatorsGdalSource {
            params: OperatorsGdalSourceParameters {
                data: serde_json::from_str::<NamedData>(&serde_json::to_string(
                    &value.params.data,
                )?)?,
            },
        })
    }
}

// TODO: OpenAPI and conversions for other operators:
//  - MockPointSource
//  - Expression
//  - RasterVectorJoin

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processes::{RasterOperator, TypedOperator};
    use geoengine_operators::engine::TypedOperator as OperatorsTypedOperator;

    #[test]
    fn it_converts_into_gdal_source() {
        let api_operator = GdalSource {
            r#type: Default::default(),
            params: GdalSourceParameters {
                data: "example_dataset".to_string(),
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
            },
        }));

        OperatorsTypedOperator::try_from(typed_operator).expect("it should convert");
    }
}
