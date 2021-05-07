use crate::engine::{RasterOperator, TypedOperator, VectorOperator};
use serde::{Deserialize, Serialize};

/// It is either a `RasterOperator` or a `VectorOperator`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RasterOrVectorOperator {
    Raster(Box<dyn RasterOperator>),
    Vector(Box<dyn VectorOperator>),
}

impl RasterOrVectorOperator {
    pub fn is_raster(&self) -> bool {
        match self {
            RasterOrVectorOperator::Raster(_) => true,
            RasterOrVectorOperator::Vector(_) => false,
        }
    }

    pub fn is_vector(&self) -> bool {
        match self {
            RasterOrVectorOperator::Raster(_) => false,
            RasterOrVectorOperator::Vector(_) => true,
        }
    }
}

impl From<RasterOrVectorOperator> for TypedOperator {
    fn from(operator: RasterOrVectorOperator) -> Self {
        match operator {
            RasterOrVectorOperator::Raster(operator) => Self::Raster(operator),
            RasterOrVectorOperator::Vector(operator) => Self::Vector(operator),
        }
    }
}

impl From<Box<dyn RasterOperator>> for RasterOrVectorOperator {
    fn from(operator: Box<dyn RasterOperator>) -> Self {
        Self::Raster(operator)
    }
}

impl From<Box<dyn VectorOperator>> for RasterOrVectorOperator {
    fn from(operator: Box<dyn VectorOperator>) -> Self {
        Self::Vector(operator)
    }
}

#[cfg(test)]
mod tests {
    use crate::source::{GdalSource, GdalSourceParameters};
    use geoengine_datatypes::dataset::InternalDatasetId;
    use std::str::FromStr;

    use super::*;

    #[test]
    fn it_serializes() {
        let operator = RasterOrVectorOperator::Raster(
            GdalSource {
                params: GdalSourceParameters {
                    dataset: InternalDatasetId::from_str("fc734022-61e0-49da-b327-257ba9d602a7")
                        .unwrap()
                        .into(),
                },
            }
            .boxed(),
        );

        assert_eq!(
            serde_json::to_value(&operator).unwrap(),
            serde_json::json!({
                "type": "GdalSource",
                "params": {
                    "dataset": {
                        "internal": "fc734022-61e0-49da-b327-257ba9d602a7"
                    }
                }
            })
        );
    }

    #[test]
    fn it_deserializes_raster_ops() {
        let workflow = serde_json::json!({
            "type": "GdalSource",
            "params": {
                "dataset": {
                    "internal": "fc734022-61e0-49da-b327-257ba9d602a7"
                }
            }
        })
        .to_string();

        let raster_or_vector_operator: RasterOrVectorOperator =
            serde_json::from_str(&workflow).unwrap();

        assert!(raster_or_vector_operator.is_raster());
        assert!(!raster_or_vector_operator.is_vector());
    }

    #[test]
    fn it_deserializes_vector_ops() {
        let workflow = serde_json::json!({
            "type": "OgrSource",
            "params": {
                "dataset": {
                    "internal": "fc734022-61e0-49da-b327-257ba9d602a7"
                },
                "attribute_projection": null,
            }
        })
        .to_string();

        let raster_or_vector_operator: RasterOrVectorOperator =
            serde_json::from_str(&workflow).unwrap();

        assert!(raster_or_vector_operator.is_vector());
        assert!(!raster_or_vector_operator.is_raster());
    }
}
