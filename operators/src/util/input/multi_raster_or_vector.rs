use crate::engine::{OperatorData, RasterOperator, VectorOperator};
use geoengine_datatypes::dataset::NamedData;
use serde::{Deserialize, Serialize};

/// It is either a set of `RasterOperator` or a single `VectorOperator`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MultiRasterOrVectorOperator {
    Raster(Vec<Box<dyn RasterOperator>>),
    Vector(Box<dyn VectorOperator>),
}

impl MultiRasterOrVectorOperator {
    pub fn is_raster(&self) -> bool {
        match self {
            Self::Raster(_) => true,
            Self::Vector(_) => false,
        }
    }

    pub fn is_vector(&self) -> bool {
        match self {
            Self::Raster(_) => false,
            Self::Vector(_) => true,
        }
    }
}

impl From<Box<dyn RasterOperator>> for MultiRasterOrVectorOperator {
    fn from(operator: Box<dyn RasterOperator>) -> Self {
        Self::Raster(vec![operator])
    }
}

impl From<Vec<Box<dyn RasterOperator>>> for MultiRasterOrVectorOperator {
    fn from(operators: Vec<Box<dyn RasterOperator>>) -> Self {
        Self::Raster(operators)
    }
}

impl From<Box<dyn VectorOperator>> for MultiRasterOrVectorOperator {
    fn from(operator: Box<dyn VectorOperator>) -> Self {
        Self::Vector(operator)
    }
}

impl OperatorData for MultiRasterOrVectorOperator {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        match self {
            Self::Raster(rs) => {
                for r in rs {
                    r.data_names_collect(data_names);
                }
            }
            Self::Vector(v) => v.data_names_collect(data_names),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::source::{GdalSource, GdalSourceParameters};
    use geoengine_datatypes::dataset::NamedData;

    use super::*;

    #[test]
    fn it_serializes() {
        let operator = MultiRasterOrVectorOperator::Raster(vec![GdalSource {
            params: GdalSourceParameters::new(NamedData::with_namespaced_name("foo", "bar")),
        }
        .boxed()]);

        assert_eq!(
            serde_json::to_value(&operator).unwrap(),
            serde_json::json!([{
                "type": "GdalSource",
                "params": {
                    "data": "foo:bar",
                    "overviewLevel": null,
                }
            }])
        );
    }

    #[test]
    fn it_deserializes_raster_ops() {
        let workflow = serde_json::json!([{
            "type": "GdalSource",
            "params": {
                "data": "foo:bar"
            }
        }])
        .to_string();

        let raster_or_vector_operator: MultiRasterOrVectorOperator =
            serde_json::from_str(&workflow).unwrap();

        assert!(raster_or_vector_operator.is_raster());
        assert!(!raster_or_vector_operator.is_vector());
    }

    #[test]
    fn it_deserializes_vector_ops() {
        let workflow = serde_json::json!({
            "type": "OgrSource",
            "params": {
                "data": "foo:bar",
                "attribute_projection": null,
            }
        })
        .to_string();

        let raster_or_vector_operator: MultiRasterOrVectorOperator =
            serde_json::from_str(&workflow).unwrap();

        assert!(raster_or_vector_operator.is_vector());
        assert!(!raster_or_vector_operator.is_raster());
    }
}
