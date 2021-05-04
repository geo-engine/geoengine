use crate::engine::{RasterOperator, TypedOperator, VectorOperator};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// It is either a `RasterOperator` or a `VectorOperator`
#[derive(Debug, Clone)]
pub enum RasterOrVectorOperator {
    Raster(Box<dyn RasterOperator>),
    Vector(Box<dyn VectorOperator>),
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

impl Serialize for RasterOrVectorOperator {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        match self {
            RasterOrVectorOperator::Raster(operator) => operator.serialize(serializer),
            RasterOrVectorOperator::Vector(operator) => operator.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for RasterOrVectorOperator {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let raster_error = match Box::<dyn RasterOperator>::deserialize(deserializer) {
            Ok(operator) => return Ok(Self::Raster(operator)),
            Err(error) => error,
        };

        let vector_error = match Box::<dyn VectorOperator>::deserialize(deserializer) {
            Ok(operator) => return Ok(Self::Vector(operator)),
            Err(error) => error,
        };

        Err(serde::de::Error::custom(format!(
            "Unable to deserialize `RasterOperator` or `VectorOperator`: {} {}",
            raster_error, vector_error
        )))
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{dataset::InternalDatasetId, util::Identifier};

    use crate::source::{GdalSource, GdalSourceParameters};

    use super::*;

    #[test]
    fn it_serializes() {
        let operator = RasterOrVectorOperator::Raster(
            GdalSource {
                params: GdalSourceParameters {
                    dataset: InternalDatasetId::new().into(),
                },
            }
            .boxed(),
        );

        assert_eq!(serde_json::to_string(&operator).unwrap(), "");
    }
}
