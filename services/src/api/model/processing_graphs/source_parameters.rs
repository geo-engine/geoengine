use crate::api::model::processing_graphs::{RasterOperator, VectorOperator};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// A single raster operator as a source for this operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[schema(no_recursion)]
#[serde(rename_all = "camelCase")]
pub struct SingleRasterSource {
    pub raster: RasterOperator,
}

impl TryFrom<SingleRasterSource> for geoengine_operators::engine::SingleRasterSource {
    type Error = anyhow::Error;

    fn try_from(value: SingleRasterSource) -> Result<Self, Self::Error> {
        Ok(Self {
            raster: value.raster.try_into()?,
        })
    }
}

/// A single vector operator or a single raster operators as source for this operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[schema(no_recursion)]
#[serde(rename_all = "camelCase")]
pub struct SingleRasterOrVectorSource {
    pub source: SingleRasterOrVectorOperator,
}

/// It is either a set of `RasterOperator` or a single `VectorOperator`
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(untagged)]
pub enum SingleRasterOrVectorOperator {
    Raster(RasterOperator),
    Vector(VectorOperator),
}

impl TryFrom<SingleRasterOrVectorSource>
    for geoengine_operators::engine::SingleRasterOrVectorSource
{
    type Error = anyhow::Error;

    fn try_from(value: SingleRasterOrVectorSource) -> Result<Self, Self::Error> {
        use geoengine_operators::util::input::RasterOrVectorOperator as OperatorsRasterOrVectorOperator;
        let source = match value.source {
            SingleRasterOrVectorOperator::Raster(raster) => {
                OperatorsRasterOrVectorOperator::Raster(raster.try_into()?)
            }
            SingleRasterOrVectorOperator::Vector(vector) => {
                OperatorsRasterOrVectorOperator::Vector(vector.try_into()?)
            }
        };
        Ok(Self { source })
    }
}

/// A single vector operator and one or more raster operators as source for this operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[schema(no_recursion)]
#[serde(rename_all = "camelCase")]
pub struct SingleVectorMultipleRasterSources {
    pub vector: VectorOperator,
    pub rasters: Vec<RasterOperator>,
}

impl TryFrom<SingleVectorMultipleRasterSources>
    for geoengine_operators::engine::SingleVectorMultipleRasterSources
{
    type Error = anyhow::Error;

    fn try_from(value: SingleVectorMultipleRasterSources) -> Result<Self, Self::Error> {
        Ok(Self {
            vector: value.vector.try_into()?,
            rasters: value
                .rasters
                .into_iter()
                .map(std::convert::TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// Either one or more raster operators or a single vector operator as source for this operator.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MultipleRasterOrSingleVectorSource {
    pub source: MultipleRasterOrSingleVectorOperator,
}

/// It is either a set of `RasterOperator` or a single `VectorOperator`
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(untagged)]
pub enum MultipleRasterOrSingleVectorOperator {
    Raster(Vec<RasterOperator>),
    Vector(VectorOperator),
}

impl TryFrom<MultipleRasterOrSingleVectorSource>
    for geoengine_operators::engine::MultipleRasterOrSingleVectorSource
{
    type Error = anyhow::Error;

    fn try_from(value: MultipleRasterOrSingleVectorSource) -> Result<Self, Self::Error> {
        use geoengine_operators::util::input::MultiRasterOrVectorOperator as OperatorsMultiRasterOrVectorOperator;
        let source = match value.source {
            MultipleRasterOrSingleVectorOperator::Raster(rasters) => {
                OperatorsMultiRasterOrVectorOperator::Raster(
                    rasters
                        .into_iter()
                        .map(std::convert::TryInto::try_into)
                        .collect::<Result<_, _>>()?,
                )
            }
            MultipleRasterOrSingleVectorOperator::Vector(vector) => {
                OperatorsMultiRasterOrVectorOperator::Vector(vector.try_into()?)
            }
        };
        Ok(Self { source })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::model::processing_graphs::parameters::SpatialBoundsDerive;
    use crate::api::model::processing_graphs::source::{
        GdalSource, GdalSourceParameters, MockPointSource, MockPointSourceParameters,
    };
    use crate::api::model::processing_graphs::{RasterOperator, VectorOperator};

    #[test]
    fn it_converts_single_raster_source() {
        let api = SingleRasterSource {
            raster: RasterOperator::GdalSource(GdalSource {
                r#type: Default::default(),
                params: GdalSourceParameters {
                    data: "example_data".to_string(),
                    overview_level: None,
                },
            }),
        };

        let _eng: geoengine_operators::engine::SingleRasterSource =
            api.try_into().expect("conversion failed");
    }

    #[test]
    fn it_converts_single_raster_or_vector_source() {
        let api = SingleRasterOrVectorSource {
            source: SingleRasterOrVectorOperator::Raster(RasterOperator::GdalSource(GdalSource {
                r#type: Default::default(),
                params: GdalSourceParameters {
                    data: "example_data".to_string(),
                    overview_level: None,
                },
            })),
        };

        let eng: geoengine_operators::engine::SingleRasterOrVectorSource =
            api.try_into().expect("conversion failed");

        assert!(eng.raster().is_some());
    }

    #[test]
    fn it_converts_single_vector_multiple_raster_sources() {
        let api = SingleVectorMultipleRasterSources {
            vector: VectorOperator::MockPointSource(MockPointSource {
                r#type: Default::default(),
                params: MockPointSourceParameters {
                    points: vec![crate::api::model::datatypes::Coordinate2D { x: 0.0, y: 0.0 }],
                    spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                },
            }),
            rasters: vec![RasterOperator::GdalSource(GdalSource {
                r#type: Default::default(),
                params: GdalSourceParameters {
                    data: "example_data".to_string(),
                    overview_level: None,
                },
            })],
        };

        let eng: geoengine_operators::engine::SingleVectorMultipleRasterSources =
            api.try_into().expect("conversion failed");

        assert_eq!(eng.rasters.len(), 1);
    }

    #[test]
    fn it_converts_multiple_raster_or_single_vector_source() {
        let api = MultipleRasterOrSingleVectorSource {
            source: MultipleRasterOrSingleVectorOperator::Raster(vec![RasterOperator::GdalSource(
                GdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: "example_data".to_string(),
                        overview_level: None,
                    },
                },
            )]),
        };

        let eng: geoengine_operators::engine::MultipleRasterOrSingleVectorSource =
            api.try_into().expect("conversion failed");

        assert!(eng.raster().is_some());
    }
}
