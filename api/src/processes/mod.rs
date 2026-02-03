#![allow(clippy::needless_for_each)] // TODO: remove when clippy is fixed for utoipa <https://github.com/juhaku/utoipa/issues/1420>

use crate::processes::{
    processing::{Expression, ExpressionParameters, RasterVectorJoin, RasterVectorJoinParameters},
    source::{GdalSource, GdalSourceParameters, MockPointSource, MockPointSourceParameters},
};
use geoengine_operators::{
    engine::{
        RasterOperator as OperatorsRasterOperator, TypedOperator as OperatorsTypedOperator,
        VectorOperator as OperatorsVectorOperator,
    },
    mock::MockPointSource as OperatorsMockPointSource,
    source::GdalSource as OperatorsGdalSource,
};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

mod processing;
mod source;

/// Operator outputs are distinguished by their data type.
/// There are `raster`, `vector` and `plot` operators.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(tag = "type", content = "operator")]
pub enum TypedOperator {
    Vector(VectorOperator),
    Raster(RasterOperator),
    // Plot(Box<dyn PlotOperator>),
}

/// An operator that produces raster data.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum RasterOperator {
    GdalSource(GdalSource),
}

/// An operator that produces vector data.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum VectorOperator {
    MockPointSource(MockPointSource),
    RasterVectorJoin(RasterVectorJoin),
}

impl TryFrom<RasterOperator> for Box<dyn OperatorsRasterOperator> {
    type Error = anyhow::Error;
    fn try_from(operator: RasterOperator) -> Result<Self, Self::Error> {
        match operator {
            RasterOperator::GdalSource(gdal_source) => {
                OperatorsGdalSource::try_from(gdal_source).map(OperatorsRasterOperator::boxed)
            }
        }
    }
}

impl TryFrom<VectorOperator> for Box<dyn OperatorsVectorOperator> {
    type Error = anyhow::Error;
    fn try_from(operator: VectorOperator) -> Result<Self, Self::Error> {
        match operator {
            VectorOperator::MockPointSource(mock_point_source) => {
                OperatorsMockPointSource::try_from(mock_point_source)
                    .map(OperatorsVectorOperator::boxed)
            }
            VectorOperator::RasterVectorJoin(_rvj) => Err(anyhow::anyhow!(
                "conversion of RasterVectorJoin to runtime operator is not supported here"
            )),
        }
    }
}

impl TryFrom<TypedOperator> for OperatorsTypedOperator {
    type Error = anyhow::Error;
    fn try_from(operator: TypedOperator) -> Result<Self, Self::Error> {
        match operator {
            TypedOperator::Raster(raster_operator) => Ok(Self::Raster(raster_operator.try_into()?)),
            TypedOperator::Vector(vector_operator) => Ok(Self::Vector(vector_operator.try_into()?)),
        }
    }
}

#[derive(OpenApi)]
#[openapi(components(schemas(
    Expression,
    ExpressionParameters,
    GdalSource,
    GdalSourceParameters,
    MockPointSource,
    MockPointSourceParameters,
    RasterVectorJoin,
    RasterVectorJoinParameters,
    RasterOperator,
    TypedOperator,
    VectorOperator,
)))]
pub struct OperatorsApi;
