#![allow(clippy::needless_for_each)] // TODO: remove when clippy is fixed for utoipa <https://github.com/juhaku/utoipa/issues/1420>

use crate::processes::source::{GdalSource, GdalSourceParameters};
use geoengine_operators::{
    engine::{RasterOperator as OperatorsRasterOperator, TypedOperator as OperatorsTypedOperator},
    source::GdalSource as OperatorsGdalSource,
};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

mod source;

/// Operator outputs are distinguished by their data type.
/// There are `raster`, `vector` and `plot` operators.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(tag = "type", content = "operator")]
pub enum TypedOperator {
    // Vector(Box<dyn VectorOperator>),
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

impl TryFrom<TypedOperator> for OperatorsTypedOperator {
    type Error = anyhow::Error;
    fn try_from(operator: TypedOperator) -> Result<Self, Self::Error> {
        match operator {
            TypedOperator::Raster(raster_operator) => Ok(Self::Raster(raster_operator.try_into()?)),
        }
    }
}

#[derive(OpenApi)]
#[openapi(components(schemas(TypedOperator, RasterOperator, GdalSource, GdalSourceParameters)))]
pub struct OperatorsApi;
