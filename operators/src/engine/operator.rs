use super::query_processor::{TypedRasterQueryProcessor, TypedVectorQueryProcessor};
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::raster::RasterDataType;
use serde::{Deserialize, Serialize};

/// Common methods for `Operator`s
pub trait Operator: std::fmt::Debug + Send + Sync {
    /// Get the sources of the Operator
    fn raster_sources(&self) -> &[Box<dyn RasterOperator>];

    /// Get the sources of the Operator
    fn vector_sources(&self) -> &[Box<dyn VectorOperator>];
}

/// Common methods for `VectorOperator`s
#[typetag::serde(tag = "type")]
pub trait VectorOperator: Operator {
    /// Get the result type of the `Operator`
    fn result_type(&self) -> VectorDataType;

    /// Instantiate a `TypedVectorQueryProcessor` from a `RasterOperator`
    fn vector_processor(&self) -> TypedVectorQueryProcessor;

    /// Wrap a box around a `VectorOperator`
    fn boxed(self) -> Box<dyn VectorOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

/// Common methods for `RasterOperator`s
#[typetag::serde(tag = "type")]
pub trait RasterOperator: Operator {
    /// Get the result type of the `Operator`
    fn result_type(&self) -> RasterDataType;

    /// Instantiate a `TypedRasterQueryProcessor` from a `RasterOperator`
    fn raster_processor(&self) -> TypedRasterQueryProcessor;

    /// Wrap a box around a `RasterOperator`
    fn boxed(self) -> Box<dyn RasterOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

/// An enum to differentiate between `Operator` variants
#[derive(Debug, Serialize, Deserialize)]
pub enum TypedOperator {
    Vector(Box<dyn VectorOperator>),
    Raster(Box<dyn RasterOperator>),
}

impl Into<TypedOperator> for Box<dyn VectorOperator> {
    fn into(self) -> TypedOperator {
        TypedOperator::Vector(self)
    }
}

impl Into<TypedOperator> for Box<dyn RasterOperator> {
    fn into(self) -> TypedOperator {
        TypedOperator::Raster(self)
    }
}
