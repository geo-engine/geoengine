use super::query_processor::{TypedRasterQueryProcessor, TypedVectorQueryProcessor};
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::raster::RasterDataType;
use serde::{Deserialize, Serialize};

pub trait Operator: std::fmt::Debug + Send + Sync {
    /// get the sources of the Operator. TODO: extra trait?
    fn raster_sources(&self) -> &[Box<dyn RasterOperator>];

    /// get the sources of the Operator. TODO: extra trait?
    fn vector_sources(&self) -> &[Box<dyn VectorOperator>] {
        &[]
    }
}

#[typetag::serde(tag = "type")]
pub trait VectorOperator: Operator {
    fn result_type(&self) -> VectorDataType;

    fn vector_processor(&self) -> TypedVectorQueryProcessor;

    fn boxed(self) -> Box<dyn VectorOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

/// The MetaRasterOperator is a trait for MetaOperators creating RasterOperators for processing Raster data
#[typetag::serde(tag = "type")]
pub trait RasterOperator: Operator {
    /// The magic method to handle the mapping of the create type to a concrete implementation. More work required! TODO: macro?
    fn raster_processor(&self) -> TypedRasterQueryProcessor;

    /// get the type the Operator creates.
    fn result_type(&self) -> RasterDataType;

    fn boxed(self) -> Box<dyn RasterOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

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
