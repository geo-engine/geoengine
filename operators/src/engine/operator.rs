use super::query_processor::{TypedRasterQueryProcessor, TypedVectorQueryProcessor};
use geoengine_datatypes::raster::RasterDataType;

pub trait Operator: std::fmt::Debug {
    /// get the sources of the Operator. TODO: extra trait?
    fn raster_sources(&self) -> &[Box<dyn RasterOperator>];

    /// get the sources of the Operator. TODO: extra trait?
    fn vector_sources(&self) -> &[Box<dyn VectorOperator>] {
        &[]
    }
}

#[typetag::serde(tag = "type")]
pub trait VectorOperator: Operator {
    fn result_type(&self) -> () {
        () // TODO: implement
    }

    fn vector_query_processor(&self) -> TypedVectorQueryProcessor;

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
    fn create_raster_op(&self) -> TypedRasterQueryProcessor;

    /// get the type the Operator creates.
    fn result_type(&self) -> RasterDataType;

    fn boxed(self) -> Box<dyn RasterOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}
