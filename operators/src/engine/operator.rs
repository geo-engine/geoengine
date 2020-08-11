use super::query_processor::{TypedRasterQueryProcessor, TypedVectorQueryProcessor};
use crate::{error, util::Result};
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::{projection::ProjectionOption, raster::RasterDataType};
use serde::{Deserialize, Serialize};
use std::ops::Range;

/// Common methods for `Operator`s
pub trait Operator: std::fmt::Debug + Send + Sync {
    /// Get the sources of the `Operator`
    fn raster_sources(&self) -> &[Box<dyn RasterOperator>];

    /// Get the sources of the `Operator`
    fn vector_sources(&self) -> &[Box<dyn VectorOperator>];

    fn validate_children(
        &self,
        number_of_raster_sources: Range<usize>,
        number_of_vector_sources: Range<usize>,
    ) -> Result<()> {
        if !number_of_raster_sources.contains(&self.raster_sources().len()) {}
        if !number_of_vector_sources.contains(&self.vector_sources().len()) {}

        let result_projection = self.projection();

        for proj in self
            .raster_sources()
            .iter()
            .map(|o| o.projection())
            .chain(self.vector_sources().iter().map(|o| o.projection()))
        {
            if proj != result_projection {
                return Err(error::Error::InvalidProjection {
                    expected: result_projection,
                    found: proj,
                });
            }
        }

        Ok(())
    }

    /// Get the projection of the result produced by this `Operator`
    fn projection(&self) -> ProjectionOption;
}

/// Common methods for `VectorOperator`s
#[typetag::serde(tag = "type")]
pub trait VectorOperator: Operator {
    /// Get the result type of the `Operator`
    fn result_type(&self) -> VectorDataType;

    /// Instantiate a `TypedVectorQueryProcessor` from a `RasterOperator`
    fn vector_processor(&self) -> Result<TypedVectorQueryProcessor>;

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
    fn raster_processor(&self) -> Result<TypedRasterQueryProcessor>;

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
