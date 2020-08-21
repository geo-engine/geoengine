use super::{
    query_processor::{TypedRasterQueryProcessor, TypedVectorQueryProcessor},
    CloneableInitializedOperator, CloneableOperator, CloneableRasterOperator,
    CloneableVectorOperator, RasterResultDescriptor, VectorResultDescriptor,
};
use crate::util::Result;

use serde::{Deserialize, Serialize};

/// Common methods for `Operator`s
pub trait Operator: std::fmt::Debug + Send + Sync + CloneableOperator {}

/// Common methods for `RasterOperator`s
#[typetag::serde(tag = "type")]
pub trait RasterOperator: Operator + CloneableRasterOperator {
    fn initialized_operator(
        self: Box<Self>,
        context: ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>>;

    /// Wrap a box around a `RasterOperator`
    fn boxed(self) -> Box<dyn RasterOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

/// Common methods for `VectorOperator`s
#[typetag::serde(tag = "type")]
pub trait VectorOperator: Operator + CloneableVectorOperator {
    fn into_initialized_operator(
        self: Box<Self>,
        context: ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>>;

    /// Wrap a box around a `VectorOperator`
    fn boxed(self) -> Box<dyn VectorOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct ExecutionContext;

pub trait InitializedOperator: CloneableInitializedOperator {
    fn execution_context(&self) -> &ExecutionContext;

    /// Get the sources of the `Operator`
    fn raster_sources(&self) -> &[Box<dyn InitializedRasterOperator>];

    /// Get the sources of the `Operator`
    fn vector_sources(&self) -> &[Box<dyn InitializedVectorOperator>];

    /// Get the sources of the `Operator`
    fn raster_sources_mut(&mut self) -> &mut [Box<dyn InitializedRasterOperator>];

    /// Get the sources of the `Operator`
    fn vector_sources_mut(&mut self) -> &mut [Box<dyn InitializedVectorOperator>];
}

pub trait InitializedVectorOperator: Send + Sync {
    /// Get the result type of the `Operator`
    fn result_descriptor(&self) -> VectorResultDescriptor;

    /// Instantiate a `TypedVectorQueryProcessor` from a `RasterOperator`
    fn vector_processor(&self) -> Result<TypedVectorQueryProcessor>;

    /// Wrap a box around a `RasterOperator`
    fn boxed(self) -> Box<dyn InitializedVectorOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

pub trait InitializedRasterOperator: Send + Sync {
    /// Get the result type of the `Operator`
    fn result_descriptor(&self) -> RasterResultDescriptor;

    /// Instantiate a `TypedRasterQueryProcessor` from a `RasterOperator`
    fn raster_processor(&self) -> Result<TypedRasterQueryProcessor>;

    /// Wrap a box around a `RasterOperator`
    fn boxed(self) -> Box<dyn InitializedRasterOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

impl InitializedRasterOperator for Box<dyn InitializedRasterOperator> {
    fn result_descriptor(&self) -> RasterResultDescriptor {
        self.as_ref().result_descriptor()
    }
    fn raster_processor(&self) -> Result<TypedRasterQueryProcessor> {
        self.as_ref().raster_processor()
    }
}

impl InitializedVectorOperator for Box<dyn InitializedVectorOperator> {
    fn result_descriptor(&self) -> VectorResultDescriptor {
        self.as_ref().result_descriptor()
    }
    fn vector_processor(&self) -> Result<TypedVectorQueryProcessor> {
        self.as_ref().vector_processor()
    }
}

/// An enum to differentiate between `Operator` variants
#[derive(Clone, Debug, Serialize, Deserialize)]
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

/// An enum to differentiate between `InitializedOperator` variants
pub enum TypedInitializedOperator {
    Vector(Box<dyn InitializedVectorOperator>),
    Raster(Box<dyn InitializedRasterOperator>),
}

impl Into<TypedInitializedOperator> for Box<dyn InitializedVectorOperator> {
    fn into(self) -> TypedInitializedOperator {
        TypedInitializedOperator::Vector(self)
    }
}

impl Into<TypedInitializedOperator> for Box<dyn InitializedRasterOperator> {
    fn into(self) -> TypedInitializedOperator {
        TypedInitializedOperator::Raster(self)
    }
}
