use super::{
    query_processor::{TypedRasterQueryProcessor, TypedVectorQueryProcessor},
    CloneableOperator, CloneableRasterOperator, CloneableVectorOperator, RasterResultDescriptor,
    VectorResultDescriptor,
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
    ) -> Result<Box<InitializedRasterOperator>>;

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
    ) -> Result<Box<InitializedVectorOperator>>;

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

pub trait InitializedOperator {
    fn execution_context(&self) -> &ExecutionContext;

    /// Get the sources of the `Operator`
    fn raster_sources(&self) -> &[Box<InitializedRasterOperator>];

    /// Get the sources of the `Operator`
    fn vector_sources(&self) -> &[Box<InitializedVectorOperator>];

    /// Get the sources of the `Operator`
    fn raster_sources_mut(&mut self) -> &mut [Box<InitializedRasterOperator>];

    /// Get the sources of the `Operator`
    fn vector_sources_mut(&mut self) -> &mut [Box<InitializedVectorOperator>];
}

pub type InitializedVectorOperator =
    dyn InitializedOperatorB<VectorResultDescriptor, TypedVectorQueryProcessor>;

pub type InitializedRasterOperator =
    dyn InitializedOperatorB<RasterResultDescriptor, TypedRasterQueryProcessor>;

pub trait InitializedOperatorB<R, Q>: InitializedOperator + Send + Sync {
    /// Get the result type of the `Operator`
    fn result_descriptor(&self) -> R;

    /// Instantiate a `TypedVectorQueryProcessor` from a `RasterOperator`
    fn query_processor(&self) -> Result<Q>;

    /// Wrap a box around a `RasterOperator`
    fn boxed(self) -> Box<dyn InitializedOperatorB<R, Q>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

impl InitializedOperator for Box<dyn InitializedOperator> {
    fn execution_context(&self) -> &ExecutionContext {
        self.as_ref().execution_context()
    }
    fn raster_sources(&self) -> &[Box<InitializedRasterOperator>] {
        self.as_ref().raster_sources()
    }
    fn vector_sources(&self) -> &[Box<InitializedVectorOperator>] {
        self.as_ref().vector_sources()
    }
    fn raster_sources_mut(&mut self) -> &mut [Box<InitializedRasterOperator>] {
        self.as_mut().raster_sources_mut()
    }
    fn vector_sources_mut(&mut self) -> &mut [Box<InitializedVectorOperator>] {
        self.as_mut().vector_sources_mut()
    }
}

impl<R, Q> InitializedOperator for Box<dyn InitializedOperatorB<R, Q>>
where
    R: Clone + 'static,
    Q: Clone + 'static,
{
    fn execution_context(&self) -> &ExecutionContext {
        self.as_ref().execution_context()
    }
    fn raster_sources(&self) -> &[Box<InitializedRasterOperator>] {
        self.as_ref().raster_sources()
    }
    fn vector_sources(&self) -> &[Box<InitializedVectorOperator>] {
        self.as_ref().vector_sources()
    }
    fn raster_sources_mut(&mut self) -> &mut [Box<InitializedRasterOperator>] {
        self.as_mut().raster_sources_mut()
    }
    fn vector_sources_mut(&mut self) -> &mut [Box<InitializedVectorOperator>] {
        self.as_mut().vector_sources_mut()
    }
}

impl<R, Q> InitializedOperatorB<R, Q> for Box<dyn InitializedOperatorB<R, Q>>
where
    R: Clone + 'static,
    Q: Clone + 'static,
{
    fn result_descriptor(&self) -> R {
        self.as_ref().result_descriptor()
    }
    fn query_processor(&self) -> Result<Q> {
        self.as_ref().query_processor()
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
    Vector(Box<InitializedVectorOperator>),
    Raster(Box<InitializedRasterOperator>),
}

impl Into<TypedInitializedOperator> for Box<InitializedVectorOperator> {
    fn into(self) -> TypedInitializedOperator {
        TypedInitializedOperator::Vector(self)
    }
}

impl Into<TypedInitializedOperator> for Box<InitializedRasterOperator> {
    fn into(self) -> TypedInitializedOperator {
        TypedInitializedOperator::Raster(self)
    }
}
