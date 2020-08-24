use super::{
    query_processor::{TypedRasterQueryProcessor, TypedVectorQueryProcessor},
    CloneableRasterOperator, CloneableVectorOperator, RasterResultDescriptor, ResultDescriptor,
    VectorResultDescriptor,
};
use crate::util::Result;

use serde::{Deserialize, Serialize};

/// Common methods for `RasterOperator`s
#[typetag::serde(tag = "type")]
pub trait RasterOperator: CloneableRasterOperator + Send + Sync + std::fmt::Debug {
    fn initialize(
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
pub trait VectorOperator: CloneableVectorOperator + Send + Sync + std::fmt::Debug {
    fn initialize(
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
    type Descriptor: ResultDescriptor + Clone;
    fn execution_context(&self) -> &ExecutionContext;

    /// Get the result descriptor of the `Operator`
    fn result_descriptor(&self) -> Self::Descriptor;

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

pub trait InitializedOperatorB<R, Q>: InitializedOperator<Descriptor = R> + Send + Sync
where
    R: ResultDescriptor + std::clone::Clone,
{
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

impl<R> InitializedOperator for Box<dyn InitializedOperator<Descriptor = R>>
where
    R: ResultDescriptor + std::clone::Clone,
{
    type Descriptor = R;

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
    fn result_descriptor(&self) -> Self::Descriptor {
        todo!()
    }
}

impl<R, Q> InitializedOperator for Box<dyn InitializedOperatorB<R, Q>>
where
    R: Clone + 'static + ResultDescriptor,
    Q: Clone + 'static,
{
    type Descriptor = R;
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
    fn result_descriptor(&self) -> Self::Descriptor {
        self.as_ref().result_descriptor()
    }
}

impl<R, Q> InitializedOperatorB<R, Q> for Box<dyn InitializedOperatorB<R, Q>>
where
    R: Clone + 'static + ResultDescriptor,
    Q: Clone + 'static,
{
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
