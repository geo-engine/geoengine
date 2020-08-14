use super::query_processor::{TypedRasterQueryProcessor, TypedVectorQueryProcessor};
use crate::util::Result;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::{projection::ProjectionOption, raster::RasterDataType};
use serde::{Deserialize, Serialize};

/// Common methods for `Operator`s
pub trait Operator: std::fmt::Debug + Send + Sync + CloneableOperator {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OperatorImpl<P> {
    pub params: P,
    pub raster_sources: Vec<Box<dyn RasterOperator>>,
    pub vector_sources: Vec<Box<dyn VectorOperator>>,
}

impl<P> Operator for OperatorImpl<P> where P: std::fmt::Debug + Send + Sync + Clone + 'static {}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct SourceOperatorImpl<P> {
    pub params: P,
}

impl<P> Operator for SourceOperatorImpl<P> where P: std::fmt::Debug + Send + Sync + Clone + 'static {}

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

pub type ExecutionContext = u32;

pub trait InitializedOperator {
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

pub struct InitilaizedOperatorImpl<Parameters, ResultDescriptor> {
    pub params: Parameters,
    pub raster_sources: Vec<Box<dyn InitializedRasterOperator>>,
    pub vector_sources: Vec<Box<dyn InitializedVectorOperator>>,
    pub context: ExecutionContext,
    pub result_descriptor: ResultDescriptor,
}

impl<P, R> InitilaizedOperatorImpl<P, R> {
    pub fn new(
        params: P,
        context: ExecutionContext,
        result_descriptor: R,
        raster_sources: Vec<Box<dyn InitializedRasterOperator>>,
        vector_sources: Vec<Box<dyn InitializedVectorOperator>>,
    ) -> Self {
        Self {
            params,
            raster_sources,
            vector_sources,
            context,
            result_descriptor,
        }
    }

    pub fn create<F>(
        params: P,
        context: ExecutionContext,
        result_descriptor_fn: F,
        raster_sources_not_init: Vec<Box<dyn RasterOperator>>,
        vector_sources_not_init: Vec<Box<dyn VectorOperator>>,
    ) -> Result<Self>
    where
        F: Fn(
            &P,
            &ExecutionContext,
            &[Box<dyn InitializedRasterOperator>],
            &[Box<dyn InitializedVectorOperator>],
        ) -> Result<R>,
    {
        let raster_sources = raster_sources_not_init
            .into_iter()
            .map(|o| o.initialized_operator(context))
            .collect::<Result<Vec<Box<dyn InitializedRasterOperator>>>>()?;
        let vector_sources = vector_sources_not_init
            .into_iter()
            .map(|o| o.into_initialized_operator(context))
            .collect::<Result<Vec<Box<dyn InitializedVectorOperator>>>>()?;
        let result_descriptor = result_descriptor_fn(
            &params,
            &context,
            raster_sources.as_slice(),
            vector_sources.as_slice(),
        )?;

        Ok(Self::new(
            params,
            context,
            result_descriptor,
            raster_sources,
            vector_sources,
        ))
    }
}

impl<P, R> InitializedOperator for InitilaizedOperatorImpl<P, R>
where
    P: std::fmt::Debug + Clone,
    R: std::fmt::Debug + Clone,
{
    fn execution_context(&self) -> &ExecutionContext {
        &self.context
    }
    fn raster_sources(&self) -> &[Box<dyn InitializedRasterOperator>] {
        self.raster_sources.as_slice()
    }
    fn vector_sources(&self) -> &[Box<dyn InitializedVectorOperator>] {
        self.vector_sources.as_slice()
    }
    fn raster_sources_mut(&mut self) -> &mut [Box<dyn InitializedRasterOperator>] {
        self.raster_sources.as_mut_slice()
    }
    fn vector_sources_mut(&mut self) -> &mut [Box<dyn InitializedVectorOperator>] {
        self.vector_sources.as_mut_slice()
    }
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct RasterResultDescriptor {
    pub data_type: RasterDataType,
    pub projection: ProjectionOption,
}

impl RasterResultDescriptor {
    pub fn map<F>(self, f: F) -> Self
    where
        F: Fn(Self) -> Self,
    {
        f(self)
    }

    pub fn map_data_type<F>(mut self, f: F) -> Self
    where
        F: Fn(RasterDataType) -> RasterDataType,
    {
        self.data_type = f(self.data_type);
        self
    }

    pub fn map_projection<F>(mut self, f: F) -> Self
    where
        F: Fn(ProjectionOption) -> ProjectionOption,
    {
        self.projection = f(self.projection);
        self
    }
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct VectorResultDescriptor {
    pub data_type: VectorDataType,
    pub projection: ProjectionOption,
}

impl VectorResultDescriptor {
    pub fn map<F>(self, f: F) -> Self
    where
        F: Fn(Self) -> Self,
    {
        f(self)
    }

    pub fn map_data_type<F>(mut self, f: F) -> Self
    where
        F: Fn(VectorDataType) -> VectorDataType,
    {
        self.data_type = f(self.data_type);
        self
    }

    pub fn map_projection<F>(mut self, f: F) -> Self
    where
        F: Fn(ProjectionOption) -> ProjectionOption,
    {
        self.projection = f(self.projection);
        self
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

/// Helper trait for making boxed `Operator`s cloneable
pub trait CloneableOperator {
    fn clone_boxed(&self) -> Box<dyn Operator>;
}

/// Helper trait for making boxed `RasterOperator`s cloneable
pub trait CloneableRasterOperator {
    fn clone_boxed_raster(&self) -> Box<dyn RasterOperator>;
}

/// Helper trait for making boxed `VectorOperator`s cloneable
pub trait CloneableVectorOperator {
    fn clone_boxed_vector(&self) -> Box<dyn VectorOperator>;
}

impl<T> CloneableOperator for T
where
    T: 'static + Operator + Clone,
{
    fn clone_boxed(&self) -> Box<dyn Operator> {
        Box::new(self.clone())
    }
}

impl<T> CloneableRasterOperator for T
where
    T: 'static + RasterOperator + Clone,
{
    fn clone_boxed_raster(&self) -> Box<dyn RasterOperator> {
        Box::new(self.clone())
    }
}

impl<T> CloneableVectorOperator for T
where
    T: 'static + VectorOperator + Clone,
{
    fn clone_boxed_vector(&self) -> Box<dyn VectorOperator> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Operator> {
    fn clone(&self) -> Box<dyn Operator> {
        self.clone_boxed()
    }
}

impl Clone for Box<dyn RasterOperator> {
    fn clone(&self) -> Box<dyn RasterOperator> {
        self.clone_boxed_raster()
    }
}

impl Clone for Box<dyn VectorOperator> {
    fn clone(&self) -> Box<dyn VectorOperator> {
        self.clone_boxed_vector()
    }
}
