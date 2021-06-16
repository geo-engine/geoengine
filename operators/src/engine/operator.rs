use serde::{Deserialize, Serialize};

use crate::error;
use crate::util::Result;
use async_trait::async_trait;

use super::{
    query_processor::{TypedRasterQueryProcessor, TypedVectorQueryProcessor},
    CloneablePlotOperator, CloneableRasterOperator, CloneableVectorOperator, ExecutionContext,
    PlotResultDescriptor, RasterResultDescriptor, TypedPlotQueryProcessor, VectorResultDescriptor,
};

/// Common methods for `RasterOperator`s
#[typetag::serde(tag = "type")]
#[async_trait]
pub trait RasterOperator: CloneableRasterOperator + Send + Sync + std::fmt::Debug {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
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
#[async_trait]
pub trait VectorOperator: CloneableVectorOperator + Send + Sync + std::fmt::Debug {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>>;

    /// Wrap a box around a `VectorOperator`
    fn boxed(self) -> Box<dyn VectorOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

/// Common methods for `PlotOperator`s
#[typetag::serde(tag = "type")]
#[async_trait]
pub trait PlotOperator: CloneablePlotOperator + Send + Sync + std::fmt::Debug {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedPlotOperator>>;

    /// Wrap a box around a `PlotOperator`
    fn boxed(self) -> Box<dyn PlotOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

// TODO: rename `query_processor` to `xyz_query_processor`

pub trait InitializedRasterOperator: Send + Sync {
    /// Get the result descriptor of the `Operator`
    fn result_descriptor(&self) -> &RasterResultDescriptor;

    /// Instantiate a `TypedVectorQueryProcessor` from a `RasterOperator`
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor>;

    /// Wrap a box around a `RasterOperator`
    fn boxed(self) -> Box<dyn InitializedRasterOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

pub trait InitializedVectorOperator: Send + Sync {
    /// Get the result descriptor of the `Operator`
    fn result_descriptor(&self) -> &VectorResultDescriptor;

    /// Instantiate a `TypedVectorQueryProcessor` from a `RasterOperator`
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor>;

    /// Wrap a box around a `RasterOperator`
    fn boxed(self) -> Box<dyn InitializedVectorOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

pub trait InitializedPlotOperator: Send + Sync {
    /// Get the result descriptor of the `Operator`
    fn result_descriptor(&self) -> &PlotResultDescriptor;

    /// Instantiate a `TypedVectorQueryProcessor` from a `RasterOperator`
    fn query_processor(&self) -> Result<TypedPlotQueryProcessor>;

    /// Wrap a box around a `RasterOperator`
    fn boxed(self) -> Box<dyn InitializedPlotOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

impl InitializedRasterOperator for Box<dyn InitializedRasterOperator> {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        self.as_ref().result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        self.as_ref().query_processor()
    }
}

impl InitializedVectorOperator for Box<dyn InitializedVectorOperator> {
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        self.as_ref().result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        self.as_ref().query_processor()
    }
}

impl InitializedPlotOperator for Box<dyn InitializedPlotOperator> {
    fn result_descriptor(&self) -> &PlotResultDescriptor {
        self.as_ref().result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        self.as_ref().query_processor()
    }
}

/// An enum to differentiate between `Operator` variants
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "operator")]
pub enum TypedOperator {
    Vector(Box<dyn VectorOperator>),
    Raster(Box<dyn RasterOperator>),
    Plot(Box<dyn PlotOperator>),
}

impl TypedOperator {
    pub fn get_vector(self) -> Result<Box<dyn VectorOperator>> {
        if let TypedOperator::Vector(o) = self {
            return Ok(o);
        }
        Err(error::Error::InvalidOperatorType)
    }

    pub fn get_raster(self) -> Result<Box<dyn RasterOperator>> {
        if let TypedOperator::Raster(o) = self {
            return Ok(o);
        }
        Err(error::Error::InvalidOperatorType)
    }

    pub fn get_plot(self) -> Result<Box<dyn PlotOperator>> {
        if let TypedOperator::Plot(o) = self {
            return Ok(o);
        }
        Err(error::Error::InvalidOperatorType)
    }
}

impl From<Box<dyn VectorOperator>> for TypedOperator {
    fn from(operator: Box<dyn VectorOperator>) -> Self {
        Self::Vector(operator)
    }
}

impl From<Box<dyn RasterOperator>> for TypedOperator {
    fn from(operator: Box<dyn RasterOperator>) -> Self {
        Self::Raster(operator)
    }
}

impl From<Box<dyn PlotOperator>> for TypedOperator {
    fn from(operator: Box<dyn PlotOperator>) -> Self {
        Self::Plot(operator)
    }
}

/// An enum to differentiate between `InitializedOperator` variants
pub enum TypedInitializedOperator {
    Vector(Box<dyn InitializedVectorOperator>),
    Raster(Box<dyn InitializedRasterOperator>),
    Plot(Box<dyn InitializedPlotOperator>),
}

impl From<Box<dyn InitializedVectorOperator>> for TypedInitializedOperator {
    fn from(operator: Box<dyn InitializedVectorOperator>) -> Self {
        TypedInitializedOperator::Vector(operator)
    }
}

impl From<Box<dyn InitializedRasterOperator>> for TypedInitializedOperator {
    fn from(operator: Box<dyn InitializedRasterOperator>) -> Self {
        TypedInitializedOperator::Raster(operator)
    }
}

impl From<Box<dyn InitializedPlotOperator>> for TypedInitializedOperator {
    fn from(operator: Box<dyn InitializedPlotOperator>) -> Self {
        TypedInitializedOperator::Plot(operator)
    }
}

#[macro_export]
macro_rules! call_on_typed_operator {
    ($typed_operator:expr, $operator_var:ident => $function_call:expr) => {
        match $typed_operator {
            $crate::engine::TypedOperator::Vector($operator_var) => $function_call,
            $crate::engine::TypedOperator::Raster($operator_var) => $function_call,
            $crate::engine::TypedOperator::Plot($operator_var) => $function_call,
        }
    };
}
