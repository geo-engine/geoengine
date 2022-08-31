use serde::{Deserialize, Serialize};

use crate::error;
use crate::util::Result;
use async_trait::async_trait;
use geoengine_datatypes::dataset::DataId;

use super::{
    query_processor::{TypedRasterQueryProcessor, TypedVectorQueryProcessor},
    CloneablePlotOperator, CloneableRasterOperator, CloneableVectorOperator, ExecutionContext,
    PlotResultDescriptor, RasterResultDescriptor, TypedPlotQueryProcessor, VectorResultDescriptor,
};

pub trait OperatorData {
    /// Get the ids of all the data involoved in this operator and its sources
    fn data_ids(&self) -> Vec<DataId> {
        let mut datasets = vec![];
        self.data_ids_collect(&mut datasets);
        datasets
    }

    fn data_ids_collect(&self, data_ids: &mut Vec<DataId>);
}

/// Common methods for `RasterOperator`s
#[typetag::serde(tag = "type")]
#[async_trait]
pub trait RasterOperator:
    CloneableRasterOperator + OperatorData + Send + Sync + std::fmt::Debug
{
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
pub trait VectorOperator:
    CloneableVectorOperator + OperatorData + Send + Sync + std::fmt::Debug
{
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
pub trait PlotOperator:
    CloneablePlotOperator + OperatorData + Send + Sync + std::fmt::Debug
{
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

impl utoipa::ToSchema for TypedOperator {
    fn schema() -> utoipa::openapi::Schema {
        use utoipa::openapi::*;
        ObjectBuilder::new()
            .property(
                "type",
                ObjectBuilder::new()
                    .schema_type(SchemaType::String)
                    .enum_values(Some(vec!["Vector", "Raster", "Plot"]))
            )
            .required("type")
            .property(
                "operator",
                ObjectBuilder::new()
                    .property(
                        "type",
                        Object::with_type(SchemaType::String)
                    )
                    .required("type")
                    .property(
                        "params",
                        Object::with_type(SchemaType::Object)
                    )
                    .property(
                        "sources",
                        Object::with_type(SchemaType::Object)
                    )
            )
            .required("operator")
            .example(Some(serde_json::json!(
                {"type": "MockPointSource", "params": {"points": [{"x": 0.0, "y": 0.1}, {"x": 1.0, "y": 1.1}]}
            })))
            .description(Some("An enum to differentiate between `Operator` variants"))
            .into()
    }
}

impl TypedOperator {
    pub fn get_vector(self) -> Result<Box<dyn VectorOperator>> {
        if let TypedOperator::Vector(o) = self {
            return Ok(o);
        }
        Err(error::Error::InvalidOperatorType {
            expected: "Vector".to_owned(),
            found: self.type_name().to_owned(),
        })
    }

    pub fn get_raster(self) -> Result<Box<dyn RasterOperator>> {
        if let TypedOperator::Raster(o) = self {
            return Ok(o);
        }
        Err(error::Error::InvalidOperatorType {
            expected: "Raster".to_owned(),
            found: self.type_name().to_owned(),
        })
    }

    pub fn get_plot(self) -> Result<Box<dyn PlotOperator>> {
        if let TypedOperator::Plot(o) = self {
            return Ok(o);
        }
        Err(error::Error::InvalidOperatorType {
            expected: "Plot".to_owned(),
            found: self.type_name().to_owned(),
        })
    }

    fn type_name(&self) -> &str {
        match self {
            TypedOperator::Vector(_) => "Vector",
            TypedOperator::Raster(_) => "Raster",
            TypedOperator::Plot(_) => "Plot",
        }
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

impl OperatorData for TypedOperator {
    fn data_ids_collect(&self, data_ids: &mut Vec<DataId>) {
        match self {
            TypedOperator::Vector(v) => v.data_ids_collect(data_ids),
            TypedOperator::Raster(r) => r.data_ids_collect(data_ids),
            TypedOperator::Plot(p) => p.data_ids_collect(data_ids),
        }
    }
}

pub trait OperatorName {
    const TYPE_NAME: &'static str;
}
