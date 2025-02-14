use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tracing::debug;

use crate::util::Result;
use crate::{error, optimization::OptimizationError};
use async_trait::async_trait;
use geoengine_datatypes::{dataset::NamedData, primitives::SpatialResolution, util::ByteSize};

use super::{
    query_processor::{TypedRasterQueryProcessor, TypedVectorQueryProcessor},
    CloneablePlotOperator, CloneableRasterOperator, CloneableVectorOperator, CreateSpan,
    ExecutionContext, PlotResultDescriptor, RasterResultDescriptor, TypedPlotQueryProcessor,
    VectorResultDescriptor, WorkflowOperatorPath,
};

pub trait OperatorData {
    /// Get the ids of all the data involoved in this operator and its sources
    fn data_names(&self) -> Vec<NamedData> {
        let mut datasets = vec![];
        self.data_names_collect(&mut datasets);
        datasets
    }

    fn data_names_collect(&self, data_names: &mut Vec<NamedData>);
}

/// Common methods for `RasterOperator`s
#[typetag::serde(tag = "type")]
#[async_trait]
pub trait RasterOperator:
    CloneableRasterOperator + OperatorData + Send + Sync + std::fmt::Debug
{
    /// Internal initialization logic of the operator
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>>;

    /// Initialize the operator
    ///
    /// This method should not be overriden because it handles wrapping the operator using the
    /// execution context. Instead, `_initialize` should be implemented.
    async fn initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let span = self.span();
        debug!("Initialize {}, path: {}", self.typetag_name(), &path);
        #[allow(clippy::used_underscore_items)] // TODO: maybe rename?
        let op = self._initialize(path.clone(), context).await?;

        Ok(context.wrap_initialized_raster_operator(op, span))
    }

    /// Wrap a box around a `RasterOperator`
    fn boxed(self) -> Box<dyn RasterOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }

    fn span(&self) -> CreateSpan;
}

/// Common methods for `VectorOperator`s
#[typetag::serde(tag = "type")]
#[async_trait]
pub trait VectorOperator:
    CloneableVectorOperator + OperatorData + Send + Sync + std::fmt::Debug
{
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>>;

    #[allow(clippy::used_underscore_items)]
    async fn initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        let span = self.span();
        debug!("Initialize {}, path: {}", self.typetag_name(), &path);
        let op = self._initialize(path.clone(), context).await?;
        Ok(context.wrap_initialized_vector_operator(op, span))
    }

    /// Wrap a box around a `VectorOperator`
    fn boxed(self) -> Box<dyn VectorOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }

    fn span(&self) -> CreateSpan;
}

/// Common methods for `PlotOperator`s
#[typetag::serde(tag = "type")]
#[async_trait]
pub trait PlotOperator:
    CloneablePlotOperator + OperatorData + Send + Sync + std::fmt::Debug
{
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedPlotOperator>>;

    async fn initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedPlotOperator>> {
        let span = self.span();
        debug!("Initialize {}, path: {}", self.typetag_name(), &path);
        #[allow(clippy::used_underscore_items)] // TODO: maybe rename?
        let op = self._initialize(path.clone(), context).await?;
        Ok(context.wrap_initialized_plot_operator(op, span))
    }

    /// Wrap a box around a `PlotOperator`
    fn boxed(self) -> Box<dyn PlotOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }

    fn span(&self) -> CreateSpan;
}

// TODO: implement a derive macro for common fields of operators: name, path, data, result_descriptor and automatically implement common trait functions
#[async_trait]
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

    /// Get a canonic representation of the operator and its sources
    fn canonic_name(&self) -> CanonicOperatorName;

    /// Get the unique name of the operator
    fn name(&self) -> &'static str;

    // Get the path of the operator in the workflow
    fn path(&self) -> WorkflowOperatorPath;

    /// Return the name of the data loaded by the operator (if any)
    fn data(&self) -> Option<String> {
        None
    }

    /// Optimize the operator graph for a given resolution
    fn optimize(
        &self,
        resolution: SpatialResolution,
    ) -> Result<Box<dyn RasterOperator>, OptimizationError>;

    /// optimize the operator graph and reinitialize it
    async fn optimize_and_reinitialize(
        &self,
        resolution: SpatialResolution,
        exe_ctx: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let optimized = self
            .optimize(resolution)
            .context(crate::error::Optimization)?;
        optimized
            .initialize(WorkflowOperatorPath::initialize_root(), exe_ctx)
            .await
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

    /// Get a canonic representation of the operator and its sources.
    /// This only includes *logical* operators, not wrappers
    fn canonic_name(&self) -> CanonicOperatorName;

    /// Get the unique name of the operator
    fn name(&self) -> &'static str;

    // Get the path of the operator in the workflow
    fn path(&self) -> WorkflowOperatorPath;

    /// Return the name of the data loaded by the operator (if any)
    fn data(&self) -> Option<String> {
        None
    }

    /// Optimize the operator graph for a given resolution
    fn optimize(
        &self,
        resolution: SpatialResolution,
    ) -> Result<Box<dyn VectorOperator>, OptimizationError>;
}

/// A canonic name for an operator and its sources
/// We use a byte representation of the operator json
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CanonicOperatorName(Vec<u8>);

impl CanonicOperatorName {
    pub fn new<T: Serialize>(value: &T) -> Result<Self> {
        Ok(CanonicOperatorName(serde_json::to_vec(&value)?))
    }

    ///
    /// # Panics
    ///
    /// if the value cannot be serialized as json
    ///
    pub fn new_unchecked<T: Serialize>(value: &T) -> Self {
        CanonicOperatorName(serde_json::to_vec(&value).expect("it should be checked by the caller"))
    }
}

impl ByteSize for CanonicOperatorName {
    fn heap_byte_size(&self) -> usize {
        self.0.heap_byte_size()
    }
}

impl<T> From<&T> for CanonicOperatorName
where
    T: Serialize,
{
    fn from(value: &T) -> Self {
        CanonicOperatorName::new_unchecked(value)
    }
}

impl std::fmt::Display for CanonicOperatorName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = String::from_utf8_lossy(&self.0);
        write!(f, "{s}")
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

    /// Get a canonic representation of the operator and its sources
    fn canonic_name(&self) -> CanonicOperatorName;

    /// Optimize the operator graph for a given resolution
    fn optimize(
        &self,
        resolution: SpatialResolution,
    ) -> Result<Box<dyn PlotOperator>, OptimizationError>;
}

impl InitializedRasterOperator for Box<dyn InitializedRasterOperator> {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        self.as_ref().result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        self.as_ref().query_processor()
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.as_ref().canonic_name()
    }

    fn name(&self) -> &'static str {
        self.as_ref().name()
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.as_ref().path()
    }

    fn data(&self) -> Option<String> {
        self.as_ref().data()
    }

    fn optimize(
        &self,
        resolution: SpatialResolution,
    ) -> Result<Box<dyn RasterOperator>, OptimizationError> {
        self.as_ref().optimize(resolution)
    }
}

impl InitializedVectorOperator for Box<dyn InitializedVectorOperator> {
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        self.as_ref().result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        self.as_ref().query_processor()
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.as_ref().canonic_name()
    }

    fn name(&self) -> &'static str {
        self.as_ref().name()
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.as_ref().path()
    }

    fn data(&self) -> Option<String> {
        self.as_ref().data()
    }

    fn optimize(
        &self,
        resolution: SpatialResolution,
    ) -> Result<Box<dyn VectorOperator>, OptimizationError> {
        self.as_ref().optimize(resolution)
    }
}

impl InitializedPlotOperator for Box<dyn InitializedPlotOperator> {
    fn result_descriptor(&self) -> &PlotResultDescriptor {
        self.as_ref().result_descriptor()
    }

    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        self.as_ref().query_processor()
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.as_ref().canonic_name()
    }

    fn optimize(
        &self,
        resolution: SpatialResolution,
    ) -> Result<Box<dyn PlotOperator>, OptimizationError> {
        self.as_ref().optimize(resolution)
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
    fn data_names_collect(&self, data_ids: &mut Vec<NamedData>) {
        match self {
            TypedOperator::Vector(v) => v.data_names_collect(data_ids),
            TypedOperator::Raster(r) => r.data_names_collect(data_ids),
            TypedOperator::Plot(p) => p.data_names_collect(data_ids),
        }
    }
}

pub trait OperatorName {
    const TYPE_NAME: &'static str;
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn op_name_byte_size() {
        let op = CanonicOperatorName::new_unchecked(&json!({"foo": "bar"}));
        assert_eq!(op.byte_size(), 37);

        let op = CanonicOperatorName::new_unchecked(&json!({"foo": {"bar": [1,2,3]}}));
        assert_eq!(op.byte_size(), 47);
    }
}
