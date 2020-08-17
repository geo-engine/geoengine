mod operator;
mod operator_impl;
mod query;
mod query_processor;
mod result_descriptor;

pub use operator::{
    ExecutionContext, InitializedOperator, InitializedRasterOperator, InitializedVectorOperator,
    Operator, RasterOperator, TypedOperator, VectorOperator,
};

pub use operator_impl::{InitilaizedOperatorImpl, OperatorImpl, SourceOperatorImpl};
pub use query::{QueryContext, QueryRectangle};
pub use query_processor::{
    QueryProcessor, RasterQueryProcessor, TypedRasterQueryProcessor, TypedVectorQueryProcessor,
    VectorQueryProcessor,
};
pub use result_descriptor::{RasterResultDescriptor, VectorResultDescriptor};
