mod clonable_operator;
mod operator;
mod operator_impl;
mod query;
mod query_processor;
mod result_descriptor;

pub use operator::{
    ExecutionContext, InitializedOperator, InitializedOperatorB, InitializedRasterOperator,
    InitializedVectorOperator, RasterOperator, TypedOperator, VectorOperator,
};

pub use clonable_operator::{
    CloneableInitializedOperator, CloneableInitializedRasterOperator,
    CloneableInitializedVectorOperator, CloneableRasterOperator, CloneableVectorOperator,
};
pub use operator_impl::{InitializedOperatorImpl, Operator, SourceOperator};
pub use query::{QueryContext, QueryRectangle};
pub use query_processor::{
    QueryProcessor, RasterQueryProcessor, TypedRasterQueryProcessor, TypedVectorQueryProcessor,
    VectorQueryProcessor,
};
pub use result_descriptor::{RasterResultDescriptor, ResultDescriptor, VectorResultDescriptor};
