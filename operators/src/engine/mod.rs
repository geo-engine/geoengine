mod operator;
mod query;
mod query_processor;
mod result_descriptor;

pub use operator::{
    ExecutionContext, InitializedOperator, InitializedRasterOperator, InitializedVectorOperator,
    InitilaizedOperatorImpl, Operator, OperatorImpl, RasterOperator, SourceOperatorImpl,
    TypedOperator, VectorOperator,
};

pub use query::{QueryContext, QueryRectangle};
pub use query_processor::{
    QueryProcessor, RasterQueryProcessor, TypedRasterQueryProcessor, TypedVectorQueryProcessor,
    VectorQueryProcessor,
};
pub use result_descriptor::{RasterResultDescriptor, VectorResultDescriptor};
