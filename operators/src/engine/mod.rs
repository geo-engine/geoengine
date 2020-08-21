mod clonable_operator;
mod operator;
mod operator_impl;
mod query;
mod query_processor;
mod result_descriptor;

pub use operator::{
    ExecutionContext, InitializedOperator, InitializedRasterOperator, InitializedVectorOperator,
    Operator, RasterOperator, TypedOperator, VectorOperator,
};

pub use clonable_operator::{
    CloneableInitializedOperator, CloneableInitializedRasterOperator,
    CloneableInitializedVectorOperator, CloneableOperator, CloneableRasterOperator,
    CloneableVectorOperator,
};
pub use operator_impl::{InitializedOperatorImpl, OperatorImpl, SourceOperatorImpl};
pub use query::{QueryContext, QueryRectangle};
pub use query_processor::{
    QueryProcessor, RasterQueryProcessor, TypedRasterQueryProcessor, TypedVectorQueryProcessor,
    VectorQueryProcessor,
};
pub use result_descriptor::{RasterResultDescriptor, ResultDescriptor, VectorResultDescriptor};

macro_rules! call_generic_raster_processor {
    ($type_enum:expr, $function_call:expr) => {
        match $type_enum {
            geoengine_datatypes::raster::RasterDataType::U8 => {
                crate::engine::TypedRasterQueryProcessor::U8($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::U16 => {
                crate::engine::TypedRasterQueryProcessor::U16($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::U32 => {
                crate::engine::TypedRasterQueryProcessor::U32($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::U64 => {
                crate::engine::TypedRasterQueryProcessor::U64($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::I8 => {
                crate::engine::TypedRasterQueryProcessor::I8($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::I16 => {
                crate::engine::TypedRasterQueryProcessor::I16($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::I32 => {
                crate::engine::TypedRasterQueryProcessor::I32($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::I64 => {
                crate::engine::TypedRasterQueryProcessor::I64($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::F32 => {
                crate::engine::TypedRasterQueryProcessor::F32($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::F64 => {
                crate::engine::TypedRasterQueryProcessor::F64($function_call)
            }
        }
    };
}
