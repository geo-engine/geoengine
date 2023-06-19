pub use clonable_operator::{
    CloneableInitializedRasterOperator, CloneableInitializedVectorOperator, CloneablePlotOperator,
    CloneableRasterOperator, CloneableVectorOperator,
};
pub use execution_context::{
    ExecutionContext, MetaData, MetaDataProvider, MockExecutionContext, StaticMetaData,
};
pub use operator::{
    CanonicOperatorName, InitializedPlotOperator, InitializedRasterOperator,
    InitializedVectorOperator, OperatorData, OperatorName, PlotOperator, RasterOperator,
    TypedOperator, VectorOperator,
};
pub use operator_impl::{
    MultipleRasterOrSingleVectorSource, MultipleRasterSources, MultipleVectorSources, Operator,
    SingleRasterOrVectorSource, SingleRasterSource, SingleVectorMultipleRasterSources,
    SingleVectorSource, SourceOperator,
};
pub use query::{
    ChunkByteSize, MockQueryContext, QueryAbortRegistration, QueryAbortTrigger, QueryContext,
    QueryContextExtensions,
};
pub use query_processor::{
    BoxRasterQueryProcessor, PlotQueryProcessor, QueryProcessor, QueryProcessorExt,
    RasterQueryProcessor, TypedPlotQueryProcessor, TypedRasterQueryProcessor,
    TypedVectorQueryProcessor, VectorQueryProcessor,
};
pub use result_descriptor::{
    PlotResultDescriptor, RasterResultDescriptor, ResultDescriptor, TypedResultDescriptor,
    VectorColumnInfo, VectorResultDescriptor,
};
use tracing::Span;

pub use workflow_path::WorkflowOperatorPath;

pub use initialized_sources::{
    InitializedMultiRasterOrVectorOperator, InitializedMultiRasterOrVectorSource,
    InitializedSingleRasterOrVectorOperator, InitializedSingleRasterOrVectorSource,
    InitializedSingleRasterSource, InitializedSingleVectorSource, InitializedSources,
};

mod clonable_operator;
mod execution_context;
mod initialized_sources;
mod operator;
mod operator_impl;
mod query;
mod workflow_path;

#[macro_use]
mod query_processor;
mod result_descriptor;

#[macro_export]
macro_rules! call_generic_raster_processor {
    ($type_enum:expr, $function_call:expr) => {
        match $type_enum {
            geoengine_datatypes::raster::RasterDataType::U8 => {
                $crate::engine::TypedRasterQueryProcessor::U8($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::U16 => {
                $crate::engine::TypedRasterQueryProcessor::U16($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::U32 => {
                $crate::engine::TypedRasterQueryProcessor::U32($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::U64 => {
                $crate::engine::TypedRasterQueryProcessor::U64($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::I8 => {
                $crate::engine::TypedRasterQueryProcessor::I8($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::I16 => {
                $crate::engine::TypedRasterQueryProcessor::I16($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::I32 => {
                $crate::engine::TypedRasterQueryProcessor::I32($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::I64 => {
                $crate::engine::TypedRasterQueryProcessor::I64($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::F32 => {
                $crate::engine::TypedRasterQueryProcessor::F32($function_call)
            }
            geoengine_datatypes::raster::RasterDataType::F64 => {
                $crate::engine::TypedRasterQueryProcessor::F64($function_call)
            }
        }
    };
}

#[macro_export]
macro_rules! call_on_generic_raster_processor {
    ($typed_raster:expr, $processor_var:ident => $function_call:expr) => {
        match $typed_raster {
            $crate::engine::TypedRasterQueryProcessor::U8($processor_var) => $function_call,
            $crate::engine::TypedRasterQueryProcessor::U16($processor_var) => $function_call,
            $crate::engine::TypedRasterQueryProcessor::U32($processor_var) => $function_call,
            $crate::engine::TypedRasterQueryProcessor::U64($processor_var) => $function_call,
            $crate::engine::TypedRasterQueryProcessor::I8($processor_var) => $function_call,
            $crate::engine::TypedRasterQueryProcessor::I16($processor_var) => $function_call,
            $crate::engine::TypedRasterQueryProcessor::I32($processor_var) => $function_call,
            $crate::engine::TypedRasterQueryProcessor::I64($processor_var) => $function_call,
            $crate::engine::TypedRasterQueryProcessor::F32($processor_var) => $function_call,
            $crate::engine::TypedRasterQueryProcessor::F64($processor_var) => $function_call,
        }
    };
}

/// like `call_on_generic_raster_processor` but produces a `Result` which is `Err` if
/// `RasterDataType` isn't supported by GDAL
#[macro_export]
macro_rules! call_on_generic_raster_processor_gdal_types {
    ($typed_raster:expr, $processor_var:ident => $function_call:expr) => {
        match $typed_raster {
            $crate::engine::TypedRasterQueryProcessor::U8($processor_var) => Ok($function_call),
            $crate::engine::TypedRasterQueryProcessor::U16($processor_var) => Ok($function_call),
            $crate::engine::TypedRasterQueryProcessor::U32($processor_var) => Ok($function_call),
            $crate::engine::TypedRasterQueryProcessor::I16($processor_var) => Ok($function_call),
            $crate::engine::TypedRasterQueryProcessor::I32($processor_var) => Ok($function_call),
            $crate::engine::TypedRasterQueryProcessor::F32($processor_var) => Ok($function_call),
            $crate::engine::TypedRasterQueryProcessor::F64($processor_var) => Ok($function_call),
            _ => Err($crate::error::Error::GdalRasterDataTypeNotSupported),
        }
    };
}

#[macro_export]
macro_rules! call_on_generic_vector_processor {
    ($typed_vector:expr, $processor_var:ident => $function_call:expr) => {
        match $typed_vector {
            $crate::engine::TypedVectorQueryProcessor::Data($processor_var) => $function_call,
            $crate::engine::TypedVectorQueryProcessor::MultiPoint($processor_var) => $function_call,
            $crate::engine::TypedVectorQueryProcessor::MultiLineString($processor_var) => {
                $function_call
            }
            $crate::engine::TypedVectorQueryProcessor::MultiPolygon($processor_var) => {
                $function_call
            }
        }
    };
}

/// Calls a function on two `TypedRasterQueryProcessor`s by calling it on their variant combination.
/// Call via `call_on_bi_generic_raster_processor!(input_a, input_b, (processor_a, processor_b) => function)`.
#[macro_export]
macro_rules! call_on_bi_generic_raster_processor {
    (
        $input_a:expr, $input_b:expr,
        ( $processor_a:ident, $processor_b:ident ) => $function_call:expr
    ) => {
        // TODO: this should be automated, but it seems like this requires a procedural macro
        call_on_bi_generic_raster_processor!(
            @variants
            $input_a, $input_b,
            ( $processor_a, $processor_b ) => $function_call,
            (U8, U8), (U8, U16), (U8, U32), (U8, U64), (U8, I8), (U8, I16), (U8, I32), (U8, I64), (U8, F32), (U8, F64),
            (U16, U8), (U16, U16), (U16, U32), (U16, U64), (U16, I8), (U16, I16), (U16, I32), (U16, I64), (U16, F32), (U16, F64),
            (U32, U8), (U32, U16), (U32, U32), (U32, U64), (U32, I8), (U32, I16), (U32, I32), (U32, I64), (U32, F32), (U32, F64),
            (U64, U8), (U64, U16), (U64, U32), (U64, U64), (U64, I8), (U64, I16), (U64, I32), (U64, I64), (U64, F32), (U64, F64),
            (I8, U8), (I8, U16), (I8, U32), (I8, U64), (I8, I8), (I8, I16), (I8, I32), (I8, I64), (I8, F32), (I8, F64),
            (I16, U8), (I16, U16), (I16, U32), (I16, U64), (I16, I8), (I16, I16), (I16, I32), (I16, I64), (I16, F32), (I16, F64),
            (I32, U8), (I32, U16), (I32, U32), (I32, U64), (I32, I8), (I32, I16), (I32, I32), (I32, I64), (I32, F32), (I32, F64),
            (I64, U8), (I64, U16), (I64, U32), (I64, U64), (I64, I8), (I64, I16), (I64, I32), (I64, I64), (I64, F32), (I64, F64),
            (F32, U8), (F32, U16), (F32, U32), (F32, U64), (F32, I8), (F32, I16), (F32, I32), (F32, I64), (F32, F32), (F32, F64),
            (F64, U8), (F64, U16), (F64, U32), (F64, U64), (F64, I8), (F64, I16), (F64, I32), (F64, I64), (F64, F32), (F64, F64)
        )
    };

    (@variants
        $input_a:expr, $input_b:expr,
        ( $processor_a:ident, $processor_b:ident ) => $function_call:expr,
        $(($variant_a:tt,$variant_b:tt)),+
    ) => {
        match ($input_a, $input_b) {
            $(
                (
                    $crate::engine::TypedRasterQueryProcessor::$variant_a($processor_a),
                    $crate::engine::TypedRasterQueryProcessor::$variant_b($processor_b),
                ) => $function_call,
            )+
        }
    };

}

/// Shorthand type for a function that creates a `Span` for tracing
pub type CreateSpan = fn(path: &WorkflowOperatorPath, query_counter: usize) -> Span;

/// Macro for creating a span-fn for a given type, e.g. `span_fn!(MyType)`
macro_rules! span_fn {
    ($op: ty) => {
        fn span(&self) -> crate::engine::CreateSpan {
            |path, query_counter| tracing::span!(
                tracing::Level::TRACE,
                <$op>::TYPE_NAME,
                path = %path,
                query_counter = %query_counter
            )
        }
    };
}
