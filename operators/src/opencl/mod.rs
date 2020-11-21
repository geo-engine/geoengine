#[macro_use]
mod typed_slice;

mod cl_program;

pub use cl_program::{
    CLProgram, CLProgramRunnable, ColumnArgument, CompiledCLProgram, IterationType, RasterArgument,
    VectorArgument,
};
pub(self) use typed_slice::{
    GenericSliceType, SliceDataType, SliceOutputBuffer, TypedSliceMut, TypedSliceRef,
};
