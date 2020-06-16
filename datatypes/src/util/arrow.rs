use std::any::Any;

use arrow::array::{Array, ArrayBuilder, ArrayRef};
use arrow::datatypes::DataType;

/// Helper function to downcast an arrow array
///
/// The caller must be sure of its type, otherwise it panics
pub fn downcast_array<T: Any>(array: &ArrayRef) -> &T {
    array.as_any().downcast_ref().unwrap() // must obey type
}

/// Helper function to downcast an arrow array
///
/// The caller must be sure of its type, otherwise it panics
pub fn downcast_dyn_array<T: Any>(array: &dyn Array) -> &T {
    array.as_any().downcast_ref().unwrap() // must obey type
}

/// Helper function to downcast a mutable arrow array from a builder
///
/// The caller must be sure of its type, otherwise it panics
pub fn downcast_mut_array<T: Any>(array: &mut dyn ArrayBuilder) -> &mut T {
    array.as_any_mut().downcast_mut().unwrap() // must obey type
}

/// A trait to get information about the corresponding `arrow` type
pub trait ArrowTyped {
    type ArrowArray: Array;
    type ArrowBuilder: ArrayBuilder;

    fn arrow_data_type() -> DataType;

    fn arrow_builder(capacity: usize) -> Self::ArrowBuilder;
}
