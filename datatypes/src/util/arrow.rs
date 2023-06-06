use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayBuilder, ArrayRef, BooleanArray};
use arrow::datatypes::{DataType, Field};
use arrow::error::ArrowError;

/// Helper function to downcast an arrow array
///
/// The caller must be sure of its type, otherwise it panics
///
/// # Panics
/// Panics if `array` is not of type `T`
///
pub fn downcast_array<T: Any>(array: &ArrayRef) -> &T {
    array.as_any().downcast_ref().unwrap() // must obey type
}

/// Helper function to downcast an arrow array
///
/// The caller must be sure of its type, otherwise it panics
///
/// # Panics
/// Panics if `array` is not of type `T`
///
pub fn downcast_dyn_array<T: Any>(array: &dyn Array) -> &T {
    array.as_any().downcast_ref().unwrap() // must obey type
}

/// Helper function to downcast a mutable arrow array from a builder
///
/// The caller must be sure of its type, otherwise it panics
/// # Panics
/// Panics if `array` is not of type `T`
///
pub fn downcast_mut_array<T: Any>(array: &mut dyn ArrayBuilder) -> &mut T {
    array.as_any_mut().downcast_mut().unwrap() // must obey type
}

/// A trait to get information about the corresponding `arrow` type
pub trait ArrowTyped {
    type ArrowArray: Array + 'static;
    type ArrowBuilder: ArrayBuilder;

    /// Return the specific arrow data type
    fn arrow_data_type() -> DataType;

    fn arrow_list_data_type() -> DataType {
        let nullable = true; // TODO: should actually be false, but arrow's builders set it to `true` currently
        DataType::List(Arc::new(Field::new(
            "item",
            Self::arrow_data_type(),
            nullable,
        )))
    }

    /// Computes the byte size of the builder
    fn builder_byte_size(builder: &mut Self::ArrowBuilder) -> usize;

    /// Return a builder for the arrow data type
    fn arrow_builder(capacity: usize) -> Self::ArrowBuilder;

    /// Create a new array by concatenating the inputs
    fn concat(a: &Self::ArrowArray, b: &Self::ArrowArray) -> Result<Self::ArrowArray, ArrowError>;

    /// Filter an array by using a boolean filter array
    fn filter(
        data_array: &Self::ArrowArray,
        filter_array: &BooleanArray,
    ) -> Result<Self::ArrowArray, ArrowError>;

    fn from_vec(data: Vec<Self>) -> Result<Self::ArrowArray, ArrowError>
    where
        Self: Sized;
}

/// Read record batches from an Arrow IPC file
pub fn arrow_ipc_file_to_record_batches(
    bytes: &[u8],
) -> Result<Vec<arrow::record_batch::RecordBatch>, arrow::error::ArrowError> {
    let reader = arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None)?;

    let mut record_batches = vec![];

    for data in reader {
        let data = data?;

        record_batches.push(data);
    }

    Ok(record_batches)
}
