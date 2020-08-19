use crate::util::arrow::ArrowTyped;
use arrow::array::{Array, ArrayBuilder, ArrayDataRef, ArrayEqual, ArrayRef, JsonEqual};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use geojson::Geometry;
use serde_json::Value;
use std::any::Any;

/// A zero-sized placeholder struct for situations where a geometry is necessary.
/// Currently, this is only required for `FeatureCollection` implementations.
#[derive(Clone, Debug)]
pub struct NoGeometry;

impl ArrowTyped for NoGeometry {
    type ArrowArray = NoArrowArray;
    type ArrowBuilder = NoArrowArray;

    fn arrow_data_type() -> DataType {
        unreachable!("There is no data type since there is no geometry")
    }

    fn arrow_builder(_capacity: usize) -> Self::ArrowBuilder {
        NoArrowArray
    }
}

impl Into<geojson::Geometry> for NoGeometry {
    fn into(self) -> Geometry {
        unreachable!("There is no geometry since there is no geometry")
    }
}

/// Zero-sized `arrow::array::Array` replacement
#[derive(Debug)]
pub struct NoArrowArray;

impl Array for NoArrowArray {
    fn as_any(&self) -> &dyn Any {
        unreachable!("There is no implementation since there is no geometry")
    }

    fn data(&self) -> ArrayDataRef {
        unreachable!("There is no implementation since there is no geometry")
    }

    fn data_ref(&self) -> &ArrayDataRef {
        unreachable!("There is no implementation since there is no geometry")
    }
}

impl ArrayBuilder for NoArrowArray {
    fn len(&self) -> usize {
        unreachable!("There is no implementation since there is no geometry")
    }

    fn is_empty(&self) -> bool {
        unreachable!("There is no implementation since there is no geometry")
    }

    fn append_data(&mut self, _data: &[ArrayDataRef]) -> Result<(), ArrowError> {
        unreachable!("There is no implementation since there is no geometry")
    }

    fn data_type(&self) -> DataType {
        unreachable!("There is no implementation since there is no geometry")
    }

    fn finish(&mut self) -> ArrayRef {
        unreachable!("There is no implementation since there is no geometry")
    }

    fn as_any(&self) -> &dyn Any {
        unreachable!("There is no implementation since there is no geometry")
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        unreachable!("There is no implementation since there is no geometry")
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        unreachable!("There is no implementation since there is no geometry")
    }
}

impl JsonEqual for NoArrowArray {
    fn equals_json(&self, _json: &[&Value]) -> bool {
        unreachable!("There is no implementation since there is no geometry")
    }
}

impl ArrayEqual for NoArrowArray {
    fn equals(&self, _other: &dyn Array) -> bool {
        unreachable!("There is no implementation since there is no geometry")
    }

    fn range_equals(
        &self,
        _other: &dyn Array,
        _start_idx: usize,
        _end_idx: usize,
        _other_start_idx: usize,
    ) -> bool {
        unreachable!("There is no implementation since there is no geometry")
    }
}
