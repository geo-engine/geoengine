use crate::collections::VectorDataType;
use crate::primitives::{BoundingBox2D, Geometry, GeometryRef};
use crate::util::arrow::ArrowTyped;
use arrow::array::{
    Array, ArrayBuilder, ArrayDataRef, ArrayEqual, ArrayRef, BooleanArray, JsonEqual,
};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::any::Any;

/// A zero-sized placeholder struct for situations where a geometry is necessary.
/// Currently, this is only required for `FeatureCollection` implementations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NoGeometry;

impl Geometry for NoGeometry {
    const IS_GEOMETRY: bool = false;
    const DATA_TYPE: VectorDataType = VectorDataType::Data;

    fn intersects_bbox(&self, _bbox: BoundingBox2D) -> bool {
        true
    }
}

impl GeometryRef for NoGeometry {}

impl ArrowTyped for NoGeometry {
    type ArrowArray = NoArrowArray;
    type ArrowBuilder = NoArrowArray;

    fn arrow_data_type() -> DataType {
        unreachable!("There is no data type since there is no geometry")
    }

    fn arrow_builder(_capacity: usize) -> Self::ArrowBuilder {
        NoArrowArray
    }

    fn concat(
        _a: &Self::ArrowArray,
        _b: &Self::ArrowArray,
    ) -> Result<Self::ArrowArray, ArrowError> {
        unreachable!("There is no concat since there is no geometry")
    }

    fn filter(
        _data_array: &Self::ArrowArray,
        _filter_array: &BooleanArray,
    ) -> Result<Self::ArrowArray, ArrowError> {
        unreachable!("There is no filter since there is no geometry")
    }

    fn from_vec(_data: Vec<Self>) -> Result<Self::ArrowArray, ArrowError>
    where
        Self: Sized,
    {
        unreachable!("There is no from since there is no geometry")
    }
}

impl Into<geojson::Geometry> for NoGeometry {
    fn into(self) -> geojson::Geometry {
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
