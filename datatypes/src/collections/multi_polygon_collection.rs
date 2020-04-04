use crate::primitives::FeatureDataType;
use arrow::array::StructArray;
use std::collections::HashMap;

/// This collection contains temporal multi-polygons miscellaneous data.
#[derive(Debug)]
pub struct MultiPolygonCollection {
    data: StructArray,
    types: HashMap<String, FeatureDataType>,
}
