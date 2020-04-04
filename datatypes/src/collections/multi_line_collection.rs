use crate::primitives::FeatureDataType;
use arrow::array::StructArray;
use std::collections::HashMap;

/// This collection contains temporal multi-lines and miscellaneous data.
#[derive(Debug)]
pub struct MultiLineCollection {
    data: StructArray,
    types: HashMap<String, FeatureDataType>,
}
