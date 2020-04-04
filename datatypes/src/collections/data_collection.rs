use crate::primitives::FeatureDataType;
use arrow::array::StructArray;
use std::collections::HashMap;

/// This collection contains temporal data but no geographical features.
#[derive(Debug)]
pub struct DataCollection {
    data: StructArray,
    types: HashMap<String, FeatureDataType>,
}
