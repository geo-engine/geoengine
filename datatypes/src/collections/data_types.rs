use serde::{Deserialize, Serialize};

/// An enum that contains all possible vector data types
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Deserialize, Serialize, Copy, Clone)]
#[allow(clippy::pub_enum_variant_names)]
pub enum VectorDataType {
    Data,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
}
