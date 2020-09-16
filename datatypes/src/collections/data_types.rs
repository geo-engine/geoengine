use crate::collections::{
    DataCollection, MultiLineStringCollection, MultiPointCollection, MultiPolygonCollection,
};
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

pub enum TypedFeatureCollection {
    Data(DataCollection),
    MultiPoint(MultiPointCollection),
    MultiLineString(MultiLineStringCollection),
    MultiPolygon(MultiPolygonCollection),
}

impl TypedFeatureCollection {
    pub fn vector_data_type(&self) -> VectorDataType {
        match self {
            TypedFeatureCollection::Data(_) => VectorDataType::Data,
            TypedFeatureCollection::MultiPoint(_) => VectorDataType::MultiPoint,
            TypedFeatureCollection::MultiLineString(_) => VectorDataType::MultiLineString,
            TypedFeatureCollection::MultiPolygon(_) => VectorDataType::MultiPolygon,
        }
    }

    // TODO: other types
    pub fn get_points(self) -> Option<MultiPointCollection> {
        if let TypedFeatureCollection::MultiPoint(points) = self {
            return Some(points);
        }
        None
    }
}
