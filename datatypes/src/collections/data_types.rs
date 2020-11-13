use crate::collections::{
    DataCollection, MultiLineStringCollection, MultiPointCollection, MultiPolygonCollection,
};
use crate::primitives::Coordinate2D;
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

#[derive(Debug)]
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

    pub fn get_points(self) -> Option<MultiPointCollection> {
        if let TypedFeatureCollection::MultiPoint(points) = self {
            return Some(points);
        }
        None
    }

    pub fn get_lines(self) -> Option<MultiLineStringCollection> {
        if let TypedFeatureCollection::MultiLineString(lines) = self {
            return Some(lines);
        }
        None
    }

    pub fn get_polygons(self) -> Option<MultiPolygonCollection> {
        if let TypedFeatureCollection::MultiPolygon(polygons) = self {
            return Some(polygons);
        }
        None
    }

    pub fn get_data(self) -> Option<DataCollection> {
        if let TypedFeatureCollection::Data(data) = self {
            return Some(data);
        }
        None
    }

    pub fn coordinates(&self) -> &[Coordinate2D] {
        match self {
            TypedFeatureCollection::Data(_) => &[],
            TypedFeatureCollection::MultiPoint(c) => c.coordinates(),
            TypedFeatureCollection::MultiLineString(c) => c.coordinates(),
            TypedFeatureCollection::MultiPolygon(c) => c.coordinates(),
        }
    }

    pub fn feature_offsets(&self) -> &[i32] {
        match self {
            TypedFeatureCollection::Data(_) => &[],
            TypedFeatureCollection::MultiPoint(c) => c.multipoint_offsets(),
            TypedFeatureCollection::MultiLineString(c) => c.multi_line_string_offsets(),
            TypedFeatureCollection::MultiPolygon(c) => c.multi_polygon_offsets(),
        }
    }
}
