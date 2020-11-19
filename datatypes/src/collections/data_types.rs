use crate::collections::{
    DataCollection, FeatureCollectionOperations, MultiLineStringCollection, MultiPointCollection,
    MultiPolygonCollection,
};
use crate::primitives::{Coordinate2D, FeatureDataRef, FeatureDataType, TimeInterval};
use crate::util::Result;
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

/// A feature collection, wrapped by type info
#[derive(Debug)]
pub enum TypedFeatureCollection {
    Data(DataCollection),
    MultiPoint(MultiPointCollection),
    MultiLineString(MultiLineStringCollection),
    MultiPolygon(MultiPolygonCollection),
}

/// A feature collection reference, wrapped by type info
#[derive(Debug)]
#[allow(dead_code)]
pub enum TypedFeatureCollectionRef<'c> {
    Data(&'c DataCollection),
    MultiPoint(&'c MultiPointCollection),
    MultiLineString(&'c MultiLineStringCollection),
    MultiPolygon(&'c MultiPolygonCollection),
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

    // TODO: create common interface between typed and non-typed collection
    pub fn time_intervals(&self) -> &[TimeInterval] {
        match self {
            TypedFeatureCollection::Data(c) => c.time_intervals(),
            TypedFeatureCollection::MultiPoint(c) => c.time_intervals(),
            TypedFeatureCollection::MultiLineString(c) => c.time_intervals(),
            TypedFeatureCollection::MultiPolygon(c) => c.time_intervals(),
        }
    }
}

/// Implements a function by forwarding its output
macro_rules! impl_function_by_forwarding_ref {
    (fn $function_name:ident( & $self_type:ident $(, $parameter_name:ident : $parameter_type:ty)* ) -> $return_type:ty) => {
        fn $function_name(&self $(, $parameter_name : $parameter_type)*) -> $return_type {
            match self {
                TypedFeatureCollection::Data(c) => c.$function_name($( $parameter_name ),*),
                TypedFeatureCollection::MultiPoint(c) => c.$function_name($( $parameter_name ),*),
                TypedFeatureCollection::MultiLineString(c) => c.$function_name($( $parameter_name ),*),
                TypedFeatureCollection::MultiPolygon(c) => c.$function_name($( $parameter_name ),*),
            }
        }
    };
}

/// Implements a function by forwarding its output
macro_rules! impl_function_by_forwarding_ref2 {
    (fn $function_name:ident( & $self_type:ident $(, $parameter_name:ident : $parameter_type:ty)* ) -> $return_type:ty) => {
        fn $function_name(&self $(, $parameter_name : $parameter_type)*) -> $return_type {
            match self {
                TypedFeatureCollectionRef::Data(c) => c.$function_name($( $parameter_name ),*),
                TypedFeatureCollectionRef::MultiPoint(c) => c.$function_name($( $parameter_name ),*),
                TypedFeatureCollectionRef::MultiLineString(c) => c.$function_name($( $parameter_name ),*),
                TypedFeatureCollectionRef::MultiPolygon(c) => c.$function_name($( $parameter_name ),*),
            }
        }
    };
}

impl FeatureCollectionOperations for TypedFeatureCollection {
    impl_function_by_forwarding_ref!(fn len(&self) -> usize);
    impl_function_by_forwarding_ref!(fn is_simple(&self) -> bool);
    impl_function_by_forwarding_ref!(fn column_type(&self, column_name: &str) -> Result<FeatureDataType>);
    impl_function_by_forwarding_ref!(fn data(&self, column_name: &str) -> Result<FeatureDataRef>);
    impl_function_by_forwarding_ref!(fn time_intervals(&self) -> &[TimeInterval]);
}

impl<'c> FeatureCollectionOperations for TypedFeatureCollectionRef<'c> {
    impl_function_by_forwarding_ref2!(fn len(&self) -> usize);
    impl_function_by_forwarding_ref2!(fn is_simple(&self) -> bool);
    impl_function_by_forwarding_ref2!(fn column_type(&self, column_name: &str) -> Result<FeatureDataType>);
    impl_function_by_forwarding_ref2!(fn data(&self, column_name: &str) -> Result<FeatureDataRef>);
    impl_function_by_forwarding_ref2!(fn time_intervals(&self) -> &[TimeInterval]);
}
