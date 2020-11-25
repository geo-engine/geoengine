use std::convert::{TryFrom, TryInto};

use serde::{Deserialize, Serialize};

use crate::collections::{
    DataCollection, FeatureCollectionError, FeatureCollectionInfos, MultiLineStringCollection,
    MultiPointCollection, MultiPolygonCollection,
};
use crate::error::Error;
use crate::primitives::{Coordinate2D, FeatureDataRef, FeatureDataType, TimeInterval};
use crate::util::Result;
use std::collections::HashMap;

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

/// Implement `TryFrom` for `TypedFeatureCollection`
macro_rules! impl_try_from {
    ($variant:ident) => {
        paste::paste! {
            impl TryFrom<TypedFeatureCollection> for [<$variant Collection>] {
                type Error = Error;

                fn try_from(typed_collection: TypedFeatureCollection) -> Result<Self, Self::Error> {
                    if let TypedFeatureCollection::$variant(collection) = typed_collection {
                        return Ok(collection);
                    }
                    Err(FeatureCollectionError::WrongDataType.into())
                }
            }
        }
    };
}

impl_try_from!(Data);
impl_try_from!(MultiPoint);
impl_try_from!(MultiLineString);
impl_try_from!(MultiPolygon);

/// Implement `TryFrom` for `TypedFeatureCollectionRef`
macro_rules! impl_try_from_ref {
    ($variant:ident) => {
        paste::paste! {
            impl<'c> TryFrom<&TypedFeatureCollectionRef<'c>> for &'c [<$variant Collection>] {
                type Error = Error;

                fn try_from(typed_collection: &TypedFeatureCollectionRef<'c>) -> Result<Self, Self::Error> {
                    if let TypedFeatureCollectionRef::$variant(collection) = typed_collection {
                        return Ok(collection);
                    }
                    Err(FeatureCollectionError::WrongDataType.into())
                }
            }
        }
    };
}

impl_try_from_ref!(Data);
impl_try_from_ref!(MultiPoint);
impl_try_from_ref!(MultiLineString);
impl_try_from_ref!(MultiPolygon);

impl TypedFeatureCollection {
    pub fn try_into_points(self) -> Result<MultiPointCollection> {
        self.try_into()
    }

    pub fn try_into_lines(self) -> Result<MultiLineStringCollection> {
        self.try_into()
    }

    pub fn try_into_polygons(self) -> Result<MultiPolygonCollection> {
        self.try_into()
    }

    pub fn try_into_data(self) -> Result<DataCollection> {
        self.try_into()
    }

    pub fn vector_data_type(&self) -> VectorDataType {
        match self {
            TypedFeatureCollection::Data(_) => VectorDataType::Data,
            TypedFeatureCollection::MultiPoint(_) => VectorDataType::MultiPoint,
            TypedFeatureCollection::MultiLineString(_) => VectorDataType::MultiLineString,
            TypedFeatureCollection::MultiPolygon(_) => VectorDataType::MultiPolygon,
        }
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

impl<'c> TypedFeatureCollectionRef<'c> {
    pub fn try_into_points(&self) -> Result<&MultiPointCollection> {
        self.try_into()
    }

    pub fn try_into_lines(&self) -> Result<&MultiLineStringCollection> {
        self.try_into()
    }

    pub fn try_into_polygons(&self) -> Result<&MultiPolygonCollection> {
        self.try_into()
    }

    pub fn try_into_data(&self) -> Result<&DataCollection> {
        self.try_into()
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

impl FeatureCollectionInfos for TypedFeatureCollection {
    impl_function_by_forwarding_ref!(fn len(&self) -> usize);
    impl_function_by_forwarding_ref!(fn is_simple(&self) -> bool);
    impl_function_by_forwarding_ref!(fn column_type(&self, column_name: &str) -> Result<FeatureDataType>);
    impl_function_by_forwarding_ref!(fn data(&self, column_name: &str) -> Result<FeatureDataRef>);
    impl_function_by_forwarding_ref!(fn time_intervals(&self) -> &[TimeInterval]);
    impl_function_by_forwarding_ref!(fn column_types(&self) -> HashMap<String, FeatureDataType>);
}

impl<'c> FeatureCollectionInfos for TypedFeatureCollectionRef<'c> {
    impl_function_by_forwarding_ref2!(fn len(&self) -> usize);
    impl_function_by_forwarding_ref2!(fn is_simple(&self) -> bool);
    impl_function_by_forwarding_ref2!(fn column_type(&self, column_name: &str) -> Result<FeatureDataType>);
    impl_function_by_forwarding_ref2!(fn data(&self, column_name: &str) -> Result<FeatureDataRef>);
    impl_function_by_forwarding_ref2!(fn time_intervals(&self) -> &[TimeInterval]);
    impl_function_by_forwarding_ref2!(fn column_types(&self) -> HashMap<String, FeatureDataType>);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append() {
        let _c1 = TypedFeatureCollection::MultiPoint(MultiPointCollection::empty());
        let _c2 = MultiPointCollection::empty();

        // c1.as_ref().append(&c2)
    }
}
