use crate::error;
use gdal::vector::OGRwkbGeometryType;
use std::collections::hash_map::Keys;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::ops::RangeBounds;
use utoipa::Component;

use serde::{Deserialize, Serialize};

use crate::collections::{
    DataCollection, FeatureCollectionError, FeatureCollectionInfos, FeatureCollectionModifications,
    FilterArray, FilteredColumnNameIter, GeometryCollection, MultiLineStringCollection,
    MultiPointCollection, MultiPolygonCollection, ToGeoJson,
};
use crate::error::Error;
use crate::primitives::{
    Coordinate2D, FeatureData, FeatureDataRef, FeatureDataType, FeatureDataValue, TimeInterval,
};
use crate::util::Result;

/// An enum that contains all possible vector data types
#[derive(
    Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Deserialize, Serialize, Copy, Clone, Component,
)]
pub enum VectorDataType {
    Data,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
}

impl VectorDataType {
    pub fn try_from_ogr_type_code(code: u32) -> Result<Self> {
        Ok(match code {
            OGRwkbGeometryType::wkbPoint | OGRwkbGeometryType::wkbMultiPoint => {
                VectorDataType::MultiPoint
            }
            OGRwkbGeometryType::wkbLineString | OGRwkbGeometryType::wkbMultiLineString => {
                VectorDataType::MultiLineString
            }
            OGRwkbGeometryType::wkbPolygon | OGRwkbGeometryType::wkbMultiPolygon => {
                VectorDataType::MultiPolygon
            }
            _ => {
                return Err(error::Error::NoMatchingVectorDataTypeForOgrGeometryType);
            }
        })
    }
}

impl std::fmt::Display for VectorDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Determine the vector data type of the collection
pub trait VectorDataTyped {
    fn vector_data_type(&self) -> VectorDataType;
}

/// A feature collection, wrapped by type info
#[derive(Clone, Debug, PartialEq)]
pub enum TypedFeatureCollection {
    Data(DataCollection),
    MultiPoint(MultiPointCollection),
    MultiLineString(MultiLineStringCollection),
    MultiPolygon(MultiPolygonCollection),
}

impl AsRef<TypedFeatureCollection> for TypedFeatureCollection {
    fn as_ref(&self) -> &TypedFeatureCollection {
        self
    }
}

/// A feature collection reference, wrapped by type info
#[derive(Clone, Debug, PartialEq)]
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

/// Implement `From<FeatureCollection<?>>` for `TypedFeatureCollection`
macro_rules! impl_from_collection {
    ($variant:ident) => {
        paste::paste! {
            impl From<[<$variant Collection>]> for TypedFeatureCollection {
                fn from(collection: [<$variant Collection>]) -> Self {
                    TypedFeatureCollection::$variant(collection)
                }
            }
        }
    };
}

impl_from_collection!(Data);
impl_from_collection!(MultiPoint);
impl_from_collection!(MultiLineString);
impl_from_collection!(MultiPolygon);

/// Implement `From<&FeatureCollection<?>>` for `TypedFeatureCollectionRef`
macro_rules! impl_from_collection_ref {
    ($variant:ident) => {
        paste::paste! {
            impl<'c> From<&'c [<$variant Collection>]> for TypedFeatureCollectionRef<'c> {
                fn from(collection: &'c [<$variant Collection>]) -> Self {
                    TypedFeatureCollectionRef::$variant(collection)
                }
            }
        }
    };
}

impl_from_collection_ref!(Data);
impl_from_collection_ref!(MultiPoint);
impl_from_collection_ref!(MultiLineString);
impl_from_collection_ref!(MultiPolygon);

/// Convenience conversion from `&TypedFeatureCollection` to `TypedFeatureCollectionRef`
impl<'c> From<&'c TypedFeatureCollection> for TypedFeatureCollectionRef<'c> {
    fn from(typed_collection: &'c TypedFeatureCollection) -> TypedFeatureCollectionRef<'c> {
        match typed_collection {
            TypedFeatureCollection::Data(c) => TypedFeatureCollectionRef::Data(c),
            TypedFeatureCollection::MultiPoint(c) => TypedFeatureCollectionRef::MultiPoint(c),
            TypedFeatureCollection::MultiLineString(c) => {
                TypedFeatureCollectionRef::MultiLineString(c)
            }
            TypedFeatureCollection::MultiPolygon(c) => TypedFeatureCollectionRef::MultiPolygon(c),
        }
    }
}

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

    pub fn try_as_data_ref(&self) -> Option<&DataCollection> {
        if let TypedFeatureCollection::Data(collections) = self {
            return Some(collections);
        }
        None
    }

    pub fn try_as_points_ref(&self) -> Option<&MultiPointCollection> {
        if let TypedFeatureCollection::MultiPoint(collections) = self {
            return Some(collections);
        }
        None
    }

    pub fn try_as_lines_ref(&self) -> Option<&MultiLineStringCollection> {
        if let TypedFeatureCollection::MultiLineString(collections) = self {
            return Some(collections);
        }
        None
    }

    pub fn try_as_collections_ref(&self) -> Option<&MultiPolygonCollection> {
        if let TypedFeatureCollection::MultiPolygon(collections) = self {
            return Some(collections);
        }
        None
    }

    pub fn try_into_data(self) -> Result<DataCollection> {
        self.try_into()
    }
}

impl GeometryCollection for TypedFeatureCollection {
    fn coordinates(&self) -> &[Coordinate2D] {
        match self {
            TypedFeatureCollection::Data(_) => &[],
            TypedFeatureCollection::MultiPoint(c) => c.coordinates(),
            TypedFeatureCollection::MultiLineString(c) => c.coordinates(),
            TypedFeatureCollection::MultiPolygon(c) => c.coordinates(),
        }
    }

    fn feature_offsets(&self) -> &[i32] {
        match self {
            TypedFeatureCollection::Data(_) => &[],
            TypedFeatureCollection::MultiPoint(c) => c.feature_offsets(),
            TypedFeatureCollection::MultiLineString(c) => c.feature_offsets(),
            TypedFeatureCollection::MultiPolygon(c) => c.feature_offsets(),
        }
    }
}

impl GeometryCollection for TypedFeatureCollectionRef<'_> {
    fn coordinates(&self) -> &[Coordinate2D] {
        match self {
            TypedFeatureCollectionRef::Data(_) => &[],
            TypedFeatureCollectionRef::MultiPoint(c) => c.coordinates(),
            TypedFeatureCollectionRef::MultiLineString(c) => c.coordinates(),
            TypedFeatureCollectionRef::MultiPolygon(c) => c.coordinates(),
        }
    }

    fn feature_offsets(&self) -> &[i32] {
        match self {
            TypedFeatureCollectionRef::Data(_) => &[],
            TypedFeatureCollectionRef::MultiPoint(c) => c.feature_offsets(),
            TypedFeatureCollectionRef::MultiLineString(c) => c.feature_offsets(),
            TypedFeatureCollectionRef::MultiPolygon(c) => c.feature_offsets(),
        }
    }
}

impl VectorDataTyped for TypedFeatureCollection {
    fn vector_data_type(&self) -> VectorDataType {
        match self {
            TypedFeatureCollection::Data(_) => VectorDataType::Data,
            TypedFeatureCollection::MultiPoint(_) => VectorDataType::MultiPoint,
            TypedFeatureCollection::MultiLineString(_) => VectorDataType::MultiLineString,
            TypedFeatureCollection::MultiPolygon(_) => VectorDataType::MultiPolygon,
        }
    }
}

impl VectorDataTyped for TypedFeatureCollectionRef<'_> {
    fn vector_data_type(&self) -> VectorDataType {
        match self {
            TypedFeatureCollectionRef::Data(_) => VectorDataType::Data,
            TypedFeatureCollectionRef::MultiPoint(_) => VectorDataType::MultiPoint,
            TypedFeatureCollectionRef::MultiLineString(_) => VectorDataType::MultiLineString,
            TypedFeatureCollectionRef::MultiPolygon(_) => VectorDataType::MultiPolygon,
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
    impl_function_by_forwarding_ref!(fn byte_size(&self) -> usize);
    impl_function_by_forwarding_ref!(fn column_names_of_type(&self, column_type: FeatureDataType) -> FilteredColumnNameIter);
    impl_function_by_forwarding_ref!(fn column_names(&self) -> Keys<String, FeatureDataType>);
}

impl<'c> FeatureCollectionInfos for TypedFeatureCollectionRef<'c> {
    impl_function_by_forwarding_ref2!(fn len(&self) -> usize);
    impl_function_by_forwarding_ref2!(fn is_simple(&self) -> bool);
    impl_function_by_forwarding_ref2!(fn column_type(&self, column_name: &str) -> Result<FeatureDataType>);
    impl_function_by_forwarding_ref2!(fn data(&self, column_name: &str) -> Result<FeatureDataRef>);
    impl_function_by_forwarding_ref2!(fn time_intervals(&self) -> &[TimeInterval]);
    impl_function_by_forwarding_ref2!(fn column_types(&self) -> HashMap<String, FeatureDataType>);
    impl_function_by_forwarding_ref2!(fn byte_size(&self) -> usize);
    impl_function_by_forwarding_ref2!(fn column_names_of_type(&self, column_type: FeatureDataType) -> FilteredColumnNameIter);
    impl_function_by_forwarding_ref2!(fn column_names(&self) -> Keys<String, FeatureDataType>);
}

impl ToGeoJson<'_> for TypedFeatureCollection {
    impl_function_by_forwarding_ref!(fn to_geo_json(&self) -> String);
}

impl<'c> ToGeoJson<'_> for TypedFeatureCollectionRef<'c> {
    impl_function_by_forwarding_ref2!(fn to_geo_json(&self) -> String);
}

/// Implements a function by forwarding its output
macro_rules! impl_mod_function_by_forwarding_ref {
    (fn $function_name:ident( & $self_type:ident $(, $parameter_name:ident : $parameter_type:ty)* ) -> $return_type:ty) => {
        fn $function_name(&self $(, $parameter_name : $parameter_type)*) -> $return_type {
            impl_mod_function_by_forwarding_ref!(@resolve self => $function_name( $( $parameter_name ),* ) )
        }
    };

    (fn $function_name:ident< $($generics:ident),* >( & $self_type:ident $(, $parameter_name:ident : $parameter_type:ty)* ) -> $return_type:ty
        where $($tail:tt)* ) => {
        fn $function_name< $( $generics ),* >(&self $(, $parameter_name : $parameter_type)*) -> $return_type where $($tail)* {
            impl_mod_function_by_forwarding_ref!(@resolve self => $function_name( $( $parameter_name ),* ) )
        }
    };

    (@resolve $typed:expr =>  $function_name:ident( $($parameter_name:ident),* )) => {
        Ok(match $typed {
            TypedFeatureCollection::Data(c) => TypedFeatureCollection::Data( c.$function_name($( $parameter_name ),*)? ),
            TypedFeatureCollection::MultiPoint(c) => TypedFeatureCollection::MultiPoint( c.$function_name($( $parameter_name ),*)? ),
            TypedFeatureCollection::MultiLineString(c) => TypedFeatureCollection::MultiLineString( c.$function_name($( $parameter_name ),*)? ),
            TypedFeatureCollection::MultiPolygon(c) => TypedFeatureCollection::MultiPolygon( c.$function_name($( $parameter_name ),*)? ),
        })
    };
}

/// Implements a function by forwarding its output
macro_rules! impl_mod_function_by_forwarding_ref2 {
    (fn $function_name:ident( & $self_type:ident $(, $parameter_name:ident : $parameter_type:ty)* ) -> $return_type:ty) => {
        fn $function_name(&self $(, $parameter_name : $parameter_type)*) -> $return_type {
            impl_mod_function_by_forwarding_ref2!(@resolve self => $function_name( $( $parameter_name ),* ) )
        }
    };

    (fn $function_name:ident< $($generics:ident),* >( & $self_type:ident $(, $parameter_name:ident : $parameter_type:ty)* ) -> $return_type:ty
        where $($tail:tt)*  ) => {
        fn $function_name< $( $generics ),* >(&self $(, $parameter_name : $parameter_type)*) -> $return_type where $($tail)* {
            impl_mod_function_by_forwarding_ref2!(@resolve self => $function_name( $( $parameter_name ),* ) )
        }
    };

    (@resolve $typed:expr =>  $function_name:ident( $($parameter_name:ident),* )) => {
        Ok(match $typed {
            TypedFeatureCollectionRef::Data(c) => TypedFeatureCollection::Data( c.$function_name($( $parameter_name ),*)? ),
            TypedFeatureCollectionRef::MultiPoint(c) => TypedFeatureCollection::MultiPoint( c.$function_name($( $parameter_name ),*)? ),
            TypedFeatureCollectionRef::MultiLineString(c) => TypedFeatureCollection::MultiLineString( c.$function_name($( $parameter_name ),*)? ),
            TypedFeatureCollectionRef::MultiPolygon(c) => TypedFeatureCollection::MultiPolygon( c.$function_name($( $parameter_name ),*)? ),
        })
    };
}

impl FeatureCollectionModifications for TypedFeatureCollection {
    type Output = Self;

    impl_mod_function_by_forwarding_ref!(fn filter<M>(&self, mask: M) -> Result<Self::Output> where M: FilterArray);

    impl_mod_function_by_forwarding_ref!(fn add_columns(&self, new_columns: &[(&str, FeatureData)]) -> Result<Self::Output>);

    impl_mod_function_by_forwarding_ref!(fn remove_columns(&self, removed_column_names: &[&str]) -> Result<Self::Output>);

    impl_mod_function_by_forwarding_ref!(fn rename_columns<S1, S2>(&self, renamings: &[(S1, S2)]) -> Result<Self::Output>
                                         where S1: AsRef<str>, S2: AsRef<str>);

    impl_mod_function_by_forwarding_ref!(fn column_range_filter<R>(&self, column: &str, ranges: &[R], keep_nulls: bool) -> Result<Self::Output>
                                         where R: RangeBounds<FeatureDataValue>);

    fn append(&self, other: &Self) -> Result<Self::Output> {
        Ok(match (self, other) {
            (TypedFeatureCollection::Data(c1), TypedFeatureCollection::Data(c2)) => {
                TypedFeatureCollection::Data(c1.append(c2)?)
            }
            (TypedFeatureCollection::MultiPoint(c1), TypedFeatureCollection::MultiPoint(c2)) => {
                TypedFeatureCollection::MultiPoint(c1.append(c2)?)
            }
            (
                TypedFeatureCollection::MultiLineString(c1),
                TypedFeatureCollection::MultiLineString(c2),
            ) => TypedFeatureCollection::MultiLineString(c1.append(c2)?),
            (
                TypedFeatureCollection::MultiPolygon(c1),
                TypedFeatureCollection::MultiPolygon(c2),
            ) => TypedFeatureCollection::MultiPolygon(c1.append(c2)?),
            _ => return Err(FeatureCollectionError::WrongDataType.into()),
        })
    }

    impl_mod_function_by_forwarding_ref!(fn sort_by_time_asc(&self) -> Result<Self::Output>);

    impl_mod_function_by_forwarding_ref!(fn replace_time(&self, time_intervals: &[TimeInterval]) -> Result<Self::Output>);
}

impl<'c> FeatureCollectionModifications for TypedFeatureCollectionRef<'c> {
    type Output = TypedFeatureCollection;

    impl_mod_function_by_forwarding_ref2!(fn filter<M>(&self, mask: M) -> Result<Self::Output> where M: FilterArray);

    impl_mod_function_by_forwarding_ref2!(fn add_columns(&self, new_columns: &[(&str, FeatureData)]) -> Result<Self::Output>);

    impl_mod_function_by_forwarding_ref2!(fn remove_columns(&self, removed_column_names: &[&str]) -> Result<Self::Output>);

    impl_mod_function_by_forwarding_ref2!(fn rename_columns<S1, S2>(&self, renamings: &[(S1, S2)]) -> Result<Self::Output>
                                          where S1: AsRef<str>, S2: AsRef<str>);

    impl_mod_function_by_forwarding_ref2!(fn column_range_filter<R>(&self, column: &str, ranges: &[R], keep_nulls: bool) -> Result<Self::Output>
                                          where R: RangeBounds<FeatureDataValue>);

    fn append(&self, other: &Self) -> Result<Self::Output> {
        Ok(match (self, other) {
            (TypedFeatureCollectionRef::Data(c1), TypedFeatureCollectionRef::Data(c2)) => {
                TypedFeatureCollection::Data(c1.append(c2)?)
            }
            (
                TypedFeatureCollectionRef::MultiPoint(c1),
                TypedFeatureCollectionRef::MultiPoint(c2),
            ) => TypedFeatureCollection::MultiPoint(c1.append(c2)?),
            (
                TypedFeatureCollectionRef::MultiLineString(c1),
                TypedFeatureCollectionRef::MultiLineString(c2),
            ) => TypedFeatureCollection::MultiLineString(c1.append(c2)?),
            (
                TypedFeatureCollectionRef::MultiPolygon(c1),
                TypedFeatureCollectionRef::MultiPolygon(c2),
            ) => TypedFeatureCollection::MultiPolygon(c1.append(c2)?),
            _ => return Err(FeatureCollectionError::WrongDataType.into()),
        })
    }

    impl_mod_function_by_forwarding_ref2!(fn sort_by_time_asc(&self) -> Result<Self::Output>);

    impl_mod_function_by_forwarding_ref2!(fn replace_time(&self, time_intervals: &[TimeInterval]) -> Result<Self::Output>);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append() {
        let c1: TypedFeatureCollection = MultiPointCollection::empty().into();
        let c2 = MultiPointCollection::empty().into();

        c1.append(&c2).unwrap();

        let c1 = MultiPointCollection::empty();
        let c2 = MultiPointCollection::empty();

        TypedFeatureCollectionRef::from(&c1)
            .append(&c2.as_ref().into())
            .unwrap();

        let c3 = MultiPolygonCollection::empty();

        TypedFeatureCollectionRef::from(&c1)
            .append(&c3.as_ref().into())
            .unwrap_err();

        let c4: TypedFeatureCollection = MultiPointCollection::empty().into();

        TypedFeatureCollectionRef::from(&c1)
            .append(&c4.as_ref().into())
            .unwrap();
    }
}
