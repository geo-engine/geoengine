use crate::primitives::GeometryRef;

/// This trait allows iterating over the geometries of a feature collection
pub trait IntoGeometryIterator<'a> {
    type GeometryIterator: Iterator<Item = Self::GeometryType>;
    type GeometryType: GeometryRef;

    /// Return an iterator over geometries
    fn geometries(&'a self) -> Self::GeometryIterator;
}

/// This trait allows iterating over the geometries of a feature collection if the collection has geometries
pub trait IntoGeometryOptionsIterator<'i> {
    type GeometryOptionIterator: Iterator<Item = Option<Self::GeometryType>>;
    type GeometryType: GeometryRef;

    /// Return an iterator over geometries
    fn geometry_options(&'i self) -> Self::GeometryOptionIterator;
}

/// This macro implements `IntoGeometryOptionsIterator` for types that implement `IntoGeometryIterator`
macro_rules! into_geometry_options_impl {
    ($Collection:ty) => {
        impl<'i> crate::collections::IntoGeometryOptionsIterator<'i> for $Collection
        where
            $Collection: crate::collections::IntoGeometryIterator<'i>,
        {
            type GeometryOptionIterator = crate::util::helpers::SomeIter<
                <Self as crate::collections::IntoGeometryIterator<'i>>::GeometryIterator,
                Self::GeometryType,
            >;
            type GeometryType =
                <Self as crate::collections::IntoGeometryIterator<'i>>::GeometryType;

            fn geometry_options(&'i self) -> Self::GeometryOptionIterator {
                crate::util::helpers::SomeIter::new(self.geometries())
            }
        }
    };
}
