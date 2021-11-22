use std::sync::Arc;

use arrow::{
    array::{Array, ArrayData},
    buffer::Buffer,
};

use crate::primitives::{BoundingBox2D, Coordinate2D, GeometryRef};
use crate::util::Result;

/// This trait allows iterating over the geometries of a feature collection
pub trait IntoGeometryIterator<'a> {
    type GeometryIterator: Iterator<Item = Self::GeometryType> + Send;
    type GeometryType: GeometryRef + Send;

    /// Return an iterator over geometries
    fn geometries(&'a self) -> Self::GeometryIterator;
}

/// This trait allows accessing the geometries of the collection by random access
pub trait GeometryRandomAccess<'a> {
    type GeometryType: GeometryRef + Send;

    /// Returns the geometry at index `feature_index`
    fn geometry_at(&'a self, feature_index: usize) -> Option<Self::GeometryType>;
}

/// This trait allows iterating over the geometries of a feature collection if the collection has geometries
pub trait IntoGeometryOptionsIterator<'i> {
    type GeometryOptionIterator: Iterator<Item = Option<Self::GeometryType>> + Send;
    type GeometryType: GeometryRef + Send;

    /// Return an iterator over geometries
    fn geometry_options(&'i self) -> Self::GeometryOptionIterator;
}

/// Common geo functionality for `FeatureCollection`s
pub trait GeometryCollection {
    fn coordinates(&self) -> &[Coordinate2D];

    fn feature_offsets(&self) -> &[i32];

    fn bbox(&self) -> Option<BoundingBox2D> {
        BoundingBox2D::from_coord_ref_iter(self.coordinates())
    }
}

pub trait ReplaceRawArrayCoords {
    fn replace_raw_coords(
        array_ref: &Arc<dyn Array>,
        new_coord_buffer: Buffer,
    ) -> Result<ArrayData>;
}
