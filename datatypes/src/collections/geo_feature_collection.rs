/// This trait allows iterating over the geometries of a feature collection
pub trait IntoGeometryIterator {
    type GeometryIterator: Iterator<Item = Self::GeometryType>;
    type GeometryType;

    /// Return an iterator over geometries
    fn geometries(&self) -> Self::GeometryIterator;
}
