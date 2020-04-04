/// This trait allows iterating over the geometries of a feature collection
pub trait IntoGeometryIterator {
    type GeometryIterator: Iterator<Item = Self::GeometryType>;
    type GeometryType: Into<geojson::Geometry>;

    /// Return an iterator over geometries
    fn geometries(&self) -> Self::GeometryIterator;
}

/// This trait allows iterating over the geometries of a feature collection if the collection has geometries
pub trait IntoGeometryOptionsIterator {
    type GeometryOptionIterator: Iterator<Item = Option<Self::GeometryType>>;
    type GeometryType: Into<geojson::Geometry>;

    /// Return an iterator over geometries
    fn geometry_options(&self) -> Self::GeometryOptionIterator;
}
