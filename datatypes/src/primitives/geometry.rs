use crate::collections::VectorDataType;
use crate::primitives::{BoundingBox2D, MultiLineString, MultiPoint, MultiPolygon, NoGeometry};

use crate::error::Error;
use std::convert::TryFrom;
use std::fmt::Debug;

/// Marker trait for geometry types
// TODO: rename to CollectionType oder something?…
pub trait Geometry: Clone + Debug + Send + Sync + TryFrom<TypedGeometry, Error = Error> {
    // TODO: introduce once generic associated types are possible due to lifetime introduction for ref type
    // type REF: GeometryRef;

    const IS_GEOMETRY: bool = true;

    // move to primitives module?
    const DATA_TYPE: VectorDataType;

    /// Is the geometry overlapping the `BoundingBox2D`?
    fn intersects_bbox(&self, bbox: &BoundingBox2D) -> bool;
}

pub trait GeometryRef: Into<geojson::Geometry> {}

#[derive(Clone, Debug)]
pub enum TypedGeometry {
    Data(NoGeometry),
    MultiPoint(MultiPoint),
    MultiLineString(MultiLineString),
    MultiPolygon(MultiPolygon),
}
