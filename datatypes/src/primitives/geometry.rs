use crate::collections::VectorDataType;
use crate::primitives::{BoundingBox2D, MultiLineString, MultiPoint, MultiPolygon, NoGeometry};

use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt::Debug;
use wkt::ToWkt;

use super::{MultiLineStringRef, MultiPointRef, MultiPolygonRef};

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

pub trait GeometryRef: Into<geojson::Geometry> + ToWkt<f64> {
    type GeometryType: Geometry;

    fn as_geometry(&self) -> Self::GeometryType;
    fn bbox(&self) -> Option<BoundingBox2D>;
}

/// Conversion from [`geo`] types to [`Geometry`] types.
// pub trait IntoGeo: Into<Self::GeoGeometryType> {
pub trait AsGeo {
    type GeoGeometryType;

    fn as_geo(&self) -> Self::GeoGeometryType;
}

/// Conversion from [`geo`] types to [`Geometry`] types.
pub trait AsGeoOption {
    type GeoGeometryType;

    fn as_geo_option(&self) -> Option<Self::GeoGeometryType>;
}

impl<'g> AsGeo for MultiPointRef<'g> {
    type GeoGeometryType = geo::MultiPoint<f64>;

    fn as_geo(&self) -> Self::GeoGeometryType {
        self.into()
    }
}

impl<'g> AsGeo for MultiLineStringRef<'g> {
    type GeoGeometryType = geo::MultiLineString<f64>;

    fn as_geo(&self) -> Self::GeoGeometryType {
        self.into()
    }
}

impl<'g> AsGeo for MultiPolygonRef<'g> {
    type GeoGeometryType = geo::MultiPolygon<f64>;

    fn as_geo(&self) -> Self::GeoGeometryType {
        self.into()
    }
}

impl<G> AsGeoOption for G
where
    G: AsGeo,
{
    type GeoGeometryType = G::GeoGeometryType;

    fn as_geo_option(&self) -> Option<Self::GeoGeometryType> {
        Some(self.as_geo())
    }
}

impl AsGeoOption for NoGeometry {
    // will never be constructed, but we have no additional type in theory
    type GeoGeometryType = geo::MultiPoint<f64>;

    fn as_geo_option(&self) -> Option<Self::GeoGeometryType> {
        None
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TypedGeometry {
    Data(NoGeometry),
    MultiPoint(MultiPoint),
    MultiLineString(MultiLineString),
    MultiPolygon(MultiPolygon),
}
