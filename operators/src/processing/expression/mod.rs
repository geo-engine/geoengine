mod error;
mod raster_operator;
mod raster_query_processor;
mod vector_operator;

use geoengine_datatypes::primitives::{
    AsGeoOption, MultiLineString, MultiLineStringRef, MultiPoint, MultiPointRef, MultiPolygon,
    MultiPolygonRef, NoGeometry,
};

pub use error::ExpressionError;
pub use raster_operator::{Expression, ExpressionParams};
pub use vector_operator::{VectorExpression, VectorExpressionError, VectorExpressionParams};

/// Convenience trait for converting [`geoengine_datatypes`] types to [`geoengine_expression`] types.
trait AsExpressionGeo: AsGeoOption {
    type ExpressionGeometryType: Send;

    fn as_expression_geo(&self) -> Option<Self::ExpressionGeometryType>;
}

/// Convenience trait for converting [`geoengine_expression`] types to [`geoengine_datatypes`] types.
trait FromExpressionGeo: Sized {
    type ExpressionGeometryType: Send;

    fn from_expression_geo(geom: Self::ExpressionGeometryType) -> Option<Self>;
}

impl<'c> AsExpressionGeo for MultiPointRef<'c> {
    type ExpressionGeometryType = geoengine_expression::MultiPoint;

    fn as_expression_geo(&self) -> Option<Self::ExpressionGeometryType> {
        self.as_geo_option().map(Into::into)
    }
}

impl<'c> AsExpressionGeo for MultiLineStringRef<'c> {
    type ExpressionGeometryType = geoengine_expression::MultiLineString;

    fn as_expression_geo(&self) -> Option<Self::ExpressionGeometryType> {
        self.as_geo_option().map(Into::into)
    }
}

impl<'c> AsExpressionGeo for MultiPolygonRef<'c> {
    type ExpressionGeometryType = geoengine_expression::MultiPolygon;

    fn as_expression_geo(&self) -> Option<Self::ExpressionGeometryType> {
        self.as_geo_option().map(Into::into)
    }
}

impl AsExpressionGeo for NoGeometry {
    // fallback type
    type ExpressionGeometryType = geoengine_expression::MultiPoint;

    fn as_expression_geo(&self) -> Option<Self::ExpressionGeometryType> {
        self.as_geo_option().map(Into::into)
    }
}

impl FromExpressionGeo for MultiPoint {
    type ExpressionGeometryType = geoengine_expression::MultiPoint;

    fn from_expression_geo(geom: Self::ExpressionGeometryType) -> Option<Self> {
        let geo_geom: geo::MultiPoint = geom.into();
        geo_geom.try_into().ok()
    }
}

impl FromExpressionGeo for MultiLineString {
    type ExpressionGeometryType = geoengine_expression::MultiLineString;

    fn from_expression_geo(geom: Self::ExpressionGeometryType) -> Option<Self> {
        let geo_geom: geo::MultiLineString = geom.into();
        Some(geo_geom.into())
    }
}

impl FromExpressionGeo for MultiPolygon {
    type ExpressionGeometryType = geoengine_expression::MultiPolygon;

    fn from_expression_geo(geom: Self::ExpressionGeometryType) -> Option<Self> {
        let geo_geom: geo::MultiPolygon = geom.into();
        Some(geo_geom.into())
    }
}

impl FromExpressionGeo for NoGeometry {
    // fallback type
    type ExpressionGeometryType = geoengine_expression::MultiPoint;

    fn from_expression_geo(_geom: Self::ExpressionGeometryType) -> Option<Self> {
        None
    }
}
