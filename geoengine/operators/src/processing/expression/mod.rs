mod environment;
mod error;
mod raster_operator;
mod raster_query_processor;
mod vector_operator;

pub use environment::{ExpressionCacheConfig, ExpressionEnvironment};
pub use error::{LinkExpressionError, RasterExpressionError, VectorExpressionError};
pub use raster_operator::{Expression, ExpressionParams}; // TODO: rename to `RasterExpression`
pub use vector_operator::{OutputColumn, VectorExpression, VectorExpressionParams};

use geoengine_datatypes::primitives::{
    AsGeoOption, MultiLineString, MultiLineStringRef, MultiPoint, MultiPointRef, MultiPolygon,
    MultiPolygonRef, NoGeometry,
};

/// Replaces all non-alphanumeric characters in a string with underscores.
/// Prepends an underscore if the string is empty or starts with a number.
fn canonicalize_name(name: &str) -> String {
    let prepend_underscore = name.chars().next().is_none_or(char::is_numeric);

    let mut canonicalized_name =
        String::with_capacity(name.len() + usize::from(prepend_underscore));

    if prepend_underscore {
        canonicalized_name.push('_');
    }

    for c in name.chars() {
        canonicalized_name.push(if c.is_alphanumeric() { c } else { '_' });
    }

    canonicalized_name
}

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

impl AsExpressionGeo for MultiPointRef<'_> {
    type ExpressionGeometryType = geoengine_expression::MultiPoint;

    fn as_expression_geo(&self) -> Option<Self::ExpressionGeometryType> {
        self.as_geo_option().map(Into::into)
    }
}

impl AsExpressionGeo for MultiLineStringRef<'_> {
    type ExpressionGeometryType = geoengine_expression::MultiLineString;

    fn as_expression_geo(&self) -> Option<Self::ExpressionGeometryType> {
        self.as_geo_option().map(Into::into)
    }
}

impl AsExpressionGeo for MultiPolygonRef<'_> {
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
