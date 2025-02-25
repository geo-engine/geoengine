mod error;
mod raster_operator;
mod raster_query_processor;
mod vector_operator;

pub use error::{RasterExpressionError, VectorExpressionError};
pub use raster_operator::{Expression, ExpressionParams}; // TODO: rename to `RasterExpression`
pub use vector_operator::{VectorExpression, VectorExpressionParams};

use self::error::ExpressionDependenciesInitializationError;
use crate::util::Result;
use geoengine_datatypes::primitives::{
    AsGeoOption, MultiLineString, MultiLineStringRef, MultiPoint, MultiPointRef, MultiPolygon,
    MultiPolygonRef, NoGeometry,
};
use geoengine_expression::{error::ExpressionExecutionError, ExpressionDependencies};
use std::sync::{Arc, OnceLock};

/// The expression dependencies are initialized once and then reused for all expression evaluations.
static EXPRESSION_DEPENDENCIES: OnceLock<
    Result<ExpressionDependencies, Arc<ExpressionExecutionError>>,
> = OnceLock::new();

/// Initializes the expression dependencies once so that they can be reused for all expression evaluations.
/// Compiling the dependencies takes a while, so this can drastically improve performance on the first expression call.
///
/// If it fails, you can retry or terminate the program.
///
pub async fn initialize_expression_dependencies(
) -> Result<(), ExpressionDependenciesInitializationError> {
    crate::util::spawn_blocking(|| {
        let dependencies = ExpressionDependencies::new()?;

        // if set returns an error, it was initialized before so it is ok for this functions purpose
        let _ = EXPRESSION_DEPENDENCIES.set(Ok(dependencies));
        Ok(())
    })
    .await?
}

fn generate_expression_dependencies(
) -> Result<ExpressionDependencies, Arc<ExpressionExecutionError>> {
    ExpressionDependencies::new().map_err(Arc::new)
}

pub fn get_expression_dependencies(
) -> Result<&'static ExpressionDependencies, Arc<ExpressionExecutionError>> {
    EXPRESSION_DEPENDENCIES
        .get_or_init(generate_expression_dependencies)
        .as_ref()
        .map_err(Clone::clone)
}

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
