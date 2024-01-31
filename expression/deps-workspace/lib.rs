use geo::{Area, Centroid};

#[derive(Debug, Clone, PartialEq)]
pub struct MultiPoint(geo::MultiPoint);
#[derive(Debug, Clone, PartialEq)]
pub struct MultiLineString(geo::MultiLineString);
#[derive(Debug, Clone, PartialEq)]
pub struct MultiPolygon(geo::MultiPolygon);

/// Implement `From` and `Into` for a geometry type.
macro_rules! impl_geo_from_to {
    ( $geom_type:ty, $geo_multi_type:ty, $geo_single_type:ty ) => {
        impl From<$geo_multi_type> for $geom_type {
            fn from(geom: $geo_multi_type) -> Self {
                Self(geom)
            }
        }

        impl From<$geo_single_type> for $geom_type {
            fn from(geom: $geo_single_type) -> Self {
                Self(<$geo_multi_type>::new(vec![geom]))
            }
        }

        impl From<$geom_type> for $geo_multi_type {
            fn from(geom: $geom_type) -> Self {
                geom.0
            }
        }
    };
}

impl_geo_from_to!(MultiPoint, geo::MultiPoint, geo::Point);
impl_geo_from_to!(MultiLineString, geo::MultiLineString, geo::LineString);
impl_geo_from_to!(MultiPolygon, geo::MultiPolygon, geo::Polygon);

/// Common operations for all geometry types.
pub trait GeoOptionOperations {
    fn area(&self) -> Option<f64>;

    fn centroid(&self) -> Option<MultiPoint>;
}

impl GeoOptionOperations for MultiPoint {
    fn area(&self) -> Option<f64> {
        Some(self.0.unsigned_area())
    }

    fn centroid(&self) -> Option<MultiPoint> {
        Some(MultiPoint(self.0.centroid()?.into()))
    }
}

impl GeoOptionOperations for MultiLineString {
    fn area(&self) -> Option<f64> {
        Some(self.0.unsigned_area())
    }

    fn centroid(&self) -> Option<MultiPoint> {
        Some(MultiPoint(self.0.centroid()?.into()))
    }
}

impl GeoOptionOperations for MultiPolygon {
    fn area(&self) -> Option<f64> {
        Some(self.0.unsigned_area())
    }

    fn centroid(&self) -> Option<MultiPoint> {
        Some(MultiPoint(self.0.centroid()?.into()))
    }
}

impl<T> GeoOptionOperations for Option<T>
where
    T: GeoOptionOperations,
{
    fn area(&self) -> Option<f64> {
        self.as_ref()?.area()
    }

    fn centroid(&self) -> Option<MultiPoint> {
        self.as_ref()?.centroid()
    }
}
