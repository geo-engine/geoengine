mod bounding_box;
mod coordinate;
pub(self) mod error;
mod feature_data;
mod geometry;
mod measurement;
mod multi_line_string;
mod multi_point;
mod multi_polygon;
mod no_geometry;
mod spatial_resolution;
mod spatio_temporal_bounded;
mod time_instance;
mod time_interval;

pub use bounding_box::BoundingBox2D;
pub use coordinate::Coordinate2D;
pub(crate) use error::PrimitivesError;
pub use feature_data::{
    CategoricalDataRef, DecimalDataRef, FeatureData, FeatureDataRef, FeatureDataType,
    FeatureDataValue, NullableCategoricalDataRef, NullableDataRef, NullableDecimalDataRef,
    NullableNumberDataRef, NullableTextDataRef, NumberDataRef, TextDataRef,
};
pub use geometry::{Geometry, GeometryRef, TypedGeometry};
pub use measurement::Measurement;
pub use multi_line_string::{MultiLineString, MultiLineStringAccess, MultiLineStringRef};
pub use multi_point::{MultiPoint, MultiPointAccess, MultiPointRef};
pub use multi_polygon::{MultiPolygon, MultiPolygonAccess, MultiPolygonRef};
pub use no_geometry::NoGeometry;
pub use spatial_resolution::SpatialResolution;
pub use spatio_temporal_bounded::{SpatialBounded, TemporalBounded};

pub use time_instance::TimeInstance;
pub use time_interval::TimeInterval;
