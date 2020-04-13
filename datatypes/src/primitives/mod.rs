mod bounding_box;
mod coordinate;
pub(self) mod error;
mod feature_data;
mod measurement;
mod multi_point;
mod spatio_temporal_bounded;
mod time_interval;

pub use bounding_box::BoundingBox2D;
pub use coordinate::Coordinate2D;
pub(crate) use error::PrimitivesError;
pub use feature_data::{
    CategoricalDataRef, DecimalDataRef, FeatureData, FeatureDataRef, FeatureDataType,
    FeatureDataValue, NullableCategoricalDataRef, NullableDataRef, NullableDecimalDataRef,
    NullableNumberDataRef, NullableTextDataRef, NumberDataRef, TextDataRef,
};
pub use measurement::Measurement;
pub use multi_point::{MultiPoint, MultiPointRef};
pub use spatio_temporal_bounded::{SpatialBounded, TemporalBounded};
pub use time_interval::TimeInterval;

/// Marker trait for geometry types
pub trait Geometry {}
