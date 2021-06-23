mod bounding_box;
mod coordinate;
pub(self) mod error;
mod feature_data;
mod geometry;
mod line;
mod measurement;
mod multi_line_string;
mod multi_point;
mod multi_polygon;
mod no_geometry;
mod spatial_partition;
mod spatial_resolution;
mod spatio_temporal_bounded;
mod time_instance;
mod time_interval;
mod time_step;

pub use bounding_box::BoundingBox2D;
pub use coordinate::Coordinate2D;
pub(crate) use error::PrimitivesError;
pub use feature_data::{
    CategoryDataRef, DataRef, FeatureData, FeatureDataRef, FeatureDataType, FeatureDataValue,
    FloatDataRef, IntDataRef, TextDataRef,
};
pub use geometry::{Geometry, GeometryRef, TypedGeometry};
pub use line::Line;
pub use measurement::Measurement;
pub use multi_line_string::{MultiLineString, MultiLineStringAccess, MultiLineStringRef};
pub use multi_point::{MultiPoint, MultiPointAccess, MultiPointRef};
pub use multi_polygon::{MultiPolygon, MultiPolygonAccess, MultiPolygonRef};
pub use no_geometry::NoGeometry;
pub use spatial_partition::{AxisAlignedRectangle, SpatialPartition2D, SpatialPartitioned};
pub use spatial_resolution::SpatialResolution;
pub use spatio_temporal_bounded::{SpatialBounded, TemporalBounded};

pub use time_instance::TimeInstance;
pub use time_interval::TimeInterval;
pub use time_step::{TimeGranularity, TimeStep, TimeStepIter};
