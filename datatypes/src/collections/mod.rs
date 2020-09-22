mod data_types;
pub(self) mod error;
mod feature_collection;
mod feature_collection_builder;
mod geo_feature_collection;

mod data_collection;
mod multi_line_string_collection;
mod multi_point_collection;
mod multi_polygon_collection;

pub(crate) use error::FeatureCollectionError;
pub use feature_collection::FeatureCollection;
pub use feature_collection_builder::{
    BuilderProvider, FeatureCollectionBuilder, FeatureCollectionRowBuilder,
    GeoFeatureCollectionRowBuilder,
};
pub use geo_feature_collection::{IntoGeometryIterator, IntoGeometryOptionsIterator};

pub use data_collection::DataCollection;
pub use data_types::VectorDataType;
pub use multi_line_string_collection::MultiLineStringCollection;
pub use multi_point_collection::MultiPointCollection;
pub use multi_polygon_collection::MultiPolygonCollection;
