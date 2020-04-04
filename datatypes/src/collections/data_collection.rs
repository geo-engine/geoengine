use crate::collections::{FeatureCollection, FeatureCollectionImplHelpers};
use arrow::array::{BooleanArray, ListArray, StructArray};
use arrow::datatypes::DataType;
use std::collections::HashMap;

/// This collection contains temporal data but no geographical features.
#[derive(Debug)]
pub struct DataCollection {
    data: StructArray,
    types: HashMap<String, crate::primitives::FeatureDataType>,
}

impl FeatureCollectionImplHelpers for DataCollection {
    fn geometry_arrow_data_type() -> DataType {
        unreachable!("This collection has no geometries")
    }

    fn filtered_geometries(
        _features: &ListArray,
        _filter_array: &BooleanArray,
    ) -> crate::util::Result<ListArray> {
        unreachable!("This collection has no geometries")
    }

    fn is_simple(&self) -> bool {
        true
    }
}

impl<'i> crate::collections::IntoGeometryOptionsIterator<'i> for DataCollection {
    type GeometryOptionIterator = std::iter::Take<std::iter::Repeat<Option<Self::GeometryType>>>;
    type GeometryType = geojson::Geometry; // fulfills the requirement but is not used anyway

    fn geometry_options(&'i self) -> Self::GeometryOptionIterator {
        std::iter::repeat(None).take(self.len())
    }
}

feature_collection_impl!(DataCollection, false);

// TODO: implement constructors

// TODO: implement builder

#[cfg(test)]
mod tests {
    // TODO: implement one test for each method
}
