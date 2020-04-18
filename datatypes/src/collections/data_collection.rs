use crate::collections::{
    BuilderProvider, FeatureCollection, FeatureCollectionBuilderImplHelpers,
    FeatureCollectionImplHelpers, SimpleFeatureCollectionBuilder,
};
use crate::error::Error;
use crate::primitives::{FeatureDataType, NoGeometry};
use crate::util::arrow::ArrowTyped;
use arrow::array::{BooleanArray, ListArray, StructArray};
use arrow::datatypes::DataType;
use std::collections::HashMap;

/// This collection contains temporal data but no geographical features.
#[derive(Debug)]
pub struct DataCollection {
    table: StructArray,
    types: HashMap<String, crate::primitives::FeatureDataType>,
}

impl FeatureCollectionImplHelpers for DataCollection {
    fn new_from_internals(table: StructArray, types: HashMap<String, FeatureDataType>) -> Self {
        Self { table, types }
    }

    fn table(&self) -> &StructArray {
        &self.table
    }

    fn types(&self) -> &HashMap<String, FeatureDataType> {
        &self.types
    }

    fn geometry_arrow_data_type() -> DataType {
        unreachable!("This collection has no geometries")
    }

    fn filtered_geometries(
        _features: &ListArray,
        _filter_array: &BooleanArray,
    ) -> crate::util::Result<ListArray> {
        unreachable!("This collection has no geometries")
    }

    fn concat_geometries(
        _geometries_a: &ListArray,
        _geometries_b: &ListArray,
    ) -> Result<ListArray, Error> {
        unreachable!("This collection has no geometries")
    }

    fn _is_simple(&self) -> bool {
        true
    }
}

impl<'i> crate::collections::IntoGeometryOptionsIterator<'i> for DataCollection {
    type GeometryOptionIterator = std::iter::Take<std::iter::Repeat<Option<Self::GeometryType>>>;
    type GeometryType = NoGeometry;

    fn geometry_options(&'i self) -> Self::GeometryOptionIterator {
        std::iter::repeat(None).take(self.len())
    }
}

feature_collection_impl!(DataCollection, false);

impl FeatureCollectionBuilderImplHelpers for DataCollection {
    type GeometriesBuilder = <NoGeometry as ArrowTyped>::ArrowBuilder;

    const HAS_GEOMETRIES: bool = false;

    fn geometries_builder() -> Self::GeometriesBuilder {
        NoGeometry::arrow_builder(0)
    }
}

impl BuilderProvider for DataCollection {
    type Builder = SimpleFeatureCollectionBuilder<Self>;
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::collections::{FeatureCollectionBuilder, FeatureCollectionRowBuilder};
    use crate::primitives::{
        FeatureData, FeatureDataRef, FeatureDataValue, NullableDataRef, TimeInterval,
    };

    #[test]
    fn time_intervals() {
        let mut builder = DataCollection::builder().finish_header();

        builder.push_time_interval(TimeInterval::default()).unwrap();
        builder.finish_row();
        builder
            .push_time_interval(TimeInterval::new(0, 1).unwrap())
            .unwrap();
        builder.finish_row();
        builder
            .push_time_interval(TimeInterval::new(2, 3).unwrap())
            .unwrap();
        builder.finish_row();

        let collection = builder.build().unwrap();

        assert_eq!(collection.len(), 3);

        assert_eq!(
            collection.time_intervals(),
            &[
                TimeInterval::default(),
                TimeInterval::new(0, 1).unwrap(),
                TimeInterval::new(2, 3).unwrap(),
            ]
        );

        let filtered_collection = collection.filter(vec![false, true, false]).unwrap();

        assert_eq!(filtered_collection.len(), 1);

        assert_eq!(
            filtered_collection.time_intervals(),
            &[TimeInterval::new(0, 1).unwrap()]
        );

        let concatenated_collection = collection.append(&filtered_collection).unwrap();

        assert_eq!(concatenated_collection.len(), 4);

        assert_eq!(
            concatenated_collection.time_intervals(),
            &[
                TimeInterval::default(),
                TimeInterval::new(0, 1).unwrap(),
                TimeInterval::new(2, 3).unwrap(),
                TimeInterval::new(0, 1).unwrap(),
            ]
        );
    }

    #[test]
    fn columns() {
        let mut builder = DataCollection::builder();
        builder
            .add_column("a".into(), FeatureDataType::NullableDecimal)
            .unwrap();
        let mut builder = builder.finish_header();

        builder.push_time_interval(TimeInterval::default()).unwrap();
        builder
            .push_data("a", FeatureDataValue::NullableDecimal(Some(42)))
            .unwrap();
        builder.finish_row();
        builder
            .push_time_interval(TimeInterval::new(0, 1).unwrap())
            .unwrap();
        builder
            .push_data("a", FeatureDataValue::Number(13.37))
            .unwrap_err();
        builder
            .push_data("a", FeatureDataValue::NullableDecimal(None))
            .unwrap();
        builder.finish_row();
        builder
            .push_time_interval(TimeInterval::new(2, 3).unwrap())
            .unwrap();
        builder
            .push_data("a", FeatureDataValue::NullableDecimal(Some(1337)))
            .unwrap();
        builder.finish_row();

        let collection = builder.build().unwrap();

        assert_eq!(collection.len(), 3);

        if let FeatureDataRef::NullableDecimal(a_column) = collection.data("a").unwrap() {
            assert_eq!(a_column.as_ref()[0], 42);
            assert_eq!(a_column.nulls()[1], true);
            assert_eq!(a_column.as_ref()[2], 1337);
        } else {
            panic!("wrong type");
        }

        let collection = collection
            .add_column(
                "b",
                FeatureData::Text(vec!["this".into(), "is".into(), "magic".into()]),
            )
            .unwrap();

        let collection = collection.remove_column("a").unwrap();

        assert!(collection.data("a").is_err());
        assert!(collection.remove_column("a").is_err());

        if let FeatureDataRef::Text(b_column) = collection.data("b").unwrap() {
            assert_eq!(b_column.text_at(0).unwrap(), "this");
            assert_eq!(b_column.text_at(1).unwrap(), "is");
            assert_eq!(b_column.text_at(2).unwrap(), "magic");
        } else {
            panic!("wrong type");
        }
    }
}
