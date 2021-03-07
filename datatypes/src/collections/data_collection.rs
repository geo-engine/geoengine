use crate::collections::{FeatureCollection, FeatureCollectionInfos, IntoGeometryOptionsIterator};
use crate::primitives::{FeatureDataValue, NoGeometry, TimeInterval};

/// This collection contains temporal data without geographical features.
pub type DataCollection = FeatureCollection<NoGeometry>;

impl<'i> IntoGeometryOptionsIterator<'i> for DataCollection {
    type GeometryOptionIterator = std::iter::Take<std::iter::Repeat<Option<Self::GeometryType>>>;
    type GeometryType = NoGeometry;

    fn geometry_options(&'i self) -> Self::GeometryOptionIterator {
        std::iter::repeat(None).take(self.len())
    }
}

pub enum DataCollectionIteratorReturnType {
    TimeInterval(TimeInterval),
    FeatureDataValue(FeatureDataValue),
}

pub struct DataCollectionIterator<'a> {
    collection: &'a DataCollection,
    column_names: Vec<&'a str>,
    time_intervals: &'a [TimeInterval],
    cur_row: usize,
    cur_col: usize,
}

impl<'a> Iterator for DataCollectionIterator<'a> {
    type Item = DataCollectionIteratorReturnType;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_row == self.time_intervals.len() {
            return None;
        }

        let res = match self.cur_col {
            0 => Self::Item::TimeInterval(
                *self
                    .time_intervals
                    .get(self.cur_row)
                    .expect("already checked"),
            ),
            _ => {
                let column_name = self
                    .column_names
                    .get(self.cur_col)
                    .expect("already checked");
                let data = self.collection.data(column_name).expect("already checked");
                Self::Item::FeatureDataValue(data.get_unchecked(self.cur_row))
            }
        };
        self.cur_col += 1;

        if self.cur_col == self.column_names.len() {
            self.cur_col = 0;
            self.cur_row += 1;
        }
        Some(res)
    }
}

impl<'a> IntoIterator for &'a DataCollection {
    type Item = DataCollectionIteratorReturnType;
    type IntoIter = DataCollectionIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        DataCollectionIterator {
            collection: self,
            column_names: self.table.column_names(),
            time_intervals: self.time_intervals(),
            cur_row: 0,
            cur_col: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::collections::BuilderProvider;
    use crate::collections::FeatureCollectionModifications;
    use crate::primitives::{
        DataRef, FeatureData, FeatureDataRef, FeatureDataType, FeatureDataValue, TimeInterval,
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
            .add_column("a".into(), FeatureDataType::Decimal)
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

        if let FeatureDataRef::Decimal(a_column) = collection.data("a").unwrap() {
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
            assert_eq!(b_column.text_at(0).unwrap().unwrap(), "this");
            assert_eq!(b_column.text_at(1).unwrap().unwrap(), "is");
            assert_eq!(b_column.text_at(2).unwrap().unwrap(), "magic");
        } else {
            panic!("wrong type");
        }
    }

    #[test]
    fn rename_column() {
        let collection = DataCollection::from_data(
            vec![],
            vec![TimeInterval::default(); 3],
            [
                ("foo".to_string(), FeatureData::Decimal(vec![1, 2, 3])),
                (
                    "bar".to_string(),
                    FeatureData::Text(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                ),
            ]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();

        assert_eq!(
            vec!["bar", "foo"],
            collection.column_types().keys().into_sorted_vec()
        );

        assert_eq!(
            vec!["baz", "foo"],
            collection
                .rename_column("bar", "baz")
                .unwrap()
                .column_types()
                .keys()
                .into_sorted_vec()
        );

        assert_eq!(
            vec!["baz", "foz"],
            collection
                .rename_columns(&[("foo", "foz"), ("bar", "baz")])
                .unwrap()
                .column_types()
                .keys()
                .into_sorted_vec()
        );

        assert!(collection.rename_column("foo", "bar").is_err());
    }

    trait IntoSortedVec: Iterator {
        fn into_sorted_vec(self) -> Vec<Self::Item>
        where
            Self: Sized,
            Self::Item: Ord,
        {
            let mut v: Vec<Self::Item> = self.collect();
            v.sort();
            v
        }
    }

    impl<T: ?Sized> IntoSortedVec for T where T: Iterator {}
}
