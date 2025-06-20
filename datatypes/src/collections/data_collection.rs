use crate::collections::{
    FeatureCollection, FeatureCollectionInfos, FeatureCollectionIterator, FeatureCollectionRow,
    IntoGeometryOptionsIterator,
};
use crate::primitives::NoGeometry;

/// This collection contains temporal data without geographical features.
pub type DataCollection = FeatureCollection<NoGeometry>;

impl<'i> IntoGeometryOptionsIterator<'i> for DataCollection {
    type GeometryOptionIterator = NoGeometryIterator;
    type GeometryType = NoGeometry;

    fn geometry_options(&'i self) -> Self::GeometryOptionIterator {
        NoGeometryIterator(std::iter::repeat_n(None, self.len()))
    }
}

pub struct NoGeometryIterator(std::iter::RepeatN<Option<NoGeometry>>);
pub struct NoGeometryParIterator(NoGeometryIterator);

impl Iterator for NoGeometryIterator {
    type Item = Option<NoGeometry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl DoubleEndedIterator for NoGeometryIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        // does not matter if we return a `None`` from the back or the front
        self.0.next()
    }
}

impl ExactSizeIterator for NoGeometryIterator {}

mod par_iter {
    use super::*;
    use rayon::iter::{
        IndexedParallelIterator, IntoParallelIterator, ParallelIterator, plumbing::Producer,
    };

    impl IntoParallelIterator for NoGeometryIterator {
        type Item = Option<NoGeometry>;
        type Iter = NoGeometryParIterator;

        fn into_par_iter(self) -> Self::Iter {
            NoGeometryParIterator(self)
        }
    }

    impl ParallelIterator for NoGeometryParIterator {
        type Item = Option<NoGeometry>;

        fn drive_unindexed<C>(self, consumer: C) -> C::Result
        where
            C: rayon::iter::plumbing::UnindexedConsumer<Self::Item>,
        {
            rayon::iter::plumbing::bridge(self, consumer)
        }
    }

    impl Producer for NoGeometryParIterator {
        type Item = Option<NoGeometry>;

        type IntoIter = NoGeometryIterator;

        fn into_iter(self) -> Self::IntoIter {
            self.0
        }

        fn split_at(self, index: usize) -> (Self, Self) {
            let n = self.0.len();
            let left_len = (0..index).len();
            let right_len = (index..n).len();

            let left = NoGeometryIterator(std::iter::repeat_n(None, left_len));

            let right = NoGeometryIterator(std::iter::repeat_n(None, right_len));

            (Self(left), Self(right))
        }
    }

    impl IndexedParallelIterator for NoGeometryParIterator {
        fn len(&self) -> usize {
            self.0.len()
        }

        fn drive<C: rayon::iter::plumbing::Consumer<Self::Item>>(self, consumer: C) -> C::Result {
            rayon::iter::plumbing::bridge(self, consumer)
        }

        fn with_producer<CB: rayon::iter::plumbing::ProducerCallback<Self::Item>>(
            self,
            callback: CB,
        ) -> CB::Output {
            callback.callback(self)
        }
    }
}

#[allow(clippy::into_iter_without_iter)] // we provide `.geometries()` instead
impl<'a> IntoIterator for &'a DataCollection {
    type Item = FeatureCollectionRow<'a, NoGeometry>;
    type IntoIter = FeatureCollectionIterator<'a, std::iter::Repeat<NoGeometry>>;

    fn into_iter(self) -> Self::IntoIter {
        FeatureCollectionIterator::new::<NoGeometry>(self, std::iter::repeat(NoGeometry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::collections::BuilderProvider;
    use crate::collections::FeatureCollectionModifications;
    use crate::primitives::CacheHint;
    use crate::primitives::{
        DataRef, FeatureData, FeatureDataRef, FeatureDataType, FeatureDataValue, TimeInterval,
    };

    #[test]
    fn time_intervals() {
        let mut builder = DataCollection::builder().finish_header();

        builder.push_time_interval(TimeInterval::default());
        builder.finish_row();
        builder.push_time_interval(TimeInterval::new(0, 1).unwrap());
        builder.finish_row();
        builder.push_time_interval(TimeInterval::new(2, 3).unwrap());
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
            .add_column("a".into(), FeatureDataType::Int)
            .unwrap();
        let mut builder = builder.finish_header();

        builder.push_time_interval(TimeInterval::default());
        builder
            .push_data("a", FeatureDataValue::NullableInt(Some(42)))
            .unwrap();
        builder.finish_row();
        builder.push_time_interval(TimeInterval::new(0, 1).unwrap());
        builder
            .push_data("a", FeatureDataValue::Float(13.37))
            .unwrap_err();
        builder
            .push_data("a", FeatureDataValue::NullableInt(None))
            .unwrap();
        builder.finish_row();
        builder.push_time_interval(TimeInterval::new(2, 3).unwrap());
        builder
            .push_data("a", FeatureDataValue::NullableInt(Some(1337)))
            .unwrap();
        builder.finish_row();

        let collection = builder.build().unwrap();

        assert_eq!(collection.len(), 3);

        if let FeatureDataRef::Int(a_column) = collection.data("a").unwrap() {
            assert_eq!(a_column.as_ref()[0], 42);
            assert!(a_column.nulls()[1]);
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
                ("foo".to_string(), FeatureData::Int(vec![1, 2, 3])),
                (
                    "bar".to_string(),
                    FeatureData::Text(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                ),
            ]
            .iter()
            .cloned()
            .collect(),
            CacheHint::default(),
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

    #[test]
    fn distinguish_null_and_empty_strings() {
        let pc = DataCollection::from_data(
            vec![],
            vec![TimeInterval::default(); 2],
            [(
                "foo".to_string(),
                FeatureData::NullableText(vec![None, Some(String::new())]),
            )]
            .into_iter()
            .collect(),
            CacheHint::default(),
        )
        .unwrap();
        let column = pc.data("foo").unwrap();

        assert_eq!(column.nulls(), vec![true, false]);
        assert_eq!(
            column.get_unchecked(0),
            FeatureDataValue::NullableText(None)
        );
        assert_eq!(
            column.get_unchecked(1),
            FeatureDataValue::NullableText(Some(String::new()))
        );
    }

    #[test]
    fn check_has_nulls() {
        let pc = DataCollection::from_data(
            vec![],
            vec![TimeInterval::default(); 1],
            [
                ("int1".to_string(), FeatureData::NullableInt(vec![Some(42)])),
                ("int2".to_string(), FeatureData::NullableInt(vec![None])),
                (
                    "text1".to_string(),
                    FeatureData::NullableText(vec![Some("a".to_string())]),
                ),
                ("text2".to_string(), FeatureData::NullableText(vec![None])),
            ]
            .into_iter()
            .collect(),
            CacheHint::default(),
        )
        .unwrap();

        assert!(!pc.data("int1").unwrap().has_nulls());
        assert!(pc.data("int2").unwrap().has_nulls());
        assert!(!pc.data("text1").unwrap().has_nulls());
        assert!(pc.data("text2").unwrap().has_nulls());
    }

    #[test]
    fn iterator() {
        let collection = DataCollection::from_data(
            vec![],
            vec![TimeInterval::default(); 3],
            [
                ("foo".to_string(), FeatureData::Int(vec![1, 2, 3])),
                (
                    "bar".to_string(),
                    FeatureData::Text(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                ),
            ]
            .iter()
            .cloned()
            .collect(),
            CacheHint::default(),
        )
        .unwrap();
        let mut iter = collection.into_iter();

        let row = iter.next().unwrap();
        assert_eq!(NoGeometry, row.geometry);
        assert_eq!(TimeInterval::default(), row.time_interval);
        assert_eq!(Some(FeatureDataValue::Int(1)), row.get("foo"));
        assert_eq!(
            Some(FeatureDataValue::Text("a".to_string())),
            row.get("bar")
        );

        let row = iter.next().unwrap();
        assert_eq!(NoGeometry, row.geometry);
        assert_eq!(TimeInterval::default(), row.time_interval);
        assert_eq!(Some(FeatureDataValue::Int(2)), row.get("foo"));
        assert_eq!(
            Some(FeatureDataValue::Text("b".to_string())),
            row.get("bar")
        );

        let row = iter.next().unwrap();
        assert_eq!(NoGeometry, row.geometry);
        assert_eq!(TimeInterval::default(), row.time_interval);
        assert_eq!(Some(FeatureDataValue::Int(3)), row.get("foo"));
        assert_eq!(
            Some(FeatureDataValue::Text("c".to_string())),
            row.get("bar")
        );

        assert!(iter.next().is_none());
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
