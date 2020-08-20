use arrow::array::{Array, FixedSizeListArray, Float64Array, ListArray};

use crate::collections::{
    FeatureCollection, FeatureCollectionRowBuilder, GeoFeatureCollectionRowBuilder,
    IntoGeometryIterator,
};
use crate::primitives::{Coordinate2D, MultiPoint, MultiPointRef};
use crate::util::arrow::downcast_array;
use crate::util::Result;
use std::slice;

/// This collection contains temporal multi points and miscellaneous data.
pub type MultiPointCollection = FeatureCollection<MultiPoint>;

impl<'l> IntoGeometryIterator<'l> for MultiPointCollection {
    type GeometryIterator = MultiPointIterator<'l>;
    type GeometryType = MultiPointRef<'l>;

    fn geometries(&'l self) -> Self::GeometryIterator {
        let geometry_column: &ListArray = downcast_array(
            &self
                .table
                .column_by_name(MultiPointCollection::GEOMETRY_COLUMN_NAME)
                .expect("Column must exist since it is in the metadata"),
        );

        Self::GeometryIterator::new(geometry_column, self.len())
    }
}

/// A collection iterator for multi points
pub struct MultiPointIterator<'l> {
    geometry_column: &'l ListArray,
    index: usize,
    length: usize,
}

impl<'l> MultiPointIterator<'l> {
    pub fn new(geometry_column: &'l ListArray, length: usize) -> Self {
        Self {
            geometry_column,
            index: 0,
            length,
        }
    }
}

impl<'l> Iterator for MultiPointIterator<'l> {
    type Item = MultiPointRef<'l>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.length {
            return None;
        }

        let multi_point_array_ref = self.geometry_column.value(self.index);
        let multi_point_array: &FixedSizeListArray = downcast_array(&multi_point_array_ref);

        let number_of_points = multi_point_array.len();

        let floats_ref = multi_point_array.value(0);
        let floats: &Float64Array = downcast_array(&floats_ref);

        let multi_point = MultiPointRef::new_unchecked(unsafe {
            slice::from_raw_parts(floats.raw_values() as *const Coordinate2D, number_of_points)
        });

        self.index += 1; // increment!

        Some(multi_point)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.length - self.index;
        (remaining, Some(remaining))
    }

    fn count(self) -> usize {
        self.length - self.index
    }
}

impl GeoFeatureCollectionRowBuilder<MultiPoint> for FeatureCollectionRowBuilder<MultiPoint> {
    fn push_geometry(&mut self, geometry: MultiPoint) -> Result<()> {
        let coordinate_builder = self.geometries_builder.values();

        for _ in geometry.as_ref() {
            coordinate_builder.append(true)?;
        }

        let float_builder = coordinate_builder.values();
        for coordinate in geometry.as_ref() {
            float_builder.append_value(coordinate.x)?;
            float_builder.append_value(coordinate.y)?;
        }

        self.geometries_builder.append(true)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::collections::BuilderProvider;
    use crate::primitives::{
        FeatureData, FeatureDataRef, FeatureDataType, FeatureDataValue, MultiPointAccess,
        NullableDataRef, TimeInterval,
    };
    use serde_json::{from_str, json};
    use std::collections::HashMap;

    #[test]
    fn clone() {
        let pc = MultiPointCollection::empty();
        let cloned = pc.clone();

        assert_eq!(pc.len(), 0);
        assert_eq!(cloned.len(), 0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn data() {
        let pc = MultiPointCollection::from_data(
            MultiPoint::many(vec![vec![(0., 0.)], vec![(1., 1.)], vec![(2., 2.)]]).unwrap(),
            vec![
                TimeInterval::new_unchecked(0, 1),
                TimeInterval::new_unchecked(1, 2),
                TimeInterval::new_unchecked(2, 3),
            ],
            {
                let mut map = HashMap::new();
                map.insert("numbers".into(), FeatureData::Number(vec![0., 1., 2.]));
                map.insert(
                    "number_nulls".into(),
                    FeatureData::NullableNumber(vec![Some(0.), None, Some(2.)]),
                );
                map
            },
        )
        .unwrap();

        assert_eq!(pc.len(), 3);

        if let FeatureDataRef::Number(numbers) = pc.data("numbers").unwrap() {
            assert_eq!(numbers.as_ref(), &[0., 1., 2.]);
        } else {
            unreachable!();
        }

        if let FeatureDataRef::NullableNumber(numbers) = pc.data("number_nulls").unwrap() {
            assert_eq!(numbers.as_ref()[0], 0.);
            assert_eq!(numbers.as_ref()[2], 2.);
            assert_eq!(numbers.nulls(), vec![false, true, false]);
        } else {
            unreachable!();
        }
    }

    #[test]
    fn time_intervals() {
        let pc = MultiPointCollection::from_data(
            MultiPoint::many(vec![vec![(0., 0.)], vec![(1., 1.)], vec![(2., 2.)]]).unwrap(),
            vec![
                TimeInterval::new_unchecked(0, 1),
                TimeInterval::new_unchecked(1, 2),
                TimeInterval::new_unchecked(2, 3),
            ],
            HashMap::new(),
        )
        .unwrap();

        assert_eq!(pc.len(), 3);

        let time_intervals = pc.time_intervals();

        assert_eq!(time_intervals.len(), 3);
        assert_eq!(
            time_intervals,
            &[
                TimeInterval::new_unchecked(0, 1),
                TimeInterval::new_unchecked(1, 2),
                TimeInterval::new_unchecked(2, 3)
            ]
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn add_column() {
        let mut builder = MultiPointCollection::builder();
        builder
            .add_column("foo".into(), FeatureDataType::Number)
            .unwrap();
        let mut builder = builder.finish_header();

        builder
            .push_geometry(Coordinate2D::new(0., 0.).into())
            .unwrap();
        builder
            .push_time_interval(TimeInterval::new_unchecked(0, 1))
            .unwrap();
        builder
            .push_data("foo", FeatureDataValue::Number(0.))
            .unwrap();
        builder.finish_row();

        builder
            .push_geometry(Coordinate2D::new(1., 1.).into())
            .unwrap();
        builder
            .push_time_interval(TimeInterval::new_unchecked(0, 1))
            .unwrap();
        builder
            .push_data("foo", FeatureDataValue::Number(1.))
            .unwrap();
        builder.finish_row();

        let collection = builder.build().unwrap();

        assert_eq!(collection.len(), 2);

        let extended_collection = collection
            .add_column("bar", FeatureData::Number(vec![2., 4.]))
            .unwrap();

        assert_eq!(extended_collection.len(), 2);
        if let FeatureDataRef::Number(numbers) = extended_collection.data("foo").unwrap() {
            assert_eq!(numbers.as_ref(), &[0., 1.]);
        } else {
            unreachable!();
        }
        if let FeatureDataRef::Number(numbers) = extended_collection.data("bar").unwrap() {
            assert_eq!(numbers.as_ref(), &[2., 4.]);
        } else {
            unreachable!();
        }
    }

    #[test]
    fn remove_column() {
        let collection = {
            let mut builder = MultiPointCollection::builder();
            builder
                .add_column("foo".into(), FeatureDataType::Number)
                .unwrap();
            let mut builder = builder.finish_header();

            builder
                .push_geometry(Coordinate2D::new(0., 0.).into())
                .unwrap();
            builder
                .push_time_interval(TimeInterval::new_unchecked(0, 1))
                .unwrap();
            builder
                .push_data("foo", FeatureDataValue::Number(0.))
                .unwrap();
            builder.finish_row();

            builder
                .push_geometry(Coordinate2D::new(1., 1.).into())
                .unwrap();
            builder
                .push_time_interval(TimeInterval::new_unchecked(0, 1))
                .unwrap();
            builder
                .push_data("foo", FeatureDataValue::Number(1.))
                .unwrap();
            builder.finish_row();

            builder.build().unwrap()
        };

        assert_eq!(collection.len(), 2);
        assert!(collection.data("foo").is_ok());

        let reduced_collection = collection.remove_column("foo").unwrap();

        assert_eq!(reduced_collection.len(), 2);
        assert!(reduced_collection.data("foo").is_err());

        assert!(reduced_collection.remove_column("foo").is_err());
    }

    #[test]
    fn filter() {
        let pc = MultiPointCollection::from_data(
            MultiPoint::many(vec![vec![(0., 0.)], vec![(1., 1.)], vec![(2., 2.)]]).unwrap(),
            vec![
                TimeInterval::new_unchecked(0, 1),
                TimeInterval::new_unchecked(1, 2),
                TimeInterval::new_unchecked(2, 3),
            ],
            HashMap::new(),
        )
        .unwrap();

        assert_eq!(pc.len(), 3);

        let filtered = pc.filter(vec![false, true, false]).unwrap();

        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn append() {
        let collection_a = MultiPointCollection::from_data(
            MultiPoint::many(vec![vec![(0., 0.)], vec![(1., 1.)], vec![(2., 2.)]]).unwrap(),
            vec![
                TimeInterval::new_unchecked(0, 1),
                TimeInterval::new_unchecked(1, 2),
                TimeInterval::new_unchecked(2, 3),
            ],
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

        let collection_b = MultiPointCollection::from_data(
            MultiPoint::many(vec![vec![(3., 3.)], vec![(4., 4.)]]).unwrap(),
            vec![
                TimeInterval::new_unchecked(3, 4),
                TimeInterval::new_unchecked(4, 5),
            ],
            [
                ("foo".to_string(), FeatureData::Decimal(vec![4, 5])),
                (
                    "bar".to_string(),
                    FeatureData::Text(vec!["d".to_string(), "e".to_string()]),
                ),
            ]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();

        let collection_c = collection_a.append(&collection_b).unwrap();

        assert_eq!(collection_a.len(), 3);
        assert_eq!(collection_b.len(), 2);
        assert_eq!(collection_c.len(), 5);

        let mut geometry_iter = collection_c.geometries();
        assert_eq!(geometry_iter.next().unwrap().points(), &[(0., 0.).into()]);
        assert_eq!(geometry_iter.next().unwrap().points(), &[(1., 1.).into()]);
        assert_eq!(geometry_iter.next().unwrap().points(), &[(2., 2.).into()]);
        assert_eq!(geometry_iter.next().unwrap().points(), &[(3., 3.).into()]);
        assert_eq!(geometry_iter.next().unwrap().points(), &[(4., 4.).into()]);
        assert!(geometry_iter.next().is_none());

        assert_eq!(
            collection_c.time_intervals(),
            &[
                TimeInterval::new_unchecked(0, 1),
                TimeInterval::new_unchecked(1, 2),
                TimeInterval::new_unchecked(2, 3),
                TimeInterval::new_unchecked(3, 4),
                TimeInterval::new_unchecked(4, 5),
            ]
        );

        if let Ok(FeatureDataRef::Decimal(data_ref)) = collection_c.data("foo") {
            assert_eq!(data_ref.as_ref(), &[1, 2, 3, 4, 5]);
        } else {
            panic!("wrong data type");
        }

        if let Ok(FeatureDataRef::Text(data_ref)) = collection_c.data("bar") {
            assert_eq!(data_ref.text_at(0).unwrap(), "a");
            assert_eq!(data_ref.text_at(1).unwrap(), "b");
            assert_eq!(data_ref.text_at(2).unwrap(), "c");
            assert_eq!(data_ref.text_at(3).unwrap(), "d");
            assert_eq!(data_ref.text_at(4).unwrap(), "e");
        } else {
            panic!("wrong data type");
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn to_geo_json() {
        let collection = {
            let mut builder = MultiPointCollection::builder();
            builder
                .add_column("foo".into(), FeatureDataType::Number)
                .unwrap();
            builder
                .add_column("bar".into(), FeatureDataType::NullableText)
                .unwrap();
            let mut builder = builder.finish_header();

            builder
                .push_geometry(Coordinate2D::new(0., 0.).into())
                .unwrap();
            builder
                .push_time_interval(TimeInterval::new_unchecked(0, 1))
                .unwrap();
            builder
                .push_data("foo", FeatureDataValue::Number(0.))
                .unwrap();
            builder
                .push_data(
                    "bar",
                    FeatureDataValue::NullableText(Some("one".to_string())),
                )
                .unwrap();
            builder.finish_row();

            builder
                .push_geometry(MultiPoint::new(vec![(1., 1.).into(), (2., 2.).into()]).unwrap())
                .unwrap();
            builder
                .push_time_interval(TimeInterval::new_unchecked(1, 2))
                .unwrap();
            builder
                .push_data("foo", FeatureDataValue::Number(1.))
                .unwrap();
            builder
                .push_data("bar", FeatureDataValue::NullableText(None))
                .unwrap();
            builder.finish_row();

            builder
                .push_geometry(Coordinate2D::new(3., 3.).into())
                .unwrap();
            builder
                .push_time_interval(TimeInterval::new_unchecked(3, 4))
                .unwrap();
            builder
                .push_data("foo", FeatureDataValue::Number(2.))
                .unwrap();
            builder
                .push_data(
                    "bar",
                    FeatureDataValue::NullableText(Some("three".to_string())),
                )
                .unwrap();
            builder.finish_row();

            builder.build().unwrap()
        };

        assert_eq!(
            from_str::<serde_json::Value>(collection.to_geo_json().as_str()).unwrap(),
            json!({
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [0.0, 0.0]
                    },
                    "properties": {
                        "bar": "one",
                        "foo": 0.0
                    },
                    "when": {
                        "start": "1970-01-01T00:00:00+00:00",
                        "end": "1970-01-01T00:00:00.001+00:00",
                        "type": "Interval"
                    }
                }, {
                    "type": "Feature",
                    "geometry": {
                        "type": "MultiPoint",
                        "coordinates": [
                            [1.0, 1.0],
                            [2.0, 2.0]
                        ]
                    },
                    "properties": {
                        "bar": null,
                        "foo": 1.0
                    },
                    "when": {
                        "start": "1970-01-01T00:00:00.001+00:00",
                        "end": "1970-01-01T00:00:00.002+00:00",
                        "type": "Interval"
                    }
                }, {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [3.0, 3.0]
                    },
                    "properties": {
                        "bar": "three",
                        "foo": 2.0
                    },
                    "when": {
                        "start": "1970-01-01T00:00:00.003+00:00",
                        "end": "1970-01-01T00:00:00.004+00:00",
                        "type": "Interval"
                    }
                }]
            })
        );
    }

    #[test]
    fn reserved_columns_in_builder() {
        let mut builder = MultiPointCollection::builder();

        builder
            .add_column("foobar".to_string(), FeatureDataType::Number)
            .unwrap();
        builder
            .add_column("foobar".to_string(), FeatureDataType::Text)
            .unwrap_err();
        builder
            .add_column("__geometry".to_string(), FeatureDataType::Number)
            .unwrap_err();
    }

    #[test]
    fn clone2() {
        let pc = MultiPointCollection::from_data(
            MultiPoint::many(vec![vec![(0., 0.)], vec![(1., 1.)]]).unwrap(),
            vec![
                TimeInterval::new_unchecked(0, 1),
                TimeInterval::new_unchecked(0, 1),
            ],
            {
                let mut map = HashMap::new();
                map.insert("number".into(), FeatureData::Number(vec![0., 1.]));
                map
            },
        )
        .unwrap();

        let cloned = pc.clone();

        assert_eq!(pc.len(), cloned.len());
        assert_eq!(pc, cloned);
    }

    #[test]
    fn equals_builder_from_data() {
        let a = MultiPointCollection::from_data(
            MultiPoint::many(vec![vec![(0., 0.)], vec![(1., 1.)]]).unwrap(),
            vec![
                TimeInterval::new_unchecked(0, 1),
                TimeInterval::new_unchecked(0, 1),
            ],
            {
                let mut map = HashMap::new();
                map.insert("number".into(), FeatureData::Number(vec![0., 1.]));
                map
            },
        )
        .unwrap();

        let b = {
            let mut builder = MultiPointCollection::builder();
            builder
                .add_column("number".into(), FeatureDataType::Number)
                .unwrap();
            let mut builder = builder.finish_header();

            assert!(builder.is_empty());

            builder
                .push_geometry(Coordinate2D::new(0., 0.).into())
                .unwrap();
            builder
                .push_time_interval(TimeInterval::new_unchecked(0, 1))
                .unwrap();
            builder
                .push_data("number", FeatureDataValue::Number(0.))
                .unwrap();
            builder.finish_row();
            builder
                .push_geometry(Coordinate2D::new(1., 1.).into())
                .unwrap();
            builder
                .push_time_interval(TimeInterval::new_unchecked(0, 1))
                .unwrap();
            builder
                .push_data("number", FeatureDataValue::Number(1.))
                .unwrap();
            builder.finish_row();

            assert_eq!(builder.len(), 2);

            builder.build().unwrap()
        };

        assert_eq!(a.len(), b.len());
        assert_eq!(a, b);
    }

    #[test]
    fn nan_equals() {
        let collection = {
            let mut builder = MultiPointCollection::builder();
            builder
                .add_column("number".into(), FeatureDataType::Number)
                .unwrap();
            let mut builder = builder.finish_header();

            assert!(builder.is_empty());

            builder
                .push_geometry(Coordinate2D::new(0., 0.).into())
                .unwrap();
            builder
                .push_time_interval(TimeInterval::new_unchecked(0, 1))
                .unwrap();
            builder
                .push_data("number", FeatureDataValue::Number(f64::NAN))
                .unwrap();
            builder.finish_row();

            assert!(!builder.is_empty());

            builder.build().unwrap()
        };

        assert_eq!(collection, collection);
    }

    #[test]
    fn null_equals() {
        let collection = {
            let mut builder = MultiPointCollection::builder();
            builder
                .add_column("number".into(), FeatureDataType::NullableNumber)
                .unwrap();
            let mut builder = builder.finish_header();

            builder
                .push_geometry(Coordinate2D::new(0., 0.).into())
                .unwrap();
            builder
                .push_time_interval(TimeInterval::new_unchecked(0, 1))
                .unwrap();
            builder
                .push_data("number", FeatureDataValue::NullableNumber(None))
                .unwrap();
            builder.finish_row();

            builder.build().unwrap()
        };

        assert_eq!(collection, collection);
    }
}
