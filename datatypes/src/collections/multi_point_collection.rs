use arrow::{
    array::{Array, ArrayData, FixedSizeListArray, Float64Array, ListArray},
    buffer::Buffer,
    datatypes::DataType,
};
use rayon::{
    iter::plumbing::Producer,
    prelude::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator},
};

use crate::collections::{
    FeatureCollection, FeatureCollectionInfos, FeatureCollectionIterator, FeatureCollectionRow,
    FeatureCollectionRowBuilder, GeoFeatureCollectionRowBuilder, GeometryCollection,
    GeometryRandomAccess, IntoGeometryIterator,
};
use crate::primitives::{Coordinate2D, MultiPoint, MultiPointRef};
use crate::util::arrow::downcast_array;
use crate::util::{arrow::ArrowTyped, Result};
use std::{slice, sync::Arc};

use super::geo_feature_collection::ReplaceRawArrayCoords;

/// This collection contains temporal multi points and miscellaneous data.
pub type MultiPointCollection = FeatureCollection<MultiPoint>;

impl<'l> IntoGeometryIterator<'l> for MultiPointCollection {
    type GeometryIterator = MultiPointIterator<'l>;
    type GeometryType = MultiPointRef<'l>;

    fn geometries(&'l self) -> Self::GeometryIterator {
        let geometry_column: &ListArray = downcast_array(
            self.table
                .column_by_name(MultiPointCollection::GEOMETRY_COLUMN_NAME)
                .expect("Column must exist since it is in the metadata"),
        );

        Self::GeometryIterator::new(geometry_column, self.len())
    }
}

impl<'a> IntoIterator for &'a MultiPointCollection {
    type Item = FeatureCollectionRow<'a, MultiPointRef<'a>>;
    type IntoIter = FeatureCollectionIterator<'a, MultiPointIterator<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        FeatureCollectionIterator::new::<MultiPoint>(self, self.geometries())
    }
}

/// A collection iterator for multi points
pub struct MultiPointIterator<'l> {
    geometry_column: &'l ListArray,
    index: usize,
    length: usize,
}

/// A parallel collection iterator for multi points
pub struct MultiPointParIterator<'l>(MultiPointIterator<'l>);

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
            slice::from_raw_parts(
                floats.values().as_ptr().cast::<Coordinate2D>(),
                number_of_points,
            )
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

impl<'l> DoubleEndedIterator for MultiPointIterator<'l> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index >= self.length {
            return None;
        }

        let multi_point_array_ref = self.geometry_column.value(self.length - 1);
        let multi_point_array: &FixedSizeListArray = downcast_array(&multi_point_array_ref);

        let number_of_points = multi_point_array.len();

        let floats_ref = multi_point_array.value(0);
        let floats: &Float64Array = downcast_array(&floats_ref);

        let multi_point = MultiPointRef::new_unchecked(unsafe {
            slice::from_raw_parts(
                floats.values().as_ptr().cast::<Coordinate2D>(),
                number_of_points,
            )
        });

        self.length -= 1; // decrement!

        Some(multi_point)
    }
}

impl<'l> ExactSizeIterator for MultiPointIterator<'l> {}

impl<'l> IntoParallelIterator for MultiPointIterator<'l> {
    type Item = MultiPointRef<'l>;
    type Iter = MultiPointParIterator<'l>;

    fn into_par_iter(self) -> Self::Iter {
        MultiPointParIterator(self)
    }
}

impl<'l> ParallelIterator for MultiPointParIterator<'l> {
    type Item = MultiPointRef<'l>;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: rayon::iter::plumbing::UnindexedConsumer<Self::Item>,
    {
        rayon::iter::plumbing::bridge(self, consumer)
    }
}

impl<'l> Producer for MultiPointParIterator<'l> {
    type Item = MultiPointRef<'l>;

    type IntoIter = MultiPointIterator<'l>;

    fn into_iter(self) -> Self::IntoIter {
        self.0
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        // Example:
        //   Index: 0, Length 3
        //   Split at 1
        //   Left: Index: 0, Length: 1 -> Elements: 0
        //   Right: Index: 1, Length: 3 -> Elements: 1, 2

        // The index is between self.0.index and self.0.length,
        // so we have to transform it to a global index
        let global_index = self.0.index + index;

        let left = Self(Self::IntoIter {
            geometry_column: self.0.geometry_column,
            index: self.0.index,
            length: global_index,
        });

        let right = Self(Self::IntoIter {
            geometry_column: self.0.geometry_column,
            index: global_index,
            length: self.0.length,
        });

        (left, right)
    }
}

impl<'l> IndexedParallelIterator for MultiPointParIterator<'l> {
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

impl<'l> GeometryRandomAccess<'l> for MultiPointCollection {
    type GeometryType = MultiPointRef<'l>;

    fn geometry_at(&'l self, index: usize) -> Option<Self::GeometryType> {
        let geometry_column: &ListArray = downcast_array(
            self.table
                .column_by_name(MultiPointCollection::GEOMETRY_COLUMN_NAME)
                .expect("Column must exist since it is in the metadata"),
        );

        if index >= self.len() {
            return None;
        }

        let multi_point_array_ref = geometry_column.value(index);
        let multi_point_array: &FixedSizeListArray = downcast_array(&multi_point_array_ref);

        let number_of_points = multi_point_array.len();

        let floats_ref = multi_point_array.value(0);
        let floats: &Float64Array = downcast_array(&floats_ref);

        let multi_point = MultiPointRef::new_unchecked(unsafe {
            slice::from_raw_parts(
                floats.values().as_ptr().cast::<Coordinate2D>(),
                number_of_points,
            )
        });

        Some(multi_point)
    }
}

impl GeoFeatureCollectionRowBuilder<MultiPoint> for FeatureCollectionRowBuilder<MultiPoint> {
    fn push_geometry(&mut self, geometry: MultiPoint) {
        let coordinate_builder = self.geometries_builder.values();

        for _ in geometry.as_ref() {
            coordinate_builder.append(true);
        }

        let float_builder = coordinate_builder.values();
        for coordinate in geometry.as_ref() {
            float_builder.append_value(coordinate.x);
            float_builder.append_value(coordinate.y);
        }

        self.geometries_builder.append(true);
    }
}

impl GeometryCollection for MultiPointCollection {
    fn coordinates(&self) -> &[Coordinate2D] {
        let geometries_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There should exist a geometry column because it is added during creation of the collection");
        let geometries: &ListArray = downcast_array(geometries_ref);

        let coordinates_ref = geometries.values();
        let coordinates: &FixedSizeListArray = downcast_array(coordinates_ref);

        let number_of_coordinates = coordinates.len();

        let floats_ref = coordinates.values();
        let floats: &Float64Array = downcast_array(floats_ref);

        unsafe {
            slice::from_raw_parts(
                floats.values().as_ptr().cast::<Coordinate2D>(),
                number_of_coordinates,
            )
        }
    }

    fn feature_offsets(&self) -> &[i32] {
        let geometries_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There should exist a geometry column because it is added during creation of the collection");
        let geometries: &ListArray = downcast_array(geometries_ref);

        geometries.offsets()
    }
}

impl ReplaceRawArrayCoords for MultiPointCollection {
    fn replace_raw_coords(array_ref: &Arc<dyn Array>, new_coords: Buffer) -> Result<ArrayData> {
        let geometries: &ListArray = downcast_array(array_ref);
        let offsets_buffer = geometries.offsets();
        let num_features = geometries.len();

        let num_coords = new_coords.len() / std::mem::size_of::<Coordinate2D>();
        let num_floats = num_coords * 2;

        Ok(ArrayData::builder(MultiPoint::arrow_data_type())
            .len(num_features)
            .add_buffer(offsets_buffer.inner().inner().clone())
            .add_child_data(
                ArrayData::builder(Coordinate2D::arrow_data_type())
                    .len(num_coords)
                    .add_child_data(
                        ArrayData::builder(DataType::Float64)
                            .len(num_floats)
                            .add_buffer(new_coords)
                            .build()?,
                    )
                    .build()?,
            )
            .build()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::collections::feature_collection::ChunksEqualIgnoringCacheHint;
    use crate::collections::{BuilderProvider, FeatureCollectionModifications, ToGeoJson};
    use crate::operations::reproject::Reproject;
    use crate::primitives::CacheHint;
    use crate::primitives::{
        DataRef, FeatureData, FeatureDataRef, FeatureDataType, FeatureDataValue, MultiPointAccess,
        TimeInstance, TimeInterval,
    };
    use float_cmp::approx_eq;
    use serde_json::{from_str, json};
    use std::collections::HashMap;

    #[test]
    #[allow(clippy::redundant_clone)] // this test is about the clone implementation
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
                map.insert("numbers".into(), FeatureData::Float(vec![0., 1., 2.]));
                map.insert(
                    "number_nulls".into(),
                    FeatureData::NullableFloat(vec![Some(0.), None, Some(2.)]),
                );
                map
            },
            CacheHint::default(),
        )
        .unwrap();

        assert_eq!(pc.len(), 3);

        if let FeatureDataRef::Float(numbers) = pc.data("numbers").unwrap() {
            assert_eq!(numbers.as_ref(), &[0., 1., 2.]);
        } else {
            unreachable!();
        }

        if let FeatureDataRef::Float(numbers) = pc.data("number_nulls").unwrap() {
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
            CacheHint::default(),
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
            .add_column("foo".into(), FeatureDataType::Float)
            .unwrap();
        let mut builder = builder.finish_header();

        builder.push_geometry(Coordinate2D::new(0., 0.).into());
        builder.push_time_interval(TimeInterval::new_unchecked(0, 1));
        builder
            .push_data("foo", FeatureDataValue::Float(0.))
            .unwrap();
        builder.finish_row();

        builder.push_geometry(Coordinate2D::new(1., 1.).into());
        builder.push_time_interval(TimeInterval::new_unchecked(0, 1));
        builder
            .push_data("foo", FeatureDataValue::Float(1.))
            .unwrap();
        builder.finish_row();

        let collection = builder.build().unwrap();

        assert_eq!(collection.len(), 2);

        let extended_collection = collection
            .add_column("bar", FeatureData::Float(vec![2., 4.]))
            .unwrap();

        assert_eq!(extended_collection.len(), 2);
        if let FeatureDataRef::Float(numbers) = extended_collection.data("foo").unwrap() {
            assert_eq!(numbers.as_ref(), &[0., 1.]);
        } else {
            unreachable!();
        }
        if let FeatureDataRef::Float(numbers) = extended_collection.data("bar").unwrap() {
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
                .add_column("foo".into(), FeatureDataType::Float)
                .unwrap();
            let mut builder = builder.finish_header();

            builder.push_geometry(Coordinate2D::new(0., 0.).into());
            builder.push_time_interval(TimeInterval::new_unchecked(0, 1));
            builder
                .push_data("foo", FeatureDataValue::Float(0.))
                .unwrap();
            builder.finish_row();

            builder.push_geometry(Coordinate2D::new(1., 1.).into());
            builder.push_time_interval(TimeInterval::new_unchecked(0, 1));
            builder
                .push_data("foo", FeatureDataValue::Float(1.))
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
            CacheHint::default(),
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

        let collection_b = MultiPointCollection::from_data(
            MultiPoint::many(vec![vec![(3., 3.)], vec![(4., 4.)]]).unwrap(),
            vec![
                TimeInterval::new_unchecked(3, 4),
                TimeInterval::new_unchecked(4, 5),
            ],
            [
                ("foo".to_string(), FeatureData::Int(vec![4, 5])),
                (
                    "bar".to_string(),
                    FeatureData::Text(vec!["d".to_string(), "e".to_string()]),
                ),
            ]
            .iter()
            .cloned()
            .collect(),
            CacheHint::default(),
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

        if let Ok(FeatureDataRef::Int(data_ref)) = collection_c.data("foo") {
            assert_eq!(data_ref.as_ref(), &[1, 2, 3, 4, 5]);
        } else {
            panic!("wrong data type");
        }

        if let Ok(FeatureDataRef::Text(data_ref)) = collection_c.data("bar") {
            assert_eq!(data_ref.text_at(0).unwrap().unwrap(), "a");
            assert_eq!(data_ref.text_at(1).unwrap().unwrap(), "b");
            assert_eq!(data_ref.text_at(2).unwrap().unwrap(), "c");
            assert_eq!(data_ref.text_at(3).unwrap().unwrap(), "d");
            assert_eq!(data_ref.text_at(4).unwrap().unwrap(), "e");
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
                .add_column("foo".into(), FeatureDataType::Float)
                .unwrap();
            builder
                .add_column("bar".into(), FeatureDataType::Text)
                .unwrap();
            let mut builder = builder.finish_header();

            builder.push_geometry(Coordinate2D::new(0., 0.).into());
            builder.push_time_interval(TimeInterval::new_unchecked(0, 1));
            builder
                .push_data("foo", FeatureDataValue::Float(0.))
                .unwrap();
            builder
                .push_data(
                    "bar",
                    FeatureDataValue::NullableText(Some("one".to_string())),
                )
                .unwrap();
            builder.finish_row();

            builder.push_geometry(MultiPoint::new(vec![(1., 1.).into(), (2., 2.).into()]).unwrap());
            builder.push_time_interval(TimeInterval::new_unchecked(1, 2));
            builder
                .push_data("foo", FeatureDataValue::Float(1.))
                .unwrap();
            builder
                .push_data("bar", FeatureDataValue::NullableText(None))
                .unwrap();
            builder.finish_row();

            builder.push_geometry(Coordinate2D::new(3., 3.).into());
            builder.push_time_interval(TimeInterval::new_unchecked(3, 4));
            builder
                .push_data("foo", FeatureDataValue::Float(2.))
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
            .add_column("foobar".to_string(), FeatureDataType::Float)
            .unwrap();
        builder
            .add_column("foobar".to_string(), FeatureDataType::Text)
            .unwrap_err();
        builder
            .add_column("__geometry".to_string(), FeatureDataType::Float)
            .unwrap_err();
    }

    #[test]
    #[allow(clippy::redundant_clone)] // this test is about the clone implementation
    fn clone2() {
        let pc = MultiPointCollection::from_data(
            MultiPoint::many(vec![vec![(0., 0.)], vec![(1., 1.)]]).unwrap(),
            vec![
                TimeInterval::new_unchecked(0, 1),
                TimeInterval::new_unchecked(0, 1),
            ],
            {
                let mut map = HashMap::new();
                map.insert("number".into(), FeatureData::Float(vec![0., 1.]));
                map
            },
            CacheHint::default(),
        )
        .unwrap();

        let cloned = pc.clone();

        assert_eq!(pc.len(), cloned.len());
        assert!(pc.chunks_equal_ignoring_cache_hint(&cloned));
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
                map.insert("number".into(), FeatureData::Float(vec![0., 1.]));
                map
            },
            CacheHint::default(),
        )
        .unwrap();

        let b = {
            let mut builder = MultiPointCollection::builder();
            builder
                .add_column("number".into(), FeatureDataType::Float)
                .unwrap();
            let mut builder = builder.finish_header();

            assert!(builder.is_empty());

            builder.push_geometry(Coordinate2D::new(0., 0.).into());
            builder.push_time_interval(TimeInterval::new_unchecked(0, 1));
            builder
                .push_data("number", FeatureDataValue::Float(0.))
                .unwrap();
            builder.finish_row();
            builder.push_geometry(Coordinate2D::new(1., 1.).into());
            builder.push_time_interval(TimeInterval::new_unchecked(0, 1));
            builder
                .push_data("number", FeatureDataValue::Float(1.))
                .unwrap();
            builder.finish_row();

            assert_eq!(builder.len(), 2);

            builder.build().unwrap()
        };

        assert_eq!(a.len(), b.len());
        assert!(a.chunks_equal_ignoring_cache_hint(&b));
    }

    #[test]
    fn nan_equals() {
        let collection = {
            let mut builder = MultiPointCollection::builder();
            builder
                .add_column("number".into(), FeatureDataType::Float)
                .unwrap();
            let mut builder = builder.finish_header();

            assert!(builder.is_empty());

            builder.push_geometry(Coordinate2D::new(0., 0.).into());
            builder.push_time_interval(TimeInterval::new_unchecked(0, 1));
            builder
                .push_data("number", FeatureDataValue::Float(f64::NAN))
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
                .add_column("number".into(), FeatureDataType::Float)
                .unwrap();
            let mut builder = builder.finish_header();

            builder.push_geometry(Coordinate2D::new(0., 0.).into());
            builder.push_time_interval(TimeInterval::new_unchecked(0, 1));
            builder
                .push_data("number", FeatureDataValue::NullableFloat(None))
                .unwrap();
            builder.finish_row();

            builder.build().unwrap()
        };

        assert_eq!(collection, collection);
    }

    #[test]
    fn range_filter_int() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                (0.0, 0.1),
                (1.0, 1.1),
                (2.0, 3.1),
                (3.0, 3.1),
                (4.0, 4.1),
            ])
            .unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 5],
            [("foo".to_string(), FeatureData::Int(vec![0, 1, 2, 3, 4]))]
                .iter()
                .cloned()
                .collect(),
            CacheHint::default(),
        )
        .unwrap();

        assert_eq!(
            collection
                .column_range_filter(
                    "foo",
                    &[FeatureDataValue::Int(1)..=FeatureDataValue::Int(3)],
                    false
                )
                .unwrap(),
            collection
                .filter(vec![false, true, true, true, false])
                .unwrap()
        );

        assert_eq!(
            collection
                .column_range_filter(
                    "foo",
                    &[FeatureDataValue::Int(1)..FeatureDataValue::Int(3)],
                    false
                )
                .unwrap(),
            collection
                .filter(vec![false, true, true, false, false])
                .unwrap()
        );

        assert_eq!(
            collection
                .column_range_filter(
                    "foo",
                    &[
                        (FeatureDataValue::Int(0)..=FeatureDataValue::Int(0)),
                        (FeatureDataValue::Int(4)..=FeatureDataValue::Int(4))
                    ],
                    false
                )
                .unwrap(),
            collection
                .filter(vec![true, false, false, false, true])
                .unwrap()
        );
    }

    #[test]
    fn range_filter_float() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                (0.0, 0.1),
                (1.0, 1.1),
                (2.0, 3.1),
                (3.0, 3.1),
                (4.0, 4.1),
            ])
            .unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 5],
            [(
                "foo".to_string(),
                FeatureData::Float(vec![0., 1., 2., 3., 4.]),
            )]
            .iter()
            .cloned()
            .collect(),
            CacheHint::default(),
        )
        .unwrap();

        assert_eq!(
            collection
                .column_range_filter("foo", &[FeatureDataValue::Float(1.5)..], false)
                .unwrap(),
            collection
                .filter(vec![false, false, true, true, true])
                .unwrap()
        );
    }

    #[test]
    fn range_filter_text() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                (0.0, 0.1),
                (1.0, 1.1),
                (2.0, 3.1),
                (3.0, 3.1),
                (4.0, 4.1),
            ])
            .unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 5],
            [(
                "foo".to_string(),
                FeatureData::Text(vec![
                    "aaa".to_string(),
                    "bbb".to_string(),
                    "ccc".to_string(),
                    "ddd".to_string(),
                    "eee".to_string(),
                ]),
            )]
            .iter()
            .cloned()
            .collect(),
            CacheHint::default(),
        )
        .unwrap();

        assert_eq!(
            collection
                .column_range_filter("foo", &[..FeatureDataValue::Text("c".into())], false)
                .unwrap(),
            collection
                .filter(vec![true, true, false, false, false])
                .unwrap()
        );
    }

    #[test]
    fn range_filter_bool() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                (0.0, 0.1),
                (1.0, 1.1),
                (2.0, 3.1),
                (3.0, 3.1),
                (4.0, 4.1),
            ])
            .unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 5],
            [(
                "foo".to_string(),
                FeatureData::Bool(vec![false, false, true, true, true]),
            )]
            .iter()
            .cloned()
            .collect(),
            CacheHint::default(),
        )
        .unwrap();

        assert_eq!(
            collection
                .column_range_filter("foo", &[FeatureDataValue::Bool(true)..], false)
                .unwrap(),
            collection
                .filter(vec![false, false, true, true, true])
                .unwrap()
        );
    }

    #[test]
    fn range_filter_datetime() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                (0.0, 0.1),
                (1.0, 1.1),
                (2.0, 3.1),
                (3.0, 3.1),
                (4.0, 4.1),
            ])
            .unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 5],
            [(
                "foo".to_string(),
                FeatureData::DateTime(vec![
                    TimeInstance::from_millis_unchecked(0),
                    TimeInstance::from_millis_unchecked(10),
                    TimeInstance::from_millis_unchecked(20),
                    TimeInstance::from_millis_unchecked(30),
                    TimeInstance::from_millis_unchecked(40),
                ]),
            )]
            .iter()
            .cloned()
            .collect(),
            CacheHint::default(),
        )
        .unwrap();

        assert_eq!(
            collection
                .column_range_filter(
                    "foo",
                    &[FeatureDataValue::DateTime(TimeInstance::from_millis_unchecked(15))..],
                    false
                )
                .unwrap(),
            collection
                .filter(vec![false, false, true, true, true])
                .unwrap()
        );
    }

    #[test]
    fn range_filter_null() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                (0.0, 0.1),
                (1.0, 1.1),
                (2.0, 3.1),
                (3.0, 3.1),
                (4.0, 4.1),
            ])
            .unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 5],
            [(
                "foo".to_string(),
                FeatureData::NullableInt(vec![Some(0), None, Some(2), Some(3), Some(4)]),
            )]
            .iter()
            .cloned()
            .collect(),
            CacheHint::default(),
        )
        .unwrap();

        assert_eq!(
            collection
                .column_range_filter(
                    "foo",
                    &[FeatureDataValue::Int(1)..=FeatureDataValue::Int(3)],
                    false
                )
                .unwrap(),
            collection
                .filter(vec![false, false, true, true, false])
                .unwrap()
        );

        assert_eq!(
            collection
                .column_range_filter(
                    "foo",
                    &[FeatureDataValue::Int(1)..=FeatureDataValue::Int(3)],
                    true
                )
                .unwrap(),
            collection
                .filter(vec![false, true, true, true, false])
                .unwrap()
        );
    }

    #[test]
    fn serde() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            [
                (
                    "foo".to_string(),
                    FeatureData::NullableInt(vec![Some(0), None, Some(2)]),
                ),
                (
                    "bar".to_string(),
                    FeatureData::Text(vec!["a".into(), "b".into(), "c".into()]),
                ),
            ]
            .iter()
            .cloned()
            .collect(),
            CacheHint::default(),
        )
        .unwrap();

        let serialized = serde_json::to_string(&collection).unwrap();
        let deserialized: MultiPointCollection = serde_json::from_str(&serialized).unwrap();

        assert!(collection.chunks_equal_ignoring_cache_hint(&deserialized));
    }

    #[test]
    fn coordinates() {
        let pc = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                vec![(0., 0.)],
                vec![(1., 1.), (1.1, 1.1)],
                vec![(2., 2.)],
            ])
            .unwrap(),
            vec![
                TimeInterval::new_unchecked(0, 1),
                TimeInterval::new_unchecked(1, 2),
                TimeInterval::new_unchecked(2, 3),
            ],
            {
                let mut map = HashMap::new();
                map.insert("numbers".into(), FeatureData::Float(vec![0., 1., 2.]));
                map.insert(
                    "number_nulls".into(),
                    FeatureData::NullableFloat(vec![Some(0.), None, Some(2.)]),
                );
                map
            },
            CacheHint::default(),
        )
        .unwrap();

        let coords = pc.coordinates();
        assert_eq!(coords.len(), 4);
        assert_eq!(
            coords,
            &[
                [0., 0.].into(),
                [1., 1.].into(),
                [1.1, 1.1].into(),
                [2., 2.].into(),
            ]
        );

        let offsets = pc.feature_offsets();
        assert_eq!(offsets.len(), 4);
        assert_eq!(offsets, &[0, 1, 3, 4]);
    }

    #[test]
    fn sort_by_time_asc() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                vec![(0., 0.)],
                vec![(1., 1.), (1.1, 1.1)],
                vec![(2., 2.)],
            ])
            .unwrap(),
            vec![
                TimeInterval::new_unchecked(1, 5),
                TimeInterval::new_unchecked(0, 3),
                TimeInterval::new_unchecked(1, 3),
            ],
            {
                let mut map = HashMap::new();
                map.insert("numbers".into(), FeatureData::Float(vec![0., 1., 2.]));
                map.insert(
                    "number_nulls".into(),
                    FeatureData::NullableFloat(vec![Some(0.), None, Some(2.)]),
                );
                map
            },
            CacheHint::default(),
        )
        .unwrap();

        let expected_collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                vec![(1., 1.), (1.1, 1.1)],
                vec![(2., 2.)],
                vec![(0., 0.)],
            ])
            .unwrap(),
            vec![
                TimeInterval::new_unchecked(0, 3),
                TimeInterval::new_unchecked(1, 3),
                TimeInterval::new_unchecked(1, 5),
            ],
            {
                let mut map = HashMap::new();
                map.insert("numbers".into(), FeatureData::Float(vec![1., 2., 0.]));
                map.insert(
                    "number_nulls".into(),
                    FeatureData::NullableFloat(vec![None, Some(2.), Some(0.)]),
                );
                map
            },
            CacheHint::default(),
        )
        .unwrap();

        let sorted_collection = collection.sort_by_time_asc().unwrap();

        assert!(
            sorted_collection.chunks_equal_ignoring_cache_hint(&expected_collection),
            "expected: {expected_collection:#?}\nactual: {sorted_collection:#?}"
        );
    }

    #[test]
    fn reproject_epsg4326_epsg900913() {
        use crate::operations::reproject::{CoordinateProjection, CoordinateProjector};
        use crate::spatial_reference::{SpatialReference, SpatialReferenceAuthority};

        use crate::util::well_known_data::{
            COLOGNE_EPSG_4326, COLOGNE_EPSG_900_913, HAMBURG_EPSG_4326, HAMBURG_EPSG_900_913,
            MARBURG_EPSG_4326, MARBURG_EPSG_900_913,
        };

        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);
        let projector = CoordinateProjector::from_known_srs(from, to).unwrap();

        let pc = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                vec![MARBURG_EPSG_4326, COLOGNE_EPSG_4326],
                vec![HAMBURG_EPSG_4326],
            ])
            .unwrap(),
            vec![
                TimeInterval::new_unchecked(0, 1),
                TimeInterval::new_unchecked(1, 2),
            ],
            {
                let mut map = HashMap::new();
                map.insert("numbers".into(), FeatureData::Float(vec![0., 1.]));
                map.insert(
                    "number_nulls".into(),
                    FeatureData::NullableFloat(vec![Some(0.), None]),
                );
                map
            },
            CacheHint::default(),
        )
        .unwrap();

        let proj_pc = pc.reproject(&projector).unwrap();

        let coords = proj_pc.coordinates();
        assert_eq!(coords.len(), 3);
        assert!(approx_eq!(f64, coords[0].x, MARBURG_EPSG_900_913.x));
        assert!(approx_eq!(f64, coords[0].y, MARBURG_EPSG_900_913.y));
        assert!(approx_eq!(f64, coords[1].x, COLOGNE_EPSG_900_913.x));
        assert!(approx_eq!(f64, coords[1].y, COLOGNE_EPSG_900_913.y));
        assert!(approx_eq!(f64, coords[2].x, HAMBURG_EPSG_900_913.x));
        assert!(approx_eq!(f64, coords[2].y, HAMBURG_EPSG_900_913.y));

        let offsets = proj_pc.feature_offsets();
        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets, &[0, 2, 3]);
    }

    #[test]
    fn reproject_epsg4326_epsg900913_collections_equal() {
        use crate::operations::reproject::{CoordinateProjection, CoordinateProjector};
        use crate::spatial_reference::{SpatialReference, SpatialReferenceAuthority};

        use crate::util::well_known_data::{
            COLOGNE_EPSG_4326, COLOGNE_EPSG_900_913, HAMBURG_EPSG_4326, HAMBURG_EPSG_900_913,
            MARBURG_EPSG_4326, MARBURG_EPSG_900_913,
        };

        let from = SpatialReference::epsg_4326();
        let to = SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);
        let projector = CoordinateProjector::from_known_srs(from, to).unwrap();

        let pc = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                vec![MARBURG_EPSG_4326, COLOGNE_EPSG_4326],
                vec![HAMBURG_EPSG_4326],
            ])
            .unwrap(),
            vec![
                TimeInterval::new_unchecked(0, 1),
                TimeInterval::new_unchecked(1, 2),
            ],
            {
                let mut map = HashMap::new();
                map.insert("numbers".into(), FeatureData::Int(vec![0, 1]));
                map.insert(
                    "number_nulls".into(),
                    FeatureData::NullableInt(vec![Some(0), None]),
                );
                map
            },
            CacheHint::default(),
        )
        .unwrap();

        let expected_points = MultiPoint::many(vec![
            vec![MARBURG_EPSG_900_913, COLOGNE_EPSG_900_913],
            vec![HAMBURG_EPSG_900_913],
        ])
        .unwrap();

        let proj_pc = pc.reproject(&projector).unwrap();

        // Assert geometrys are approx equal
        proj_pc
            .geometries()
            .zip(expected_points.iter())
            .for_each(|(a, e)| {
                assert!(approx_eq!(&MultiPoint, &a.into(), e, epsilon = 0.000_001));
            });

        // Assert that feature time intervals did not move around
        assert_eq!(proj_pc.time_intervals().len(), 2);
        assert_eq!(
            proj_pc.time_intervals(),
            &[
                TimeInterval::new_unchecked(0, 1),
                TimeInterval::new_unchecked(1, 2),
            ]
        );

        // Assert that feature data did not magicaly disappear
        if let FeatureDataRef::Int(numbers) = proj_pc.data("numbers").unwrap() {
            assert_eq!(numbers.as_ref(), &[0, 1]);
        } else {
            unreachable!();
        }

        if let FeatureDataRef::Int(numbers) = proj_pc.data("number_nulls").unwrap() {
            assert_eq!(numbers.as_ref()[1], 0);
            assert_eq!(numbers.nulls(), vec![false, true]);
        } else {
            unreachable!();
        }
    }

    #[test]
    fn iterator() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            [
                (
                    "foo".to_string(),
                    FeatureData::NullableInt(vec![Some(0), None, Some(2)]),
                ),
                (
                    "bar".to_string(),
                    FeatureData::Text(vec!["a".into(), "b".into(), "c".into()]),
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
        assert_eq!(&[Coordinate2D::new(0.0, 0.1)], row.geometry.points());
        assert_eq!(TimeInterval::new_unchecked(0, 1), row.time_interval);
        assert_eq!(Some(FeatureDataValue::NullableInt(Some(0))), row.get("foo"));
        assert_eq!(
            Some(FeatureDataValue::Text("a".to_string())),
            row.get("bar")
        );

        let row = iter.next().unwrap();
        assert_eq!(&[Coordinate2D::new(1.0, 1.1)], row.geometry.points());
        assert_eq!(TimeInterval::new_unchecked(0, 1), row.time_interval);

        assert_eq!(Some(FeatureDataValue::NullableInt(None)), row.get("foo"));
        assert_eq!(
            Some(FeatureDataValue::Text("b".to_string())),
            row.get("bar")
        );

        let row = iter.next().unwrap();
        assert_eq!(&[Coordinate2D::new(2.0, 3.1)], row.geometry.points());
        assert_eq!(TimeInterval::new_unchecked(0, 1), row.time_interval);
        assert_eq!(Some(FeatureDataValue::NullableInt(Some(2))), row.get("foo"));
        assert_eq!(
            Some(FeatureDataValue::Text("c".to_string())),
            row.get("bar")
        );

        assert!(iter.next().is_none());
    }

    #[test]
    fn replace_time() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::default(); 3],
            [
                (
                    "foo".to_string(),
                    FeatureData::NullableInt(vec![Some(0), None, Some(2)]),
                ),
                (
                    "bar".to_string(),
                    FeatureData::Text(vec!["a".into(), "b".into(), "c".into()]),
                ),
            ]
            .iter()
            .cloned()
            .collect(),
            CacheHint::default(),
        )
        .unwrap();

        let new_time_intervals = vec![
            TimeInterval::new(0, 1).unwrap(),
            TimeInterval::new(1, 2).unwrap(),
            TimeInterval::new(2, 3).unwrap(),
        ];

        let new_collection = collection.replace_time(&new_time_intervals).unwrap();

        assert_eq!(collection.len(), new_collection.len());

        assert_eq!(
            collection.geometries().collect::<Vec<_>>(),
            new_collection.geometries().collect::<Vec<_>>()
        );

        assert_eq!(
            collection.data("foo").unwrap(),
            new_collection.data("foo").unwrap()
        );
        assert_eq!(
            collection.data("bar").unwrap(),
            new_collection.data("bar").unwrap()
        );

        assert_eq!(new_collection.time_intervals(), new_time_intervals);
    }

    #[test]
    fn test_geo_iter() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap();
        let mut iter = collection.geometries();

        assert_eq!(iter.len(), 3);

        let geometry = iter.next().unwrap();
        assert_eq!(&[Coordinate2D::new(0.0, 0.1)], geometry.points());

        assert_eq!(iter.len(), 2);

        let geometry = iter.next().unwrap();
        assert_eq!(&[Coordinate2D::new(1.0, 1.1)], geometry.points());

        assert_eq!(iter.len(), 1);

        let geometry = iter.next().unwrap();
        assert_eq!(&[Coordinate2D::new(2.0, 3.1)], geometry.points());

        assert_eq!(iter.len(), 0);

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_geo_iter_reverse() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap();
        let mut iter = collection.geometries();

        assert_eq!(iter.len(), 3);

        let geometry = iter.next_back().unwrap();
        assert_eq!(&[Coordinate2D::new(2.0, 3.1)], geometry.points());

        assert_eq!(iter.len(), 2);

        let geometry = iter.next_back().unwrap();
        assert_eq!(&[Coordinate2D::new(1.0, 1.1)], geometry.points());

        assert_eq!(iter.len(), 1);

        let geometry = iter.next_back().unwrap();
        assert_eq!(&[Coordinate2D::new(0.0, 0.1)], geometry.points());

        assert_eq!(iter.len(), 0);

        assert!(iter.next_back().is_none());
    }

    #[test]
    fn test_par_iter() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                vec![(0.0, 0.1)],
                vec![(1.0, 1.1), (1.2, 1.3)],
                vec![(2.0, 3.1)],
            ])
            .unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap();

        //  check splitting
        let iter = collection.geometries().into_par_iter();

        let (iter_left, iter_right) = iter.split_at(1);

        assert_eq!(iter_left.count(), 1);
        assert_eq!(iter_right.count(), 2);

        // new iter
        let iter = collection.geometries().into_par_iter();

        let result = iter
            .map(|geometry| geometry.points().len())
            .collect::<Vec<_>>();

        assert_eq!(result, vec![1, 2, 1]);
    }
}
