use std::collections::HashMap;

use arrow::array::{
    Array, ArrayBuilder, ArrayData, BooleanArray, Date64Builder, FixedSizeListArray,
    FixedSizeListBuilder, Float64Array, Float64Builder, ListArray, ListBuilder, StructArray,
    StructBuilder,
};
use arrow::datatypes::{DataType, Field};
use snafu::ensure;

use crate::collections::feature_collection_builder::BuilderProvider;
use crate::collections::{
    FeatureCollection, FeatureCollectionBuilderImplHelpers, FeatureCollectionImplHelpers,
    GeoFeatureCollectionRowBuilder, IntoGeometryIterator, SimpleFeatureCollectionBuilder,
    SimpleFeatureCollectionRowBuilder,
};
use crate::error::Error;
use crate::primitives::{
    Coordinate2D, FeatureData, FeatureDataType, MultiPoint, MultiPointRef, TimeInterval,
};
use crate::util::arrow::{downcast_array, ArrowTyped};
use crate::util::Result;
use crate::{error, json_map};
use std::slice;

/// This collection contains temporal multi-points and miscellaneous data.
#[derive(Debug)]
pub struct MultiPointCollection {
    data: StructArray,
    types: HashMap<String, FeatureDataType>,
}

impl FeatureCollectionImplHelpers for MultiPointCollection {
    fn new_from_internals(data: StructArray, types: HashMap<String, FeatureDataType>) -> Self {
        Self { data, types }
    }

    fn table(&self) -> &StructArray {
        &self.data
    }

    fn types(&self) -> &HashMap<String, FeatureDataType> {
        &self.types
    }

    /// `MultiPoint`s
    fn geometry_arrow_data_type() -> DataType {
        MultiPoint::arrow_data_type()
    }

    fn filtered_geometries(
        features: &ListArray,
        filter_array: &BooleanArray,
    ) -> crate::util::Result<ListArray> {
        let mut new_features =
            ListBuilder::new(FixedSizeListBuilder::new(Float64Builder::new(2), 2));

        for feature_index in 0..features.len() {
            if filter_array.value(feature_index) {
                let coordinate_builder = new_features.values();

                let old_coordinates = features.value(feature_index);

                for coordinate_index in 0..features.value_length(feature_index) {
                    let old_floats_array = downcast_array::<FixedSizeListArray>(&old_coordinates)
                        .value(coordinate_index as usize);

                    let old_floats: &Float64Array = downcast_array(&old_floats_array);

                    let float_builder = coordinate_builder.values();
                    float_builder.append_slice(old_floats.value_slice(0, 2))?;

                    coordinate_builder.append(true)?;
                }

                new_features.append(true)?;
            }
        }

        Ok(new_features.finish())
    }

    fn concat_geometries(
        geometries_a: &ListArray,
        geometries_b: &ListArray,
    ) -> Result<ListArray, Error> {
        let mut new_multipoints =
            ListBuilder::new(FixedSizeListBuilder::new(Float64Builder::new(2), 2));

        for old_multipoints in &[geometries_a, geometries_b] {
            for multipoint_index in 0..old_multipoints.len() {
                let multipoint_ref = old_multipoints.value(multipoint_index);
                let multipoint: &FixedSizeListArray = downcast_array(&multipoint_ref);

                let new_points = new_multipoints.values();

                for point_index in 0..multipoint.len() {
                    let floats_ref = multipoint.value(point_index);
                    let floats: &Float64Array = downcast_array(&floats_ref);

                    let new_floats = new_points.values();
                    new_floats.append_slice(floats.value_slice(0, 2))?;

                    new_points.append(true)?;
                }

                new_multipoints.append(true)?;
            }
        }

        Ok(new_multipoints.finish())
    }

    fn _is_simple(&self) -> bool {
        self.len() == self.coordinates().len()
    }
}

into_geometry_options_impl!(MultiPointCollection);

feature_collection_impl!(MultiPointCollection, true);

impl MultiPointCollection {
    /// Create an empty `MultiPointCollection`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{MultiPointCollection, FeatureCollection};
    ///
    /// let pc = MultiPointCollection::empty();
    ///
    /// assert_eq!(pc.len(), 0);
    /// ```
    pub fn empty() -> Self {
        Self {
            data: {
                let columns = vec![
                    Field::new(
                        Self::GEOMETRY_COLUMN_NAME,
                        Self::geometry_arrow_data_type(),
                        false,
                    ),
                    Field::new(Self::TIME_COLUMN_NAME, Self::time_arrow_data_type(), false),
                ];

                StructArray::from(ArrayData::builder(DataType::Struct(columns)).len(0).build())
            },
            types: Default::default(),
        }
    }

    /// Create a point collection from data
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{MultiPointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{Coordinate2D, TimeInterval, FeatureData};
    /// use std::collections::HashMap;
    ///
    /// let pc = MultiPointCollection::from_data(
    ///     vec![vec![(0., 0.).into()], vec![(1., 1.).into()]],
    ///     vec![TimeInterval::new_unchecked(0, 1), TimeInterval::new_unchecked(0, 1)],
    ///     {
    ///         let mut map = HashMap::new();
    ///         map.insert("number".into(), FeatureData::Number(vec![0., 1.]));
    ///         map
    ///     },
    /// ).unwrap();
    ///
    /// assert_eq!(pc.len(), 2);
    /// ```
    ///
    /// # Errors
    ///
    /// This constructor fails if the data lenghts are different or `data`'s keys use a reserved name
    ///
    pub fn from_data(
        coordinates: Vec<Vec<Coordinate2D>>,
        time_intervals: Vec<TimeInterval>,
        data: HashMap<String, FeatureData>,
    ) -> Result<Self> {
        let capacity = coordinates.len();

        let mut columns = vec![
            Field::new(
                Self::GEOMETRY_COLUMN_NAME,
                Self::geometry_arrow_data_type(),
                false,
            ),
            Field::new(Self::TIME_COLUMN_NAME, Self::time_arrow_data_type(), false),
        ];

        let mut builders: Vec<Box<dyn ArrayBuilder>> = vec![
            Box::new({
                let mut builder =
                    ListBuilder::new(FixedSizeListBuilder::new(Float64Builder::new(2), 2));
                for multi_point in coordinates {
                    let coordinate_builder = builder.values();
                    for coordinate in multi_point {
                        let float_builder = coordinate_builder.values();
                        float_builder.append_value(coordinate.x)?;
                        float_builder.append_value(coordinate.y)?;
                        coordinate_builder.append(true)?;
                    }
                    builder.append(true)?;
                }

                builder
            }),
            Box::new({
                let mut builder = FixedSizeListBuilder::new(Date64Builder::new(capacity), 2);
                for time_interval in time_intervals {
                    let date_builder = builder.values();
                    date_builder.append_value(time_interval.start().into())?;
                    date_builder.append_value(time_interval.end().into())?;
                    builder.append(true)?;
                }

                builder
            }),
        ];

        let mut data_types = HashMap::with_capacity(data.len());

        for (name, feature_data) in data {
            ensure!(
                !Self::is_reserved_name(&name),
                error::ColumnNameConflict { name }
            );

            let column = Field::new(
                &name,
                feature_data.arrow_data_type(),
                feature_data.nullable(),
            );

            columns.push(column);
            builders.push(feature_data.arrow_builder()?);

            data_types.insert(name, FeatureDataType::from(&feature_data));
        }

        let mut struct_builder = StructBuilder::new(columns, builders);
        for _ in 0..capacity {
            struct_builder.append(true)?;
        }

        // TODO: performance improvements by creating the buffers directly and not using so many loops

        // TODO: wrap error for unequal number of rows in custom error

        Ok(Self {
            data: struct_builder.finish(),
            types: data_types,
        })
    }

    /// Retrieves the coordinates of this point collection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{MultiPointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{Coordinate2D, TimeInterval, FeatureData};
    /// use std::collections::HashMap;
    ///
    /// let pc = MultiPointCollection::from_data(
    ///     vec![vec![(0., 0.).into()], vec![(1., 1.).into()], vec![(2., 2.).into()]],
    ///     vec![TimeInterval::new_unchecked(0, 1), TimeInterval::new_unchecked(1, 2), TimeInterval::new_unchecked(2, 3)],
    ///     HashMap::new(),
    /// ).unwrap();
    ///
    /// assert_eq!(pc.len(), 3);
    ///
    /// let coords = pc.coordinates();
    ///
    /// assert_eq!(coords.len(), 3);
    /// assert_eq!(coords, &[(0., 0.).into(), (1., 1.).into(), (2., 2.).into()]);
    /// ```
    ///
    pub fn coordinates(&self) -> &[Coordinate2D] {
        let features_ref = self
            .data
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There must exist a feature column");
        let features: &ListArray = downcast_array(features_ref);

        let feature_coordinates_ref = features.values();
        let feature_coordinates: &FixedSizeListArray = downcast_array(&feature_coordinates_ref);

        let number_of_coordinates = feature_coordinates.data().len();

        let floats_ref = feature_coordinates.values();
        let floats: &Float64Array = downcast_array(&floats_ref);

        unsafe {
            slice::from_raw_parts(
                floats.raw_values() as *const Coordinate2D,
                number_of_coordinates,
            )
        }
    }
}

impl<'l> IntoGeometryIterator<'l> for MultiPointCollection {
    type GeometryIterator = MultiPointIterator<'l>;
    type GeometryType = MultiPointRef<'l>;

    fn geometries(&'l self) -> Self::GeometryIterator {
        let geometry_column: &ListArray = downcast_array(
            &self
                .data
                .column_by_name(MultiPointCollection::GEOMETRY_COLUMN_NAME)
                .expect("Column must exist since it is in the metadata"),
        );

        MultiPointIterator {
            geometry_column,
            index: 0,
            length: self.len(),
        }
    }
}

/// A collection iterator for multi points
pub struct MultiPointIterator<'l> {
    geometry_column: &'l ListArray,
    index: usize,
    length: usize,
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

impl FeatureCollectionBuilderImplHelpers for MultiPointCollection {
    type GeometriesBuilder = <MultiPoint as ArrowTyped>::ArrowBuilder;

    const HAS_GEOMETRIES: bool = true;

    fn geometries_builder() -> Self::GeometriesBuilder {
        MultiPoint::arrow_builder(0)
    }
}

impl BuilderProvider for MultiPointCollection {
    type Builder = SimpleFeatureCollectionBuilder<Self>;
}

impl GeoFeatureCollectionRowBuilder<MultiPoint>
    for SimpleFeatureCollectionRowBuilder<MultiPointCollection>
{
    fn push_geometry(&mut self, geometry: MultiPoint) -> Result<(), Error> {
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

    use crate::collections::{FeatureCollectionBuilder, FeatureCollectionRowBuilder};
    use crate::primitives::{FeatureDataRef, FeatureDataValue, MultiPointAccess, NullableDataRef};
    use serde_json::{from_str, json};

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
            vec![
                vec![(0., 0.).into()],
                vec![(1., 1.).into()],
                vec![(2., 2.).into()],
            ],
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
            vec![
                vec![(0., 0.).into()],
                vec![(1., 1.).into()],
                vec![(2., 2.).into()],
            ],
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
            vec![
                vec![(0., 0.).into()],
                vec![(1., 1.).into()],
                vec![(2., 2.).into()],
            ],
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
            vec![
                vec![(0., 0.).into()],
                vec![(1., 1.).into()],
                vec![(2., 2.).into()],
            ],
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
            vec![vec![(3., 3.).into()], vec![(4., 4.).into()]],
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
            vec![vec![(0., 0.).into()], vec![(1., 1.).into()]],
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
        assert_eq!(pc.coordinates(), cloned.coordinates());
    }
}
