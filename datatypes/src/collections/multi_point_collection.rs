use std::collections::HashMap;

use arrow::array::{
    Array, ArrayBuilder, ArrayData, ArrayRef, BooleanArray, Date64Array, Date64Builder,
    FixedSizeListArray, FixedSizeListBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, ListArray, ListBuilder, PrimitiveArray, StringArray, StringBuilder, StructArray,
    StructBuilder, UInt8Array, UInt8Builder,
};
use arrow::compute::kernels::filter::filter;
use arrow::datatypes::{ArrowNumericType, DataType, DateUnit, Field};
use snafu::ensure;

use crate::collections::{FeatureCollection, HasGeometryIterator};
use crate::error;
use crate::operations::Filterable;
use crate::primitives::{
    CategoricalDataRef, Coordinate2D, DecimalDataRef, FeatureData, FeatureDataRef, FeatureDataType,
    FeatureDataValue, MultiPoint, NullableCategoricalDataRef, NullableDecimalDataRef,
    NullableNumberDataRef, NullableTextDataRef, NumberDataRef, TextDataRef, TimeInterval,
};
use crate::util::arrow::{downcast_array, downcast_mut_array};
use crate::util::Result;
use std::mem;
use std::slice;
use std::sync::Arc;

#[derive(Debug)]
pub struct MultiPointCollection {
    data: StructArray,
    types: HashMap<String, FeatureDataType>,
}

impl Clone for MultiPointCollection {
    /// Clone the MultiPointCollection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{MultiPointCollection, FeatureCollection};
    ///
    /// let pc = MultiPointCollection::empty();
    /// let cloned = pc.clone();
    ///
    /// assert_eq!(pc.len(), 0);
    /// assert_eq!(cloned.len(), 0);
    /// ```
    ///
    fn clone(&self) -> Self {
        Self {
            data: StructArray::from(self.data.data()),
            types: self.types.clone(),
        }
    }
}

impl MultiPointCollection {
    /// Retrieve the composite arrow data type for multi points
    pub(self) fn multi_points_data_type() -> DataType {
        DataType::List(DataType::FixedSizeList(DataType::Float64.into(), 2).into())
    }

    /// Retrieve the composite arrow data type for multi points
    pub(self) fn time_data_type() -> DataType {
        DataType::FixedSizeList(DataType::Date64(DateUnit::Millisecond).into(), 2)
    }

    /// Create an empty MultiPointCollection.
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
                        Self::FEATURE_COLUMN_NAME,
                        Self::multi_points_data_type(),
                        false,
                    ),
                    Field::new(Self::TIME_COLUMN_NAME, Self::time_data_type(), false),
                ];

                StructArray::from(ArrayData::builder(DataType::Struct(columns)).len(0).build())
            },
            types: Default::default(),
        }
    }

    /// Use a builder for creating the point collection
    pub fn builder() -> MultiPointCollectionBuilder {
        Default::default()
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
    pub fn from_data(
        coordinates: Vec<Vec<Coordinate2D>>,
        time_intervals: Vec<TimeInterval>,
        data: HashMap<String, FeatureData>,
    ) -> Result<Self> {
        let capacity = coordinates.len();

        let mut columns = vec![
            Field::new(
                Self::FEATURE_COLUMN_NAME,
                Self::multi_points_data_type(),
                false,
            ),
            Field::new(Self::TIME_COLUMN_NAME, Self::time_data_type(), false),
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
                    date_builder.append_value(time_interval.start())?;
                    date_builder.append_value(time_interval.end())?;
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
            .column_by_name(Self::FEATURE_COLUMN_NAME)
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

    fn array_refs_of_reserved_fields(&self) -> Vec<ArrayRef> {
        vec![
            self.data
                .column_by_name(Self::FEATURE_COLUMN_NAME)
                .unwrap()
                .clone(),
            self.data
                .column_by_name(Self::TIME_COLUMN_NAME)
                .unwrap()
                .clone(),
        ]
    }

    /// Helper function to append all column values to a json map
    fn append_feature_column_to_geo_json_map(
        &self,
        maps: &mut [serde_json::Map<String, serde_json::Value>],
        column_name: &str,
        column_type: FeatureDataType,
    ) {
        assert_eq!(self.len(), maps.len());

        let column = self
            .data
            .column_by_name(column_name)
            .expect("Column must exist since it is in the metadata");

        match column_type {
            FeatureDataType::Text => {
                let text_column: &StringArray = downcast_array(&column);
                for (i, map) in maps.iter_mut().enumerate() {
                    map.insert(column_name.into(), text_column.value(i).into());
                }
            }
            FeatureDataType::NullableText => {
                let text_column: &StringArray = downcast_array(&column);
                for (i, map) in maps.iter_mut().enumerate() {
                    map.insert(
                        column_name.into(),
                        if text_column.is_null(i) {
                            serde_json::Value::Null
                        } else {
                            text_column.value(i).into()
                        },
                    );
                }
            }
            FeatureDataType::Number => insert_geo_json_values_in_maps(
                downcast_array::<Float64Array>(&column),
                maps,
                column_name,
            ),
            FeatureDataType::NullableNumber => insert_nullable_geo_json_values_in_maps(
                downcast_array::<Float64Array>(&column),
                maps,
                column_name,
            ),
            FeatureDataType::Decimal => insert_geo_json_values_in_maps(
                downcast_array::<Int64Array>(&column),
                maps,
                column_name,
            ),
            FeatureDataType::NullableDecimal => insert_nullable_geo_json_values_in_maps(
                downcast_array::<Int64Array>(&column),
                maps,
                column_name,
            ),
            FeatureDataType::Categorical => insert_geo_json_values_in_maps(
                // TODO: use category names
                downcast_array::<UInt8Array>(&column),
                maps,
                column_name,
            ),
            FeatureDataType::NullableCategorical => insert_nullable_geo_json_values_in_maps(
                // TODO: use category names
                downcast_array::<UInt8Array>(&column),
                maps,
                column_name,
            ),
        }
    }
}

impl FeatureCollection for MultiPointCollection {
    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_simple(&self) -> bool {
        self.len() == self.coordinates().len()
    }

    /// Retrieves a data column of this point collection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{MultiPointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{Coordinate2D, TimeInterval, FeatureData, FeatureDataRef, NullableDataRef};
    /// use std::collections::HashMap;
    ///
    /// let pc = MultiPointCollection::from_data(
    ///     vec![vec![(0., 0.).into()], vec![(1., 1.).into()], vec![(2., 2.).into()]],
    ///     vec![TimeInterval::new_unchecked(0, 1), TimeInterval::new_unchecked(1, 2), TimeInterval::new_unchecked(2, 3)],
    ///     {
    ///         let mut map = HashMap::new();
    ///         map.insert("numbers".into(), FeatureData::Number(vec![0., 1., 2.]));
    ///         map.insert("number_nulls".into(), FeatureData::NullableNumber(vec![Some(0.), None, Some(2.)]));
    ///         map
    ///     },
    /// ).unwrap();
    ///
    /// assert_eq!(pc.len(), 3);
    ///
    /// if let FeatureDataRef::Number(numbers) = pc.data("numbers").unwrap() {
    ///     assert_eq!(numbers.as_ref(), &[0., 1., 2.]);
    /// } else {
    ///     unreachable!();
    /// }
    ///
    /// if let FeatureDataRef::NullableNumber(numbers) = pc.data("number_nulls").unwrap() {
    ///     assert_eq!(numbers.as_ref()[0], 0.);
    ///     assert_eq!(numbers.as_ref()[2], 2.);
    ///     assert_eq!(numbers.nulls(), vec![false, true, false]);
    /// } else {
    ///     unreachable!();
    /// }
    /// ```
    ///
    fn data(&self, column_name: &str) -> Result<FeatureDataRef> {
        ensure!(
            !Self::is_reserved_name(column_name),
            error::FeatureCollection {
                details: "Cannot access reserved columns via `data()` method"
            }
        );

        let column = self.data.column_by_name(column_name);

        ensure!(
            column.is_some(),
            error::FeatureCollection {
                details: format!(
                    "The column {} does not exist in the point collection",
                    column_name
                )
            }
        );

        let column = column.unwrap(); // previously checked

        Ok(match self.types.get(column_name).unwrap() {
            // previously checked
            FeatureDataType::Number => {
                let array: &Float64Array = downcast_array(column);
                NumberDataRef::new(array.values()).into()
            }
            FeatureDataType::NullableNumber => {
                let array: &Float64Array = downcast_array(column);
                NullableNumberDataRef::new(array.values(), array.data_ref().null_bitmap()).into()
            }
            FeatureDataType::Text => {
                let array: &StringArray = downcast_array(column);
                TextDataRef::new(array.value_data(), array.value_offsets()).into()
            }
            FeatureDataType::NullableText => {
                let array: &StringArray = downcast_array(column);
                NullableTextDataRef::new(array.value_data(), array.value_offsets()).into()
            }
            FeatureDataType::Decimal => {
                let array: &Int64Array = downcast_array(column);
                DecimalDataRef::new(array.values()).into()
            }
            FeatureDataType::NullableDecimal => {
                let array: &Int64Array = downcast_array(column);
                NullableDecimalDataRef::new(array.values(), array.data_ref().null_bitmap()).into()
            }
            FeatureDataType::Categorical => {
                let array: &UInt8Array = downcast_array(column);
                CategoricalDataRef::new(array.values()).into()
            }
            FeatureDataType::NullableCategorical => {
                let array: &UInt8Array = downcast_array(column);
                NullableCategoricalDataRef::new(array.values(), array.data_ref().null_bitmap())
                    .into()
            }
        })
    }

    /// Retrieves the time intervals of this point collection
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
    /// let time_intervals = pc.time_intervals();
    ///
    /// assert_eq!(time_intervals.len(), 3);
    /// assert_eq!(
    ///     time_intervals,
    ///     &[TimeInterval::new_unchecked(0, 1), TimeInterval::new_unchecked(1, 2), TimeInterval::new_unchecked(2, 3)]
    /// );
    /// ```
    ///
    fn time_intervals(&self) -> &[TimeInterval] {
        let features_ref = self
            .data
            .column_by_name(Self::TIME_COLUMN_NAME)
            .expect("There must exist a time interval column");
        let features: &FixedSizeListArray = downcast_array(features_ref);

        let number_of_time_intervals = self.len();

        let timestamps_ref = features.values();
        let timestamps: &Date64Array = downcast_array(&timestamps_ref);

        unsafe {
            slice::from_raw_parts(
                timestamps.raw_values() as *const TimeInterval,
                number_of_time_intervals,
            )
        }
    }

    /// Extend the collection by an additional column
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{MultiPointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{FeatureData, FeatureDataType, TimeInterval, FeatureDataValue, FeatureDataRef};
    ///
    /// let collection = {
    ///     let mut builder = MultiPointCollection::builder();
    ///     builder.add_column("foo", FeatureDataType::Number);
    ///
    ///     builder.append_coordinate((0., 0.).into());
    ///     builder.append_time_interval(TimeInterval::new_unchecked(0, 1));
    ///     builder.append_data("foo", FeatureDataValue::Number(0.));
    ///     builder.finish_row();
    ///
    ///     builder.append_coordinate((1., 1.).into());
    ///     builder.append_time_interval(TimeInterval::new_unchecked(0, 1));
    ///     builder.append_data("foo", FeatureDataValue::Number(1.));
    ///     builder.finish_row();
    ///
    ///     builder.build().unwrap()
    /// };
    ///
    /// assert_eq!(collection.len(), 2);
    ///
    /// let extended_collection = collection.add_column("bar", FeatureData::Number(vec![2., 4.])).unwrap();
    ///
    /// assert_eq!(extended_collection.len(), 2);
    /// if let FeatureDataRef::Number(numbers) = extended_collection.data("foo").unwrap() {
    ///     assert_eq!(numbers.as_ref(), &[0., 1.]);
    /// } else {
    ///     unreachable!();
    /// }
    /// if let FeatureDataRef::Number(numbers) = extended_collection.data("bar").unwrap() {
    ///     assert_eq!(numbers.as_ref(), &[2., 4.]);
    /// } else {
    ///     unreachable!();
    /// }
    /// ```
    fn add_column(&self, new_column: &str, data: FeatureData) -> Result<Self> {
        ensure!(
            !Self::is_reserved_name(new_column) && self.data.column_by_name(new_column).is_none(),
            error::FeatureCollection {
                details: "Cannot extend collection with name that is reserved or already in use"
            }
        );

        ensure!(
            data.len() == self.data.len(),
            error::FeatureCollection {
                details: "Length of new feature data column must match length of collection"
            }
        );

        let mut columns = vec![
            Field::new(
                Self::FEATURE_COLUMN_NAME,
                Self::multi_points_data_type(),
                false,
            ),
            Field::new(Self::TIME_COLUMN_NAME, Self::time_data_type(), false),
        ];
        let mut column_values: Vec<ArrayRef> = self.array_refs_of_reserved_fields();

        for (column_name, column_type) in &self.types {
            columns.push(Field::new(
                &column_name,
                column_type.arrow_data_type(),
                column_type.nullable(),
            ));
            column_values.push(self.data.column_by_name(&column_name).unwrap().clone());
        }

        columns.push(Field::new(
            new_column,
            data.arrow_data_type(),
            data.nullable(),
        ));
        column_values.push(data.arrow_builder().map(|mut builder| builder.finish())?);

        let mut types = self.types.clone();
        types.insert(new_column.to_string(), FeatureDataType::from(&data));

        Ok(Self {
            data: struct_array_from_data(columns, column_values, self.data.len()),
            types,
        })
    }

    /// Removes a column and returns an updated collection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{MultiPointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{FeatureData, FeatureDataType, TimeInterval, FeatureDataValue, FeatureDataRef};
    ///
    /// let collection = {
    ///     let mut builder = MultiPointCollection::builder();
    ///     builder.add_column("foo", FeatureDataType::Number);
    ///
    ///     builder.append_coordinate((0., 0.).into());
    ///     builder.append_time_interval(TimeInterval::new_unchecked(0, 1));
    ///     builder.append_data("foo", FeatureDataValue::Number(0.));
    ///     builder.finish_row();
    ///
    ///     builder.append_coordinate((1., 1.).into());
    ///     builder.append_time_interval(TimeInterval::new_unchecked(0, 1));
    ///     builder.append_data("foo", FeatureDataValue::Number(1.));
    ///     builder.finish_row();
    ///
    ///     builder.build().unwrap()
    /// };
    ///
    /// assert_eq!(collection.len(), 2);
    /// assert!(collection.data("foo").is_ok());
    ///
    /// let reduced_collection = collection.remove_column("foo").unwrap();
    ///
    /// assert_eq!(reduced_collection.len(), 2);
    /// assert!(reduced_collection.data("foo").is_err());
    ///
    /// assert!(reduced_collection.remove_column("foo").is_err());
    /// ```
    fn remove_column(&self, column: &str) -> Result<Self> {
        ensure!(
            !Self::is_reserved_name(column) && self.data.column_by_name(column).is_some(),
            error::FeatureCollection {
                details: "Must not remove a non-existing or mandatory column"
            }
        );

        let mut columns = vec![
            Field::new(
                Self::FEATURE_COLUMN_NAME,
                Self::multi_points_data_type(),
                false,
            ),
            Field::new(Self::TIME_COLUMN_NAME, Self::time_data_type(), false),
        ];
        let mut column_values: Vec<ArrayRef> = vec![
            self.data
                .column_by_name(Self::FEATURE_COLUMN_NAME)
                .unwrap()
                .clone(),
            self.data
                .column_by_name(Self::TIME_COLUMN_NAME)
                .unwrap()
                .clone(),
        ];

        for (column_name, column_type) in &self.types {
            if column_name == column {
                continue;
            }

            columns.push(Field::new(
                &column_name,
                column_type.arrow_data_type(),
                column_type.nullable(),
            ));
            column_values.push(self.data.column_by_name(&column_name).unwrap().clone());
        }

        let mut types = self.types.clone();
        types.remove(column);

        Ok(Self {
            data: struct_array_from_data(columns, column_values, self.data.len()),
            types,
        })
    }

    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{MultiPointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{FeatureDataType, FeatureDataValue, TimeInterval};
    /// use serde_json::{from_str, json};
    ///
    /// let collection = {
    ///     let mut builder = MultiPointCollection::builder();
    ///     builder.add_column("foo", FeatureDataType::Number);
    ///     builder.add_column("bar", FeatureDataType::NullableText);
    ///
    ///     builder.append_coordinate((0., 0.).into());
    ///     builder.append_time_interval(TimeInterval::new_unchecked(0, 1));
    ///     builder.append_data("foo", FeatureDataValue::Number(0.));
    ///     builder.append_data("bar", FeatureDataValue::NullableText(Some("one".to_string())));
    ///     builder.finish_row();
    ///
    ///     builder.append_multi_coordinate(vec![(1., 1.).into(), (2., 2.).into()]);
    ///     builder.append_time_interval(TimeInterval::new_unchecked(1, 2));
    ///     builder.append_data("foo", FeatureDataValue::Number(1.));
    ///     builder.append_data("bar", FeatureDataValue::NullableText(None));
    ///     builder.finish_row();
    ///
    ///     builder.append_coordinate((3., 3.).into());
    ///     builder.append_time_interval(TimeInterval::new_unchecked(3, 4));
    ///     builder.append_data("foo", FeatureDataValue::Number(2.));
    ///     builder.append_data("bar", FeatureDataValue::NullableText(Some("three".to_string())));
    ///     builder.finish_row();
    ///
    ///     builder.build().unwrap()
    /// };
    ///
    /// assert_eq!(
    ///     from_str::<serde_json::Value>(collection.to_geo_json().as_str()).unwrap(),
    ///     json!({
    ///     	"type": "FeatureCollection",
    ///     	"features": [{
    ///     		"type": "Feature",
    ///     		"geometry": {
    ///     			"type": "Point",
    ///     			"coordinates": [0.0, 0.0]
    ///     		},
    ///     		"properties": {
    ///     			"bar": "one",
    ///     			"foo": 0.0
    ///     		},
    ///     		"when": {
    ///     			"start": "1970-01-01T00:00:00+00:00",
    ///     			"end": "1970-01-01T00:00:00.001+00:00",
    ///     			"type": "Interval"
    ///     		}
    ///     	}, {
    ///     		"type": "Feature",
    ///     		"geometry": {
    ///     			"type": "MultiPoint",
    ///     			"coordinates": [
    ///     				[1.0, 1.0],
    ///     				[2.0, 2.0]
    ///     			]
    ///     		},
    ///     		"properties": {
    ///     			"bar": null,
    ///     			"foo": 1.0
    ///     		},
    ///     		"when": {
    ///     			"start": "1970-01-01T00:00:00.001+00:00",
    ///     			"end": "1970-01-01T00:00:00.002+00:00",
    ///     			"type": "Interval"
    ///     		}
    ///     	}, {
    ///     		"type": "Feature",
    ///     		"geometry": {
    ///     			"type": "Point",
    ///     			"coordinates": [3.0, 3.0]
    ///     		},
    ///     		"properties": {
    ///     			"bar": "three",
    ///     			"foo": 2.0
    ///     		},
    ///     		"when": {
    ///     			"start": "1970-01-01T00:00:00.003+00:00",
    ///     			"end": "1970-01-01T00:00:00.004+00:00",
    ///     			"type": "Interval"
    ///     		}
    ///     	}]
    ///     })
    /// );
    /// ```
    ///
    fn to_geo_json(&self) -> String {
        let mut property_maps = (0..self.len())
            .map(|_| serde_json::Map::with_capacity(self.types.len()))
            .collect::<Vec<_>>();

        for (column_name, &column_type) in &self.types {
            self.append_feature_column_to_geo_json_map(
                &mut property_maps,
                column_name,
                column_type,
            );
        }

        // creates a foreign member object out of a *when* event value
        fn foreign_memberize(
            when_value: serde_json::Value,
        ) -> serde_json::Map<String, serde_json::Value> {
            let mut map = serde_json::Map::with_capacity(1);
            map.insert("when".to_string(), when_value);
            map
        };

        let features = self
            .geometries()
            .zip(self.time_intervals())
            .zip(property_maps)
            .map(|((geometry, time_interval), properties)| geojson::Feature {
                bbox: None,
                geometry: Some(geometry.into()),
                id: None,
                properties: Some(properties),
                foreign_members: Some(foreign_memberize(time_interval.to_geo_json_event())),
            })
            .collect();

        let feature_collection = geojson::FeatureCollection {
            bbox: None,
            features,
            foreign_members: None,
        };

        feature_collection.to_string()
    }
}

impl<'l> HasGeometryIterator for &'l MultiPointCollection {
    type GeometryIterator = MultiPointIterator<'l>;
    type GeometryType = MultiPoint<'l>;

    fn geometries(&self) -> Self::GeometryIterator {
        let geometry_column: &ListArray = downcast_array(
            &self
                .data
                .column_by_name(MultiPointCollection::FEATURE_COLUMN_NAME)
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
    type Item = MultiPoint<'l>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.length {
            return None;
        }

        let multi_point_array_ref = self.geometry_column.value(self.index);
        let multi_point_array: &FixedSizeListArray = downcast_array(&multi_point_array_ref);

        let number_of_points = multi_point_array.len();

        let floats_ref = multi_point_array.value(multi_point_array.offset());
        let floats: &Float64Array = downcast_array(&floats_ref);

        let multi_point = MultiPoint::new_unchecked(unsafe {
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

impl Filterable for MultiPointCollection {
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{MultiPointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{Coordinate2D, TimeInterval, FeatureData};
    /// use geoengine_datatypes::operations::Filterable;
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
    /// let filtered = pc.filter(vec![false, true, false]).unwrap();
    ///
    /// assert_eq!(filtered.len(), 1);
    /// ```
    fn filter(&self, mask: Vec<bool>) -> Result<Self> {
        ensure!(
            mask.len() == self.data.len(),
            error::MaskLengthDoesNotMatchCollectionLength {
                mask_length: mask.len(),
                collection_length: self.data.len(),
            }
        );

        let filter_array: BooleanArray = mask.into();

        // TODO: use filter directly on struct array when it is implemented

        let filtered_data: Vec<(Field, ArrayRef)> =
            if let DataType::Struct(columns) = self.data.data().data_type() {
                let mut filtered_data: Vec<(Field, ArrayRef)> = Vec::with_capacity(columns.len());
                for (column, array) in columns.iter().zip(self.data.columns()) {
                    match column.name().as_str() {
                        Self::FEATURE_COLUMN_NAME => filtered_data.push((
                            column.clone(),
                            Arc::new(coordinates_filter(downcast_array(array), &filter_array)?),
                        )),
                        Self::TIME_COLUMN_NAME => filtered_data.push((
                            column.clone(),
                            Arc::new(time_interval_filter(downcast_array(array), &filter_array)?),
                        )),
                        _ => filtered_data
                            .push((column.clone(), filter(array.as_ref(), &filter_array)?)),
                    }
                }
                filtered_data
            } else {
                unreachable!("data column must be a struct")
            };

        Ok(Self {
            data: filtered_data.into(),
            types: self.types.clone(),
        })
    }
}

fn coordinates_filter(features: &ListArray, filter_array: &BooleanArray) -> Result<ListArray> {
    let mut new_features = ListBuilder::new(FixedSizeListBuilder::new(Float64Builder::new(2), 2));

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

fn time_interval_filter(
    time_intervals: &FixedSizeListArray,
    filter_array: &BooleanArray,
) -> Result<FixedSizeListArray> {
    let mut new_time_intervals = FixedSizeListBuilder::new(Date64Builder::new(2), 2);

    for feature_index in 0..time_intervals.len() {
        if !filter_array.value(feature_index) {
            continue;
        }

        let old_timestamps_ref = time_intervals.value(feature_index);
        let old_timestamps: &Date64Array = downcast_array(&old_timestamps_ref);

        let date_builder = new_time_intervals.values();
        date_builder.append_slice(old_timestamps.value_slice(0, 2))?;

        new_time_intervals.append(true)?;
    }

    Ok(new_time_intervals.finish())
}

/// A row-by-row builder for a point collection
pub struct MultiPointCollectionBuilder {
    coordinates_builder: ListBuilder<FixedSizeListBuilder<Float64Builder>>,
    time_intervals_builder: FixedSizeListBuilder<Date64Builder>,
    builders: HashMap<String, Box<dyn ArrayBuilder>>,
    types: HashMap<String, FeatureDataType>,
    rows: usize,
}

impl Default for MultiPointCollectionBuilder {
    /// Creates a builder for a point collection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::MultiPointCollectionBuilder;
    ///
    /// let builder = MultiPointCollectionBuilder::default();
    /// ```
    ///
    fn default() -> Self {
        Self {
            coordinates_builder: ListBuilder::new(FixedSizeListBuilder::new(
                Float64Builder::new(0),
                2,
            )),
            time_intervals_builder: FixedSizeListBuilder::new(Date64Builder::new(0), 2),
            builders: Default::default(),
            types: Default::default(),
            rows: 0,
        }
    }
}

impl MultiPointCollectionBuilder {
    /// Adds a column to the collection.
    /// Must happen before data insertions.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::MultiPointCollectionBuilder;
    /// use geoengine_datatypes::primitives::FeatureDataType;
    ///
    /// let mut builder = MultiPointCollectionBuilder::default();
    ///
    /// builder.add_column("foobar", FeatureDataType::Number).unwrap();
    /// builder.add_column("__features", FeatureDataType::Number).unwrap_err();
    /// ```
    ///
    pub fn add_column(&mut self, name: &str, data_type: FeatureDataType) -> Result<()> {
        ensure!(
            self.rows == 0,
            error::FeatureCollectionBuilderException {
                details: "It is not allowed to add further columns after data was inserted",
            }
        );
        ensure!(
            !MultiPointCollection::is_reserved_name(name) && !self.types.contains_key(name),
            error::ColumnNameConflict {
                name: name.to_string()
            }
        );

        self.builders
            .insert(name.into(), data_type.arrow_builder(0));
        self.types.insert(name.into(), data_type);

        Ok(())
    }

    /// Finishes a row and checks for completion
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::MultiPointCollectionBuilder;
    /// use geoengine_datatypes::primitives::{FeatureData, TimeInterval};
    ///
    /// let mut builder = MultiPointCollectionBuilder::default();
    ///
    /// builder.append_coordinate((0.0, 0.0).into());
    /// builder.append_time_interval(TimeInterval::new_unchecked(0, 1));
    ///
    /// builder.finish_row().unwrap();
    ///
    /// builder.append_coordinate((0.0, 0.0).into());
    ///
    /// builder.finish_row().unwrap_err();
    /// ```
    ///
    pub fn finish_row(&mut self) -> Result<()> {
        let rows = self.rows + 1;

        ensure!(
            self.coordinates_builder.len() == rows
                && self.time_intervals_builder.len() == rows
                && self.builders.values().all(|builder| builder.len() == rows),
            error::FeatureCollectionBuilderException {
                details: "Cannot finish row when child data is missing",
            }
        );

        self.rows = rows;

        Ok(())
    }

    /// Adds a coordinate to the builder
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::MultiPointCollectionBuilder;
    /// use geoengine_datatypes::primitives::{FeatureData, TimeInterval};
    ///
    /// let mut builder = MultiPointCollectionBuilder::default();
    ///
    /// builder.append_coordinate((0.0, 0.0).into()).unwrap();
    /// builder.append_coordinate((1.0, 1.0).into()).unwrap_err();
    /// ```
    ///
    pub fn append_coordinate(&mut self, coordinate: Coordinate2D) -> Result<()> {
        ensure!(
            self.coordinates_builder.len() <= self.rows,
            error::FeatureCollectionBuilderException {
                details: "Cannot add another coordinate until row is finished",
            }
        );

        Self::append_single_coordinate_to_builder(self.coordinates_builder.values(), coordinate)?;

        self.coordinates_builder.append(true)?;

        Ok(())
    }

    /// Adds a multi coordinate to the builder
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::MultiPointCollectionBuilder;
    /// use geoengine_datatypes::primitives::{FeatureData, TimeInterval};
    ///
    /// let mut builder = MultiPointCollectionBuilder::default();
    ///
    /// builder.append_multi_coordinate(vec![(0.0, 0.1).into(), (1.0, 1.1).into()]).unwrap();
    /// builder.append_multi_coordinate(vec![(2.0, 2.1).into()]).unwrap_err();
    /// ```
    ///
    pub fn append_multi_coordinate(&mut self, coordinates: Vec<Coordinate2D>) -> Result<()> {
        ensure!(
            self.coordinates_builder.len() <= self.rows,
            error::FeatureCollectionBuilderException {
                details: "Cannot add another coordinate until row is finished",
            }
        );

        let coordinate_builder = self.coordinates_builder.values();
        for coordinate in coordinates {
            Self::append_single_coordinate_to_builder(coordinate_builder, coordinate)?;
        }

        self.coordinates_builder.append(true)?;

        Ok(())
    }

    fn append_single_coordinate_to_builder(
        coordinate_builder: &mut FixedSizeListBuilder<Float64Builder>,
        coordinate: Coordinate2D,
    ) -> Result<()> {
        let float_builder = coordinate_builder.values();
        float_builder.append_value(coordinate.x)?;
        float_builder.append_value(coordinate.y)?;

        coordinate_builder.append(true)?;

        Ok(())
    }

    /// Adds a time interval to the builder
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::MultiPointCollectionBuilder;
    /// use geoengine_datatypes::primitives::{FeatureData, TimeInterval};
    ///
    /// let mut builder = MultiPointCollectionBuilder::default();
    ///
    /// builder.append_time_interval(TimeInterval::new_unchecked(0, 1)).unwrap();
    /// builder.append_time_interval(TimeInterval::new_unchecked(1, 2)).unwrap_err();
    /// ```
    ///
    pub fn append_time_interval(&mut self, time_interval: TimeInterval) -> Result<()> {
        ensure!(
            self.time_intervals_builder.len() <= self.rows,
            error::FeatureCollectionBuilderException {
                details: "Cannot add another time interval until row is finished",
            }
        );

        let date_builder = self.time_intervals_builder.values();
        date_builder.append_value(time_interval.start())?;
        date_builder.append_value(time_interval.end())?;

        self.time_intervals_builder.append(true)?;

        Ok(())
    }

    /// Adds a data item to the current row
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::MultiPointCollectionBuilder;
    /// use geoengine_datatypes::primitives::{FeatureDataValue, FeatureDataType, TimeInterval};
    ///
    /// let mut builder = MultiPointCollectionBuilder::default();
    /// builder.add_column("foobar", FeatureDataType::Number);
    ///
    /// builder.append_data("foobar", FeatureDataValue::Number(0.)).unwrap();
    /// builder.append_data("foobar", FeatureDataValue::Number(1.)).unwrap_err();
    /// ```
    ///
    pub fn append_data(&mut self, column: &str, data: FeatureDataValue) -> Result<()> {
        ensure!(
            self.types.contains_key(column),
            error::FeatureCollectionBuilderException {
                details: format!("Column {} does not exist", column),
            }
        );

        let data_builder = self.builders.get_mut(column).unwrap(); // previously checked

        ensure!(
            data_builder.len() <= self.rows,
            error::FeatureCollectionBuilderException {
                details: "Cannot add another data item until row is finished",
            }
        );

        let _data_type = self.types.get(column).unwrap(); // previously checked

        ensure!(
            mem::discriminant(&FeatureDataType::from(&data)) == mem::discriminant(_data_type), // same enum variant
            error::FeatureCollectionBuilderException {
                details: "Data type is wrong for the column",
            }
        );

        match data {
            FeatureDataValue::Number(value) => {
                let number_builder: &mut Float64Builder = downcast_mut_array(data_builder.as_mut());
                number_builder.append_value(value)?;
            }
            FeatureDataValue::NullableNumber(value) => {
                let number_builder: &mut Float64Builder = downcast_mut_array(data_builder.as_mut());
                number_builder.append_option(value)?;
            }
            FeatureDataValue::Text(value) => {
                let string_builder: &mut StringBuilder = downcast_mut_array(data_builder.as_mut());
                string_builder.append_value(&value)?;
            }
            FeatureDataValue::NullableText(value) => {
                let string_builder: &mut StringBuilder = downcast_mut_array(data_builder.as_mut());
                if let Some(v) = &value {
                    string_builder.append_value(&v)?;
                } else {
                    string_builder.append_null()?;
                }
            }
            FeatureDataValue::Decimal(value) => {
                let decimal_builder: &mut Int64Builder = downcast_mut_array(data_builder.as_mut());
                decimal_builder.append_value(value)?;
            }
            FeatureDataValue::NullableDecimal(value) => {
                let decimal_builder: &mut Int64Builder = downcast_mut_array(data_builder.as_mut());
                decimal_builder.append_option(value)?;
            }
            FeatureDataValue::Categorical(value) => {
                let categorical_builder: &mut UInt8Builder =
                    downcast_mut_array(data_builder.as_mut());
                categorical_builder.append_value(value)?;
            }
            FeatureDataValue::NullableCategorical(value) => {
                let categorical_builder: &mut UInt8Builder =
                    downcast_mut_array(data_builder.as_mut());
                categorical_builder.append_option(value)?;
            }
        }

        Ok(())
    }

    /// Builds the point collection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{MultiPointCollectionBuilder, FeatureCollection};
    /// use geoengine_datatypes::primitives::{TimeInterval, FeatureDataType, FeatureDataValue};
    ///
    /// let mut builder = MultiPointCollectionBuilder::default();
    /// builder.add_column("foobar", FeatureDataType::Number).unwrap();
    ///
    /// builder.append_coordinate((0.0, 0.1).into()).unwrap();
    /// builder.append_time_interval(TimeInterval::new_unchecked(0, 1)).unwrap();
    /// builder.append_data("foobar", FeatureDataValue::Number(0.));
    ///
    /// builder.finish_row().unwrap();
    ///
    /// builder.append_coordinate((1.0, 1.1).into()).unwrap();
    /// builder.append_time_interval(TimeInterval::new_unchecked(1, 2)).unwrap();
    /// builder.append_data("foobar", FeatureDataValue::Number(1.));
    ///
    /// builder.finish_row().unwrap();
    ///
    /// let point_collection = builder.build().unwrap();
    ///
    /// assert_eq!(point_collection.len(), 2);
    /// assert_eq!(point_collection.coordinates(), &[(0.0, 0.1).into(), (1.0, 1.1).into()]);
    /// ```
    ///
    pub fn build(mut self) -> Result<MultiPointCollection> {
        ensure!(
            self.coordinates_builder.len() == self.rows
                && self.time_intervals_builder.len() == self.rows
                && self
                    .builders
                    .values()
                    .all(|builder| builder.len() == self.rows),
            error::FeatureCollectionBuilderException {
                details: "Cannot build a point collection out of unfinished rows",
            }
        );

        let mut columns = Vec::with_capacity(self.types.len() + 2);
        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(self.types.len() + 2);

        columns.push(Field::new(
            MultiPointCollection::FEATURE_COLUMN_NAME,
            MultiPointCollection::multi_points_data_type(),
            false,
        ));
        builders.push(Box::new(self.coordinates_builder));

        columns.push(Field::new(
            MultiPointCollection::TIME_COLUMN_NAME,
            MultiPointCollection::time_data_type(),
            false,
        ));
        builders.push(Box::new(self.time_intervals_builder));

        for (column_name, builder) in self.builders.drain() {
            let column_type = self.types.get(&column_name).unwrap(); // column must exist
            columns.push(Field::new(
                &column_name,
                column_type.arrow_data_type(),
                column_type.nullable(),
            ));
            builders.push(builder);
        }

        let mut struct_builder = StructBuilder::new(columns, builders);

        for _ in 0..self.rows {
            struct_builder.append(true)?;
        }

        Ok(MultiPointCollection {
            data: struct_builder.finish(),
            types: self.types,
        })
    }
}

fn struct_array_from_data(
    columns: Vec<Field>,
    column_values: Vec<ArrayRef>,
    number_of_features: usize,
) -> StructArray {
    StructArray::from(
        ArrayData::builder(DataType::Struct(columns))
            .child_data(column_values.into_iter().map(|a| a.data()).collect())
            .len(number_of_features)
            .build(),
    )
}

/// Helper function for inserting a primitive arrow array into a serde json map
fn insert_geo_json_values_in_maps<ArrayType>(
    array: &PrimitiveArray<ArrayType>,
    maps: &mut [serde_json::Map<String, serde_json::Value>],
    column_name: &str,
) where
    ArrayType: ArrowNumericType,
    ArrayType::Native: std::convert::Into<serde_json::Value>,
{
    for (&number, map) in array.value_slice(0, maps.len()).iter().zip(maps) {
        map.insert(column_name.into(), number.into());
    }
}

/// Helper function for inserting a nullable primitive arrow array into a serde json map
fn insert_nullable_geo_json_values_in_maps<ArrayType>(
    array: &PrimitiveArray<ArrayType>,
    maps: &mut [serde_json::Map<String, serde_json::Value>],
    column_name: &str,
) where
    ArrayType: ArrowNumericType,
    ArrayType::Native: std::convert::Into<serde_json::Value>,
{
    for (i, (&number, map)) in array
        .value_slice(0, maps.len())
        .iter()
        .zip(maps)
        .enumerate()
    {
        map.insert(
            column_name.into(),
            if array.is_null(i) {
                serde_json::Value::Null
            } else {
                number.into()
            },
        );
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn clone() {
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
