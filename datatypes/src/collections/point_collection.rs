use std::collections::HashMap;

use arrow::array::{
    Array, ArrayBuilder, ArrayData, ArrayRef, BooleanArray, Date64Array, Date64Builder,
    FixedSizeListArray, FixedSizeListBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, ListArray, ListBuilder, StringArray, StringBuilder, StructArray, StructBuilder,
    UInt8Array, UInt8Builder,
};
use arrow::compute::kernels::filter::filter;
use arrow::datatypes::DataType::Struct;
use arrow::datatypes::{DataType, DateUnit, Field};
use snafu::ensure;

use crate::collections::FeatureCollection;
use crate::error;
use crate::operations::Filterable;
use crate::primitives::{
    CategoricalDataRef, Coordinate, DecimalDataRef, FeatureData, FeatureDataRef, FeatureDataType,
    FeatureDataValue, NullableCategoricalDataRef, NullableDecimalDataRef, NullableNumberDataRef,
    NullableTextDataRef, NumberDataRef, TextDataRef, TimeInterval,
};
use crate::util::Result;
use std::mem;
use std::slice;
use std::sync::Arc;

#[derive(Debug)]
pub struct PointCollection {
    data: StructArray,
    types: HashMap<String, FeatureDataType>,
}

impl Clone for PointCollection {
    /// Clone the PointCollection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    ///
    /// let pc = PointCollection::empty();
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

impl PointCollection {
    /// Retrieve the composite arrow data type for multi points
    #[inline]
    pub(self) fn multi_points_data_type() -> DataType {
        DataType::List(DataType::FixedSizeList(DataType::Float64.into(), 2).into())
        // DataType::List(DataType::FixedSizeBinary(mem::size_of::<Coordinate>() as i32).into())
    }

    /// Retrieve the composite arrow data type for multi points
    #[inline]
    pub(self) fn time_data_type() -> DataType {
        DataType::FixedSizeList(DataType::Date64(DateUnit::Millisecond).into(), 2)
    }

    /// Create an empty PointCollection.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    ///
    /// let pc = PointCollection::empty();
    ///
    /// assert_eq!(pc.len(), 0);
    /// ```
    pub fn empty() -> Self {
        Self {
            data: {
                let fields = vec![
                    Field::new(Self::FEATURE_FIELD, Self::multi_points_data_type(), false),
                    Field::new(Self::TIME_FIELD, Self::time_data_type(), false),
                ];

                StructArray::from(ArrayData::builder(DataType::Struct(fields)).len(0).build())
            },
            types: Default::default(),
        }
    }

    /// Use a builder for creating the point collection
    pub fn builder() -> PointCollectionBuilder {
        Default::default()
    }

    /// Create a point collection from data
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{Coordinate, TimeInterval, FeatureData};
    /// use std::collections::HashMap;
    ///
    /// let pc = PointCollection::from_data(
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
        coordinates: Vec<Vec<Coordinate>>,
        time_intervals: Vec<TimeInterval>,
        data: HashMap<String, FeatureData>,
    ) -> Result<Self> {
        let capacity = coordinates.len();

        let mut fields = vec![
            Field::new(Self::FEATURE_FIELD, Self::multi_points_data_type(), false),
            Field::new(Self::TIME_FIELD, Self::time_data_type(), false),
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
                error::FieldNameConflict { name }
            );

            let field = Field::new(
                &name,
                feature_data.arrow_data_type(),
                feature_data.nullable(),
            );

            fields.push(field);
            builders.push(feature_data.arrow_builder()?);

            data_types.insert(name, FeatureDataType::from(&feature_data));
        }

        let mut struct_builder = StructBuilder::new(fields, builders);
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
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{Coordinate, TimeInterval, FeatureData};
    /// use std::collections::HashMap;
    ///
    /// let pc = PointCollection::from_data(
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
    pub fn coordinates(&self) -> &[Coordinate] {
        let features_ref = self
            .data
            .column_by_name(Self::FEATURE_FIELD)
            .expect("There must exist a feature field");
        let features: &ListArray = features_ref.as_any().downcast_ref().unwrap();

        let feature_coordinates_ref = features.values();
        let feature_coordinates: &FixedSizeListArray =
            feature_coordinates_ref.as_any().downcast_ref().unwrap();

        let number_of_coordinates = feature_coordinates.data().len();

        let floats_ref = feature_coordinates.values();
        let floats: &Float64Array = floats_ref.as_any().downcast_ref().unwrap();

        unsafe {
            slice::from_raw_parts(
                floats.raw_values() as *const Coordinate,
                number_of_coordinates,
            )
        }
    }

    /// Retrieves the time intervals of this point collection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{Coordinate, TimeInterval, FeatureData};
    /// use std::collections::HashMap;
    ///
    /// let pc = PointCollection::from_data(
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
    pub fn time_intervals(&self) -> &[TimeInterval] {
        let features_ref = self
            .data
            .column_by_name(Self::TIME_FIELD)
            .expect("There must exist a time interval field");
        let features: &FixedSizeListArray = features_ref.as_any().downcast_ref().unwrap();

        let number_of_time_intervals = self.len();

        let timestamps_ref = features.values();
        let timestamps: &Date64Array = timestamps_ref.as_any().downcast_ref().unwrap();

        unsafe {
            slice::from_raw_parts(
                timestamps.raw_values() as *const TimeInterval,
                number_of_time_intervals,
            )
        }
    }
}

impl FeatureCollection for PointCollection {
    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_simple(&self) -> bool {
        self.len() == self.coordinates().len()
    }

    /// Retrieves a data field of this point collection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{Coordinate, TimeInterval, FeatureData, FeatureDataRef, DataRef, NullableDataRef};
    /// use std::collections::HashMap;
    ///
    /// let pc = PointCollection::from_data(
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
    ///     assert_eq!(numbers.data(), &[0., 1., 2.]);
    /// } else {
    ///     unreachable!();
    /// }
    ///
    /// if let FeatureDataRef::NullableNumber(numbers) = pc.data("number_nulls").unwrap() {
    ///     assert_eq!(numbers.data()[0], 0.);
    ///     assert_eq!(numbers.data()[2], 2.);
    ///     assert_eq!(numbers.nulls(), vec![false, true, false]);
    /// } else {
    ///     unreachable!();
    /// }
    /// ```
    ///
    fn data(&self, field: &str) -> Result<FeatureDataRef> {
        ensure!(
            !Self::is_reserved_name(field),
            error::FeatureCollection {
                details: "Cannot access reserved fields via `data()` method"
            }
        );

        let column = self.data.column_by_name(field);

        ensure!(
            column.is_some(),
            error::FeatureCollection {
                details: format!("The field {} does not exist in the point collection", field)
            }
        );

        let column = column.unwrap();

        Ok(match self.types.get(field).unwrap() {
            FeatureDataType::Number => {
                let array: &Float64Array = column.as_any().downcast_ref().unwrap();
                NumberDataRef::new(array.values()).into()
            }
            FeatureDataType::NullableNumber => {
                let array: &Float64Array = column.as_any().downcast_ref().unwrap();
                NullableNumberDataRef::new(array.values(), array.data_ref().null_bitmap()).into()
            }
            FeatureDataType::Text => {
                let array: &StringArray = column.as_any().downcast_ref().unwrap();
                TextDataRef::new(array.value_data(), array.value_offsets()).into()
            }
            FeatureDataType::NullableText => {
                let array: &StringArray = column.as_any().downcast_ref().unwrap();
                NullableTextDataRef::new(array.value_data(), array.value_offsets()).into()
            }
            FeatureDataType::Decimal => {
                let array: &Int64Array = column.as_any().downcast_ref().unwrap();
                DecimalDataRef::new(array.values()).into()
            }
            FeatureDataType::NullableDecimal => {
                let array: &Int64Array = column.as_any().downcast_ref().unwrap();
                NullableDecimalDataRef::new(array.values(), array.data_ref().null_bitmap()).into()
            }
            FeatureDataType::Categorical => {
                let array: &UInt8Array = column.as_any().downcast_ref().unwrap();
                CategoricalDataRef::new(array.values()).into()
            }
            FeatureDataType::NullableCategorical => {
                let array: &UInt8Array = column.as_any().downcast_ref().unwrap();
                NullableCategoricalDataRef::new(array.values(), array.data_ref().null_bitmap())
                    .into()
            }
        })
    }
}

impl Filterable for PointCollection {
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{PointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{Coordinate, TimeInterval, FeatureData};
    /// use geoengine_datatypes::operations::Filterable;
    /// use std::collections::HashMap;
    ///
    /// let pc = PointCollection::from_data(
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
            if let Struct(fields) = self.data.data().data_type() {
                let mut filtered_data: Vec<(Field, ArrayRef)> = Vec::with_capacity(fields.len());
                for (field, array) in fields.iter().zip(self.data.columns()) {
                    match field.name().as_str() {
                        Self::FEATURE_FIELD => filtered_data.push((
                            field.clone(),
                            Arc::new(coordinates_filter(
                                array.as_any().downcast_ref().unwrap(),
                                &filter_array,
                            )?),
                        )),
                        Self::TIME_FIELD => filtered_data.push((
                            field.clone(),
                            Arc::new(time_interval_filter(
                                array.as_any().downcast_ref().unwrap(),
                                &filter_array,
                            )?),
                        )),
                        _ => filtered_data
                            .push((field.clone(), filter(array.as_ref(), &filter_array)?)),
                    }
                }
                filtered_data
            } else {
                unreachable!("data field must be a struct")
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
                let old_floats_array = old_coordinates
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap()
                    .value(coordinate_index as usize);

                let old_floats: &Float64Array = old_floats_array.as_any().downcast_ref().unwrap();

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
        let old_timestamps: &Date64Array = old_timestamps_ref.as_any().downcast_ref().unwrap();

        let date_builder = new_time_intervals.values();
        date_builder.append_slice(old_timestamps.value_slice(0, 2))?;

        new_time_intervals.append(true)?;
    }

    Ok(new_time_intervals.finish())
}

/// A row-by-row builder for a point collection
pub struct PointCollectionBuilder {
    coordinates_builder: ListBuilder<FixedSizeListBuilder<Float64Builder>>,
    time_intervals_builder: FixedSizeListBuilder<Date64Builder>,
    builders: HashMap<String, Box<dyn ArrayBuilder>>,
    types: HashMap<String, FeatureDataType>,
    rows: usize,
}

impl Default for PointCollectionBuilder {
    /// Creates a builder for a point collection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::PointCollectionBuilder;
    ///
    /// let builder = PointCollectionBuilder::default();
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

impl PointCollectionBuilder {
    /// Creates a builder for a point collection
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::PointCollectionBuilder;
    /// use geoengine_datatypes::primitives::FeatureDataType;
    ///
    /// let mut builder = PointCollectionBuilder::default();
    ///
    /// builder.add_field("foobar", FeatureDataType::Number).unwrap();
    /// builder.add_field("__features", FeatureDataType::Number).unwrap_err();
    /// ```
    ///
    pub fn add_field(&mut self, name: &str, data_type: FeatureDataType) -> Result<()> {
        ensure!(
            self.rows == 0,
            error::FeatureCollectionBuilderException {
                details: "It is not allowed to add further fields after data was inserted",
            }
        );
        ensure!(
            !PointCollection::is_reserved_name(name) && !self.types.contains_key(name),
            error::FieldNameConflict {
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
    /// use geoengine_datatypes::collections::PointCollectionBuilder;
    /// use geoengine_datatypes::primitives::{FeatureData, TimeInterval};
    ///
    /// let mut builder = PointCollectionBuilder::default();
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
    /// use geoengine_datatypes::collections::PointCollectionBuilder;
    /// use geoengine_datatypes::primitives::{FeatureData, TimeInterval};
    ///
    /// let mut builder = PointCollectionBuilder::default();
    ///
    /// builder.append_coordinate((0.0, 0.0).into()).unwrap();
    /// builder.append_coordinate((1.0, 1.0).into()).unwrap_err();
    /// ```
    ///
    pub fn append_coordinate(&mut self, coordinate: Coordinate) -> Result<()> {
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
    /// use geoengine_datatypes::collections::PointCollectionBuilder;
    /// use geoengine_datatypes::primitives::{FeatureData, TimeInterval};
    ///
    /// let mut builder = PointCollectionBuilder::default();
    ///
    /// builder.append_multi_coordinate(vec![(0.0, 0.1).into(), (1.0, 1.1).into()]).unwrap();
    /// builder.append_multi_coordinate(vec![(2.0, 2.1).into()]).unwrap_err();
    /// ```
    ///
    pub fn append_multi_coordinate(&mut self, coordinates: Vec<Coordinate>) -> Result<()> {
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
        coordinate: Coordinate,
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
    /// use geoengine_datatypes::collections::PointCollectionBuilder;
    /// use geoengine_datatypes::primitives::{FeatureData, TimeInterval};
    ///
    /// let mut builder = PointCollectionBuilder::default();
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

    /// Adds a time interval to the builder
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::PointCollectionBuilder;
    /// use geoengine_datatypes::primitives::{FeatureDataValue, FeatureDataType, TimeInterval};
    ///
    /// let mut builder = PointCollectionBuilder::default();
    /// builder.add_field("foobar", FeatureDataType::Number);
    ///
    /// builder.append_data("foobar", FeatureDataValue::Number(0.)).unwrap();
    /// builder.append_data("foobar", FeatureDataValue::Number(1.)).unwrap_err();
    /// ```
    ///
    pub fn append_data(&mut self, field: &str, data: FeatureDataValue) -> Result<()> {
        ensure!(
            self.types.contains_key(field),
            error::FeatureCollectionBuilderException {
                details: format!("Field {} does not exist", field),
            }
        );

        let data_builder = self.builders.get_mut(field).unwrap();

        ensure!(
            data_builder.len() <= self.rows,
            error::FeatureCollectionBuilderException {
                details: "Cannot add another data item until row is finished",
            }
        );

        let data_type = self.types.get(field).unwrap();

        ensure!(
            mem::discriminant(&FeatureDataType::from(&data)) == mem::discriminant(&data_type), // same enum variant
            error::FeatureCollectionBuilderException {
                details: "Data type is wrong for the field",
            }
        );

        match data {
            FeatureDataValue::Number(value) => {
                let number_builder: &mut Float64Builder =
                    data_builder.as_any_mut().downcast_mut().unwrap();
                number_builder.append_value(value)?;
            }
            FeatureDataValue::NullableNumber(value) => {
                let number_builder: &mut Float64Builder =
                    data_builder.as_any_mut().downcast_mut().unwrap();
                number_builder.append_option(value)?;
            }
            FeatureDataValue::Text(value) => {
                let string_builder: &mut StringBuilder =
                    data_builder.as_any_mut().downcast_mut().unwrap();
                string_builder.append_value(&value)?;
            }
            FeatureDataValue::NullableText(value) => {
                let string_builder: &mut StringBuilder =
                    data_builder.as_any_mut().downcast_mut().unwrap();
                if let Some(v) = &value {
                    string_builder.append_value(&v)?;
                } else {
                    string_builder.append_null()?;
                }
            }
            FeatureDataValue::Decimal(value) => {
                let decimal_builder: &mut Int64Builder =
                    data_builder.as_any_mut().downcast_mut().unwrap();
                decimal_builder.append_value(value)?;
            }
            FeatureDataValue::NullableDecimal(value) => {
                let decimal_builder: &mut Int64Builder =
                    data_builder.as_any_mut().downcast_mut().unwrap();
                decimal_builder.append_option(value)?;
            }
            FeatureDataValue::Categorical(value) => {
                let categorical_builder: &mut UInt8Builder =
                    data_builder.as_any_mut().downcast_mut().unwrap();
                categorical_builder.append_value(value)?;
            }
            FeatureDataValue::NullableCategorical(value) => {
                let categorical_builder: &mut UInt8Builder =
                    data_builder.as_any_mut().downcast_mut().unwrap();
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
    /// use geoengine_datatypes::collections::{PointCollectionBuilder, FeatureCollection};
    /// use geoengine_datatypes::primitives::{TimeInterval, FeatureDataType, FeatureDataValue};
    ///
    /// let mut builder = PointCollectionBuilder::default();
    /// builder.add_field("foobar", FeatureDataType::Number).unwrap();
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
    pub fn build(mut self) -> Result<PointCollection> {
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

        let mut fields = Vec::with_capacity(self.types.len() + 2);
        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(self.types.len() + 2);

        fields.push(Field::new(
            PointCollection::FEATURE_FIELD,
            PointCollection::multi_points_data_type(),
            false,
        ));
        builders.push(Box::new(self.coordinates_builder));

        fields.push(Field::new(
            PointCollection::TIME_FIELD,
            PointCollection::time_data_type(),
            false,
        ));
        builders.push(Box::new(self.time_intervals_builder));

        for (field_name, builder) in self.builders.drain() {
            let field_type = self.types.get(&field_name).unwrap();
            fields.push(Field::new(
                &field_name,
                field_type.arrow_data_type(),
                field_type.nullable(),
            ));
            builders.push(builder);
        }

        let mut struct_builder = StructBuilder::new(fields, builders);

        for _ in 0..self.rows {
            struct_builder.append(true)?;
        }

        Ok(PointCollection {
            data: struct_builder.finish(),
            types: self.types,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn clone() {
        let pc = PointCollection::from_data(
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
