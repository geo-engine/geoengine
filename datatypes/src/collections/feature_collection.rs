use std::collections::{HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use arrow::array::{
    as_primitive_array, as_string_array, Array, ArrayData, ArrayRef, BooleanArray, Float64Array,
    ListArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type};
use arrow::error::ArrowError;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::collections::{error, IntoGeometryIterator};
use crate::collections::{FeatureCollectionError, IntoGeometryOptionsIterator};
use crate::json_map;
use crate::primitives::{
    CategoricalDataRef, DecimalDataRef, FeatureData, FeatureDataRef, FeatureDataType,
    FeatureDataValue, Geometry, NullableCategoricalDataRef, NullableDecimalDataRef,
    NullableNumberDataRef, NullableTextDataRef, NumberDataRef, TextDataRef, TimeInterval,
};
use crate::util::arrow::{downcast_array, ArrowTyped};
use crate::util::helpers::SomeIter;
use crate::util::Result;
use std::mem;

#[allow(clippy::unsafe_derive_deserialize)]
#[derive(Debug, Deserialize, Serialize)]
pub struct FeatureCollection<CollectionType> {
    #[serde(with = "struct_serde")]
    pub(super) table: StructArray,

    // TODO: make it a `CoW`?
    pub(super) types: HashMap<String, FeatureDataType>,

    #[serde(skip)]
    collection_type: PhantomData<CollectionType>,
}

impl<CollectionType> FeatureCollection<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
{
    /// Reserved name for geometry column
    pub const GEOMETRY_COLUMN_NAME: &'static str = "__geometry";

    /// Reserved name for time column
    pub const TIME_COLUMN_NAME: &'static str = "__time";

    /// Create a `FeatureCollection` by populating its internal fields
    /// This provides no checks for validity.
    pub(super) fn new_from_internals(
        table: StructArray,
        types: HashMap<String, FeatureDataType>,
    ) -> Self {
        Self {
            table,
            types,
            collection_type: Default::default(),
        }
    }

    /// Create an empty `FeatureCollection`.
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
        let time_field = Field::new(
            Self::TIME_COLUMN_NAME,
            TimeInterval::arrow_data_type(),
            false,
        );

        let columns = if CollectionType::IS_GEOMETRY {
            let feature_field = Field::new(
                Self::GEOMETRY_COLUMN_NAME,
                CollectionType::arrow_data_type(),
                false,
            );
            vec![feature_field, time_field]
        } else {
            vec![time_field]
        };

        Self::new_from_internals(
            StructArray::from(ArrayData::builder(DataType::Struct(columns)).len(0).build()),
            Default::default(),
        )
    }

    /// Create a `FeatureCollection` from data
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{MultiPointCollection, FeatureCollection};
    /// use geoengine_datatypes::primitives::{Coordinate2D, TimeInterval, FeatureData, MultiPoint};
    /// use std::collections::HashMap;
    ///
    /// let pc = MultiPointCollection::from_data(
    ///     MultiPoint::many(vec![vec![(0., 0.)], vec![(1., 1.)]]).unwrap(),
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
        features: Vec<CollectionType>,
        time_intervals: Vec<TimeInterval>,
        data: HashMap<String, FeatureData>,
    ) -> Result<Self> {
        let number_of_rows = time_intervals.len();
        let number_of_column: usize =
            data.len() + 1 + (if CollectionType::IS_GEOMETRY { 1 } else { 0 });

        let mut columns: Vec<Field> = Vec::with_capacity(number_of_column);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(number_of_column);

        if CollectionType::IS_GEOMETRY {
            ensure!(
                features.len() == number_of_rows,
                error::UnmatchedLength {
                    a: features.len(),
                    b: number_of_rows
                }
            );

            columns.push(Field::new(
                Self::GEOMETRY_COLUMN_NAME,
                CollectionType::arrow_data_type(),
                false,
            ));

            arrays.push(Arc::new(CollectionType::from_vec(features)?));
        }

        columns.push(Field::new(
            Self::TIME_COLUMN_NAME,
            TimeInterval::arrow_data_type(),
            false,
        ));
        arrays.push(Arc::new(TimeInterval::from_vec(time_intervals)?));

        let mut types = HashMap::with_capacity(data.len());

        for (name, feature_data) in data {
            ensure!(
                !Self::is_reserved_name(&name),
                error::CannotAccessReservedColumn { name }
            );
            ensure!(
                feature_data.len() == number_of_rows,
                error::UnmatchedLength {
                    a: feature_data.len(),
                    b: number_of_rows
                }
            );

            let column = Field::new(
                &name,
                feature_data.arrow_data_type(),
                feature_data.nullable(),
            );

            columns.push(column);
            arrays.push(feature_data.arrow_builder()?.finish());

            types.insert(name, FeatureDataType::from(&feature_data));
        }

        Ok(Self::new_from_internals(
            struct_array_from_data(columns, arrays, number_of_rows),
            types,
        ))
    }

    /// Returns the number of features
    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// Returns whether the feature collection contains no features
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns whether this feature collection is simple, i.e., contains no multi-types
    pub fn is_simple(&self) -> bool {
        if !CollectionType::IS_GEOMETRY {
            return true; // a `FeatureCollection` without geometry column is simple by default
        }

        let array_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("must be there for collections with geometry");

        let features_array: &ListArray = downcast_array(array_ref);

        // TODO: assumes multi features, could be moved to `Geometry`
        let multi_geometry_array_ref = features_array.values();
        let multi_geometry_array: &ListArray = downcast_array(&multi_geometry_array_ref);

        multi_geometry_array.len() == features_array.len()
    }

    /// Checks for name conflicts with reserved names
    pub(super) fn is_reserved_name(name: &str) -> bool {
        name == Self::GEOMETRY_COLUMN_NAME || name == Self::TIME_COLUMN_NAME
    }

    /// Retrieves the column's `FeatureDataType`
    ///
    /// # Errors
    ///
    /// This method fails if there is no `column_name` with that name
    ///
    #[allow(clippy::option_if_let_else)]
    pub fn column_type(&self, column_name: &str) -> Result<FeatureDataType> {
        ensure!(
            !Self::is_reserved_name(column_name),
            error::CannotAccessReservedColumn {
                name: column_name.to_string(),
            }
        );

        if let Some(feature_data_type) = self.types.get(column_name) {
            Ok(*feature_data_type)
        } else {
            Err(error::FeatureCollectionError::ColumnDoesNotExist {
                name: column_name.to_string(),
            }
            .into())
        }
    }

    /// Retrieve column data
    ///
    /// # Errors
    ///
    /// This method fails if there is no `column_name` with that name
    ///
    pub fn data(&self, column_name: &str) -> Result<FeatureDataRef> {
        ensure!(
            !Self::is_reserved_name(column_name),
            error::CannotAccessReservedColumn {
                name: column_name.to_string(),
            }
        );

        let column = self.table.column_by_name(column_name).ok_or_else(|| {
            FeatureCollectionError::ColumnDoesNotExist {
                name: column_name.to_string(),
            }
        })?;

        Ok(
            match self.types.get(column_name).expect("previously checked") {
                FeatureDataType::Number => {
                    let array: &arrow::array::Float64Array = downcast_array(column);
                    NumberDataRef::new(array.values()).into()
                }
                FeatureDataType::NullableNumber => {
                    let array: &arrow::array::Float64Array = downcast_array(column);
                    NullableNumberDataRef::new(array.values(), array.data_ref().null_bitmap())
                        .into()
                }
                FeatureDataType::Text => {
                    let array: &arrow::array::StringArray = downcast_array(column);
                    TextDataRef::new(array.value_data(), array.value_offsets()).into()
                }
                FeatureDataType::NullableText => {
                    let array: &arrow::array::StringArray = downcast_array(column);
                    NullableTextDataRef::new(array.value_data(), array.value_offsets()).into()
                }
                FeatureDataType::Decimal => {
                    let array: &arrow::array::Int64Array = downcast_array(column);
                    DecimalDataRef::new(array.values()).into()
                }
                FeatureDataType::NullableDecimal => {
                    let array: &arrow::array::Int64Array = downcast_array(column);
                    NullableDecimalDataRef::new(array.values(), array.data_ref().null_bitmap())
                        .into()
                }
                FeatureDataType::Categorical => {
                    let array: &arrow::array::UInt8Array = downcast_array(column);
                    CategoricalDataRef::new(array.values()).into()
                }
                FeatureDataType::NullableCategorical => {
                    let array: &arrow::array::UInt8Array = downcast_array(column);
                    NullableCategoricalDataRef::new(array.values(), array.data_ref().null_bitmap())
                        .into()
                }
            },
        )
    }

    /// Retrieve time intervals
    pub fn time_intervals(&self) -> &[TimeInterval] {
        let features_ref = self
            .table
            .column_by_name(Self::TIME_COLUMN_NAME)
            .expect("Time interval column must exist");
        let features: &<TimeInterval as ArrowTyped>::ArrowArray = downcast_array(features_ref);

        let number_of_time_intervals = self.len();

        let timestamps_ref = features.values();
        let timestamps: &arrow::array::Date64Array = downcast_array(&timestamps_ref);

        unsafe {
            std::slice::from_raw_parts(
                timestamps.raw_values() as *const crate::primitives::TimeInterval,
                number_of_time_intervals,
            )
        }
    }

    /// Creates a copy of the collection with an additional column
    ///
    /// # Errors
    ///
    /// Adding a column fails if the column does already exist or the length does not match the length of the collection
    ///
    pub fn add_column(&self, new_column_name: &str, data: FeatureData) -> Result<Self> {
        self.add_columns(&[(new_column_name, data)])
    }

    /// Creates a copy of the collection with additional columns
    ///
    /// # Errors
    ///
    /// Adding columns fails if any column does already exist or the lengths do not match the length of the collection
    ///
    pub fn add_columns(&self, new_columns: &[(&str, FeatureData)]) -> Result<Self> {
        for &(new_column_name, ref data) in new_columns {
            ensure!(
                !Self::is_reserved_name(new_column_name)
                    && self.table.column_by_name(new_column_name).is_none(),
                error::ColumnAlreadyExists {
                    name: new_column_name.to_string(),
                }
            );

            ensure!(
                data.len() == self.table.len(),
                error::UnmatchedLength {
                    a: self.table.len(),
                    b: data.len(),
                }
            );
        }

        let number_of_old_columns = self.table.num_columns();
        let number_of_new_columns = new_columns.len();

        let mut columns = Vec::<arrow::datatypes::Field>::with_capacity(
            number_of_old_columns + number_of_new_columns,
        );
        let mut column_values = Vec::<arrow::array::ArrayRef>::with_capacity(
            number_of_old_columns + number_of_new_columns,
        );

        // copy geometry data if feature collection is geo collection
        if CollectionType::IS_GEOMETRY {
            columns.push(arrow::datatypes::Field::new(
                Self::GEOMETRY_COLUMN_NAME,
                CollectionType::arrow_data_type(),
                false,
            ));
            column_values.push(
                self.table
                    .column_by_name(Self::GEOMETRY_COLUMN_NAME)
                    .expect("The geometry column must exist")
                    .clone(),
            );
        }

        // copy time data
        columns.push(arrow::datatypes::Field::new(
            Self::TIME_COLUMN_NAME,
            TimeInterval::arrow_data_type(),
            false,
        ));
        column_values.push(
            self.table
                .column_by_name(Self::TIME_COLUMN_NAME)
                .expect("The time column must exist")
                .clone(),
        );

        // copy attribute data
        for (column_name, column_type) in &self.types {
            columns.push(arrow::datatypes::Field::new(
                &column_name,
                column_type.arrow_data_type(),
                column_type.nullable(),
            ));
            column_values.push(
                self.table
                    .column_by_name(&column_name)
                    .expect("The attribute column must exist")
                    .clone(),
            );
        }

        // create new type map
        let mut types = self.types.clone();

        // append new columns
        for &(new_column_name, ref data) in new_columns {
            columns.push(arrow::datatypes::Field::new(
                &new_column_name,
                data.arrow_data_type(),
                data.nullable(),
            ));
            column_values.push(data.arrow_builder().map(|mut builder| builder.finish())?);

            types.insert(
                new_column_name.to_string(),
                crate::primitives::FeatureDataType::from(data),
            );
        }

        Ok(Self::new_from_internals(
            struct_array_from_data(columns, column_values, self.table.len()),
            types,
        ))
    }

    /// Removes a column and returns an updated collection
    ///
    /// # Errors
    ///
    /// Removing a column fails if the column does not exist (or is reserved, e.g., the geometry column)
    ///
    pub fn remove_column(&self, column_name: &str) -> Result<Self> {
        self.remove_columns(&[column_name])
    }

    /// Removes columns and returns an updated collection
    ///
    /// # Errors
    ///
    /// Removing columns fails if any column does not exist (or is reserved, e.g., the geometry column)
    ///
    pub fn remove_columns(&self, removed_column_names: &[&str]) -> Result<Self> {
        for &removed_column_name in removed_column_names {
            ensure!(
                !Self::is_reserved_name(removed_column_name),
                error::CannotAccessReservedColumn {
                    name: removed_column_name.to_string(),
                }
            );
            ensure!(
                self.table.column_by_name(removed_column_name).is_some(),
                error::ColumnDoesNotExist {
                    name: removed_column_name.to_string(),
                }
            );
        }

        let number_of_old_columns = self.table.num_columns();
        let number_of_removed_columns = 1;

        let mut columns = Vec::<arrow::datatypes::Field>::with_capacity(
            number_of_old_columns - number_of_removed_columns,
        );
        let mut column_values = Vec::<arrow::array::ArrayRef>::with_capacity(
            number_of_old_columns - number_of_removed_columns,
        );
        let mut types = HashMap::<String, FeatureDataType>::with_capacity(
            number_of_old_columns - number_of_removed_columns,
        );

        // copy geometry data if feature collection is geo collection
        if CollectionType::IS_GEOMETRY {
            columns.push(arrow::datatypes::Field::new(
                Self::GEOMETRY_COLUMN_NAME,
                CollectionType::arrow_data_type(),
                false,
            ));
            column_values.push(
                self.table
                    .column_by_name(Self::GEOMETRY_COLUMN_NAME)
                    .expect("The geometry column must exist")
                    .clone(),
            );
        }

        // copy time data
        columns.push(arrow::datatypes::Field::new(
            Self::TIME_COLUMN_NAME,
            TimeInterval::arrow_data_type(),
            false,
        ));
        column_values.push(
            self.table
                .column_by_name(Self::TIME_COLUMN_NAME)
                .expect("The time column must exist")
                .clone(),
        );

        // copy remaining attribute data
        let removed_name_set: HashSet<&str> = removed_column_names.iter().cloned().collect();
        for (column_name, column_type) in &self.types {
            if removed_name_set.contains(column_name.as_str()) {
                continue;
            }

            columns.push(arrow::datatypes::Field::new(
                &column_name,
                column_type.arrow_data_type(),
                column_type.nullable(),
            ));
            column_values.push(
                self.table
                    .column_by_name(&column_name)
                    .expect("The attribute column must exist")
                    .clone(),
            );

            types.insert(column_name.to_string(), self.types[column_name]);
        }

        Ok(Self::new_from_internals(
            struct_array_from_data(columns, column_values, self.table.len()),
            types,
        ))
    }

    /// Filters the feature collection by copying the data into a new feature collection
    ///
    /// # Errors
    ///
    /// This method fails if the `mask`'s length does not equal the length of the feature collection
    ///
    pub fn filter<M>(&self, mask: M) -> Result<Self>
    where
        M: FilterArray,
    {
        ensure!(
            mask.len() == self.table.len(),
            error::UnmatchedLength {
                a: mask.len(),
                b: self.table.len(),
            }
        );

        let filter_array: arrow::array::BooleanArray = mask.into();

        // TODO: use filter directly on struct array when it is implemented

        let table_data = self.table.data();
        let columns = if let arrow::datatypes::DataType::Struct(columns) = table_data.data_type() {
            columns
        } else {
            unreachable!("`table` field must be a struct")
        };

        let mut filtered_data =
            Vec::<(arrow::datatypes::Field, arrow::array::ArrayRef)>::with_capacity(columns.len());

        for (column, array) in columns.iter().zip(self.table.columns()) {
            filtered_data.push((
                column.clone(),
                match column.name().as_str() {
                    Self::GEOMETRY_COLUMN_NAME => Arc::new(CollectionType::filter(
                        downcast_array(array),
                        &filter_array,
                    )?),
                    Self::TIME_COLUMN_NAME => {
                        Arc::new(TimeInterval::filter(downcast_array(array), &filter_array)?)
                    }
                    _ => arrow::compute::filter(array.as_ref(), &filter_array)?,
                },
            ));
        }

        Ok(Self::new_from_internals(
            filtered_data.into(),
            self.types.clone(),
        ))
    }

    /// Filter a column by one or more ranges.
    /// If `keep_nulls` is false, then all nulls will be discarded.
    pub fn column_range_filter<R>(
        &self,
        column: &str,
        ranges: &[R],
        keep_nulls: bool,
    ) -> Result<Self>
    where
        R: RangeBounds<FeatureDataValue>,
    {
        let column_type = self.types.get(column);
        ensure!(
            column_type.is_some(),
            error::ColumnDoesNotExist {
                name: column.to_string()
            }
        );

        let column = self
            .table
            .column_by_name(column)
            .expect("checked by ensure");
        let column_type = column_type.expect("checked by ensure");

        let mut filter_array = None;

        match column_type {
            FeatureDataType::Number | FeatureDataType::NullableNumber => {
                apply_filters(
                    as_primitive_array::<Float64Type>(column),
                    &mut filter_array,
                    ranges,
                    arrow::compute::gt_eq_scalar,
                    arrow::compute::gt_scalar,
                    arrow::compute::lt_eq_scalar,
                    arrow::compute::lt_scalar,
                )?;
            }
            FeatureDataType::Decimal | FeatureDataType::NullableDecimal => {
                apply_filters(
                    as_primitive_array::<Int64Type>(column),
                    &mut filter_array,
                    ranges,
                    arrow::compute::gt_eq_scalar,
                    arrow::compute::gt_scalar,
                    arrow::compute::lt_eq_scalar,
                    arrow::compute::lt_scalar,
                )?;
            }
            FeatureDataType::Text | FeatureDataType::NullableText => {
                apply_filters(
                    as_string_array(column),
                    &mut filter_array,
                    ranges,
                    arrow::compute::gt_eq_utf8_scalar,
                    arrow::compute::gt_utf8_scalar,
                    arrow::compute::lt_eq_utf8_scalar,
                    arrow::compute::lt_utf8_scalar,
                )?;
            }
            FeatureDataType::Categorical | FeatureDataType::NullableCategorical => {
                return Err(error::FeatureCollectionError::WrongDataType.into());
            }
        }

        ensure!(filter_array.is_some(), error::EmptyPredicate);

        // update filter array with nulls from original array
        if keep_nulls && column.null_count() > 0 {
            let null_bitmap = column
                .data_ref()
                .null_bitmap()
                .as_ref()
                .expect("must exist if null_count > 0");

            let mut null_array_builder = BooleanArray::builder(column.len());
            for i in 0..column.len() {
                null_array_builder.append_value(!null_bitmap.is_set(i))?;
            }
            let null_array = null_array_builder.finish();

            update_filter_array(&mut filter_array, Some(null_array), None)?;
        }

        self.filter(filter_array.expect("checked by ensure"))
    }

    /// Appends a collection to another one
    ///
    /// # Errors
    ///
    /// This method fails if the columns do not match
    ///
    pub fn append(&self, other: &Self) -> Result<Self> {
        ensure!(
            self.types == other.types,
            error::UnmatchedSchema {
                a: self.types.keys().cloned().collect::<Vec<String>>(),
                b: other.types.keys().cloned().collect::<Vec<String>>(),
            }
        );

        let table_data = self.table.data();
        let columns = if let DataType::Struct(columns) = table_data.data_type() {
            columns
        } else {
            unreachable!("`tables` field must be a struct")
        };

        let mut new_data = Vec::<(Field, ArrayRef)>::with_capacity(columns.len());

        // concat data column by column
        for (column, array_a) in columns.iter().zip(self.table.columns()) {
            let array_b = other
                .table
                .column_by_name(&column.name())
                .expect("column must occur in both collections");

            new_data.push((
                column.clone(),
                match column.name().as_str() {
                    Self::GEOMETRY_COLUMN_NAME => Arc::new(CollectionType::concat(
                        downcast_array(array_a),
                        downcast_array(array_b),
                    )?),
                    Self::TIME_COLUMN_NAME => Arc::new(TimeInterval::concat(
                        downcast_array(array_a),
                        downcast_array(array_b),
                    )?),
                    _ => arrow::compute::concat(&[array_a.clone(), array_b.clone()])?,
                },
            ));
        }

        Ok(Self::new_from_internals(
            new_data.into(),
            self.types.clone(),
        ))
    }

    /// Serialize the feature collection to a geo json string
    pub fn to_geo_json<'i>(&'i self) -> String
    where
        Self: IntoGeometryOptionsIterator<'i>, // TODO: remove here and impl for struct?
    {
        let mut property_maps = (0..self.len())
            .map(|_| serde_json::Map::with_capacity(self.types.len()))
            .collect::<Vec<_>>();

        for column_name in self.types.keys() {
            for (json_value, map) in self
                .data(column_name)
                .expect("must exist since it's in `types`")
                .json_values()
                .zip(property_maps.as_mut_slice())
            {
                map.insert(column_name.clone(), json_value);
            }
        }

        let features = self
            .geometry_options()
            .zip(self.time_intervals())
            .zip(property_maps)
            .map(
                |((geometry_option, time_interval), properties)| geojson::Feature {
                    bbox: None,
                    geometry: geometry_option.map(Into::into),
                    id: None,
                    properties: Some(properties),
                    foreign_members: Some(
                        json_map! {"when".to_string() => time_interval.to_geo_json_event()},
                    ),
                },
            )
            .collect();

        let feature_collection = geojson::FeatureCollection {
            bbox: None,
            features,
            foreign_members: None,
        };

        feature_collection.to_string()
    }

    /// Returns the byte-size of this collection
    pub fn byte_size(&self) -> usize {
        let table_size = get_array_memory_size(&self.table.data()) + mem::size_of_val(&self.table);

        // TODO: store information? avoid re-calculation?
        let map_size = mem::size_of_val(&self.types)
            + self
                .types
                .iter()
                .map(|(k, v)| mem::size_of_val(k) + k.as_bytes().len() + mem::size_of_val(v))
                .sum::<usize>();

        table_size + map_size
    }
}

impl<CollectionType> Clone for FeatureCollection<CollectionType> {
    fn clone(&self) -> Self {
        Self {
            table: StructArray::from(self.table.data()),
            types: self.types.clone(),
            collection_type: Default::default(),
        }
    }
}

impl<CollectionType> PartialEq for FeatureCollection<CollectionType> {
    fn eq(&self, other: &Self) -> bool {
        /// compares two `f64` typed columns
        /// treats `f64::NAN` values as if they are equal
        fn f64_column_equals(a: &Float64Array, b: &Float64Array) -> bool {
            if (a.len() != b.len()) || (a.null_count() != b.null_count()) {
                return false;
            }
            let number_of_values = a.len();

            if a.null_count() == 0 {
                let a_values: &[f64] = a.value_slice(0, number_of_values);
                let b_values: &[f64] = a.value_slice(0, number_of_values);

                for (&v1, &v2) in a_values.iter().zip(b_values) {
                    match (v1.is_nan(), v2.is_nan()) {
                        (true, true) => continue,
                        (false, false) if float_cmp::approx_eq!(f64, v1, v2) => continue,
                        _ => return false,
                    }
                }
            } else {
                for i in 0..number_of_values {
                    match (a.is_null(i), b.is_null(i)) {
                        (true, true) => continue,
                        (false, false) => (), // need to compare values
                        _ => return false,
                    };

                    let v1: f64 = a.value(i);
                    let v2: f64 = b.value(i);

                    match (v1.is_nan(), v2.is_nan()) {
                        (true, true) => continue,
                        (false, false) if float_cmp::approx_eq!(f64, v1, v2) => continue,
                        _ => return false,
                    }
                }
            }

            true
        }

        if self.types != other.types {
            return false;
        }

        for key in self.types.keys() {
            let c1 = self.table.column_by_name(key).expect("column must exist");
            let c2 = other.table.column_by_name(key).expect("column must exist");

            match (c1.data_type(), c2.data_type()) {
                (DataType::Float64, DataType::Float64) => {
                    if !f64_column_equals(downcast_array(c1), downcast_array(c2)) {
                        return false;
                    }
                }
                _ => {
                    if !c1.equals(c2.as_ref()) {
                        return false;
                    }
                }
            }
        }

        true
    }
}

/// This implements `IntoGeometryOptionsIterator` for `FeatureCollection`s that implement `IntoGeometryIterator`
impl<'i, CollectionType> IntoGeometryOptionsIterator<'i> for FeatureCollection<CollectionType>
where
    CollectionType: Geometry,
    Self: IntoGeometryIterator<'i>,
{
    type GeometryOptionIterator =
        SomeIter<<Self as IntoGeometryIterator<'i>>::GeometryIterator, Self::GeometryType>;
    type GeometryType = <Self as crate::collections::IntoGeometryIterator<'i>>::GeometryType;

    fn geometry_options(&'i self) -> Self::GeometryOptionIterator {
        SomeIter::new(self.geometries())
    }
}

/// Create an `arrow` struct from column meta data and data
fn struct_array_from_data(
    columns: Vec<Field>,
    column_values: Vec<ArrayRef>,
    number_of_features: usize,
) -> StructArray {
    StructArray::from(
        ArrayData::builder(arrow::datatypes::DataType::Struct(columns))
            .child_data(column_values.into_iter().map(|a| a.data()).collect())
            .len(number_of_features)
            .build(),
    )
}

/// Types that are suitable to act as filters
pub trait FilterArray: Into<BooleanArray> {
    fn len(&self) -> usize;
}

impl FilterArray for Vec<bool> {
    fn len(&self) -> usize {
        Vec::<_>::len(self)
    }
}

impl FilterArray for BooleanArray {
    fn len(&self) -> usize {
        <Self as arrow::array::Array>::len(self)
    }
}

fn update_filter_array(
    filter_array: &mut Option<BooleanArray>,
    partial_filter_a: Option<BooleanArray>,
    partial_filter_b: Option<BooleanArray>,
) -> Result<()> {
    let partial_filter = match (partial_filter_a, partial_filter_b) {
        (Some(f1), Some(f2)) => Some(arrow::compute::and(&f1, &f2)?),
        (Some(f1), None) => Some(f1),
        (None, Some(f2)) => Some(f2),
        (None, None) => None,
    };

    *filter_array = match (filter_array.take(), partial_filter) {
        (Some(f1), Some(f2)) => Some(arrow::compute::or(&f1, &f2)?),
        (Some(f1), None) => Some(f1),
        (None, Some(f2)) => Some(f2),
        (None, None) => None,
    };

    Ok(())
}

fn apply_filter_on_bound<'b, T, A>(
    bound: Bound<&'b FeatureDataValue>,
    array: &'b A,
    included_fn: fn(&'b A, T) -> Result<BooleanArray, ArrowError>,
    excluded_fn: fn(&'b A, T) -> Result<BooleanArray, ArrowError>,
) -> Result<Option<BooleanArray>>
where
    T: TryFrom<&'b FeatureDataValue, Error = error::FeatureCollectionError>,
{
    Ok(match bound {
        Bound::Included(v) => Some(included_fn(array, v.try_into()?)?),
        Bound::Excluded(v) => Some(excluded_fn(array, v.try_into()?)?),
        Bound::Unbounded => None,
    })
}

fn apply_filters<'b, T, A, R>(
    column: &'b A,
    filter_array: &mut Option<BooleanArray>,
    ranges: &'b [R],
    included_lower_fn: fn(&'b A, T) -> Result<BooleanArray, ArrowError>,
    excluded_lower_fn: fn(&'b A, T) -> Result<BooleanArray, ArrowError>,
    included_upper_fn: fn(&'b A, T) -> Result<BooleanArray, ArrowError>,
    excluded_upper_fn: fn(&'b A, T) -> Result<BooleanArray, ArrowError>,
) -> Result<()>
where
    T: TryFrom<&'b FeatureDataValue, Error = error::FeatureCollectionError>,
    R: RangeBounds<FeatureDataValue>,
{
    for range in ranges {
        update_filter_array(
            filter_array,
            apply_filter_on_bound(
                range.start_bound(),
                column,
                included_lower_fn,
                excluded_lower_fn,
            )?,
            apply_filter_on_bound(
                range.end_bound(),
                column,
                included_upper_fn,
                excluded_upper_fn,
            )?,
        )?;
    }

    Ok(())
}

/// Taken from <https://github.com/apache/arrow/blob/master/rust/arrow/src/array/data.rs>
/// TODO: replace with existing call on next version
pub fn get_array_memory_size(data: &ArrayData) -> usize {
    let mut size = 0;
    // Calculate size of the fields that don't have [get_array_memory_size] method internally.
    size += mem::size_of_val(data)
        - mem::size_of_val(&data.buffers())
        - mem::size_of_val(&data.null_bitmap())
        - mem::size_of_val(&data.child_data());

    // Calculate rest of the fields top down which contain actual data
    for buffer in data.buffers() {
        size += mem::size_of_val(&buffer);
        size += buffer.capacity();
    }
    if let Some(bitmap) = data.null_bitmap() {
        size += bitmap.buffer_ref().capacity() + mem::size_of_val(bitmap);
    }
    for child in data.child_data() {
        size += get_array_memory_size(child);
    }

    size
}

/// Custom serializer for Arrow's `StructArray`
mod struct_serde {
    use super::*;

    use arrow::record_batch::{RecordBatch, RecordBatchReader};
    use serde::de::{SeqAccess, Visitor};
    use serde::ser::Error;
    use serde::{Deserializer, Serializer};
    use std::fmt::Formatter;
    use std::io::Cursor;

    pub fn serialize<S>(struct_array: &StructArray, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let batch = RecordBatch::from(struct_array);

        let mut serialized_struct = Vec::<u8>::new();

        let mut writer = arrow::ipc::writer::FileWriter::try_new(
            &mut serialized_struct,
            batch.schema().as_ref(),
        )
        .map_err(|error| S::Error::custom(error.to_string()))?;
        writer
            .write(&batch)
            .map_err(|error| S::Error::custom(error.to_string()))?;
        writer
            .finish()
            .map_err(|error| S::Error::custom(error.to_string()))?;

        drop(writer);

        serializer.serialize_bytes(&serialized_struct)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<StructArray, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(StructArrayDeserializer)
    }

    struct StructArrayDeserializer;

    impl<'de> Visitor<'de> for StructArrayDeserializer {
        type Value = StructArray;

        fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
            formatter.write_str("an Arrow StructArray")
        }

        fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            let cursor = Cursor::new(v);

            let mut reader = arrow::ipc::reader::FileReader::try_new(cursor)
                .map_err(|error| E::custom(error.to_string()))?;

            if reader.num_batches() != 1 {
                return Err(E::custom(
                    "there must be exactly one batch for deserializing this struct",
                ));
            }

            let batch = reader
                .next_batch()
                .map_err(|error| E::custom(error.to_string()))?
                .expect("checked");

            Ok(batch.into())
        }

        // TODO: this is super stupid, but serde calls this function somehow
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0));

            while let Some(byte) = seq.next_element()? {
                bytes.push(byte);
            }

            self.visit_byte_buf(bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::primitives::{MultiPoint, NoGeometry};

    use super::*;

    #[test]
    fn is_reserved_name() {
        assert!(FeatureCollection::<MultiPoint>::is_reserved_name(
            "__geometry"
        ));
        assert!(!FeatureCollection::<NoGeometry>::is_reserved_name("foobar"));
    }

    #[test]
    fn byte_size() {
        fn gen_collection(length: usize) -> FeatureCollection<NoGeometry> {
            FeatureCollection::<NoGeometry>::from_data(
                vec![],
                vec![TimeInterval::new(0, 1).unwrap(); length],
                Default::default(),
            )
            .unwrap()
        }

        fn time_interval_size(length: usize) -> usize {
            if length == 0 {
                return 0;
            }

            let base = 64;
            let buffer = (((length - 1) / 4) + 1) * ((8 + 8) * 4);

            base + buffer
        }

        let empty_hash_map_size = 48;
        assert_eq!(
            mem::size_of::<HashMap<String, FeatureData>>(),
            empty_hash_map_size
        );

        let struct_stack_size = 32;
        assert_eq!(mem::size_of::<StructArray>(), struct_stack_size);

        for i in 0..10 {
            assert_eq!(
                gen_collection(i).byte_size(),
                empty_hash_map_size + struct_stack_size + 264 + time_interval_size(i)
            );
        }

        // TODO: rely on numbers once the arrow library provides this feature
    }
}
