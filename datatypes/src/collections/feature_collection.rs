use arrow::datatypes::{DataType, Date64Type, Field, Float64Type, Int64Type};
use arrow::error::ArrowError;
use arrow::{
    array::{
        as_boolean_array, as_primitive_array, as_string_array, Array, ArrayRef, BooleanArray,
        ListArray, StructArray,
    },
    buffer::Buffer,
};
use serde::{Deserialize, Serialize};
use serde_json::Map;
use snafu::ensure;

use std::collections::hash_map;
use std::collections::{HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::rc::Rc;
use std::sync::Arc;
use std::{mem, slice};

use crate::primitives::CacheHint;
use crate::primitives::{BoolDataRef, Coordinate2D, DateTimeDataRef, TimeInstance};
use crate::primitives::{
    CategoryDataRef, FeatureData, FeatureDataRef, FeatureDataType, FeatureDataValue, FloatDataRef,
    Geometry, IntDataRef, TextDataRef, TimeInterval,
};
use crate::util::arrow::{downcast_array, ArrowTyped};
use crate::util::helpers::SomeIter;
use crate::util::Result;
use crate::{
    collections::{error, IntoGeometryIterator, VectorDataType, VectorDataTyped},
    operations::reproject::Reproject,
};
use crate::{
    collections::{FeatureCollectionError, IntoGeometryOptionsIterator},
    operations::reproject::CoordinateProjection,
};
use std::iter::FromIterator;

use super::{geo_feature_collection::ReplaceRawArrayCoords, GeometryCollection};

#[allow(clippy::unsafe_derive_deserialize)]
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureCollection<CollectionType> {
    #[serde(with = "struct_serde")]
    pub(super) table: StructArray,

    // TODO: make it a `CoW`?
    pub(super) types: HashMap<String, FeatureDataType>,

    #[serde(skip)]
    collection_type: PhantomData<CollectionType>,

    pub cache_hint: CacheHint,
}

impl<CollectionType> FeatureCollection<CollectionType> {
    /// Reserved name for geometry column
    pub const GEOMETRY_COLUMN_NAME: &'static str = "__geometry";

    /// Reserved name for time column
    pub const TIME_COLUMN_NAME: &'static str = "__time";

    /// Create a `FeatureCollection` by populating its internal fields
    /// This provides no checks for validity.
    pub(super) fn new_from_internals(
        table: StructArray,
        types: HashMap<String, FeatureDataType>,
        cache_hint: CacheHint,
    ) -> Self {
        Self {
            table,
            types,
            collection_type: Default::default(),
            cache_hint,
        }
    }
}

impl<CollectionType> AsRef<FeatureCollection<CollectionType>>
    for FeatureCollection<CollectionType>
{
    fn as_ref(&self) -> &FeatureCollection<CollectionType> {
        self
    }
}

#[derive(Debug, Clone)]
pub struct FeatureCollectionInternals<G> {
    pub table: StructArray,
    pub types: HashMap<String, FeatureDataType>,
    pub collection_type: PhantomData<G>,
    pub cache_hint: CacheHint,
}

impl<G> From<FeatureCollectionInternals<G>> for FeatureCollection<G> {
    fn from(internals: FeatureCollectionInternals<G>) -> FeatureCollection<G> {
        FeatureCollection {
            table: internals.table,
            types: internals.types,
            collection_type: internals.collection_type,
            cache_hint: internals.cache_hint,
        }
    }
}

impl<G> From<FeatureCollection<G>> for FeatureCollectionInternals<G> {
    fn from(collection: FeatureCollection<G>) -> FeatureCollectionInternals<G> {
        FeatureCollectionInternals {
            table: collection.table,
            types: collection.types,
            collection_type: collection.collection_type,
            cache_hint: collection.cache_hint,
        }
    }
}

/// A trait for common feature collection modifications that are independent of the geometry type
pub trait FeatureCollectionModifications {
    type Output;

    /// Filters the feature collection by copying the data into a new feature collection
    ///
    /// # Errors
    ///
    /// This method fails if the `mask`'s length does not equal the length of the feature collection
    ///
    fn filter<M>(&self, mask: M) -> Result<Self::Output>
    where
        M: FilterArray;

    /// Creates a copy of the collection with an additional column
    ///
    /// # Errors
    ///
    /// Adding a column fails if the column does already exist or the length does not match the length of the collection
    ///
    fn add_column(&self, new_column_name: &str, data: FeatureData) -> Result<Self::Output> {
        self.add_columns(&[(new_column_name, data)])
    }

    /// Creates a copy of the collection with additional columns
    ///
    /// # Errors
    ///
    /// Adding columns fails if any column does already exist or the lengths do not match the length of the collection
    ///
    fn add_columns(&self, new_columns: &[(&str, FeatureData)]) -> Result<Self::Output>;

    /// Removes a column and returns an updated collection
    ///
    /// # Errors
    ///
    /// Removing a column fails if the column does not exist (or is reserved, e.g., the geometry column)
    ///
    fn remove_column(&self, column_name: &str) -> Result<Self::Output> {
        self.remove_columns(&[column_name])
    }

    /// Removes columns and returns an updated collection
    ///
    /// # Errors
    ///
    /// Removing columns fails if any column does not exist (or is reserved, e.g., the geometry column)
    ///
    fn remove_columns(&self, removed_column_names: &[&str]) -> Result<Self::Output>;

    /// Filter a column by one or more ranges.
    /// If `keep_nulls` is false, then all nulls will be discarded.
    fn column_range_filter<R>(
        &self,
        column: &str,
        ranges: &[R],
        keep_nulls: bool,
    ) -> Result<Self::Output>
    where
        R: RangeBounds<FeatureDataValue>;

    /// Appends a collection to another one
    ///
    /// # Errors
    ///
    /// This method fails if the columns do not match
    ///
    fn append(&self, other: &Self) -> Result<Self::Output>;

    /// Rename column `old_column_name` to `new_column_name`.
    fn rename_column(&self, old_column_name: &str, new_column_name: &str) -> Result<Self::Output> {
        self.rename_columns(&[(old_column_name, new_column_name)])
    }

    /// Rename selected columns with (from, to) tuples.
    fn rename_columns<S1, S2>(&self, renamings: &[(S1, S2)]) -> Result<Self::Output>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;

    /// Sorts the features in this collection by their timestamps ascending.
    fn sort_by_time_asc(&self) -> Result<Self::Output>;

    /// Replaces the current time intervals and returns an updated collection.
    fn replace_time(&self, time_intervals: &[TimeInterval]) -> Result<Self::Output>;
}

impl<CollectionType> FeatureCollectionModifications for FeatureCollection<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
{
    type Output = Self;

    fn filter<M>(&self, mask: M) -> Result<Self::Output>
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

        let arrow::datatypes::DataType::Struct(columns) = self.table.data_type() else {
            unreachable!("`table` field must be a struct")
        };

        let mut filtered_data = Vec::<ArrayRef>::with_capacity(columns.len());

        for (column, array) in columns.iter().zip(self.table.columns()) {
            filtered_data.push(match column.name().as_str() {
                Self::GEOMETRY_COLUMN_NAME => Arc::new(CollectionType::filter(
                    downcast_array(array),
                    &filter_array,
                )?),
                Self::TIME_COLUMN_NAME => {
                    Arc::new(TimeInterval::filter(downcast_array(array), &filter_array)?)
                }
                _ => arrow::compute::filter(array.as_ref(), &filter_array)?,
            });
        }

        Ok(Self::new_from_internals(
            StructArray::try_new(columns.clone(), filtered_data, None)?,
            self.types.clone(),
            self.cache_hint,
        ))
    }

    fn add_columns(&self, new_columns: &[(&str, FeatureData)]) -> Result<Self::Output> {
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
                    .expect("The geometry column should have been added during creation of the collection")
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
                .expect("The time column should have been added during creation of the collection")
                .clone(),
        );

        // copy attribute data
        for (column_name, column_type) in &self.types {
            columns.push(arrow::datatypes::Field::new(
                column_name,
                column_type.arrow_data_type(),
                column_type.nullable(),
            ));
            column_values.push(
                self.table
                    .column_by_name(column_name)
                    .expect("The attribute column should exist because the `types` are kept in sync with the `table`")
                    .clone(),
            );
        }

        // create new type map
        let mut types = self.types.clone();

        // append new columns
        for &(new_column_name, ref data) in new_columns {
            columns.push(arrow::datatypes::Field::new(
                new_column_name,
                data.arrow_data_type(),
                data.nullable(),
            ));
            column_values.push(data.arrow_builder().finish());

            types.insert(
                new_column_name.to_string(),
                crate::primitives::FeatureDataType::from(data),
            );
        }

        Ok(Self::new_from_internals(
            StructArray::try_new(columns.into(), column_values, None)?,
            types,
            self.cache_hint,
        ))
    }

    fn remove_columns(&self, removed_column_names: &[&str]) -> Result<Self::Output> {
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
                    .expect("The geometry column should have been added during creation of the collection")
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
                .expect("The time column should have been added during creation of the collection")
                .clone(),
        );

        // copy remaining attribute data
        let removed_name_set: HashSet<&str> = removed_column_names.iter().copied().collect();
        for (column_name, column_type) in &self.types {
            if removed_name_set.contains(column_name.as_str()) {
                continue;
            }

            columns.push(arrow::datatypes::Field::new(
                column_name,
                column_type.arrow_data_type(),
                column_type.nullable(),
            ));
            column_values.push(
                self.table
                    .column_by_name(column_name)
                    .expect("The attribute column should exist because the `types` are kept in sync with the `table`")
                    .clone(),
            );

            types.insert(column_name.to_string(), self.types[column_name]);
        }

        Ok(Self::new_from_internals(
            StructArray::try_new(columns.into(), column_values, None)?,
            types,
            self.cache_hint,
        ))
    }

    fn column_range_filter<R>(
        &self,
        column: &str,
        ranges: &[R],
        keep_nulls: bool,
    ) -> Result<Self::Output>
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
            FeatureDataType::Float => {
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
            FeatureDataType::Int => {
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
            FeatureDataType::Text => {
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
            FeatureDataType::Bool => {
                apply_filters(
                    as_boolean_array(column),
                    &mut filter_array,
                    ranges,
                    arrow::compute::gt_eq_bool_scalar,
                    arrow::compute::gt_bool_scalar,
                    arrow::compute::lt_eq_bool_scalar,
                    arrow::compute::lt_bool_scalar,
                )?;
            }
            FeatureDataType::DateTime => {
                apply_filters(
                    as_primitive_array::<Date64Type>(column),
                    &mut filter_array,
                    ranges,
                    arrow::compute::gt_eq_scalar,
                    arrow::compute::gt_scalar,
                    arrow::compute::lt_eq_scalar,
                    arrow::compute::lt_scalar,
                )?;
            }
            FeatureDataType::Category => {
                return Err(error::FeatureCollectionError::WrongDataType.into());
            }
        }

        ensure!(filter_array.is_some(), error::EmptyPredicate);
        let mut filter_array = filter_array.expect("checked by ensure");

        if keep_nulls && column.null_count() > 0 {
            let null_flags = arrow::compute::is_null(column.as_ref())?;
            filter_array = arrow::compute::or_kleene(&filter_array, &null_flags)?;
        }

        self.filter(filter_array)
    }

    fn append(&self, other: &Self) -> Result<Self::Output> {
        ensure!(
            self.types == other.types,
            error::UnmatchedSchema {
                a: self.types.keys().cloned().collect::<Vec<String>>(),
                b: other.types.keys().cloned().collect::<Vec<String>>(),
            }
        );

        let DataType::Struct(columns) = self.table.data_type() else {
            unreachable!("`tables` field must be a struct")
        };

        let mut new_data = Vec::<ArrayRef>::with_capacity(columns.len());

        // concat data column by column
        for (column, array_a) in columns.iter().zip(self.table.columns()) {
            let array_b = other
                .table
                .column_by_name(column.name())
                .expect("column must occur in both collections");

            new_data.push(match column.name().as_str() {
                Self::GEOMETRY_COLUMN_NAME => Arc::new(CollectionType::concat(
                    downcast_array(array_a),
                    downcast_array(array_b),
                )?),
                Self::TIME_COLUMN_NAME => Arc::new(TimeInterval::concat(
                    downcast_array(array_a),
                    downcast_array(array_b),
                )?),
                _ => arrow::compute::concat(&[array_a.as_ref(), array_b.as_ref()])?,
            });
        }

        Ok(Self::new_from_internals(
            StructArray::try_new(columns.clone(), new_data, None)?,
            self.types.clone(),
            self.cache_hint.merged(&other.cache_hint),
        ))
    }

    fn rename_columns<S1, S2>(&self, renamings: &[(S1, S2)]) -> Result<Self::Output>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let mut rename_map: HashMap<&str, &str> = HashMap::with_capacity(renamings.len());
        let mut value_set: HashSet<&str> = HashSet::with_capacity(renamings.len());

        for (old_column_name, new_column_name) in renamings {
            let old_column_name = old_column_name.as_ref();
            let new_column_name = new_column_name.as_ref();

            ensure!(
                !Self::is_reserved_name(new_column_name),
                error::CannotAccessReservedColumn {
                    name: new_column_name.to_string(),
                }
            );
            ensure!(
                self.table.column_by_name(old_column_name).is_some(),
                error::ColumnDoesNotExist {
                    name: old_column_name.to_string(),
                }
            );
            ensure!(
                self.table.column_by_name(new_column_name).is_none(),
                error::ColumnAlreadyExists {
                    name: new_column_name.to_string(),
                }
            );

            if let Some(duplicate) = rename_map.insert(old_column_name, new_column_name) {
                return Err(FeatureCollectionError::ColumnDuplicate {
                    name: duplicate.to_string(),
                }
                .into());
            }

            if !value_set.insert(new_column_name) {
                return Err(FeatureCollectionError::ColumnDuplicate {
                    name: new_column_name.to_string(),
                }
                .into());
            }
        }

        let mut columns = Vec::<arrow::datatypes::Field>::with_capacity(self.table.num_columns());
        let mut column_values =
            Vec::<arrow::array::ArrayRef>::with_capacity(self.table.num_columns());
        let mut types = HashMap::<String, FeatureDataType>::with_capacity(self.table.num_columns());

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
                    .expect("The geometry column should have been added during creation of the collection")
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
                .expect("The time column should have been added during creation of the collection")
                .clone(),
        );

        // copy remaining attribute data
        for (old_column_name, column_type) in &self.types {
            let new_column_name: &str = rename_map
                .get(&old_column_name.as_str())
                .unwrap_or(&old_column_name.as_str());

            columns.push(arrow::datatypes::Field::new(
                new_column_name,
                column_type.arrow_data_type(),
                column_type.nullable(),
            ));
            column_values.push(
                self.table
                    .column_by_name(old_column_name)
                    .expect("The attribute column should exist because the `types` are kept in sync with the `table`")
                    .clone(),
            );

            types.insert(new_column_name.to_string(), self.types[old_column_name]);
        }

        Ok(Self::new_from_internals(
            StructArray::try_new(columns.into(), column_values, None)?,
            types,
            self.cache_hint,
        ))
    }

    #[allow(clippy::too_many_lines)]
    fn sort_by_time_asc(&self) -> Result<Self::Output> {
        let time_column = self
            .table
            .column_by_name(Self::TIME_COLUMN_NAME)
            .expect("should exist");

        let sort_options = Some(arrow::compute::SortOptions {
            descending: false,
            nulls_first: false,
        });

        let sort_indices = arrow::compute::sort_to_indices(time_column, sort_options, None)?;

        let table_ref = arrow::compute::take(&self.table, &sort_indices, None)?;

        let table = StructArray::from(table_ref.into_data());

        Ok(Self::new_from_internals(
            table,
            self.types.clone(),
            self.cache_hint,
        ))
    }

    fn replace_time(&self, time_intervals: &[TimeInterval]) -> Result<Self::Output> {
        let mut time_intervals_builder = TimeInterval::arrow_builder(time_intervals.len());

        for time_interval in time_intervals {
            let date_builder = time_intervals_builder.values();
            date_builder.append_value(time_interval.start().inner());
            date_builder.append_value(time_interval.end().inner());
            time_intervals_builder.append(true);
        }

        let time_intervals = time_intervals_builder.finish();

        let mut columns = Vec::<arrow::datatypes::Field>::with_capacity(self.table.num_columns());
        let mut column_values =
            Vec::<arrow::array::ArrayRef>::with_capacity(self.table.num_columns());

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
                    .expect("There should exist a geometry column")
                    .clone(),
            );
        }

        // copy time data
        columns.push(arrow::datatypes::Field::new(
            Self::TIME_COLUMN_NAME,
            TimeInterval::arrow_data_type(),
            false,
        ));
        column_values.push(Arc::new(time_intervals));

        // copy remaining attribute data
        for (column_name, column_type) in &self.types {
            columns.push(arrow::datatypes::Field::new(
                column_name,
                column_type.arrow_data_type(),
                column_type.nullable(),
            ));
            column_values.push(
                self.table
                    .column_by_name(column_name)
                    .expect("The attribute column should exist")
                    .clone(),
            );
        }

        Ok(Self::new_from_internals(
            StructArray::try_new(columns.into(), column_values, None)?,
            self.types.clone(),
            self.cache_hint,
        ))
    }
}

/// A trait for common feature collection information
pub trait FeatureCollectionInfos {
    /// Returns the number of features
    fn len(&self) -> usize;

    /// Returns whether the feature collection contains no features
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns whether this feature collection is simple, i.e., contains no multi-types
    fn is_simple(&self) -> bool;

    /// Retrieves the column's `FeatureDataType`
    ///
    /// # Errors
    ///
    /// This method fails if there is no `column_name` with that name
    ///
    fn column_type(&self, column_name: &str) -> Result<FeatureDataType>;

    /// Get a copy of the column type information
    fn column_types(&self) -> HashMap<String, FeatureDataType>;

    /// Return the column names of all attributes
    fn column_names(&self) -> hash_map::Keys<String, FeatureDataType>;

    /// Return the names of the columns of this type
    fn column_names_of_type(&self, column_type: FeatureDataType) -> FilteredColumnNameIter;

    /// Retrieve column data
    ///
    /// # Errors
    ///
    /// This method fails if there is no `column_name` with that name
    ///
    fn data(&self, column_name: &str) -> Result<FeatureDataRef>;

    /// Retrieve time intervals
    fn time_intervals(&self) -> &[TimeInterval];

    /// Calculate the collection bounds over all time intervals,
    /// i.e., an interval of the smallest and largest time start and end.
    fn time_bounds(&self) -> Option<TimeInterval> {
        self.time_intervals()
            .iter()
            .copied()
            .reduce(|t1, t2| t1.extend(&t2))
    }

    /// Returns the byte-size of this collection
    fn byte_size(&self) -> usize;
}

pub struct ColumnNamesIter<'i, I>
where
    I: Iterator<Item = &'i str> + 'i,
{
    iter: I,
}

impl<'i, I> Iterator for ColumnNamesIter<'i, I>
where
    I: Iterator<Item = &'i str> + 'i,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[derive(Debug)]
pub struct FeatureCollectionRow<'a, GeometryRef> {
    pub geometry: GeometryRef,
    pub time_interval: TimeInterval,
    data: Rc<HashMap<String, FeatureDataRef<'a>>>,
    row_num: usize,
}

impl<'a, GeometryRef> FeatureCollectionRow<'a, GeometryRef> {
    pub fn get(&self, column_name: &str) -> Option<FeatureDataValue> {
        self.data
            .get(column_name)
            .map(|col| col.get_unchecked(self.row_num))
    }

    pub fn index(&self) -> usize {
        self.row_num
    }
}

pub struct FeatureCollectionIterator<'a, GeometryIter> {
    geometries: GeometryIter,
    time_intervals: slice::Iter<'a, TimeInterval>,
    data: Rc<HashMap<String, FeatureDataRef<'a>>>,
    row_num: usize,
}

impl<'a, GeometryIter, GeometryRef> FeatureCollectionIterator<'a, GeometryIter>
where
    GeometryIter: std::iter::Iterator<Item = GeometryRef>,
    GeometryRef: crate::primitives::GeometryRef,
{
    #[allow(clippy::missing_panics_doc)]
    pub fn new<CollectionType: Geometry + ArrowTyped>(
        collection: &'a FeatureCollection<CollectionType>,
        geometries: GeometryIter,
    ) -> Self {
        FeatureCollectionIterator {
            geometries,
            time_intervals: collection.time_intervals().iter(),
            data: Rc::new(
                collection
                    .column_names()
                    .filter(|x| !FeatureCollection::<CollectionType>::is_reserved_name(x))
                    .map(|x| {
                        (
                            x.to_string(),
                            collection.data(x).expect("reserved columns were filtered"),
                        )
                    })
                    .collect(),
            ),
            row_num: 0,
        }
    }
}

impl<'a, GeometryIter, GeometryRef> Iterator for FeatureCollectionIterator<'a, GeometryIter>
where
    GeometryIter: std::iter::Iterator<Item = GeometryRef>,
    GeometryRef: crate::primitives::GeometryRef,
{
    type Item = FeatureCollectionRow<'a, GeometryRef>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self
            .time_intervals
            .next()
            .map(|time_interval| FeatureCollectionRow {
                geometry: self.geometries.next().unwrap(),
                time_interval: *time_interval,
                data: Rc::clone(&self.data),
                row_num: self.row_num,
            });
        self.row_num += 1;
        res
    }
}

/// Transform an object to the `GeoJson` format
pub trait ToGeoJson<'i> {
    /// Serialize the feature collection to a geo json string
    fn to_geo_json(&'i self) -> String;
}

impl<'i, CollectionType> ToGeoJson<'i> for FeatureCollection<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
    Self: IntoGeometryOptionsIterator<'i>,
{
    fn to_geo_json(&'i self) -> String {
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
                    foreign_members: Some(Map::from_iter([(
                        "when".to_string(),
                        time_interval.as_geo_json_event(),
                    )])),
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
}

impl<CollectionType> FeatureCollectionInfos for FeatureCollection<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
{
    fn len(&self) -> usize {
        self.table.len()
    }

    fn is_simple(&self) -> bool {
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
        let multi_geometry_array: &ListArray = downcast_array(multi_geometry_array_ref);

        multi_geometry_array.len() == features_array.len()
    }

    fn column_type(&self, column_name: &str) -> Result<FeatureDataType> {
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

    fn data(&self, column_name: &str) -> Result<FeatureDataRef> {
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
                FeatureDataType::Float => {
                    let array: &arrow::array::Float64Array = downcast_array(column);
                    FloatDataRef::new(array.values(), array.nulls()).into()
                }
                FeatureDataType::Text => {
                    let array: &arrow::array::StringArray = downcast_array(column);
                    let fixed_nulls = if column.null_count() > 0 {
                        array.nulls()
                    } else {
                        None // StringBuilder assigns some null_bitmap even if there are no nulls
                    };
                    TextDataRef::new(array.value_data(), array.value_offsets(), fixed_nulls).into()
                }
                FeatureDataType::Int => {
                    let array: &arrow::array::Int64Array = downcast_array(column);
                    IntDataRef::new(array.values(), array.nulls()).into()
                }
                FeatureDataType::Category => {
                    let array: &arrow::array::UInt8Array = downcast_array(column);
                    CategoryDataRef::new(array.values(), array.nulls()).into()
                }
                FeatureDataType::Bool => {
                    let array: &arrow::array::BooleanArray = downcast_array(column);
                    // TODO: This operation is quite expensive for getting a reference
                    let transformed: Vec<_> = array.iter().map(|x| x.unwrap_or(false)).collect();
                    BoolDataRef::new(transformed, array.nulls()).into()
                }
                FeatureDataType::DateTime => {
                    let array: &arrow::array::Date64Array = downcast_array(column);
                    let timestamps = unsafe {
                        slice::from_raw_parts(
                            array.values().as_ptr().cast::<TimeInstance>(),
                            array.len(),
                        )
                    };
                    DateTimeDataRef::new(timestamps, array.nulls()).into()
                }
            },
        )
    }

    fn time_intervals(&self) -> &[TimeInterval] {
        let features_ref = self
            .table
            .column_by_name(Self::TIME_COLUMN_NAME)
            .expect("The time column should have been added during creation of the collection");
        let features: &<TimeInterval as ArrowTyped>::ArrowArray = downcast_array(features_ref);

        let number_of_time_intervals = self.len();

        let timestamps_ref = features.values();
        let timestamps: &arrow::array::Int64Array = downcast_array(timestamps_ref);

        unsafe {
            slice::from_raw_parts(
                timestamps.values().as_ptr().cast::<TimeInterval>(),
                number_of_time_intervals,
            )
        }
    }

    fn column_types(&self) -> HashMap<String, FeatureDataType> {
        self.types.clone()
    }

    fn byte_size(&self) -> usize {
        let table_size = self.table.get_array_memory_size();

        // TODO: store information? avoid re-calculation?
        let map_size = mem::size_of_val(&self.types)
            + self
                .types
                .iter()
                .map(|(k, v)| mem::size_of_val(k) + k.as_bytes().len() + mem::size_of_val(v))
                .sum::<usize>();

        table_size + map_size
    }

    fn column_names_of_type(&self, column_type: FeatureDataType) -> FilteredColumnNameIter {
        FilteredColumnNameIter {
            iter: self.types.iter(),
            column_type,
        }
    }

    fn column_names(&self) -> hash_map::Keys<String, FeatureDataType> {
        self.types.keys()
    }
}

pub struct FilteredColumnNameIter<'i> {
    iter: hash_map::Iter<'i, String, FeatureDataType>,
    column_type: FeatureDataType,
}

impl<'i> Iterator for FilteredColumnNameIter<'i> {
    type Item = &'i str;

    fn next(&mut self) -> Option<Self::Item> {
        let column_type = &self.column_type;
        self.iter.find_map(|(k, v)| {
            if v == column_type {
                Some(k.as_str())
            } else {
                None
            }
        })
    }
}

impl<CollectionType> FeatureCollection<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
{
    /// Create an empty `FeatureCollection`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{MultiPointCollection, FeatureCollection, FeatureCollectionInfos};
    ///
    /// let pc = MultiPointCollection::empty();
    ///
    /// assert_eq!(pc.len(), 0);
    /// ```
    #[allow(clippy::missing_panics_doc)]
    pub fn empty() -> Self {
        Self::from_data(vec![], vec![], Default::default(), CacheHint::default())
            .expect("should not fail because no data is given")
    }

    /// Create a `FeatureCollection` from data
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::collections::{MultiPointCollection, FeatureCollection, FeatureCollectionInfos};
    /// use geoengine_datatypes::primitives::{Coordinate2D, TimeInterval, FeatureData, MultiPoint, CacheHint};
    /// use std::collections::HashMap;
    ///
    /// let pc = MultiPointCollection::from_data(
    ///     MultiPoint::many(vec![vec![(0., 0.)], vec![(1., 1.)]]).unwrap(),
    ///     vec![TimeInterval::new_unchecked(0, 1), TimeInterval::new_unchecked(0, 1)],
    ///     {
    ///         let mut map = HashMap::new();
    ///         map.insert("float".into(), FeatureData::Float(vec![0., 1.]));
    ///         map
    ///     },
    ///     CacheHint::default(),
    /// ).unwrap();
    ///
    /// assert_eq!(pc.len(), 2);
    /// ```
    ///
    /// # Errors
    ///
    /// This constructor fails if the data lengths are different or `data`'s keys use a reserved name
    ///
    pub fn from_data(
        features: Vec<CollectionType>,
        time_intervals: Vec<TimeInterval>,
        data: HashMap<String, FeatureData>,
        cache_hint: CacheHint,
    ) -> Result<Self> {
        let number_of_rows = time_intervals.len();
        let number_of_column: usize = data.len() + 1 + usize::from(CollectionType::IS_GEOMETRY);

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
            arrays.push(feature_data.arrow_builder().finish());

            types.insert(name, FeatureDataType::from(&feature_data));
        }

        Ok(Self::new_from_internals(
            StructArray::try_new(columns.into(), arrays, None)?,
            types,
            cache_hint,
        ))
    }

    /// A convenient method for creating feature collections in tests
    pub fn from_slices<F, T, DK, DV>(
        features: &[F],
        time_intervals: &[T],
        data: &[(DK, DV)],
    ) -> Result<Self>
    where
        F: Into<CollectionType> + Clone,
        T: Into<TimeInterval> + Clone,
        DK: Into<String> + Clone,
        DV: Into<FeatureData> + Clone,
    {
        Self::from_data(
            features.iter().cloned().map(Into::into).collect(),
            time_intervals.iter().cloned().map(Into::into).collect(),
            data.iter()
                .map(|(k, v)| (k.clone().into(), v.clone().into()))
                .collect(),
            CacheHint::default(),
        )
    }

    /// Checks for name conflicts with reserved names
    pub(super) fn is_reserved_name(name: &str) -> bool {
        name == Self::GEOMETRY_COLUMN_NAME || name == Self::TIME_COLUMN_NAME
    }

    fn equals_ignoring_cache_hint(&self, other: &Self) -> bool {
        if self.types != other.types {
            return false;
        }

        let mandatory_keys = if CollectionType::IS_GEOMETRY {
            vec![Self::GEOMETRY_COLUMN_NAME, Self::TIME_COLUMN_NAME]
        } else {
            vec![Self::TIME_COLUMN_NAME]
        };

        for key in self.types.keys().map(String::as_str).chain(mandatory_keys) {
            let c1 = self
                .table
                .column_by_name(key)
                .expect("column should exist because `types` and `table` are in sync");
            let c2 = other
                .table
                .column_by_name(key)
                .expect("column should exist because `types` and `table` are in sync");

            if c1 != c2 {
                return false;
            }
        }

        true
    }
}

impl<CollectionType> Clone for FeatureCollection<CollectionType> {
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            types: self.types.clone(),
            collection_type: Default::default(),
            cache_hint: self.cache_hint.clone_with_current_datetime(),
        }
    }
}

impl<CollectionType> PartialEq for FeatureCollection<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
{
    fn eq(&self, other: &Self) -> bool {
        self.equals_ignoring_cache_hint(other) && self.cache_hint == other.cache_hint
    }
}

impl<CollectionType> VectorDataTyped for FeatureCollection<CollectionType>
where
    CollectionType: Geometry,
{
    fn vector_data_type(&self) -> VectorDataType {
        CollectionType::DATA_TYPE
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

/// A way to compare two `FeatureCollection`s ignoring the `CacheHint` and only considering the actual data.
pub trait ChunksEqualIgnoringCacheHint<C> {
    fn chunks_equal_ignoring_cache_hint(&self, other: &dyn IterableFeatureCollection<C>) -> bool;
}

/// Allow comparing Iterables of `BaseTile` ignoring the `CacheHint` and only considering the actual data.
pub trait IterableFeatureCollection<C> {
    fn iter_chunks(&self) -> Box<dyn Iterator<Item = &FeatureCollection<C>> + '_>;
}

struct SingleChunkIter<'a, C> {
    chunk: Option<&'a FeatureCollection<C>>,
}

impl<'a, C> Iterator for SingleChunkIter<'a, C> {
    type Item = &'a FeatureCollection<C>;

    fn next(&mut self) -> Option<Self::Item> {
        self.chunk.take()
    }
}

impl<C> IterableFeatureCollection<C> for FeatureCollection<C> {
    fn iter_chunks(&self) -> Box<dyn Iterator<Item = &FeatureCollection<C>> + '_> {
        Box::new(SingleChunkIter { chunk: Some(self) })
    }
}

impl<C> IterableFeatureCollection<C> for Vec<FeatureCollection<C>> {
    fn iter_chunks(&self) -> Box<dyn Iterator<Item = &FeatureCollection<C>> + '_> {
        Box::new(self.iter())
    }
}

impl<C, const N: usize> IterableFeatureCollection<C> for [FeatureCollection<C>; N] {
    fn iter_chunks(&self) -> Box<dyn Iterator<Item = &FeatureCollection<C>> + '_> {
        Box::new(self.iter())
    }
}

impl<C, I: IterableFeatureCollection<C>> ChunksEqualIgnoringCacheHint<C> for I
where
    C: Geometry + ArrowTyped,
{
    fn chunks_equal_ignoring_cache_hint(&self, other: &dyn IterableFeatureCollection<C>) -> bool {
        let mut iter_self = self.iter_chunks();
        let mut iter_other = other.iter_chunks();

        loop {
            match (iter_self.next(), iter_other.next()) {
                (Some(a), Some(b)) => {
                    if !a.equals_ignoring_cache_hint(b) {
                        return false;
                    }
                }
                // both iterators are exhausted
                (None, None) => return true,
                // one iterator is exhausted, the other is not, so they are not equal
                _ => return false,
            }
        }
    }
}

/// Custom serializer for Arrow's `StructArray`
mod struct_serde {
    use arrow::record_batch::RecordBatch;
    use serde::de::{SeqAccess, Visitor};
    use serde::ser::Error;
    use serde::{Deserializer, Serializer};

    use std::fmt::Formatter;
    use std::io::Cursor;

    use super::*;

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

            let mut reader = arrow::ipc::reader::FileReader::try_new(cursor, None)
                .map_err(|error| E::custom(error.to_string()))?;

            if reader.num_batches() != 1 {
                return Err(E::custom(
                    "there must be exactly one batch for deserializing this struct",
                ));
            }

            let batch = reader
                .next()
                .expect("checked")
                .map_err(|error| E::custom(error.to_string()))?;

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

impl<P, G> Reproject<P> for FeatureCollection<G>
where
    P: CoordinateProjection,
    G: Geometry + ArrowTyped,
    Self: ReplaceRawArrayCoords + GeometryCollection,
{
    type Out = Self;

    fn reproject(&self, projector: &P) -> Result<Self::Out> {
        // get the coordinates
        let coords_ref = self.coordinates();
        // reproject them...
        let projected_coords = projector.project_coordinates(coords_ref)?;

        // transform the coordinates into a byte slice and create a Buffer from it.
        let coords_buffer = unsafe {
            let coord_bytes: &[u8] = slice::from_raw_parts(
                projected_coords.as_ptr().cast::<u8>(),
                projected_coords.len() * std::mem::size_of::<Coordinate2D>(),
            );
            Buffer::from(coord_bytes)
        };

        // get the offsets (reuse)
        let geometries_ref = self
            .table
            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
            .expect("There should exist a geometry column because it was added to the collection during construction.");

        let feature_array = Self::replace_raw_coords(geometries_ref, coords_buffer)?;

        let mut columns = Vec::<arrow::datatypes::Field>::with_capacity(self.table.num_columns());
        let mut column_values =
            Vec::<arrow::array::ArrayRef>::with_capacity(self.table.num_columns());

        // copy geometry data if feature collection is geo collection
        // if CollectionType::IS_GEOMETRY {
        columns.push(arrow::datatypes::Field::new(
            Self::GEOMETRY_COLUMN_NAME,
            G::arrow_data_type(),
            false,
        ));
        column_values.push(Arc::new(ListArray::from(feature_array)));
        // }

        // copy time data
        columns.push(arrow::datatypes::Field::new(
            Self::TIME_COLUMN_NAME,
            TimeInterval::arrow_data_type(),
            false,
        ));
        column_values.push(
            self.table
                .column_by_name(Self::TIME_COLUMN_NAME)
                .expect("The time column should exist because it was added to the collection during construction.")
                .clone(),
        );

        // copy remaining attribute data
        for (column_name, column_type) in &self.types {
            columns.push(arrow::datatypes::Field::new(
                column_name,
                column_type.arrow_data_type(),
                column_type.nullable(),
            ));
            column_values.push(
                self.table
                    .column_by_name(column_name)
                    .expect("The attribute column should exist becasue `types` and `table` are in sync.")
                    .clone(),
            );
        }

        Ok(Self::new_from_internals(
            StructArray::try_new(columns.into(), column_values, None)?,
            self.types.clone(),
            self.cache_hint,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::collections::DataCollection;
    use crate::primitives::{MultiPoint, NoGeometry};

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
                CacheHint::default(),
            )
            .unwrap()
        }

        fn time_interval_size(length: usize) -> usize {
            assert_eq!(mem::size_of::<arrow::array::FixedSizeListArray>(), 104);

            let base = 104;

            if length == 0 {
                return base;
            }
            let buffer = (((length - 1) / 4) + 1) * ((8 + 8) * 4);

            base + buffer
        }

        let empty_hash_map_size = 48;
        assert_eq!(
            mem::size_of::<HashMap<String, FeatureData>>(),
            empty_hash_map_size
        );

        let struct_stack_size = 104;
        assert_eq!(mem::size_of::<StructArray>(), struct_stack_size);

        let arrow_overhead_bytes = 96;

        for i in 0..10 {
            assert_eq!(
                gen_collection(i).byte_size(),
                empty_hash_map_size
                    + struct_stack_size
                    + arrow_overhead_bytes
                    + time_interval_size(i),
                "failed for i={i}"
            );
        }
    }

    #[test]
    fn rename_columns_fails() {
        let collection = DataCollection::from_data(
            vec![],
            vec![TimeInterval::new(0, 1).unwrap(); 1],
            [
                ("foo".to_string(), FeatureData::Int(vec![1])),
                ("bar".to_string(), FeatureData::Int(vec![2])),
            ]
            .iter()
            .cloned()
            .collect(),
            CacheHint::default(),
        )
        .unwrap();

        assert!(collection
            .rename_columns(&[("foo", "baz"), ("bar", "baz")])
            .is_err());
    }

    #[test]
    fn it_does_not_json_serialize() {
        let collection = FeatureCollection::<MultiPoint>::from_data(
            vec![(0.0, 0.1).into()],
            vec![TimeInterval::new(0, 1).unwrap(); 1],
            [
                ("foo".to_string(), FeatureData::Int(vec![1])),
                ("bar".to_string(), FeatureData::Int(vec![2])),
            ]
            .iter()
            .cloned()
            .collect(),
            CacheHint::default(),
        )
        .unwrap();

        let struct_array = collection.table;
        let array: Arc<dyn arrow::array::Array> = Arc::new(struct_array);

        // TODO: if this stops failing, change the strange custom byte serialization to use JSON
        arrow::json::writer::array_to_json_array(&array).unwrap_err();
    }
}
