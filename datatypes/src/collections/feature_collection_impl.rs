use crate::primitives::FeatureDataType;
use crate::raster::TimeInterval;
use crate::util::arrow::{downcast_array, downcast_dyn_array, ArrowTyped};
use crate::util::Result;
use arrow::array::{
    Array, ArrayData, ArrayRef, BooleanArray, Date64Array, ListArray, PrimitiveArray,
    PrimitiveArrayOps, PrimitiveBuilder, StringArray, StringBuilder, StructArray,
};
use arrow::datatypes::{
    ArrowPrimitiveType, BooleanType, DataType, Field, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use std::collections::HashMap;
use std::sync::Arc;

/// This trait defines required helper methods of the feature collections in order to allow using the macro implementer.
pub trait FeatureCollectionImplHelpers {
    /// A constructor from internals for the macro implementer
    fn new_from_internals(data: StructArray, types: HashMap<String, FeatureDataType>) -> Self;

    fn table(&self) -> &StructArray;

    fn types(&self) -> &HashMap<String, FeatureDataType>;

    /// Return an `arrow` data type for the geometry
    fn geometry_arrow_data_type() -> DataType;

    /// Return an `arrow` data type for time
    fn time_arrow_data_type() -> DataType {
        TimeInterval::arrow_data_type()
    }

    /// Create an `arrow` struct from column names and data
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

    /// Filter geometries if the collection contains geometries
    fn filtered_geometries(features: &ListArray, filter_array: &BooleanArray) -> Result<ListArray>;

    /// Filter time intervals
    fn filtered_time_intervals(
        time_intervals: &<TimeInterval as ArrowTyped>::ArrowArray,
        filter_array: &BooleanArray,
    ) -> Result<<TimeInterval as ArrowTyped>::ArrowArray> {
        let mut new_time_intervals = TimeInterval::arrow_builder(0);

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

    /// Concatenate two geometry arrays if the collection contains geometries
    fn concat_geometries(geometries_a: &ListArray, geometries_b: &ListArray) -> Result<ListArray>;

    /// Concatenate two time interval arrays
    fn concat_time_intervals(
        time_intervals_a: &<TimeInterval as ArrowTyped>::ArrowArray,
        time_intervals_b: &<TimeInterval as ArrowTyped>::ArrowArray,
    ) -> Result<<TimeInterval as ArrowTyped>::ArrowArray> {
        let mut new_time_intervals =
            TimeInterval::arrow_builder(time_intervals_a.len() + time_intervals_b.len());

        {
            let int_builder = new_time_intervals.values();

            let ints_a_ref = time_intervals_a.values();
            let ints_b_ref = time_intervals_b.values();

            let ints_a: &Date64Array = downcast_array(&ints_a_ref);
            let ints_b: &Date64Array = downcast_array(&ints_b_ref);

            int_builder.append_slice(ints_a.value_slice(0, ints_a.len()))?;
            int_builder.append_slice(ints_b.value_slice(0, ints_b.len()))?;
        }

        for _ in 0..(time_intervals_a.len() + time_intervals_b.len()) {
            new_time_intervals.append(true)?;
        }

        Ok(new_time_intervals.finish())
    }

    /// Concatenate two primitive arrow arrays
    fn concat_primitive_array(array_a: &dyn Array, array_b: &dyn Array) -> Result<ArrayRef> {
        fn concat_values<T>(
            a: &dyn arrow::array::Array,
            b: &dyn arrow::array::Array,
        ) -> Result<ArrayRef>
        where
            T: ArrowPrimitiveType,
        {
            let array_a: &PrimitiveArray<T> = downcast_dyn_array(a);
            let array_b: &PrimitiveArray<T> = downcast_dyn_array(b);

            let mut new_array = PrimitiveBuilder::<T>::new(array_a.len() + array_b.len());

            for old_array in &[array_a, array_b] {
                for i in 0..old_array.len() {
                    if old_array.is_null(i) {
                        new_array.append_null()?;
                    } else {
                        new_array.append_value(old_array.value(i))?;
                    }
                }
            }

            Ok(Arc::new(new_array.finish()))
        }

        debug_assert_eq!(array_a.data_type(), array_b.data_type());

        // since both types are the same, it is sufficient to just lookup one
        Ok(match array_a.data_type() {
            DataType::Boolean => concat_values::<BooleanType>(array_a, array_b),
            DataType::Int8 => concat_values::<Int8Type>(array_a, array_b),
            DataType::Int16 => concat_values::<Int16Type>(array_a, array_b),
            DataType::Int32 => concat_values::<Int32Type>(array_a, array_b),
            DataType::Int64 => concat_values::<Int64Type>(array_a, array_b),
            DataType::UInt8 => concat_values::<UInt8Type>(array_a, array_b),
            DataType::UInt16 => concat_values::<UInt16Type>(array_a, array_b),
            DataType::UInt32 => concat_values::<UInt32Type>(array_a, array_b),
            DataType::UInt64 => concat_values::<UInt64Type>(array_a, array_b),
            DataType::Float32 => concat_values::<Float32Type>(array_a, array_b),
            DataType::Float64 => concat_values::<Float64Type>(array_a, array_b),
            DataType::Utf8 => {
                let array_a: &StringArray = downcast_dyn_array(array_a);
                let array_b: &StringArray = downcast_dyn_array(array_b);

                let mut new_array = StringBuilder::new(array_a.len() + array_b.len());

                for old_array in &[array_a, array_b] {
                    for i in 0..old_array.len() {
                        if old_array.is_null(i) {
                            new_array.append_null()?;
                        } else {
                            new_array.append_value(old_array.value(i))?;
                        }
                    }
                }

                Ok(Arc::new(new_array.finish()) as ArrayRef)
            }
            t => unimplemented!(
                "`concat_primitive_array` not supported for data type {:?}",
                t
            ),
        }?)
    }

    /// Is the feature collection simple or does it contain multi-features?
    fn is_simple(&self) -> bool;
}

/// This macro implements a `FeatureCollection` (and `Clone`)
macro_rules! feature_collection_impl {
    ($Collection:ty, $hasGeometry:literal) => {
        impl crate::collections::FeatureCollection for $Collection
        where
            Self: crate::collections::FeatureCollectionImplHelpers,
        {
            fn len(&self) -> usize {
                use crate::collections::FeatureCollectionImplHelpers;
                use arrow::array::Array;

                self.table().len()
            }

            fn is_simple(&self) -> bool {
                crate::collections::FeatureCollectionImplHelpers::is_simple(self)
            }

            fn data(
                &self,
                column_name: &str,
            ) -> crate::util::Result<crate::primitives::FeatureDataRef> {
                use crate::collections::{
                    error, FeatureCollectionError, FeatureCollectionImplHelpers,
                };
                use crate::primitives::{
                    CategoricalDataRef, DecimalDataRef, FeatureDataType,
                    NullableCategoricalDataRef, NullableDecimalDataRef, NullableNumberDataRef,
                    NullableTextDataRef, NumberDataRef, TextDataRef,
                };
                use crate::util::arrow::downcast_array;
                use arrow::array::Array;

                snafu::ensure!(
                    !Self::is_reserved_name(column_name),
                    error::CannotAccessReservedColumn {
                        name: column_name.to_string(),
                    }
                );

                let column = self.table().column_by_name(column_name).ok_or_else(|| {
                    FeatureCollectionError::ColumnDoesNotExist {
                        name: column_name.to_string(),
                    }
                })?;

                Ok(
                    match self.types().get(column_name).expect("previously checked") {
                        FeatureDataType::Number => {
                            let array: &arrow::array::Float64Array = downcast_array(column);
                            NumberDataRef::new(array.values()).into()
                        }
                        FeatureDataType::NullableNumber => {
                            let array: &arrow::array::Float64Array = downcast_array(column);
                            NullableNumberDataRef::new(
                                array.values(),
                                array.data_ref().null_bitmap(),
                            )
                            .into()
                        }
                        FeatureDataType::Text => {
                            let array: &arrow::array::StringArray = downcast_array(column);
                            TextDataRef::new(array.value_data(), array.value_offsets()).into()
                        }
                        FeatureDataType::NullableText => {
                            let array: &arrow::array::StringArray = downcast_array(column);
                            NullableTextDataRef::new(array.value_data(), array.value_offsets())
                                .into()
                        }
                        FeatureDataType::Decimal => {
                            let array: &arrow::array::Int64Array = downcast_array(column);
                            DecimalDataRef::new(array.values()).into()
                        }
                        FeatureDataType::NullableDecimal => {
                            let array: &arrow::array::Int64Array = downcast_array(column);
                            NullableDecimalDataRef::new(
                                array.values(),
                                array.data_ref().null_bitmap(),
                            )
                            .into()
                        }
                        FeatureDataType::Categorical => {
                            let array: &arrow::array::UInt8Array = downcast_array(column);
                            CategoricalDataRef::new(array.values()).into()
                        }
                        FeatureDataType::NullableCategorical => {
                            let array: &arrow::array::UInt8Array = downcast_array(column);
                            NullableCategoricalDataRef::new(
                                array.values(),
                                array.data_ref().null_bitmap(),
                            )
                            .into()
                        }
                    },
                )
            }

            fn time_intervals(&self) -> &[crate::primitives::TimeInterval] {
                use crate::collections::FeatureCollectionImplHelpers;
                use crate::primitives::TimeInterval;
                use crate::util::arrow::{downcast_array, ArrowTyped};

                let features_ref = self
                    .table()
                    .column_by_name(Self::TIME_COLUMN_NAME)
                    .expect("There must exist a time interval column");
                let features: &<TimeInterval as ArrowTyped>::ArrowArray =
                    downcast_array(features_ref);

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

            fn add_column(
                &self,
                new_column_name: &str,
                data: crate::primitives::FeatureData,
            ) -> crate::util::Result<Self> {
                self.add_columns(&[(new_column_name, data)])
            }

            fn add_columns(
                &self,
                new_columns: &[(&str, crate::primitives::FeatureData)],
            ) -> crate::util::Result<Self> {
                use crate::collections::{error, FeatureCollectionImplHelpers};
                use arrow::array::Array;

                for (new_column_name, data) in new_columns {
                    snafu::ensure!(
                        !Self::is_reserved_name(new_column_name)
                            && self.table().column_by_name(new_column_name).is_none(),
                        error::ColumnAlreadyExists {
                            name: new_column_name.to_string(),
                        }
                    );

                    snafu::ensure!(
                        data.len() == self.table().len(),
                        error::UnmatchedLength {
                            a: self.table().len(),
                            b: data.len(),
                        }
                    );
                }

                let number_of_old_columns = self.table().num_columns();
                let number_of_new_columns = new_columns.len();

                let mut columns = Vec::<arrow::datatypes::Field>::with_capacity(
                    number_of_old_columns + number_of_new_columns,
                );
                let mut column_values = Vec::<arrow::array::ArrayRef>::with_capacity(
                    number_of_old_columns + number_of_new_columns,
                );

                // copy geometry data if feature collection is geo collection
                if $hasGeometry {
                    columns.push(arrow::datatypes::Field::new(
                        Self::GEOMETRY_COLUMN_NAME,
                        Self::geometry_arrow_data_type(),
                        false,
                    ));
                    column_values.push(
                        self.table()
                            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
                            .expect("The geometry column must exist")
                            .clone(),
                    );
                }

                // copy time data
                columns.push(arrow::datatypes::Field::new(
                    Self::TIME_COLUMN_NAME,
                    Self::time_arrow_data_type(),
                    false,
                ));
                column_values.push(
                    self.table()
                        .column_by_name(Self::TIME_COLUMN_NAME)
                        .expect("The time column must exist")
                        .clone(),
                );

                // copy attribute data
                for (column_name, column_type) in self.types() {
                    columns.push(arrow::datatypes::Field::new(
                        &column_name,
                        column_type.arrow_data_type(),
                        column_type.nullable(),
                    ));
                    column_values.push(
                        self.table()
                            .column_by_name(&column_name)
                            .expect("The attribute column must exist")
                            .clone(),
                    );
                }

                // create new type map
                let mut types = self.types().clone();

                // append new columns
                for (new_column_name, data) in new_columns {
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
                    Self::struct_array_from_data(columns, column_values, self.table().len()),
                    types,
                ))
            }

            fn remove_column(&self, removed_column_name: &str) -> crate::util::Result<Self> {
                self.remove_columns(&[removed_column_name])
            }

            fn remove_columns(&self, removed_column_names: &[&str]) -> crate::util::Result<Self> {
                use crate::collections::{error, FeatureCollectionImplHelpers};
                use crate::primitives::FeatureDataType;
                use arrow::array::Array;
                use std::collections::{HashMap, HashSet};

                for removed_column_name in removed_column_names {
                    snafu::ensure!(
                        !Self::is_reserved_name(removed_column_name),
                        error::CannotAccessReservedColumn {
                            name: removed_column_name.to_string(),
                        }
                    );
                    snafu::ensure!(
                        self.table().column_by_name(removed_column_name).is_some(),
                        error::ColumnDoesNotExist {
                            name: removed_column_name.to_string(),
                        }
                    );
                }

                let number_of_old_columns = self.table().num_columns();
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
                if $hasGeometry {
                    columns.push(arrow::datatypes::Field::new(
                        Self::GEOMETRY_COLUMN_NAME,
                        Self::geometry_arrow_data_type(),
                        false,
                    ));
                    column_values.push(
                        self.table()
                            .column_by_name(Self::GEOMETRY_COLUMN_NAME)
                            .expect("The geometry column must exist")
                            .clone(),
                    );
                }

                // copy time data
                columns.push(arrow::datatypes::Field::new(
                    Self::TIME_COLUMN_NAME,
                    Self::time_arrow_data_type(),
                    false,
                ));
                column_values.push(
                    self.table()
                        .column_by_name(Self::TIME_COLUMN_NAME)
                        .expect("The time column must exist")
                        .clone(),
                );

                // copy remaining attribute data
                let removed_name_set: HashSet<&str> =
                    removed_column_names.iter().cloned().collect();
                for (column_name, column_type) in self.types() {
                    if removed_name_set.contains(column_name.as_str()) {
                        continue;
                    }

                    columns.push(arrow::datatypes::Field::new(
                        &column_name,
                        column_type.arrow_data_type(),
                        column_type.nullable(),
                    ));
                    column_values.push(
                        self.table()
                            .column_by_name(&column_name)
                            .expect("The attribute column must exist")
                            .clone(),
                    );

                    types.insert(column_name.to_string(), self.types()[column_name]);
                }

                Ok(Self::new_from_internals(
                    Self::struct_array_from_data(columns, column_values, self.table().len()),
                    types,
                ))
            }

            fn filter(&self, mask: Vec<bool>) -> crate::util::Result<Self> {
                use crate::collections::{error, FeatureCollectionImplHelpers};
                use crate::util::arrow::downcast_array;
                use arrow::array::Array;
                use std::sync::Arc;

                snafu::ensure!(
                    mask.len() == self.table().len(),
                    error::UnmatchedLength {
                        a: mask.len(),
                        b: self.table().len(),
                    }
                );

                let filter_array: arrow::array::BooleanArray = mask.into();

                // TODO: use filter directly on struct array when it is implemented

                if let arrow::datatypes::DataType::Struct(columns) = self.table().data().data_type()
                {
                    let mut filtered_data =
                        Vec::<(arrow::datatypes::Field, arrow::array::ArrayRef)>::with_capacity(
                            columns.len(),
                        );

                    for (column, array) in columns.iter().zip(self.table().columns()) {
                        filtered_data.push((
                            column.clone(),
                            match column.name().as_str() {
                                Self::GEOMETRY_COLUMN_NAME => Arc::new(Self::filtered_geometries(
                                    downcast_array(array),
                                    &filter_array,
                                )?),
                                Self::TIME_COLUMN_NAME => Arc::new(Self::filtered_time_intervals(
                                    downcast_array(array),
                                    &filter_array,
                                )?),
                                _ => arrow::compute::kernels::filter::filter(
                                    array.as_ref(),
                                    &filter_array,
                                )?,
                            },
                        ));
                    }

                    Ok(Self::new_from_internals(
                        filtered_data.into(),
                        self.types().clone(),
                    ))
                } else {
                    unreachable!("`tables` field must be a struct")
                }
            }

            fn append(&self, other: &Self) -> crate::util::Result<Self> {
                use crate::collections::{error, FeatureCollectionImplHelpers};
                use crate::util::arrow::downcast_array;
                use arrow::array::{Array, ArrayRef};
                use arrow::datatypes::{DataType, Field};
                use std::sync::Arc;

                snafu::ensure!(
                    self.types() == other.types(),
                    error::UnmatchedSchema {
                        a: self.types().keys().cloned().collect::<Vec<String>>(),
                        b: self.types().keys().cloned().collect::<Vec<String>>(),
                    }
                );

                let table_data = self.table().data();
                let columns = if let DataType::Struct(columns) = table_data.data_type() {
                    columns
                } else {
                    unreachable!("`tables` field must be a struct")
                };

                let mut new_data = Vec::<(Field, ArrayRef)>::with_capacity(columns.len());

                // concat data column by column
                for (column, array_a) in columns.iter().zip(self.table().columns()) {
                    let array_b = other
                        .table()
                        .column_by_name(&column.name())
                        .expect("column must occur in both collections");

                    new_data.push((
                        column.clone(),
                        match column.name().as_str() {
                            Self::GEOMETRY_COLUMN_NAME => Arc::new(Self::concat_geometries(
                                downcast_array(array_a),
                                downcast_array(array_b),
                            )?),
                            Self::TIME_COLUMN_NAME => Arc::new(Self::concat_time_intervals(
                                downcast_array(array_a),
                                downcast_array(array_b),
                            )?),
                            _ => Self::concat_primitive_array(array_a.as_ref(), array_b.as_ref())?,
                        },
                    ));
                }

                Ok(Self::new_from_internals(
                    new_data.into(),
                    self.types().clone(),
                ))
            }

            fn to_geo_json<'i>(&'i self) -> String
            where
                Self: crate::collections::IntoGeometryOptionsIterator<'i>,
            {
                use crate::collections::{
                    FeatureCollectionImplHelpers, IntoGeometryOptionsIterator,
                };
                use crate::json_map;

                let mut property_maps = (0..self.len())
                    .map(|_| serde_json::Map::with_capacity(self.types().len()))
                    .collect::<Vec<_>>();

                for column_name in self.types().keys() {
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
        }

        impl Clone for $Collection {
            fn clone(&self) -> Self {
                use crate::collections::FeatureCollectionImplHelpers;
                use arrow::array::{Array, StructArray};

                Self::new_from_internals(
                    StructArray::from(self.table().data()),
                    self.types().clone(),
                )
            }
        }
    };
}

/// This module checks whether the macro has hygienic imports, i.e., it does not require imports from the place where it is called.
/// If it has not, the macro should be changed accordingly.
/// Do __not__ use any `use xyz;` in here.
#[cfg(test)]
mod macro_hygiene_test {
    struct HygienicCollection {
        should_not_use_this_field_directly: arrow::array::StructArray,
        should_not_use_this_field_either:
            std::collections::HashMap<String, crate::primitives::FeatureDataType>,
    }

    feature_collection_impl!(HygienicCollection, false);

    impl crate::collections::FeatureCollectionImplHelpers for HygienicCollection {
        fn new_from_internals(
            _data: arrow::array::StructArray,
            _types: std::collections::HashMap<String, crate::primitives::FeatureDataType>,
        ) -> Self {
            unimplemented!()
        }

        fn table(&self) -> &arrow::array::StructArray {
            &self.should_not_use_this_field_directly
        }

        fn types(&self) -> &std::collections::HashMap<String, crate::primitives::FeatureDataType> {
            &self.should_not_use_this_field_either
        }

        fn geometry_arrow_data_type() -> arrow::datatypes::DataType {
            unimplemented!()
        }

        fn filtered_geometries(
            _features: &arrow::array::ListArray,
            _filter_array: &arrow::array::BooleanArray,
        ) -> crate::util::Result<arrow::array::ListArray> {
            unimplemented!()
        }

        fn concat_geometries(
            _geometries_a: &arrow::array::ListArray,
            _geometries_b: &arrow::array::ListArray,
        ) -> crate::util::Result<arrow::array::ListArray> {
            unimplemented!()
        }

        fn is_simple(&self) -> bool {
            unimplemented!()
        }
    }

    impl<'i> crate::collections::IntoGeometryOptionsIterator<'i> for HygienicCollection {
        type GeometryOptionIterator = std::iter::Once<Option<Self::GeometryType>>;
        type GeometryType = geojson::Geometry;

        fn geometry_options(&self) -> Self::GeometryOptionIterator {
            unimplemented!()
        }
    }
}
