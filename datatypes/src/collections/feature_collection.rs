use crate::collections::IntoGeometryOptionsIterator;
use crate::primitives::{FeatureData, FeatureDataRef, TimeInterval};
use crate::util::Result;

/// This trait defines common features of all feature collections
pub trait FeatureCollection
where
    Self: Clone + Sized,
{
    /// Returns the number of features
    fn len(&self) -> usize;

    /// Returns whether the feature collection contains no features
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns whether this feature collection is simple, i.e., contains no multi-types
    fn is_simple(&self) -> bool;

    /// Reserved name for geometry column
    const GEOMETRY_COLUMN_NAME: &'static str = "__geometry";

    /// Reserved name for time column
    const TIME_COLUMN_NAME: &'static str = "__time";

    /// Checks for name conflicts with reserved names
    fn is_reserved_name(name: &str) -> bool {
        name == Self::GEOMETRY_COLUMN_NAME || name == Self::TIME_COLUMN_NAME
    }

    /// Retrieve column data
    ///
    /// # Errors
    ///
    /// This method fails if there is no `column` with that name
    ///
    fn data(&self, column: &str) -> Result<FeatureDataRef>;

    /// Retrieve time intervals
    fn time_intervals(&self) -> &[TimeInterval];

    /// Creates a copy of the collection with an additional column
    ///
    /// # Errors
    ///
    /// Adding a column fails if the column does already exist or the length does not match the length of the collection
    ///
    fn add_column(&self, new_column: &str, data: FeatureData) -> Result<Self>;

    // TODO: add_columns - multi

    /// Removes a column and returns an updated collection
    ///
    /// # Errors
    ///
    /// Removing a column fails if the column does not exist (or is reserved, e.g., the geometry column)
    ///
    fn remove_column(&self, column: &str) -> Result<Self>;

    // TODO: remove_columns - multi

    /// Filters the feature collection by copying the data into a new feature collection
    ///
    /// # Errors
    ///
    /// This method fails if the `mask`'s length does not equal the length of the feature collection
    ///
    fn filter(&self, mask: Vec<bool>) -> Result<Self>;

    // TODO: append(FeatureCollection) - add rows

    /// Serialize the feature collection to a geo json string
    fn to_geo_json<'i>(&'i self) -> String
    where
        Self: IntoGeometryOptionsIterator<'i>;
}

#[cfg(test)]
mod test_default_impls {
    use super::*;

    #[derive(Clone)]
    struct Dummy(Vec<u16>);

    impl FeatureCollection for Dummy {
        fn len(&self) -> usize {
            self.0.len()
        }
        fn is_simple(&self) -> bool {
            unimplemented!()
        }
        fn data(&self, _column: &str) -> Result<FeatureDataRef> {
            unimplemented!()
        }
        fn time_intervals(&self) -> &[TimeInterval] {
            unimplemented!()
        }
        fn add_column(&self, _new_column: &str, _data: FeatureData) -> Result<Self> {
            unimplemented!()
        }
        fn remove_column(&self, _column: &str) -> Result<Self> {
            unimplemented!()
        }
        fn filter(&self, _mask: Vec<bool>) -> Result<Self> {
            unimplemented!()
        }
        fn to_geo_json<'i>(&'i self) -> String
        where
            Self: IntoGeometryOptionsIterator<'i>,
        {
            unimplemented!()
        }
    }

    #[test]
    fn is_empty() {
        assert!(Dummy(Vec::new()).is_empty());
        assert!(!Dummy(vec![1, 2, 3]).is_empty());
    }

    #[test]
    fn is_reserved_name() {
        assert!(Dummy::is_reserved_name("__geometry"));
        assert!(!Dummy::is_reserved_name("foobar"));
    }
}

/// This trait defines required helper methods of the feature collections in order to allow using the macro implementer.
pub trait FeatureCollectionImplHelpers {
    /// Return an `arrow` data type for the geometry
    fn geometry_arrow_data_type() -> arrow::datatypes::DataType;

    /// Return an `arrow` data type for time
    fn time_arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::FixedSizeList(
            arrow::datatypes::DataType::Date64(arrow::datatypes::DateUnit::Millisecond).into(),
            2,
        )
    }

    /// Create an `arrow` struct from column names and data
    fn struct_array_from_data(
        columns: Vec<arrow::datatypes::Field>,
        column_values: Vec<arrow::array::ArrayRef>,
        number_of_features: usize,
    ) -> arrow::array::StructArray {
        arrow::array::StructArray::from(
            arrow::array::ArrayData::builder(arrow::datatypes::DataType::Struct(columns))
                .child_data(column_values.into_iter().map(|a| a.data()).collect())
                .len(number_of_features)
                .build(),
        )
    }

    /// Filter geometries if the collection contains geometries
    fn filtered_geometries(
        features: &arrow::array::ListArray,
        filter_array: &arrow::array::BooleanArray,
    ) -> Result<arrow::array::ListArray>;

    /// Filter time intervals
    fn filtered_time_intervals(
        time_intervals: &arrow::array::FixedSizeListArray,
        filter_array: &arrow::array::BooleanArray,
    ) -> Result<arrow::array::FixedSizeListArray> {
        use arrow::array::Array;

        let mut new_time_intervals =
            arrow::array::FixedSizeListBuilder::new(arrow::array::Date64Builder::new(2), 2);

        for feature_index in 0..time_intervals.len() {
            if !filter_array.value(feature_index) {
                continue;
            }

            let old_timestamps_ref = time_intervals.value(feature_index);
            let old_timestamps: &arrow::array::Date64Array =
                crate::util::arrow::downcast_array(&old_timestamps_ref);

            let date_builder = new_time_intervals.values();
            date_builder.append_slice(old_timestamps.value_slice(0, 2))?;

            new_time_intervals.append(true)?;
        }

        Ok(new_time_intervals.finish())
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
                use arrow::array::Array;

                self.data.len()
            }

            fn is_simple(&self) -> bool {
                crate::collections::FeatureCollectionImplHelpers::is_simple(self)
            }

            fn data(
                &self,
                column_name: &str,
            ) -> crate::util::Result<crate::primitives::FeatureDataRef> {
                use crate::collections::{error, FeatureCollectionError};
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

                let column = self.data.column_by_name(column_name).ok_or_else(|| {
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
                use crate::util::arrow::downcast_array;

                let features_ref = self
                    .data
                    .column_by_name(Self::TIME_COLUMN_NAME)
                    .expect("There must exist a time interval column");
                let features: &arrow::array::FixedSizeListArray = downcast_array(features_ref);

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
                use crate::collections::error;
                use crate::collections::FeatureCollectionImplHelpers;
                use arrow::array::Array;

                snafu::ensure!(
                    !Self::is_reserved_name(new_column_name)
                        && self.data.column_by_name(new_column_name).is_none(),
                    error::ColumnAlreadyExists {
                        name: new_column_name.to_string(),
                    }
                );

                snafu::ensure!(
                    data.len() == self.data.len(),
                    error::UnmatchedLength {
                        a: self.data.len(),
                        b: data.len(),
                    }
                );

                let number_of_old_columns = if $hasGeometry { 2 } else { 1 } + self.types.len();
                let number_of_new_columns = 1;

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
                        self.data
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
                    self.data
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
                        self.data
                            .column_by_name(&column_name)
                            .expect("The attribute column must exist")
                            .clone(),
                    );
                }

                // append new column
                columns.push(arrow::datatypes::Field::new(
                    new_column_name,
                    data.arrow_data_type(),
                    data.nullable(),
                ));
                column_values.push(data.arrow_builder().map(|mut builder| builder.finish())?);

                // create new type map
                let mut types = self.types.clone();
                types.insert(
                    new_column_name.to_string(),
                    crate::primitives::FeatureDataType::from(&data),
                );

                Ok(Self {
                    data: Self::struct_array_from_data(columns, column_values, self.data.len()),
                    types,
                })
            }

            fn remove_column(&self, removed_column_name: &str) -> crate::util::Result<Self> {
                use crate::collections::{error, FeatureCollectionImplHelpers};
                use arrow::array::Array;

                snafu::ensure!(
                    !Self::is_reserved_name(removed_column_name),
                    error::CannotAccessReservedColumn {
                        name: removed_column_name.to_string(),
                    }
                );
                snafu::ensure!(
                    self.data.column_by_name(removed_column_name).is_some(),
                    error::ColumnDoesNotExist {
                        name: removed_column_name.to_string(),
                    }
                );

                let number_of_old_columns = if $hasGeometry { 2 } else { 1 } + self.types.len();
                let number_of_removed_columns = 1;

                let mut columns = Vec::<arrow::datatypes::Field>::with_capacity(
                    number_of_old_columns - number_of_removed_columns,
                );
                let mut column_values = Vec::<arrow::array::ArrayRef>::with_capacity(
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
                        self.data
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
                    self.data
                        .column_by_name(Self::TIME_COLUMN_NAME)
                        .expect("The time column must exist")
                        .clone(),
                );

                // copy remaining attribute data
                for (column_name, column_type) in &self.types {
                    if column_name == removed_column_name {
                        continue;
                    }

                    columns.push(arrow::datatypes::Field::new(
                        &column_name,
                        column_type.arrow_data_type(),
                        column_type.nullable(),
                    ));
                    column_values.push(
                        self.data
                            .column_by_name(&column_name)
                            .expect("The attribute column must exist")
                            .clone(),
                    );
                }

                // create new type map
                let mut types = self.types.clone();
                types.remove(removed_column_name);

                Ok(Self {
                    data: Self::struct_array_from_data(columns, column_values, self.data.len()),
                    types,
                })
            }

            fn filter(&self, mask: Vec<bool>) -> crate::util::Result<Self> {
                use crate::collections::{error, FeatureCollectionImplHelpers};
                use crate::util::arrow::downcast_array;
                use arrow::array::Array;
                use std::sync::Arc;

                snafu::ensure!(
                    mask.len() == self.data.len(),
                    error::UnmatchedLength {
                        a: mask.len(),
                        b: self.data.len(),
                    }
                );

                let filter_array: arrow::array::BooleanArray = mask.into();

                // TODO: use filter directly on struct array when it is implemented

                if let arrow::datatypes::DataType::Struct(columns) = self.data.data().data_type() {
                    let mut filtered_data =
                        Vec::<(arrow::datatypes::Field, arrow::array::ArrayRef)>::with_capacity(
                            columns.len(),
                        );

                    for (column, array) in columns.iter().zip(self.data.columns()) {
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

                    Ok(Self {
                        data: filtered_data.into(),
                        types: self.types.clone(),
                    })
                } else {
                    unreachable!("`data` field must be a struct")
                }
            }

            fn to_geo_json<'i>(&'i self) -> String
            where
                Self: crate::collections::IntoGeometryOptionsIterator<'i>,
            {
                use crate::collections::IntoGeometryOptionsIterator;
                use crate::json_map;

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
        }

        impl Clone for $Collection {
            fn clone(&self) -> Self {
                use arrow::array::{Array, StructArray};

                Self {
                    data: StructArray::from(self.data.data()),
                    types: self.types.clone(),
                }
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
        data: arrow::array::StructArray,
        types: std::collections::HashMap<String, crate::primitives::FeatureDataType>,
    }

    feature_collection_impl!(HygienicCollection, false);

    impl crate::collections::FeatureCollectionImplHelpers for HygienicCollection {
        fn geometry_arrow_data_type() -> arrow::datatypes::DataType {
            unimplemented!()
        }

        fn filtered_geometries(
            _features: &arrow::array::ListArray,
            _filter_array: &arrow::array::BooleanArray,
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

// TODO: impl MultiPointCollection, MultiLineCollection, MultiPolygonCollection
