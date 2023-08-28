use crate::collections::batch_builder::RawFeatureCollectionBuilder;
use crate::collections::{error, FeatureCollection, FeatureCollectionError};
use crate::primitives::CacheHint;
use crate::primitives::{FeatureDataType, FeatureDataValue, Geometry, TimeInstance, TimeInterval};
use crate::util::arrow::{
    downcast_dyn_array_builder, downcast_mut_array, padded_buffer_size, ArrowTyped,
};
use crate::util::Result;
use arrow::array::{
    ArrayBuilder, BooleanArray, BooleanBuilder, Date64Builder, Float64Builder, Int64Builder,
    PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder, StructArray, StructBuilder,
    UInt8Builder,
};
use arrow::datatypes::{ArrowPrimitiveType, Date64Type, Field, Float64Type, Int64Type, UInt8Type};
use snafu::ensure;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::iter;
use std::marker::PhantomData;

pub trait BuilderProvider {
    type CollectionType: Geometry + ArrowTyped;

    /// Return a builder for the feature collection
    fn builder() -> FeatureCollectionBuilder<Self::CollectionType> {
        Default::default()
    }
}

impl<CollectionType> BuilderProvider for FeatureCollection<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
{
    type CollectionType = CollectionType;
}

pub trait GeoFeatureCollectionRowBuilder<G>
where
    G: Geometry,
{
    /// Push a single geometry feature to the collection.
    fn push_geometry(&mut self, geometry: G);
}

/// A default implementation of a feature collection builder
#[derive(Debug, Clone)]
pub struct FeatureCollectionBuilder<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
{
    types: HashMap<String, FeatureDataType>,
    _collection_type: PhantomData<CollectionType>,
    cache_hint: CacheHint,
}

impl<CollectionType> FeatureCollectionBuilder<CollectionType>
where
    CollectionType: Geometry + ArrowTyped + 'static,
{
    /// Adds a column to the collection.
    ///
    /// # Errors
    ///
    /// Adding a column fails if there are already rows in the builder or the column name is reserved
    ///
    pub fn add_column(&mut self, name: String, data_type: FeatureDataType) -> Result<()> {
        ensure!(
            !FeatureCollection::<CollectionType>::is_reserved_name(&name),
            error::ColumnAlreadyExists { name }
        );

        match self.types.entry(name) {
            Entry::Occupied(mut e) => {
                e.insert(data_type);
                Err(FeatureCollectionError::ColumnAlreadyExists {
                    name: e.key().into(),
                }
                .into())
            }
            Entry::Vacant(e) => {
                e.insert(data_type);
                Ok(())
            }
        }
    }

    /// Stop finishing the header, i.e., the columns of the feature collection to build and return a row builder
    pub fn finish_header(self) -> FeatureCollectionRowBuilder<CollectionType> {
        FeatureCollectionRowBuilder {
            geometries_builder: CollectionType::arrow_builder(0),
            time_intervals_builder: TimeInterval::arrow_builder(0),
            builders: self
                .types
                .iter()
                .map(|(key, value)| (key.clone(), value.arrow_builder(0)))
                .collect(),
            types: self.types,
            rows: 0,
            _collection_type: PhantomData,
            cache_hint: self.cache_hint.clone_with_current_datetime(),
        }
    }

    pub fn batch_builder(
        self,
        num_features: usize,
        num_coords: usize,
    ) -> RawFeatureCollectionBuilder {
        RawFeatureCollectionBuilder::new(
            CollectionType::DATA_TYPE,
            self.types,
            num_features,
            num_coords,
        )
    }

    pub fn cache_hint(&mut self, cache_hint: CacheHint) {
        self.cache_hint = cache_hint;
    }
}

/// A default implementation of a feature collection row builder
pub struct FeatureCollectionRowBuilder<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
{
    pub(super) geometries_builder: CollectionType::ArrowBuilder,
    time_intervals_builder: <TimeInterval as ArrowTyped>::ArrowBuilder,
    builders: HashMap<String, Box<dyn ArrayBuilder>>,
    types: HashMap<String, FeatureDataType>,
    rows: usize,
    // bool builder have no access to nulls, so we have to store it externally
    cache_hint: CacheHint,
    _collection_type: PhantomData<CollectionType>,
}

impl<CollectionType> FeatureCollectionRowBuilder<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
{
    /// Add a time interval to the collection
    pub fn push_time_interval(&mut self, time_interval: TimeInterval) {
        let date_builder = self.time_intervals_builder.values();
        date_builder.append_value(time_interval.start().inner());
        date_builder.append_value(time_interval.end().inner());

        self.time_intervals_builder.append(true);
    }

    /// Add data to the builder
    ///
    /// # Errors
    ///
    /// This call fails if the data types of the column and the data item do not match
    ///
    pub fn push_data(&mut self, column: &str, data: FeatureDataValue) -> Result<()> {
        // also checks that column exists
        let (Some(data_builder), Some(column_type)) =
            (self.builders.get_mut(column), self.types.get(column))
         else {
            return Err(FeatureCollectionError::ColumnDoesNotExist {
                name: column.to_string(),
            }
            .into());
        };

        // check that data types match
        if column_type != &FeatureDataType::from(&data) {
            return Err(FeatureCollectionError::WrongDataType.into());
        }

        match data {
            FeatureDataValue::Float(value) => {
                let float_builder: &mut Float64Builder = downcast_mut_array(data_builder.as_mut());
                float_builder.append_value(value);
            }
            FeatureDataValue::NullableFloat(value) => {
                let float_builder: &mut Float64Builder = downcast_mut_array(data_builder.as_mut());
                float_builder.append_option(value);
            }
            FeatureDataValue::Text(value) => {
                let string_builder: &mut StringBuilder = downcast_mut_array(data_builder.as_mut());
                string_builder.append_value(&value);
            }
            FeatureDataValue::NullableText(value) => {
                let string_builder: &mut StringBuilder = downcast_mut_array(data_builder.as_mut());
                if let Some(v) = &value {
                    string_builder.append_value(v);
                } else {
                    string_builder.append_null();
                }
            }
            FeatureDataValue::Int(value) => {
                let int_builder: &mut Int64Builder = downcast_mut_array(data_builder.as_mut());
                int_builder.append_value(value);
            }
            FeatureDataValue::NullableInt(value) => {
                let int_builder: &mut Int64Builder = downcast_mut_array(data_builder.as_mut());
                int_builder.append_option(value);
            }
            FeatureDataValue::Category(value) => {
                let category_builder: &mut UInt8Builder = downcast_mut_array(data_builder.as_mut());
                category_builder.append_value(value);
            }
            FeatureDataValue::NullableCategory(value) => {
                let category_builder: &mut UInt8Builder = downcast_mut_array(data_builder.as_mut());
                category_builder.append_option(value);
            }
            FeatureDataValue::Bool(value) => {
                let bool_builder: &mut BooleanBuilder = downcast_mut_array(data_builder.as_mut());
                bool_builder.append_value(value);
            }
            FeatureDataValue::NullableBool(value) => {
                let bool_builder: &mut BooleanBuilder = downcast_mut_array(data_builder.as_mut());
                bool_builder.append_option(value);
            }
            FeatureDataValue::DateTime(value) => {
                let dt_builder: &mut Date64Builder = downcast_mut_array(data_builder.as_mut());
                dt_builder.append_value(value.inner());
            }
            FeatureDataValue::NullableDateTime(value) => {
                let dt_builder: &mut Date64Builder = downcast_mut_array(data_builder.as_mut());
                dt_builder.append_option(value.map(TimeInstance::inner));
            }
        }

        Ok(())
    }

    /// Append a null to `column` if possible
    pub fn push_null(&mut self, column: &str) -> Result<()> {
        // also checks that column exists
        let Some(data_builder) = self.builders.get_mut(column) else {
            return Err(FeatureCollectionError::ColumnDoesNotExist {
                name: column.to_string(),
            }
            .into());
        };

        match self.types.get(column).expect("column should exist, because the corresponding builder exists and the `types` are in sync with the builders") {
            FeatureDataType::Category => {
                let category_builder: &mut UInt8Builder = downcast_mut_array(data_builder.as_mut());
                category_builder.append_null();
            }
            FeatureDataType::Int => {
                let category_builder: &mut Int64Builder = downcast_mut_array(data_builder.as_mut());
                category_builder.append_null();
            }
            FeatureDataType::Float => {
                let category_builder: &mut Float64Builder =
                    downcast_mut_array(data_builder.as_mut());
                category_builder.append_null();
            }
            FeatureDataType::Text => {
                let category_builder: &mut StringBuilder =
                    downcast_mut_array(data_builder.as_mut());
                category_builder.append_null();
            }
            FeatureDataType::Bool => {
                let bool_builder: &mut BooleanBuilder = downcast_mut_array(data_builder.as_mut());
                bool_builder.append_null();
            }
            FeatureDataType::DateTime => {
                let dt_builder: &mut Date64Builder = downcast_mut_array(data_builder.as_mut());
                dt_builder.append_null();
            }
        }

        Ok(())
    }

    /// Indicate a finished row
    pub fn finish_row(&mut self) {
        self.rows += 1;
    }

    /// Return the number of finished rows
    pub fn len(&self) -> usize {
        self.rows
    }

    /// Checks whether there was no row finished yet
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Outputs the memory size in bytes that the feature collection will have after building
    ///
    /// # Panics
    /// * if the data type is unknown or unexpected which must not be the case
    ///
    pub fn estimate_memory_size(&mut self) -> usize {
        // TODO: store information? avoid re-calculation?
        let map_size = std::mem::size_of_val(&self.types)
            + self
                .types
                .iter()
                .map(|(k, v)| {
                    std::mem::size_of_val(k) + k.as_bytes().len() + std::mem::size_of_val(v)
                })
                .sum::<usize>();

        let geometry_size =
            CollectionType::estimate_array_memory_size(&mut self.geometries_builder);

        let time_intervals_size =
            TimeInterval::estimate_array_memory_size(&mut self.time_intervals_builder);

        let attributes_size = self
            .builders
            .values()
            .map(|builder| {
                let values_size = if builder.as_any().is::<Float64Builder>() {
                    estimate_primitive_arrow_array_size::<Float64Type>(builder.as_ref())
                } else if builder.as_any().is::<Int64Builder>() {
                    estimate_primitive_arrow_array_size::<Int64Type>(builder.as_ref())
                } else if builder.as_any().is::<UInt8Builder>() {
                    estimate_primitive_arrow_array_size::<UInt8Type>(builder.as_ref())
                } else if builder.as_any().is::<StringBuilder>() {
                    // Text
                    let builder: &StringBuilder = downcast_dyn_array_builder(builder.as_ref());

                    let static_size = std::mem::size_of::<StringArray>();
                    let buffer_size = std::mem::size_of_val(builder.values_slice());
                    let offsets_size = std::mem::size_of_val(builder.offsets_slice());
                    let null_buffer_size =
                        builder.validity_slice().map_or(0, std::mem::size_of_val);

                    static_size
                        + padded_buffer_size(buffer_size, 64)
                        + padded_buffer_size(offsets_size, 64)
                        + padded_buffer_size(null_buffer_size, 64)
                } else if builder.as_any().is::<BooleanBuilder>() {
                    // Bool
                    let builder: &BooleanBuilder = downcast_dyn_array_builder(builder.as_ref());

                    let static_size = std::mem::size_of::<BooleanArray>();
                    // arrow buffer internally packs 8 bools in 1 byte
                    let buffer_size = arrow::util::bit_util::ceil(builder.len(), 8);
                    let null_buffer_size =
                        builder.validity_slice().map_or(0, std::mem::size_of_val);

                    static_size
                        + padded_buffer_size(buffer_size, 64)
                        + padded_buffer_size(null_buffer_size, 64)
                } else if builder.as_any().is::<Date64Builder>() {
                    estimate_primitive_arrow_array_size::<Date64Type>(builder.as_ref())
                } else {
                    debug_assert!(
                        false,
                        "Unknown attribute builder type: {:?}",
                        builder.type_id()
                    );
                    0
                };

                values_size
            })
            .sum::<usize>();

        let arrow_struct_size = std::mem::size_of::<StructArray>()
            + geometry_size
            + time_intervals_size
            + attributes_size;

        map_size + arrow_struct_size
    }

    /// Build the feature collection
    ///
    /// # Errors
    ///
    /// This call fails if the lengths of the columns do not match the number of times `finish_row()` was called.
    ///
    pub fn build(mut self) -> Result<FeatureCollection<CollectionType>> {
        for builder in self
            .builders
            .values()
            .map(AsRef::as_ref)
            .chain(if CollectionType::IS_GEOMETRY {
                Some(&self.geometries_builder as &dyn ArrayBuilder)
            } else {
                None
            })
            .chain(iter::once(
                &self.time_intervals_builder as &dyn ArrayBuilder,
            ))
        {
            if builder.len() != self.rows {
                return Err(FeatureCollectionError::UnmatchedLength {
                    a: self.rows,
                    b: builder.len(),
                }
                .into());
            }
        }

        let mut columns = Vec::with_capacity(self.types.len() + 2);
        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(self.types.len() + 2);

        if CollectionType::IS_GEOMETRY {
            columns.push(Field::new(
                FeatureCollection::<CollectionType>::GEOMETRY_COLUMN_NAME,
                CollectionType::arrow_data_type(),
                false,
            ));
            builders.push(Box::new(self.geometries_builder));
        }

        columns.push(Field::new(
            FeatureCollection::<CollectionType>::TIME_COLUMN_NAME,
            TimeInterval::arrow_data_type(),
            false,
        ));
        builders.push(Box::new(self.time_intervals_builder));

        for (column_name, builder) in self.builders.drain() {
            let column_type = self.types.get(&column_name).expect("column should exist, because the corresponding builder exists and the `types` are in sync with the builders");
            columns.push(Field::new(
                &column_name,
                column_type.arrow_data_type(),
                column_type.nullable(),
            ));
            builders.push(builder);
        }

        let table = {
            let mut struct_builder = StructBuilder::new(columns, builders);

            for _ in 0..self.rows {
                struct_builder.append(true);
            }

            // we use `finish_cloned` here instead of `finish`, because we want to shrink the memory to fit
            struct_builder.finish_cloned()
        };

        Ok(FeatureCollection::<CollectionType>::new_from_internals(
            table,
            self.types,
            self.cache_hint,
        ))
    }

    pub fn cache_hint(&mut self, cache_hint: CacheHint) {
        self.cache_hint = cache_hint;
    }
}

/// By implementing `Default` ourselves we omit `CollectionType` implementing `Default`
impl<CollectionType> Default for FeatureCollectionBuilder<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
{
    fn default() -> Self {
        Self {
            types: Default::default(),
            _collection_type: Default::default(),
            cache_hint: CacheHint::default(),
        }
    }
}

fn estimate_primitive_arrow_array_size<T: ArrowPrimitiveType>(builder: &dyn ArrayBuilder) -> usize {
    let builder: &PrimitiveBuilder<T> = downcast_dyn_array_builder(builder);

    let static_size = std::mem::size_of::<PrimitiveArray<T>>();
    let buffer_size = builder.len() * std::mem::size_of::<T::Native>();
    let null_buffer_size = builder.validity_slice().map_or(0, std::mem::size_of_val);

    static_size + padded_buffer_size(buffer_size, 64) + padded_buffer_size(null_buffer_size, 64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{collections::FeatureCollectionInfos, primitives::MultiPoint};

    #[test]
    fn test_estimate_multi_point_collection_size() {
        let builder = FeatureCollectionBuilder::<MultiPoint>::default();

        let mut builder = builder.finish_header();

        for _ in 0..20 {
            builder.push_geometry(MultiPoint::new(vec![(0., 0.).into()]).unwrap());
            builder.push_time_interval(TimeInterval::new(0, 1).unwrap());
            builder.finish_row();
        }

        let estimated_size = builder.estimate_memory_size();

        let feature_collection = builder.build().unwrap();

        assert_eq!(feature_collection.byte_size(), estimated_size);
    }

    #[test]
    fn test_estimate_multi_point_collection_size_with_columns_no_nulls() {
        let mut builder = FeatureCollectionBuilder::<MultiPoint>::default();

        builder
            .add_column("a".into(), FeatureDataType::Category)
            .unwrap();
        builder
            .add_column("b".into(), FeatureDataType::Int)
            .unwrap();
        builder
            .add_column("c".into(), FeatureDataType::Float)
            .unwrap();
        builder
            .add_column("d".into(), FeatureDataType::Text)
            .unwrap();
        builder
            .add_column("e".into(), FeatureDataType::Bool)
            .unwrap();
        builder
            .add_column("f".into(), FeatureDataType::DateTime)
            .unwrap();

        let mut builder = builder.finish_header();

        for _ in 0..20 {
            builder.push_geometry(MultiPoint::new(vec![(0., 0.).into()]).unwrap());
            builder.push_time_interval(TimeInterval::new(0, 1).unwrap());

            builder
                .push_data("a", FeatureDataValue::Category(0))
                .unwrap();
            builder.push_data("b", FeatureDataValue::Int(0)).unwrap();
            builder.push_data("c", FeatureDataValue::Float(0.)).unwrap();
            builder
                .push_data("d", FeatureDataValue::Text("foobar".into()))
                .unwrap();
            builder
                .push_data("e", FeatureDataValue::Bool(true))
                .unwrap();
            builder
                .push_data(
                    "f",
                    FeatureDataValue::DateTime(TimeInstance::from_millis_unchecked(0)),
                )
                .unwrap();

            builder.finish_row();
        }

        let estimated_size = builder.estimate_memory_size();

        let feature_collection = builder.build().unwrap();

        assert_eq!(feature_collection.byte_size(), estimated_size);
    }

    #[test]
    fn test_estimate_multi_point_collection_size_with_columns_with_nulls() {
        let mut builder = FeatureCollectionBuilder::<MultiPoint>::default();

        builder
            .add_column("a".into(), FeatureDataType::Category)
            .unwrap();
        builder
            .add_column("b".into(), FeatureDataType::Int)
            .unwrap();
        builder
            .add_column("c".into(), FeatureDataType::Float)
            .unwrap();
        builder
            .add_column("d".into(), FeatureDataType::Text)
            .unwrap();
        builder
            .add_column("e".into(), FeatureDataType::Bool)
            .unwrap();
        builder
            .add_column("f".into(), FeatureDataType::DateTime)
            .unwrap();

        let mut builder = builder.finish_header();

        for _ in 0..10 {
            builder.push_geometry(MultiPoint::new(vec![(0., 0.).into()]).unwrap());
            builder.push_time_interval(TimeInterval::new(0, 1).unwrap());

            builder
                .push_data("a", FeatureDataValue::Category(0))
                .unwrap();
            builder.push_data("b", FeatureDataValue::Int(0)).unwrap();
            builder.push_data("c", FeatureDataValue::Float(0.)).unwrap();
            builder
                .push_data("d", FeatureDataValue::Text("foobar".into()))
                .unwrap();
            builder
                .push_data("e", FeatureDataValue::Bool(true))
                .unwrap();
            builder
                .push_data(
                    "f",
                    FeatureDataValue::DateTime(TimeInstance::from_millis_unchecked(0)),
                )
                .unwrap();

            builder.finish_row();
        }

        for _ in 0..10 {
            builder.push_geometry(MultiPoint::new(vec![(0., 0.).into()]).unwrap());
            builder.push_time_interval(TimeInterval::new(0, 1).unwrap());

            builder.push_null("a").unwrap();
            builder.push_null("b").unwrap();
            builder.push_null("c").unwrap();
            builder.push_null("d").unwrap();
            builder.push_null("e").unwrap();
            builder.push_null("f").unwrap();

            builder.finish_row();
        }

        let estimated_size = builder.estimate_memory_size();

        let feature_collection = builder.build().unwrap();

        assert_eq!(feature_collection.byte_size(), estimated_size);
    }
}
