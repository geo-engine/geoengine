use crate::collections::batch_builder::RawFeatureCollectionBuilder;
use crate::collections::{error, FeatureCollection, FeatureCollectionError};
use crate::primitives::{FeatureDataType, FeatureDataValue, Geometry, TimeInstance, TimeInterval};
use crate::util::arrow::{downcast_mut_array, ArrowTyped};
use crate::util::Result;
use arrow::array::{
    ArrayBuilder, BooleanBuilder, Date64Builder, Float64Builder, Int64Builder, StringBuilder,
    StructBuilder, UInt8Builder,
};
use arrow::datatypes::Field;
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
            string_bytes: 0,
            _collection_type: PhantomData,
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
    _collection_type: PhantomData<CollectionType>,
    string_bytes: usize, // TODO: remove when `StringBuilder` is able to output those
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
        let (data_builder, column_type) = if let (Some(builder), Some(column_type)) =
            (self.builders.get_mut(column), self.types.get(column))
        {
            (builder, column_type)
        } else {
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
                self.string_bytes += value.as_bytes().len();

                let string_builder: &mut StringBuilder = downcast_mut_array(data_builder.as_mut());
                string_builder.append_value(&value);
            }
            FeatureDataValue::NullableText(value) => {
                self.string_bytes += value.as_ref().map_or(0, |s| s.as_bytes().len());

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
        let data_builder = if let Some(builder) = self.builders.get_mut(column) {
            builder
        } else {
            return Err(FeatureCollectionError::ColumnDoesNotExist {
                name: column.to_string(),
            }
            .into());
        };

        match self.types.get(column).expect("checked before") {
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

    /// Outputs the number of bytes that is occupied in the builder buffers
    ///
    /// # Panics
    /// * if the data type is unknown or unexpected which must not be the case
    ///
    pub fn byte_size(&mut self) -> usize {
        let geometry_size = CollectionType::builder_byte_size(&mut self.geometries_builder);
        let time_intervals_size = TimeInterval::builder_byte_size(&mut self.time_intervals_builder);

        let attributes_size = self
            .builders
            .values()
            .map(|builder| {
                let values_size = if builder.as_any().is::<Float64Builder>() {
                    builder.len() * std::mem::size_of::<f64>()
                } else if builder.as_any().is::<Int64Builder>() {
                    builder.len() * std::mem::size_of::<i64>()
                } else if builder.as_any().is::<UInt8Builder>() {
                    builder.len() * std::mem::size_of::<u8>()
                } else if builder.as_any().is::<StringBuilder>() {
                    0 // TODO: how to get this dynamic value
                } else if builder.as_any().is::<BooleanBuilder>() {
                    // arrow buffer internally packs 8 bools in 1 byte
                    arrow::util::bit_util::ceil(builder.len(), 8)
                } else if builder.as_any().is::<Date64Builder>() {
                    builder.len() * std::mem::size_of::<i64>()
                } else {
                    unreachable!("This type is not an attribute type");
                };

                let null_size_estimate = builder.len() / 8;

                values_size + null_size_estimate + self.string_bytes
            })
            .sum::<usize>();

        geometry_size + time_intervals_size + attributes_size
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
            let column_type = self.types.get(&column_name).expect("column must exist");
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

            struct_builder.finish()
        };

        Ok(FeatureCollection::<CollectionType>::new_from_internals(
            table, self.types,
        ))
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
        }
    }
}
