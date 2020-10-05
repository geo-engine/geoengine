use crate::collections::{error, FeatureCollection, FeatureCollectionError};
use crate::primitives::{FeatureDataType, FeatureDataValue, Geometry, TimeInterval};
use crate::util::arrow::{downcast_mut_array, ArrowTyped};
use crate::util::Result;
use arrow::array::{
    ArrayBuilder, Float64Builder, Int64Builder, StringBuilder, StructBuilder, UInt8Builder,
};
use arrow::datatypes::Field;
use snafu::ensure;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::{iter, mem};

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
    ///
    /// # Errors
    ///
    /// This call fails on internal errors of the builder
    ///
    fn push_geometry(&mut self, geometry: G) -> Result<()>;
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
    CollectionType: Geometry + ArrowTyped,
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
        }
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
}

impl<CollectionType> FeatureCollectionRowBuilder<CollectionType>
where
    CollectionType: Geometry + ArrowTyped,
{
    /// Add a time interval to the collection
    ///
    /// # Errors
    ///
    /// This call fails on internal errors of the builder
    ///
    pub fn push_time_interval(&mut self, time_interval: TimeInterval) -> Result<()> {
        let date_builder = self.time_intervals_builder.values();
        date_builder.append_value(time_interval.start().inner())?;
        date_builder.append_value(time_interval.end().inner())?;

        self.time_intervals_builder.append(true)?;

        Ok(())
    }

    /// Add data to the builder
    ///
    /// # Errors
    ///
    /// This call fails if the data types of the column and the data item do not match
    ///
    pub fn push_data(&mut self, column: &str, data: FeatureDataValue) -> Result<()> {
        // also checks that column exists
        let data_builder = if let Some(builder) = self.builders.get_mut(column) {
            builder
        } else {
            return Err(FeatureCollectionError::ColumnDoesNotExist {
                name: column.to_string(),
            }
            .into());
        };

        // check that data types match
        // TODO: think of cheaper call for checking data type match
        let data_type_variant = mem::discriminant(&FeatureDataType::from(&data));
        match self.types.get(column) {
            Some(data_type) if data_type_variant != mem::discriminant(data_type) => {
                return Err(FeatureCollectionError::WrongDataType.into());
            }
            None => {
                return Err(FeatureCollectionError::ColumnDoesNotExist {
                    name: column.to_string(),
                }
                .into());
            }
            Some(_) => (),
        }

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
            let column_type = self.types.get(&column_name).unwrap(); // column must exist
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
                struct_builder.append(true)?;
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
