use crate::error;
use crate::primitives::PrimitivesError;
use crate::util::Result;
use arrow::bitmap::Bitmap;
use gdal::vector::OGRFieldType;
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ensure;
use std::convert::TryFrom;
use std::str;
use std::{marker::PhantomData, slice};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum FeatureDataType {
    Category,
    Int,
    Float,
    Text,
    Bool
}

impl FeatureDataType {
    pub fn try_from_ogr_field_type_code(code: u32) -> Result<Self> {
        Ok(match code {
            OGRFieldType::OFTInteger | OGRFieldType::OFTInteger64 => Self::Int,
            OGRFieldType::OFTReal => Self::Float,
            OGRFieldType::OFTString => Self::Text,
            OGRFieldType::OFTBinary => Self::Bool,
            _ => return Err(error::Error::NoMatchingFeatureDataTypeForOgrFieldType),
        })
    }

    pub fn is_numeric(self) -> bool {
        matches!(self, Self::Int | Self::Float)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum FeatureData {
    Category(Vec<u8>), // TODO: add names to categories
    NullableCategory(Vec<Option<u8>>),
    Int(Vec<i64>),
    NullableInt(Vec<Option<i64>>),
    Float(Vec<f64>),
    NullableFloat(Vec<Option<f64>>),
    Text(Vec<String>),
    NullableText(Vec<Option<String>>),
    Bool(Vec<bool>),
    NullableBool(Vec<Option<bool>>)
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum FeatureDataValue {
    Category(u8),
    NullableCategory(Option<u8>),
    Int(i64),
    NullableInt(Option<i64>),
    Float(f64),
    NullableFloat(Option<f64>),
    Text(String),
    NullableText(Option<String>),
    Bool(bool),
    NullableBool(Option<bool>)
}

#[derive(Clone, Debug, PartialEq)]
pub enum FeatureDataRef<'f> {
    Category(CategoryDataRef<'f>),
    Int(IntDataRef<'f>),
    Float(FloatDataRef<'f>),
    Text(TextDataRef<'f>),
    Bool(BoolDataRef<'f>),
}

impl<'f> FeatureDataRef<'f> {
    /// Computes JSON value lists for data elements
    pub fn json_values(&self) -> Box<dyn Iterator<Item = serde_json::Value> + '_> {
        match self {
            FeatureDataRef::Text(data_ref) => data_ref.json_values(),
            FeatureDataRef::Float(data_ref) => data_ref.json_values(),
            FeatureDataRef::Int(data_ref) => data_ref.json_values(),
            FeatureDataRef::Category(data_ref) => data_ref.json_values(),
            FeatureDataRef::Bool(data_ref) => data_ref.json_values()
        }
    }

    /// Computes a vector of null flags.
    pub fn nulls(&self) -> Vec<bool> {
        match self {
            FeatureDataRef::Text(data_ref) => data_ref.nulls(),
            FeatureDataRef::Float(data_ref) => data_ref.nulls(),
            FeatureDataRef::Int(data_ref) => data_ref.nulls(),
            FeatureDataRef::Category(data_ref) => data_ref.nulls(),
            FeatureDataRef::Bool(data_ref) => data_ref.nulls()
        }
    }

    /// Is any of the data elements null?
    pub fn has_nulls(&self) -> bool {
        match self {
            FeatureDataRef::Text(data_ref) => data_ref.has_nulls(),
            FeatureDataRef::Float(data_ref) => data_ref.has_nulls(),
            FeatureDataRef::Int(data_ref) => data_ref.has_nulls(),
            FeatureDataRef::Category(data_ref) => data_ref.has_nulls(),
            FeatureDataRef::Bool(data_ref) => data_ref.has_nulls(),
        }
    }

    /// Get the `FeatureDataValue` value at position `i`
    pub fn get_unchecked(&self, i: usize) -> FeatureDataValue {
        match self {
            FeatureDataRef::Text(data_ref) => data_ref.get_unchecked(i),
            FeatureDataRef::Float(data_ref) => data_ref.get_unchecked(i),
            FeatureDataRef::Int(data_ref) => data_ref.get_unchecked(i),
            FeatureDataRef::Category(data_ref) => data_ref.get_unchecked(i),
            FeatureDataRef::Bool(data_ref) => data_ref.get_unchecked(i),
        }
    }

    /// Creates an iterator over all values as string
    /// Null-values are empty strings.
    pub fn strings_iter(&self) -> Box<dyn Iterator<Item = String> + '_> {
        match self {
            FeatureDataRef::Text(data_ref) => Box::new(data_ref.strings_iter()),
            FeatureDataRef::Float(data_ref) => Box::new(data_ref.strings_iter()),
            FeatureDataRef::Int(data_ref) => Box::new(data_ref.strings_iter()),
            FeatureDataRef::Category(data_ref) => Box::new(data_ref.strings_iter()),
            FeatureDataRef::Bool(data_ref) => Box::new(data_ref.strings_iter()),
        }
    }

    /// Creates an iterator over all values as [`Option<f64>`]
    /// Null values or non-convertible values are [`None`]
    pub fn float_options_iter(&self) -> Box<dyn Iterator<Item = Option<f64>> + '_> {
        match self {
            FeatureDataRef::Text(data_ref) => Box::new(data_ref.float_options_iter()),
            FeatureDataRef::Float(data_ref) => Box::new(data_ref.float_options_iter()),
            FeatureDataRef::Int(data_ref) => Box::new(data_ref.float_options_iter()),
            FeatureDataRef::Category(data_ref) => Box::new(data_ref.float_options_iter()),
            FeatureDataRef::Bool(data_ref) => Box::new(data_ref.float_options_iter()),
        }
    }
}

/// Common methods for feature data references
pub trait DataRef<'r, T>: AsRef<[T]> + Into<FeatureDataRef<'r>>
where
    T: 'static,
{
    type StringsIter: Iterator<Item = String>;
    type FloatOptionsIter: Iterator<Item = Option<f64>>;

    /// Computes JSON value lists for data elements
    fn json_values(&'r self) -> Box<dyn Iterator<Item = serde_json::Value> + 'r> {
        if self.has_nulls() {
            Box::new(self.as_ref().iter().enumerate().map(move |(i, v)| {
                if self.is_null(i) {
                    serde_json::Value::Null
                } else {
                    Self::json_value(v)
                }
            }))
        } else {
            Box::new(self.as_ref().iter().map(Self::json_value))
        }
    }

    /// Creates a JSON value out of the owned type
    fn json_value(value: &T) -> serde_json::Value;

    /// Computes a vector of null flags.
    fn nulls(&self) -> Vec<bool>;

    /// Is the `i`th value null?
    /// This method panics if `i` is too large.
    fn is_null(&self, i: usize) -> bool {
        !self.is_valid(i)
    }

    /// Is the `i`th value valid, i.e., not null?
    /// This method panics if `i` is too large.
    fn is_valid(&self, i: usize) -> bool;

    /// Is any of the data elements null?
    fn has_nulls(&self) -> bool;

    fn get_unchecked(&self, i: usize) -> FeatureDataValue;

    /// Number of values
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Creates an iterator over all values as string
    /// Null values are empty strings.
    fn strings_iter(&'r self) -> Self::StringsIter;

    /// Creates an iterator over all values as [`Option<f64>`]
    /// Null values or non-convertible values are [`None`]
    fn float_options_iter(&'r self) -> Self::FloatOptionsIter;
}

#[derive(Clone, Debug, PartialEq)]
pub struct FloatDataRef<'f> {
    buffer: &'f [f64],
    valid_bitmap: &'f Option<arrow::bitmap::Bitmap>,
}

impl<'f> DataRef<'f, f64> for FloatDataRef<'f> {
    fn json_value(value: &f64) -> serde_json::Value {
        (*value).into()
    }

    fn nulls(&self) -> Vec<bool> {
        null_bitmap_to_bools(self.valid_bitmap, self.as_ref().len())
    }

    fn is_valid(&self, i: usize) -> bool {
        self.valid_bitmap
            .as_ref()
            .map_or(true, |bitmap| bitmap.is_set(i))
    }

    fn has_nulls(&self) -> bool {
        self.valid_bitmap.is_some()
    }

    fn get_unchecked(&self, i: usize) -> FeatureDataValue {
        if self.has_nulls() {
            FeatureDataValue::NullableFloat(if self.is_null(i) {
                None
            } else {
                Some(self.as_ref()[i])
            })
        } else {
            FeatureDataValue::Float(self.as_ref()[i])
        }
    }

    type StringsIter = NumberDataRefStringIter<'f, Self, f64>;

    fn strings_iter(&'f self) -> Self::StringsIter {
        NumberDataRefStringIter::new(self)
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    type FloatOptionsIter = NumberDataRefFloatOptionIter<'f, Self, f64>;

    fn float_options_iter(&'f self) -> Self::FloatOptionsIter {
        NumberDataRefFloatOptionIter::new(self)
    }
}

pub struct NumberDataRefStringIter<'r, D, T>
where
    D: DataRef<'r, T>,
    T: 'static,
{
    data_ref: &'r D,
    i: usize,
    t: PhantomData<T>,
}

impl<'r, D, T> NumberDataRefStringIter<'r, D, T>
where
    D: DataRef<'r, T>,
    T: 'static,
{
    pub fn new(data_ref: &'r D) -> Self {
        Self {
            data_ref,
            i: 0,
            t: PhantomData::default(),
        }
    }
}

impl<'f, D, T> Iterator for NumberDataRefStringIter<'f, D, T>
where
    D: DataRef<'f, T>,
    T: 'static + ToString,
{
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.data_ref.len() {
            return None;
        }

        let i = self.i;
        self.i += 1;

        if self.data_ref.is_null(i) {
            return Some(String::default());
        }

        Some(self.data_ref.as_ref()[i].to_string())
    }
}

pub struct NumberDataRefFloatOptionIter<'r, D, T>
where
    D: DataRef<'r, T>,
    T: 'static,
{
    data_ref: &'r D,
    i: usize,
    t: PhantomData<T>,
}

impl<'r, D, T> NumberDataRefFloatOptionIter<'r, D, T>
where
    D: DataRef<'r, T>,
    T: 'static,
{
    pub fn new(data_ref: &'r D) -> Self {
        Self {
            data_ref,
            i: 0,
            t: PhantomData::default(),
        }
    }
}

impl<'f, D, T> Iterator for NumberDataRefFloatOptionIter<'f, D, T>
where
    D: DataRef<'f, T>,
    T: 'static + AsPrimitive<f64>,
{
    type Item = Option<f64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.data_ref.len() {
            return None;
        }

        let i = self.i;
        self.i += 1;

        Some(if self.data_ref.is_null(i) {
            None
        } else {
            Some(self.data_ref.as_ref()[i].as_())
        })
    }
}

impl AsRef<[f64]> for FloatDataRef<'_> {
    fn as_ref(&self) -> &[f64] {
        self.buffer
    }
}

impl<'f> From<FloatDataRef<'f>> for FeatureDataRef<'f> {
    fn from(data_ref: FloatDataRef<'f>) -> FeatureDataRef<'f> {
        FeatureDataRef::Float(data_ref)
    }
}

impl<'f> FloatDataRef<'f> {
    pub fn new(buffer: &'f [f64], null_bitmap: &'f Option<arrow::bitmap::Bitmap>) -> Self {
        Self {
            buffer,
            valid_bitmap: null_bitmap,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct IntDataRef<'f> {
    buffer: &'f [i64],
    valid_bitmap: &'f Option<arrow::bitmap::Bitmap>,
}

impl<'f> IntDataRef<'f> {
    pub fn new(buffer: &'f [i64], null_bitmap: &'f Option<arrow::bitmap::Bitmap>) -> Self {
        Self {
            buffer,
            valid_bitmap: null_bitmap,
        }
    }
}

impl<'f> DataRef<'f, i64> for IntDataRef<'f> {
    fn json_value(value: &i64) -> serde_json::Value {
        (*value).into()
    }

    fn nulls(&self) -> Vec<bool> {
        null_bitmap_to_bools(self.valid_bitmap, self.as_ref().len())
    }

    fn is_valid(&self, i: usize) -> bool {
        self.valid_bitmap
            .as_ref()
            .map_or(true, |bitmap| bitmap.is_set(i))
    }

    fn has_nulls(&self) -> bool {
        self.valid_bitmap.is_some()
    }

    fn get_unchecked(&self, i: usize) -> FeatureDataValue {
        if self.has_nulls() {
            FeatureDataValue::NullableInt(if self.is_null(i) {
                None
            } else {
                Some(self.as_ref()[i])
            })
        } else {
            FeatureDataValue::Int(self.as_ref()[i])
        }
    }

    type StringsIter = NumberDataRefStringIter<'f, Self, i64>;

    fn strings_iter(&'f self) -> Self::StringsIter {
        NumberDataRefStringIter::new(self)
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    type FloatOptionsIter = NumberDataRefFloatOptionIter<'f, Self, i64>;

    fn float_options_iter(&'f self) -> Self::FloatOptionsIter {
        NumberDataRefFloatOptionIter::new(self)
    }
}

impl AsRef<[i64]> for IntDataRef<'_> {
    fn as_ref(&self) -> &[i64] {
        self.buffer
    }
}

impl<'f> From<IntDataRef<'f>> for FeatureDataRef<'f> {
    fn from(data_ref: IntDataRef<'f>) -> FeatureDataRef<'f> {
        FeatureDataRef::Int(data_ref)
    }
}

fn null_bitmap_to_bools(null_bitmap: &Option<Bitmap>, len: usize) -> Vec<bool> {
    if let Some(nulls) = null_bitmap {
        (0..len).map(|i| !nulls.is_set(i)).collect()
    } else {
        vec![false; len]
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CategoryDataRef<'f> {
    buffer: &'f [u8],
    valid_bitmap: &'f Option<arrow::bitmap::Bitmap>,
}

impl<'f> DataRef<'f, u8> for CategoryDataRef<'f> {
    fn json_value(value: &u8) -> serde_json::Value {
        (*value).into()
    }

    fn nulls(&self) -> Vec<bool> {
        null_bitmap_to_bools(self.valid_bitmap, self.as_ref().len())
    }

    fn is_valid(&self, i: usize) -> bool {
        self.valid_bitmap
            .as_ref()
            .map_or(true, |bitmap| bitmap.is_set(i))
    }

    fn has_nulls(&self) -> bool {
        self.valid_bitmap.is_some()
    }

    fn get_unchecked(&self, i: usize) -> FeatureDataValue {
        if self.has_nulls() {
            FeatureDataValue::NullableCategory(if self.is_null(i) {
                None
            } else {
                Some(self.as_ref()[i])
            })
        } else {
            FeatureDataValue::Category(self.as_ref()[i])
        }
    }

    type StringsIter = NumberDataRefStringIter<'f, Self, u8>;

    fn strings_iter(&'f self) -> Self::StringsIter {
        NumberDataRefStringIter::new(self)
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    type FloatOptionsIter = NumberDataRefFloatOptionIter<'f, Self, u8>;

    fn float_options_iter(&'f self) -> Self::FloatOptionsIter {
        NumberDataRefFloatOptionIter::new(self)
    }
}

impl AsRef<[u8]> for CategoryDataRef<'_> {
    fn as_ref(&self) -> &[u8] {
        self.buffer
    }
}

impl<'f> From<CategoryDataRef<'f>> for FeatureDataRef<'f> {
    fn from(data_ref: CategoryDataRef<'f>) -> FeatureDataRef<'f> {
        FeatureDataRef::Category(data_ref)
    }
}

impl<'f> CategoryDataRef<'f> {
    pub fn new(buffer: &'f [u8], null_bitmap: &'f Option<arrow::bitmap::Bitmap>) -> Self {
        Self {
            buffer,
            valid_bitmap: null_bitmap,
        }
    }
}

unsafe fn byte_ptr_to_str<'d>(bytes: *const u8, length: usize) -> &'d str {
    let text_ref = slice::from_raw_parts(bytes, length);
    str::from_utf8_unchecked(text_ref)
}

/// A reference to nullable text data
///
/// # Examples
///
/// ```rust
/// use geoengine_datatypes::primitives::TextDataRef;
/// use arrow::array::{StringBuilder, Array};
///
/// let string_array = {
///     let mut builder = StringBuilder::new(3);
///     builder.append_value("foobar");
///     builder.append_null();
///     builder.append_value("bar");
///     builder.finish()
/// };
///
/// assert_eq!(string_array.len(), 3);
///
/// let text_data_ref = TextDataRef::new(string_array.value_data(), string_array.value_offsets(), string_array.data_ref().null_bitmap());
///
/// assert_eq!(text_data_ref.as_ref().len(), 9);
/// assert_eq!(text_data_ref.offsets().len(), 4);
///
/// assert_eq!(text_data_ref.text_at(0).unwrap(), Some("foobar"));
/// assert_eq!(text_data_ref.text_at(1).unwrap(), None);
/// assert_eq!(text_data_ref.text_at(2).unwrap(), Some("bar"));
/// assert!(text_data_ref.text_at(3).is_err());
/// ```
///
#[derive(Clone, Debug, PartialEq)]
pub struct TextDataRef<'f> {
    data_buffer: arrow::buffer::Buffer,
    offsets: &'f [i32],
    valid_bitmap: &'f Option<arrow::bitmap::Bitmap>,
}

impl<'f> AsRef<[u8]> for TextDataRef<'f> {
    fn as_ref(&self) -> &[u8] {
        self.data_buffer.as_slice()
    }
}

impl<'r> DataRef<'r, u8> for TextDataRef<'r> {
    fn json_values(&'r self) -> Box<dyn Iterator<Item = serde_json::Value> + 'r> {
        let offsets = self.offsets;
        let number_of_values = offsets.len() - 1;

        Box::new((0..number_of_values).map(move |pos| {
            let start = offsets[pos];
            let end = offsets[pos + 1];

            if start == end {
                return serde_json::Value::Null;
            }

            let text = unsafe {
                byte_ptr_to_str(
                    self.data_buffer.slice(start as usize).as_ptr(),
                    (end - start) as usize,
                )
            };

            text.into()
        }))
    }

    fn json_value(value: &u8) -> Value {
        (*value).into()
    }

    /// A null vector for text data
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::primitives::{TextDataRef, DataRef};
    /// use arrow::array::{StringBuilder, Array};
    ///
    /// let string_array = {
    ///     let mut builder = StringBuilder::new(3);
    ///     builder.append_value("foobar");
    ///     builder.append_null();
    ///     builder.append_value("bar");
    ///     builder.finish()
    /// };
    ///
    /// assert_eq!(string_array.len(), 3);
    ///
    /// let text_data_ref = TextDataRef::new(string_array.value_data(), string_array.value_offsets(), string_array.data_ref().null_bitmap());
    ///
    /// assert_eq!(text_data_ref.nulls(), vec![false, true, false]);
    /// ```
    ///
    fn nulls(&self) -> Vec<bool> {
        let mut nulls = Vec::with_capacity(self.offsets.len() - 1);
        for window in self.offsets.windows(2) {
            let (start, end) = (window[0], window[1]);
            nulls.push(start == end);
        }
        nulls
    }

    fn is_valid(&self, i: usize) -> bool {
        self.valid_bitmap
            .as_ref()
            .map_or(true, |bitmap| bitmap.is_set(i))
    }

    fn has_nulls(&self) -> bool {
        self.valid_bitmap.is_some()
    }

    fn get_unchecked(&self, i: usize) -> FeatureDataValue {
        let text = self.text_at(i).expect("unchecked").map(ToString::to_string);

        if self.has_nulls() {
            FeatureDataValue::NullableText(text)
        } else {
            FeatureDataValue::Text(text.expect("cannot be null"))
        }
    }

    type StringsIter = TextDataRefStringIter<'r>;

    fn strings_iter(&'r self) -> Self::StringsIter {
        Self::StringsIter::new(self)
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    type FloatOptionsIter = TextDataRefFloatOptionIter<'r>;

    fn float_options_iter(&'r self) -> Self::FloatOptionsIter {
        Self::FloatOptionsIter::new(self)
    }
}

pub struct TextDataRefStringIter<'r> {
    data_ref: &'r TextDataRef<'r>,
    i: usize,
}

impl<'r> TextDataRefStringIter<'r> {
    pub fn new(data_ref: &'r TextDataRef<'r>) -> Self {
        Self { data_ref, i: 0 }
    }
}

impl<'r> Iterator for TextDataRefStringIter<'r> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.i;
        self.i += 1;

        self.data_ref
            .text_at(i)
            .map(|text_option| match text_option {
                Some(text) => text.to_owned(),
                None => String::default(),
            })
            .ok()
    }
}

pub struct TextDataRefFloatOptionIter<'r> {
    data_ref: &'r TextDataRef<'r>,
    i: usize,
}

impl<'r> TextDataRefFloatOptionIter<'r> {
    pub fn new(data_ref: &'r TextDataRef<'r>) -> Self {
        Self { data_ref, i: 0 }
    }
}

impl<'r> Iterator for TextDataRefFloatOptionIter<'r> {
    type Item = Option<f64>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.i;
        self.i += 1;

        self.data_ref
            .text_at(i)
            .map(|text_option| match text_option {
                Some(text) => text.parse().ok(),
                None => None,
            })
            .ok()
    }
}

impl<'r> From<TextDataRef<'r>> for FeatureDataRef<'r> {
    fn from(data_ref: TextDataRef<'r>) -> Self {
        Self::Text(data_ref)
    }
}

impl<'r> TextDataRef<'r> {
    pub fn new(
        data_buffer: arrow::buffer::Buffer,
        offsets: &'r [i32],
        valid_bitmap: &'r Option<arrow::bitmap::Bitmap>,
    ) -> Self {
        Self {
            data_buffer,
            offsets,
            valid_bitmap,
        }
    }

    /// Returns the offsets of the individual strings
    pub fn offsets(&self) -> &[i32] {
        self.offsets
    }

    /// Returns the text reference at a certain position in the feature collection
    ///
    /// # Errors
    ///
    /// This method fails if `pos` is out of bounds
    ///
    pub fn text_at(&self, pos: usize) -> Result<Option<&str>> {
        ensure!(
            pos < (self.offsets.len() - 1),
            error::FeatureData {
                details: "Position must be in data range"
            }
        );

        let start = self.offsets[pos];
        let end = self.offsets[pos + 1];

        if start == end {
            return Ok(None);
        }

        let text = unsafe {
            byte_ptr_to_str(
                self.data_buffer.slice(start as usize).as_ptr(),
                (end - start) as usize,
            )
        };

        Ok(Some(text))
    }
}

impl FeatureDataType {
    pub fn arrow_data_type(self) -> arrow::datatypes::DataType {
        match self {
            Self::Text => arrow::datatypes::DataType::Utf8,
            Self::Float => arrow::datatypes::DataType::Float64,
            Self::Int => arrow::datatypes::DataType::Int64,
            Self::Category => arrow::datatypes::DataType::UInt8,
            Self::Bool => arrow::datatypes::DataType::Boolean,
        }
    }

    #[allow(clippy::unused_self)]
    pub fn nullable(self) -> bool {
        true
    }

    pub fn arrow_builder(self, len: usize) -> Box<dyn arrow::array::ArrayBuilder> {
        match self {
            Self::Text => Box::new(arrow::array::StringBuilder::new(len)),
            Self::Float => Box::new(arrow::array::Float64Builder::new(len)),
            Self::Int => Box::new(arrow::array::Int64Builder::new(len)),
            Self::Category => Box::new(arrow::array::UInt8Builder::new(len)),
            Self::Bool => Box::new(arrow::array::BooleanBuilder::new(len)),
        }
    }
}

impl FeatureData {
    pub fn arrow_data_type(&self) -> arrow::datatypes::DataType {
        FeatureDataType::from(self).arrow_data_type()
    }

    pub fn nullable(&self) -> bool {
        FeatureDataType::from(self).nullable()
    }

    pub fn len(&self) -> usize {
        match self {
            FeatureData::Text(v) => v.len(),
            FeatureData::NullableText(v) => v.len(),
            FeatureData::Float(v) => v.len(),
            FeatureData::NullableFloat(v) => v.len(),
            FeatureData::Int(v) => v.len(),
            FeatureData::NullableInt(v) => v.len(),
            FeatureData::Category(v) => v.len(),
            FeatureData::NullableCategory(v) => v.len(),
            FeatureData::Bool(v) => v.len(),
            FeatureData::NullableBool(v) => v.len()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Creates an `arrow` array builder.
    ///
    /// # Errors
    ///
    /// This method fails if an `arrow` internal error occurs
    ///
    pub(crate) fn arrow_builder(
        &self,
    ) -> Result<Box<dyn arrow::array::ArrayBuilder>, PrimitivesError> {
        Ok(match self {
            Self::Text(v) => {
                let mut builder = arrow::array::StringBuilder::new(v.len());
                for text in v {
                    builder.append_value(text)?;
                }
                Box::new(builder)
            }
            Self::NullableText(v) => {
                let mut builder = arrow::array::StringBuilder::new(v.len());
                for text_opt in v {
                    if let Some(text) = text_opt {
                        builder.append_value(text)?;
                    } else {
                        builder.append_null()?;
                    }
                }
                Box::new(builder)
            }
            Self::Float(v) => {
                let mut builder = arrow::array::Float64Builder::new(v.len());
                builder.append_slice(v)?;
                Box::new(builder)
            }
            Self::NullableFloat(v) => {
                let mut builder = arrow::array::Float64Builder::new(v.len());
                for &number_option in v {
                    builder.append_option(number_option)?;
                }
                Box::new(builder)
            }
            Self::Int(v) => {
                let mut builder = arrow::array::Int64Builder::new(v.len());
                builder.append_slice(v)?;
                Box::new(builder)
            }
            Self::NullableInt(v) => {
                let mut builder = arrow::array::Int64Builder::new(v.len());
                for &int_option in v {
                    builder.append_option(int_option)?;
                }
                Box::new(builder)
            }
            Self::Category(v) => {
                let mut builder = arrow::array::UInt8Builder::new(v.len());
                builder.append_slice(v)?;
                Box::new(builder)
            }
            Self::NullableCategory(v) => {
                let mut builder = arrow::array::UInt8Builder::new(v.len());
                for &float_option in v {
                    builder.append_option(float_option)?;
                }
                Box::new(builder)
            }
            FeatureData::Bool(v) => {
                let mut builder = arrow::array::BooleanBuilder::new(v.len());
                builder.append_slice(v)?;
                Box::new(builder)
            }
            FeatureData::NullableBool(v) => {
                let mut builder = arrow::array::BooleanBuilder::new(v.len());
                for &bool_option in v {
                    builder.append_option(bool_option)?;
                }
                Box::new(builder)
            }
        })
    }
}

impl From<&FeatureData> for FeatureDataType {
    fn from(value: &FeatureData) -> Self {
        match value {
            FeatureData::Text(_) | FeatureData::NullableText(_) => Self::Text,
            FeatureData::Float(_) | FeatureData::NullableFloat(_) => Self::Float,
            FeatureData::Int(_) | FeatureData::NullableInt(_) => Self::Int,
            FeatureData::Category(_) | FeatureData::NullableCategory(_) => Self::Category,
            FeatureData::Bool(_) | FeatureData::NullableBool(_) => Self::Bool
        }
    }
}

impl From<&FeatureDataValue> for FeatureDataType {
    fn from(value: &FeatureDataValue) -> Self {
        match value {
            FeatureDataValue::Text(_) | FeatureDataValue::NullableText(_) => Self::Text,
            FeatureDataValue::Float(_) | FeatureDataValue::NullableFloat(_) => Self::Float,
            FeatureDataValue::Int(_) | FeatureDataValue::NullableInt(_) => Self::Int,
            FeatureDataValue::Category(_) | FeatureDataValue::NullableCategory(_) => Self::Category,
            FeatureDataValue::Bool(_) | FeatureDataValue::NullableBool(_) => Self::Bool
        }
    }
}

impl<'f> From<&'f FeatureDataRef<'f>> for FeatureDataType {
    fn from(value: &FeatureDataRef) -> Self {
        match value {
            FeatureDataRef::Text(_) => Self::Text,
            FeatureDataRef::Float(..) => Self::Float,
            FeatureDataRef::Int(_) => Self::Int,
            FeatureDataRef::Category(_) => Self::Category,
        }
    }
}

impl TryFrom<&FeatureDataValue> for f64 {
    type Error = crate::collections::FeatureCollectionError;

    fn try_from(value: &FeatureDataValue) -> Result<Self, Self::Error> {
        Ok(match value {
            FeatureDataValue::Float(v) => *v,
            FeatureDataValue::NullableFloat(v) if v.is_some() => v.unwrap(),
            _ => return Err(crate::collections::FeatureCollectionError::WrongDataType),
        })
    }
}

impl TryFrom<FeatureDataValue> for f64 {
    type Error = crate::collections::FeatureCollectionError;

    fn try_from(value: FeatureDataValue) -> Result<Self, Self::Error> {
        f64::try_from(&value)
    }
}

impl TryFrom<&FeatureDataValue> for i64 {
    type Error = crate::collections::FeatureCollectionError;

    fn try_from(value: &FeatureDataValue) -> Result<i64, Self::Error> {
        Ok(match value {
            FeatureDataValue::Int(v) => *v,
            FeatureDataValue::NullableInt(v) if v.is_some() => v.unwrap(),
            _ => return Err(crate::collections::FeatureCollectionError::WrongDataType),
        })
    }
}

impl TryFrom<FeatureDataValue> for i64 {
    type Error = crate::collections::FeatureCollectionError;

    fn try_from(value: FeatureDataValue) -> Result<i64, Self::Error> {
        i64::try_from(&value)
    }
}

impl<'s> TryFrom<&'s FeatureDataValue> for &'s str {
    type Error = crate::collections::FeatureCollectionError;

    fn try_from(value: &FeatureDataValue) -> Result<&str, Self::Error> {
        Ok(match value {
            FeatureDataValue::Text(v) => v.as_ref(),
            FeatureDataValue::NullableText(v) if v.is_some() => v.as_ref().unwrap(),
            _ => return Err(crate::collections::FeatureCollectionError::WrongDataType),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        collections::{DataCollection, FeatureCollectionInfos},
        primitives::{NoGeometry, TimeInterval},
    };

    use super::*;

    #[test]
    fn strings_iter() {
        let collection = DataCollection::from_slices(
            &[] as &[NoGeometry],
            &[TimeInterval::default(); 3],
            &[
                ("ints", FeatureData::Int(vec![1, 2, 3])),
                (
                    "floats",
                    FeatureData::NullableFloat(vec![Some(1.0), None, Some(3.0)]),
                ),
                (
                    "texts",
                    FeatureData::NullableText(vec![
                        Some("a".to_owned()),
                        Some("b".to_owned()),
                        None,
                    ]),
                ),
            ],
        )
        .unwrap();

        let from_ints: Vec<String> = collection.data("ints").unwrap().strings_iter().collect();
        let from_ints_cmp: Vec<String> = ["1", "2", "3"].iter().map(ToString::to_string).collect();
        assert_eq!(from_ints, from_ints_cmp);

        let from_floats: Vec<String> = collection.data("floats").unwrap().strings_iter().collect();
        let from_floats_cmp: Vec<String> = ["1", "", "3"].iter().map(ToString::to_string).collect();
        assert_eq!(from_floats, from_floats_cmp);

        let from_strings: Vec<String> = collection.data("texts").unwrap().strings_iter().collect();
        let from_strings_cmp: Vec<String> =
            ["a", "b", ""].iter().map(ToString::to_string).collect();
        assert_eq!(from_strings, from_strings_cmp);
    }

    #[test]
    fn float_options_iter() {
        let collection = DataCollection::from_slices(
            &[] as &[NoGeometry],
            &[TimeInterval::default(); 3],
            &[
                ("ints", FeatureData::Int(vec![1, 2, 3])),
                (
                    "floats",
                    FeatureData::NullableFloat(vec![Some(1.0), None, Some(3.0)]),
                ),
                (
                    "texts",
                    FeatureData::NullableText(vec![
                        Some("1".to_owned()),
                        Some("f".to_owned()),
                        None,
                    ]),
                ),
            ],
        )
        .unwrap();

        let from_ints: Vec<Option<f64>> = collection
            .data("ints")
            .unwrap()
            .float_options_iter()
            .collect();
        let from_ints_cmp: Vec<Option<f64>> = vec![Some(1.0), Some(2.0), Some(3.0)];
        assert_eq!(from_ints, from_ints_cmp);

        let from_floats: Vec<Option<f64>> = collection
            .data("floats")
            .unwrap()
            .float_options_iter()
            .collect();
        let from_floats_cmp: Vec<Option<f64>> = vec![Some(1.0), None, Some(3.0)];
        assert_eq!(from_floats, from_floats_cmp);

        let from_strings: Vec<Option<f64>> = collection
            .data("texts")
            .unwrap()
            .float_options_iter()
            .collect();
        let from_strings_cmp: Vec<Option<f64>> = vec![Some(1.0), None, None];
        assert_eq!(from_strings, from_strings_cmp);
    }
}
