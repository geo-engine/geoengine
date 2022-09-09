use crate::error;
use crate::primitives::TimeInstance;
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
    Bool,
    DateTime,
}

impl FeatureDataType {
    pub fn try_from_ogr_field_type_code(code: u32) -> Result<Self> {
        Ok(match code {
            OGRFieldType::OFTInteger | OGRFieldType::OFTInteger64 => Self::Int,
            OGRFieldType::OFTReal => Self::Float,
            OGRFieldType::OFTString => Self::Text,
            OGRFieldType::OFTBinary => Self::Bool,
            OGRFieldType::OFTDateTime | OGRFieldType::OFTDate => Self::DateTime,
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
    NullableBool(Vec<Option<bool>>),
    DateTime(Vec<TimeInstance>),
    NullableDateTime(Vec<Option<TimeInstance>>),
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
    NullableBool(Option<bool>),
    DateTime(TimeInstance),
    NullableDateTime(Option<TimeInstance>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum FeatureDataRef<'f> {
    Category(CategoryDataRef<'f>),
    Int(IntDataRef<'f>),
    Float(FloatDataRef<'f>),
    Text(TextDataRef<'f>),
    Bool(BoolDataRef<'f>),
    DateTime(DateTimeDataRef<'f>),
}

impl<'f> FeatureDataRef<'f> {
    /// Computes JSON value lists for data elements
    pub fn json_values(&self) -> Box<dyn Iterator<Item = serde_json::Value> + '_> {
        match self {
            FeatureDataRef::Text(data_ref) => data_ref.json_values(),
            FeatureDataRef::Float(data_ref) => data_ref.json_values(),
            FeatureDataRef::Int(data_ref) => data_ref.json_values(),
            FeatureDataRef::Category(data_ref) => data_ref.json_values(),
            FeatureDataRef::Bool(data_ref) => data_ref.json_values(),
            FeatureDataRef::DateTime(data_ref) => data_ref.json_values(),
        }
    }

    /// Computes a vector of null flags.
    pub fn nulls(&self) -> Vec<bool> {
        match self {
            FeatureDataRef::Text(data_ref) => data_ref.nulls(),
            FeatureDataRef::Float(data_ref) => data_ref.nulls(),
            FeatureDataRef::Int(data_ref) => data_ref.nulls(),
            FeatureDataRef::Category(data_ref) => data_ref.nulls(),
            FeatureDataRef::Bool(data_ref) => data_ref.nulls(),
            FeatureDataRef::DateTime(data_ref) => data_ref.nulls(),
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
            FeatureDataRef::DateTime(data_ref) => data_ref.has_nulls(),
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
            FeatureDataRef::DateTime(data_ref) => data_ref.get_unchecked(i),
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
            FeatureDataRef::DateTime(data_ref) => Box::new(data_ref.strings_iter()),
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
            FeatureDataRef::DateTime(data_ref) => Box::new(data_ref.float_options_iter()),
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
    valid_bitmap: Option<&'f arrow::bitmap::Bitmap>,
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
    pub fn new(buffer: &'f [f64], null_bitmap: Option<&'f arrow::bitmap::Bitmap>) -> Self {
        Self {
            buffer,
            valid_bitmap: null_bitmap,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct IntDataRef<'f> {
    buffer: &'f [i64],
    valid_bitmap: Option<&'f arrow::bitmap::Bitmap>,
}

impl<'f> IntDataRef<'f> {
    pub fn new(buffer: &'f [i64], null_bitmap: Option<&'f arrow::bitmap::Bitmap>) -> Self {
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

fn null_bitmap_to_bools(null_bitmap: Option<&Bitmap>, len: usize) -> Vec<bool> {
    if let Some(nulls) = null_bitmap {
        (0..len).map(|i| !nulls.is_set(i)).collect()
    } else {
        vec![false; len]
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BoolDataRef<'f> {
    buffer: Vec<bool>,
    valid_bitmap: Option<&'f arrow::bitmap::Bitmap>,
}

impl<'f> BoolDataRef<'f> {
    pub fn new(buffer: Vec<bool>, null_bitmap: Option<&'f arrow::bitmap::Bitmap>) -> Self {
        Self {
            buffer,
            valid_bitmap: null_bitmap,
        }
    }
}

impl<'f> DataRef<'f, bool> for BoolDataRef<'f> {
    fn json_value(value: &bool) -> serde_json::Value {
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
            FeatureDataValue::NullableBool(if self.is_null(i) {
                None
            } else {
                Some(self.as_ref()[i])
            })
        } else {
            FeatureDataValue::Bool(self.as_ref()[i])
        }
    }

    type StringsIter = NumberDataRefStringIter<'f, Self, bool>;

    fn strings_iter(&'f self) -> Self::StringsIter {
        NumberDataRefStringIter::new(self)
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    type FloatOptionsIter = BoolDataRefFloatOptionIter<'f>;

    fn float_options_iter(&'f self) -> Self::FloatOptionsIter {
        BoolDataRefFloatOptionIter::new(self)
    }
}

impl AsRef<[bool]> for BoolDataRef<'_> {
    fn as_ref(&self) -> &[bool] {
        &self.buffer
    }
}

impl<'f> From<BoolDataRef<'f>> for FeatureDataRef<'f> {
    fn from(data_ref: BoolDataRef<'f>) -> Self {
        FeatureDataRef::Bool(data_ref)
    }
}

pub struct BoolDataRefFloatOptionIter<'f> {
    data_ref: &'f BoolDataRef<'f>,
    i: usize,
}

impl<'f> BoolDataRefFloatOptionIter<'f> {
    pub fn new(data_ref: &'f BoolDataRef<'f>) -> Self {
        Self { data_ref, i: 0 }
    }
}

impl<'f> Iterator for BoolDataRefFloatOptionIter<'f> {
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
            Some(f64::from(u8::from(self.data_ref.as_ref()[i])))
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DateTimeDataRef<'f> {
    buffer: &'f [TimeInstance],
    valid_bitmap: Option<&'f arrow::bitmap::Bitmap>,
}

impl<'f> DateTimeDataRef<'f> {
    pub fn new(buffer: &'f [TimeInstance], null_bitmap: Option<&'f arrow::bitmap::Bitmap>) -> Self {
        Self {
            buffer,
            valid_bitmap: null_bitmap,
        }
    }
}

impl<'f> DataRef<'f, TimeInstance> for DateTimeDataRef<'f> {
    fn json_value(value: &TimeInstance) -> serde_json::Value {
        serde_json::to_value(value).expect("TimeInstance can be serialized")
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
            FeatureDataValue::NullableDateTime(if self.is_null(i) {
                None
            } else {
                Some(self.as_ref()[i])
            })
        } else {
            FeatureDataValue::DateTime(self.as_ref()[i])
        }
    }

    type StringsIter = NumberDataRefStringIter<'f, Self, TimeInstance>;

    fn strings_iter(&'f self) -> Self::StringsIter {
        NumberDataRefStringIter::new(self)
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    type FloatOptionsIter = DateTimeDataRefFloatOptionIter<'f>;

    fn float_options_iter(&'f self) -> Self::FloatOptionsIter {
        Self::FloatOptionsIter::new(self)
    }
}

impl AsRef<[TimeInstance]> for DateTimeDataRef<'_> {
    fn as_ref(&self) -> &[TimeInstance] {
        self.buffer
    }
}

impl<'f> From<DateTimeDataRef<'f>> for FeatureDataRef<'f> {
    fn from(data_ref: DateTimeDataRef<'f>) -> Self {
        FeatureDataRef::DateTime(data_ref)
    }
}

pub struct DateTimeDataRefFloatOptionIter<'f> {
    data_ref: &'f DateTimeDataRef<'f>,
    i: usize,
}

impl<'f> DateTimeDataRefFloatOptionIter<'f> {
    pub fn new(data_ref: &'f DateTimeDataRef<'f>) -> Self {
        Self { data_ref, i: 0 }
    }
}

impl<'f> Iterator for DateTimeDataRefFloatOptionIter<'f> {
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
            Some(self.data_ref.as_ref()[i].inner() as f64)
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CategoryDataRef<'f> {
    buffer: &'f [u8],
    valid_bitmap: Option<&'f arrow::bitmap::Bitmap>,
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
    pub fn new(buffer: &'f [u8], null_bitmap: Option<&'f arrow::bitmap::Bitmap>) -> Self {
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
///     let mut builder = StringBuilder::with_capacity(3, 6+3);
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
    valid_bitmap: Option<&'f arrow::bitmap::Bitmap>,
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
                return if self.is_valid(pos) {
                    serde_json::Value::String(String::default())
                } else {
                    serde_json::Value::Null
                };
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
    ///     let mut builder = StringBuilder::with_capacity(3, 6+3);
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
        null_bitmap_to_bools(self.valid_bitmap, self.offsets.len() - 1)
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
        valid_bitmap: Option<&'r arrow::bitmap::Bitmap>,
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
            return Ok(if self.is_valid(pos) { Some("") } else { None });
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
            Self::DateTime => arrow::datatypes::DataType::Date64,
        }
    }

    #[allow(clippy::unused_self)]
    pub fn nullable(self) -> bool {
        true
    }

    pub fn arrow_builder(self, len: usize) -> Box<dyn arrow::array::ArrayBuilder> {
        match self {
            Self::Text => Box::new(arrow::array::StringBuilder::with_capacity(len, 0)),
            Self::Float => Box::new(arrow::array::Float64Builder::with_capacity(len)),
            Self::Int => Box::new(arrow::array::Int64Builder::with_capacity(len)),
            Self::Category => Box::new(arrow::array::UInt8Builder::with_capacity(len)),
            Self::Bool => Box::new(arrow::array::BooleanBuilder::with_capacity(len)),
            Self::DateTime => Box::new(arrow::array::Date64Builder::with_capacity(len)),
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
            FeatureData::NullableBool(v) => v.len(),
            FeatureData::DateTime(v) => v.len(),
            FeatureData::NullableDateTime(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Creates an `arrow` array builder.
    pub(crate) fn arrow_builder(&self) -> Box<dyn arrow::array::ArrayBuilder> {
        match self {
            Self::Text(v) => {
                let mut builder = arrow::array::StringBuilder::with_capacity(
                    v.len(),
                    v.iter().map(String::len).sum(),
                );
                for text in v {
                    builder.append_value(text);
                }
                Box::new(builder)
            }
            Self::NullableText(v) => {
                let mut builder = arrow::array::StringBuilder::with_capacity(
                    v.len(),
                    v.iter()
                        .map(|text_option| text_option.as_ref().map_or(0, String::len))
                        .sum(),
                );
                for text_opt in v {
                    if let Some(text) = text_opt {
                        builder.append_value(text);
                    } else {
                        builder.append_null();
                    }
                }
                Box::new(builder)
            }
            Self::Float(v) => {
                let mut builder = arrow::array::Float64Builder::with_capacity(v.len());
                builder.append_slice(v);
                Box::new(builder)
            }
            Self::NullableFloat(v) => {
                let mut builder = arrow::array::Float64Builder::with_capacity(v.len());
                for &number_option in v {
                    builder.append_option(number_option);
                }
                Box::new(builder)
            }
            Self::Int(v) => {
                let mut builder = arrow::array::Int64Builder::with_capacity(v.len());
                builder.append_slice(v);
                Box::new(builder)
            }
            Self::NullableInt(v) => {
                let mut builder = arrow::array::Int64Builder::with_capacity(v.len());
                for &int_option in v {
                    builder.append_option(int_option);
                }
                Box::new(builder)
            }
            Self::Category(v) => {
                let mut builder = arrow::array::UInt8Builder::with_capacity(v.len());
                builder.append_slice(v);
                Box::new(builder)
            }
            Self::NullableCategory(v) => {
                let mut builder = arrow::array::UInt8Builder::with_capacity(v.len());
                for &float_option in v {
                    builder.append_option(float_option);
                }
                Box::new(builder)
            }
            FeatureData::Bool(v) => {
                let mut builder = arrow::array::BooleanBuilder::with_capacity(v.len());
                builder.append_slice(v);
                Box::new(builder)
            }
            FeatureData::NullableBool(v) => {
                let mut builder = arrow::array::BooleanBuilder::with_capacity(v.len());
                for &bool_option in v {
                    builder.append_option(bool_option);
                }
                Box::new(builder)
            }
            FeatureData::DateTime(v) => {
                let mut builder = arrow::array::Date64Builder::with_capacity(v.len());
                let x: Vec<_> = v.iter().map(|x| x.inner()).collect();
                builder.append_slice(&x);
                Box::new(builder)
            }
            FeatureData::NullableDateTime(v) => {
                let mut builder = arrow::array::Date64Builder::with_capacity(v.len());
                for &dt_option in v {
                    builder.append_option(dt_option.map(TimeInstance::inner));
                }
                Box::new(builder)
            }
        }
    }
}

impl From<&FeatureData> for FeatureDataType {
    fn from(value: &FeatureData) -> Self {
        match value {
            FeatureData::Text(_) | FeatureData::NullableText(_) => Self::Text,
            FeatureData::Float(_) | FeatureData::NullableFloat(_) => Self::Float,
            FeatureData::Int(_) | FeatureData::NullableInt(_) => Self::Int,
            FeatureData::Category(_) | FeatureData::NullableCategory(_) => Self::Category,
            FeatureData::Bool(_) | FeatureData::NullableBool(_) => Self::Bool,
            FeatureData::DateTime(_) | FeatureData::NullableDateTime(_) => Self::DateTime,
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
            FeatureDataValue::Bool(_) | FeatureDataValue::NullableBool(_) => Self::Bool,
            FeatureDataValue::DateTime(_) | FeatureDataValue::NullableDateTime(_) => Self::DateTime,
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
            FeatureDataRef::Bool(_) => Self::Bool,
            FeatureDataRef::DateTime(_) => Self::DateTime,
        }
    }
}

impl TryFrom<&FeatureDataValue> for f64 {
    type Error = crate::collections::FeatureCollectionError;

    fn try_from(value: &FeatureDataValue) -> Result<Self, Self::Error> {
        Ok(match value {
            FeatureDataValue::Float(v) | FeatureDataValue::NullableFloat(Some(v)) => *v,
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
            FeatureDataValue::Int(v) | FeatureDataValue::NullableInt(Some(v)) => *v,
            FeatureDataValue::DateTime(v) | FeatureDataValue::NullableDateTime(Some(v)) => {
                v.inner()
            }
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
            FeatureDataValue::Text(v) | FeatureDataValue::NullableText(Some(v)) => v.as_ref(),
            _ => return Err(crate::collections::FeatureCollectionError::WrongDataType),
        })
    }
}

impl TryFrom<&FeatureDataValue> for bool {
    type Error = crate::collections::FeatureCollectionError;

    fn try_from(value: &FeatureDataValue) -> Result<bool, Self::Error> {
        Ok(match value {
            FeatureDataValue::Bool(v) | FeatureDataValue::NullableBool(Some(v)) => *v,
            _ => return Err(crate::collections::FeatureCollectionError::WrongDataType),
        })
    }
}

impl TryFrom<&FeatureDataValue> for TimeInstance {
    type Error = crate::collections::FeatureCollectionError;

    fn try_from(value: &FeatureDataValue) -> Result<TimeInstance, Self::Error> {
        Ok(match value {
            FeatureDataValue::DateTime(v) | FeatureDataValue::NullableDateTime(Some(v)) => *v,
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
                (
                    "bools",
                    FeatureData::NullableBool(vec![Some(true), Some(false), None]),
                ),
                (
                    "dates",
                    FeatureData::NullableDateTime(vec![
                        Some(TimeInstance::from_millis_unchecked(946_681_200_000)),
                        None,
                        Some(TimeInstance::from_millis_unchecked(1_636_448_729_000)),
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

        let from_bools: Vec<String> = collection.data("bools").unwrap().strings_iter().collect();
        let from_bools_cmp: Vec<String> = ["true", "false", ""]
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(from_bools, from_bools_cmp);

        let from_dates: Vec<String> = collection.data("dates").unwrap().strings_iter().collect();
        let from_dates_cmp: Vec<String> =
            ["1999-12-31T23:00:00.000Z", "", "2021-11-09T09:05:29.000Z"]
                .iter()
                .map(ToString::to_string)
                .collect();
        assert_eq!(from_dates, from_dates_cmp);
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
                (
                    "bools",
                    FeatureData::NullableBool(vec![Some(true), Some(false), None]),
                ),
                (
                    "dates",
                    FeatureData::NullableDateTime(vec![
                        Some(TimeInstance::from_millis_unchecked(946_681_200_000)),
                        None,
                        Some(TimeInstance::from_millis_unchecked(1_636_448_729_000)),
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

        let from_bools: Vec<Option<f64>> = collection
            .data("bools")
            .unwrap()
            .float_options_iter()
            .collect();
        let from_bools_cmp: Vec<Option<f64>> = vec![Some(1.0), Some(0.0), None];
        assert_eq!(from_bools, from_bools_cmp);

        let from_dates: Vec<Option<f64>> = collection
            .data("dates")
            .unwrap()
            .float_options_iter()
            .collect();
        let from_dates_cmp: Vec<Option<f64>> =
            vec![Some(946_681_200_000.0), None, Some(1_636_448_729_000.0)];
        assert_eq!(from_dates, from_dates_cmp);
    }
}
