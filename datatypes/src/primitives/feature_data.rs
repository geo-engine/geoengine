use crate::error;
use crate::primitives::PrimitivesError;
use crate::util::Result;
use arrow::bitmap::Bitmap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ensure;
use std::convert::TryFrom;
use std::slice;
use std::str;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum FeatureDataType {
    Categorical,
    Decimal,
    Number,
    Text,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum FeatureData {
    Categorical(Vec<u8>), // TODO: add names to categories
    NullableCategorical(Vec<Option<u8>>),
    Decimal(Vec<i64>),
    NullableDecimal(Vec<Option<i64>>),
    Number(Vec<f64>),
    NullableNumber(Vec<Option<f64>>),
    Text(Vec<String>),
    NullableText(Vec<Option<String>>),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum FeatureDataValue {
    Categorical(u8),
    NullableCategorical(Option<u8>),
    Decimal(i64),
    NullableDecimal(Option<i64>),
    Number(f64),
    NullableNumber(Option<f64>),
    Text(String),
    NullableText(Option<String>),
}

#[derive(Clone, Debug)]
pub enum FeatureDataRef<'f> {
    Categorical(CategoricalDataRef<'f>),
    Decimal(DecimalDataRef<'f>),
    Number(NumberDataRef<'f>),
    Text(TextDataRef<'f>),
}

impl<'f> FeatureDataRef<'f> {
    /// Computes JSON value lists for data elements
    pub fn json_values(&self) -> Box<dyn Iterator<Item = serde_json::Value> + '_> {
        match self {
            FeatureDataRef::Text(data_ref) => data_ref.json_values(),
            FeatureDataRef::Number(data_ref) => data_ref.json_values(),
            FeatureDataRef::Decimal(data_ref) => data_ref.json_values(),
            FeatureDataRef::Categorical(data_ref) => data_ref.json_values(),
        }
    }

    /// Computes a vector of null flags.
    pub fn nulls(&self) -> Vec<bool> {
        match self {
            FeatureDataRef::Text(data_ref) => data_ref.nulls(),
            FeatureDataRef::Number(data_ref) => data_ref.nulls(),
            FeatureDataRef::Decimal(data_ref) => data_ref.nulls(),
            FeatureDataRef::Categorical(data_ref) => data_ref.nulls(),
        }
    }

    /// Is any of the data elements null?
    pub fn has_nulls(&self) -> bool {
        match self {
            FeatureDataRef::Text(data_ref) => data_ref.has_nulls(),
            FeatureDataRef::Number(data_ref) => data_ref.has_nulls(),
            FeatureDataRef::Decimal(data_ref) => data_ref.has_nulls(),
            FeatureDataRef::Categorical(data_ref) => data_ref.has_nulls(),
        }
    }

    /// Get the `FeatureDataValue` value at position `i`
    pub fn get_unchecked(&self, i: usize) -> FeatureDataValue {
        match self {
            FeatureDataRef::Text(data_ref) => data_ref.get_unchecked(i),
            FeatureDataRef::Number(data_ref) => data_ref.get_unchecked(i),
            FeatureDataRef::Decimal(data_ref) => data_ref.get_unchecked(i),
            FeatureDataRef::Categorical(data_ref) => data_ref.get_unchecked(i),
        }
    }
}

/// Common methods for feature data references
pub trait DataRef<'r, T>: AsRef<[T]> + Into<FeatureDataRef<'r>>
where
    T: 'r,
{
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
}

#[derive(Clone, Debug)]
pub struct NumberDataRef<'f> {
    buffer: arrow::buffer::Buffer,
    valid_bitmap: &'f Option<arrow::bitmap::Bitmap>,
}

impl<'f> DataRef<'f, f64> for NumberDataRef<'f> {
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
            FeatureDataValue::NullableNumber(if self.is_null(i) {
                None
            } else {
                Some(self.as_ref()[i])
            })
        } else {
            FeatureDataValue::Number(self.as_ref()[i])
        }
    }
}

impl AsRef<[f64]> for NumberDataRef<'_> {
    fn as_ref(&self) -> &[f64] {
        unsafe { self.buffer.typed_data() }
    }
}

impl<'f> From<NumberDataRef<'f>> for FeatureDataRef<'f> {
    fn from(data_ref: NumberDataRef<'f>) -> FeatureDataRef<'f> {
        FeatureDataRef::Number(data_ref)
    }
}

impl<'f> NumberDataRef<'f> {
    pub fn new(
        buffer: arrow::buffer::Buffer,
        null_bitmap: &'f Option<arrow::bitmap::Bitmap>,
    ) -> Self {
        Self {
            buffer,
            valid_bitmap: null_bitmap,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DecimalDataRef<'f> {
    buffer: arrow::buffer::Buffer,
    valid_bitmap: &'f Option<arrow::bitmap::Bitmap>,
}

impl<'f> DecimalDataRef<'f> {
    pub fn new(
        buffer: arrow::buffer::Buffer,
        null_bitmap: &'f Option<arrow::bitmap::Bitmap>,
    ) -> Self {
        Self {
            buffer,
            valid_bitmap: null_bitmap,
        }
    }
}

impl<'f> DataRef<'f, i64> for DecimalDataRef<'f> {
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
            FeatureDataValue::NullableDecimal(if self.is_null(i) {
                None
            } else {
                Some(self.as_ref()[i])
            })
        } else {
            FeatureDataValue::Decimal(self.as_ref()[i])
        }
    }
}

impl AsRef<[i64]> for DecimalDataRef<'_> {
    fn as_ref(&self) -> &[i64] {
        unsafe { self.buffer.typed_data() }
    }
}

impl<'f> From<DecimalDataRef<'f>> for FeatureDataRef<'f> {
    fn from(data_ref: DecimalDataRef<'f>) -> FeatureDataRef<'f> {
        FeatureDataRef::Decimal(data_ref)
    }
}

fn null_bitmap_to_bools(null_bitmap: &Option<Bitmap>, len: usize) -> Vec<bool> {
    if let Some(nulls) = null_bitmap {
        (0..len).map(|i| !nulls.is_set(i)).collect()
    } else {
        vec![false; len]
    }
}

#[derive(Clone, Debug)]
pub struct CategoricalDataRef<'f> {
    buffer: arrow::buffer::Buffer,
    valid_bitmap: &'f Option<arrow::bitmap::Bitmap>,
}

impl<'f> DataRef<'f, u8> for CategoricalDataRef<'f> {
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
            FeatureDataValue::NullableCategorical(if self.is_null(i) {
                None
            } else {
                Some(self.as_ref()[i])
            })
        } else {
            FeatureDataValue::Categorical(self.as_ref()[i])
        }
    }
}

impl AsRef<[u8]> for CategoricalDataRef<'_> {
    fn as_ref(&self) -> &[u8] {
        self.buffer.data()
    }
}

impl<'f> From<CategoricalDataRef<'f>> for FeatureDataRef<'f> {
    fn from(data_ref: CategoricalDataRef<'f>) -> FeatureDataRef<'f> {
        FeatureDataRef::Categorical(data_ref)
    }
}

impl<'f> CategoricalDataRef<'f> {
    pub fn new(
        buffer: arrow::buffer::Buffer,
        null_bitmap: &'f Option<arrow::bitmap::Bitmap>,
    ) -> Self {
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
#[derive(Clone, Debug)]
pub struct TextDataRef<'f> {
    data_buffer: arrow::buffer::Buffer,
    offsets_buffer: arrow::buffer::Buffer,
    valid_bitmap: &'f Option<arrow::bitmap::Bitmap>,
}

impl<'f> AsRef<[u8]> for TextDataRef<'f> {
    fn as_ref(&self) -> &[u8] {
        self.data_buffer.data()
    }
}

impl<'r> DataRef<'r, u8> for TextDataRef<'r> {
    fn json_values(&'r self) -> Box<dyn Iterator<Item = serde_json::Value> + 'r> {
        let offsets = self.offsets();
        let number_of_values = offsets.len() - 1;

        Box::new((0..number_of_values).map(move |pos| {
            let start = offsets[pos];
            let end = offsets[pos + 1];

            if start == end {
                return serde_json::Value::Null;
            }

            let text = unsafe {
                byte_ptr_to_str(
                    self.data_buffer.slice(start as usize).raw_data(),
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
        let mut nulls = Vec::with_capacity(self.offsets().len() - 1);
        for window in self.offsets().windows(2) {
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
}

impl<'r> From<TextDataRef<'r>> for FeatureDataRef<'r> {
    fn from(data_ref: TextDataRef<'r>) -> Self {
        Self::Text(data_ref)
    }
}

impl<'r> TextDataRef<'r> {
    pub fn new(
        data_buffer: arrow::buffer::Buffer,
        offsets_buffer: arrow::buffer::Buffer,
        valid_bitmap: &'r Option<arrow::bitmap::Bitmap>,
    ) -> Self {
        Self {
            data_buffer,
            offsets_buffer,
            valid_bitmap,
        }
    }

    pub fn offsets(&self) -> &[i32] {
        unsafe { self.offsets_buffer.typed_data() }
    }

    /// Returns the text reference at a certain position in the feature collection
    ///
    /// # Errors
    ///
    /// This method fails if `pos` is out of bounds
    ///
    pub fn text_at(&self, pos: usize) -> Result<Option<&str>> {
        ensure!(
            pos < (self.offsets().len() - 1),
            error::FeatureData {
                details: "Position must be in data range"
            }
        );

        let start = self.offsets()[pos];
        let end = self.offsets()[pos + 1];

        if start == end {
            return Ok(None);
        }

        let text = unsafe {
            byte_ptr_to_str(
                self.data_buffer.slice(start as usize).raw_data(),
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
            Self::Number => arrow::datatypes::DataType::Float64,
            Self::Decimal => arrow::datatypes::DataType::Int64,
            Self::Categorical => arrow::datatypes::DataType::UInt8,
        }
    }

    #[allow(clippy::unused_self)]
    pub fn nullable(self) -> bool {
        true
    }

    pub fn arrow_builder(self, len: usize) -> Box<dyn arrow::array::ArrayBuilder> {
        match self {
            Self::Text => Box::new(arrow::array::StringBuilder::new(len)),
            Self::Number => Box::new(arrow::array::Float64Builder::new(len)),
            Self::Decimal => Box::new(arrow::array::Int64Builder::new(len)),
            Self::Categorical => Box::new(arrow::array::UInt8Builder::new(len)),
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
            FeatureData::Number(v) => v.len(),
            FeatureData::NullableNumber(v) => v.len(),
            FeatureData::Decimal(v) => v.len(),
            FeatureData::NullableDecimal(v) => v.len(),
            FeatureData::Categorical(v) => v.len(),
            FeatureData::NullableCategorical(v) => v.len(),
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
            Self::Number(v) => {
                let mut builder = arrow::array::Float64Builder::new(v.len());
                builder.append_slice(v)?;
                Box::new(builder)
            }
            Self::NullableNumber(v) => {
                let mut builder = arrow::array::Float64Builder::new(v.len());
                for &number_option in v {
                    builder.append_option(number_option)?;
                }
                Box::new(builder)
            }
            Self::Decimal(v) => {
                let mut builder = arrow::array::Int64Builder::new(v.len());
                builder.append_slice(v)?;
                Box::new(builder)
            }
            Self::NullableDecimal(v) => {
                let mut builder = arrow::array::Int64Builder::new(v.len());
                for &decimal_option in v {
                    builder.append_option(decimal_option)?;
                }
                Box::new(builder)
            }
            Self::Categorical(v) => {
                let mut builder = arrow::array::UInt8Builder::new(v.len());
                builder.append_slice(v)?;
                Box::new(builder)
            }
            Self::NullableCategorical(v) => {
                let mut builder = arrow::array::UInt8Builder::new(v.len());
                for &number_option in v {
                    builder.append_option(number_option)?;
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
            FeatureData::Number(_) | FeatureData::NullableNumber(_) => Self::Number,
            FeatureData::Decimal(_) | FeatureData::NullableDecimal(_) => Self::Decimal,
            FeatureData::Categorical(_) | FeatureData::NullableCategorical(_) => Self::Categorical,
        }
    }
}

impl From<&FeatureDataValue> for FeatureDataType {
    fn from(value: &FeatureDataValue) -> Self {
        match value {
            FeatureDataValue::Text(_) | FeatureDataValue::NullableText(_) => Self::Text,
            FeatureDataValue::Number(_) | FeatureDataValue::NullableNumber(_) => Self::Number,
            FeatureDataValue::Decimal(_) | FeatureDataValue::NullableDecimal(_) => Self::Decimal,
            FeatureDataValue::Categorical(_) | FeatureDataValue::NullableCategorical(_) => {
                Self::Categorical
            }
        }
    }
}

impl<'f> From<&'f FeatureDataRef<'f>> for FeatureDataType {
    fn from(value: &FeatureDataRef) -> Self {
        match value {
            FeatureDataRef::Text(_) => Self::Text,
            FeatureDataRef::Number(..) => Self::Number,
            FeatureDataRef::Decimal(_) => Self::Decimal,
            FeatureDataRef::Categorical(_) => Self::Categorical,
        }
    }
}

impl TryFrom<&FeatureDataValue> for f64 {
    type Error = crate::collections::FeatureCollectionError;

    fn try_from(value: &FeatureDataValue) -> Result<Self, Self::Error> {
        Ok(match value {
            FeatureDataValue::Number(v) => *v,
            FeatureDataValue::NullableNumber(v) if v.is_some() => v.unwrap(),
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
            FeatureDataValue::Decimal(v) => *v,
            FeatureDataValue::NullableDecimal(v) if v.is_some() => v.unwrap(),
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
