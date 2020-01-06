use crate::error;
use crate::util::Result;
use arrow;
use arrow::bitmap::Bitmap;
use snafu::ensure;
use std::slice;
use std::str;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeatureDataType {
    Text,
    NullableText,
    Number,
    NullableNumber,
    Decimal,
    NullableDecimal,
    Categorical,
    NullableCategorical,
}

#[derive(Clone, Debug)]
pub enum FeatureData {
    Text(Vec<String>),
    NullableText(Vec<Option<String>>),
    Number(Vec<f64>),
    NullableNumber(Vec<Option<f64>>),
    Decimal(Vec<i64>),
    NullableDecimal(Vec<Option<i64>>),
    Categorical(Vec<u8>), // TODO: add names to categories
    NullableCategorical(Vec<Option<u8>>),
}

#[derive(Clone, Debug)]
pub enum FeatureDataValue {
    Text(String),
    NullableText(Option<String>),
    Number(f64),
    NullableNumber(Option<f64>),
    Decimal(i64),
    NullableDecimal(Option<i64>),
    Categorical(u8),
    NullableCategorical(Option<u8>),
}

#[derive(Clone, Debug)]
pub enum FeatureDataRef<'f> {
    Text(TextDataRef),
    NullableText(NullableTextDataRef),
    Number(NumberDataRef),
    NullableNumber(NullableNumberDataRef<'f>),
    Decimal(DecimalDataRef),
    NullableDecimal(NullableDecimalDataRef<'f>),
    Categorical(CategoricalDataRef),
    NullableCategorical(NullableCategoricalDataRef<'f>),
}

pub trait DataRef<T> {
    fn data(&self) -> &[T];
}

pub trait NullableDataRef {
    fn nulls(&self) -> Vec<bool>; // TODO: return bitmap directly or IndexedSlice trait?...
}

#[derive(Clone, Debug)]
pub struct NumberDataRef {
    buffer: arrow::buffer::Buffer,
}

impl DataRef<f64> for NumberDataRef {
    fn data(&self) -> &[f64] {
        self.buffer.typed_data()
    }
}

impl From<NumberDataRef> for FeatureDataRef<'_> {
    fn from(data_ref: NumberDataRef) -> Self {
        Self::Number(data_ref)
    }
}

impl NumberDataRef {
    pub fn new(buffer: arrow::buffer::Buffer) -> Self {
        Self { buffer }
    }
}

#[derive(Clone, Debug)]
pub struct NullableNumberDataRef<'f> {
    buffer: arrow::buffer::Buffer,
    null_bitmap: &'f Option<arrow::bitmap::Bitmap>,
}

impl DataRef<f64> for NullableNumberDataRef<'_> {
    fn data(&self) -> &[f64] {
        self.buffer.typed_data()
    }
}

impl<'f> NullableDataRef for NullableNumberDataRef<'f> {
    fn nulls(&self) -> Vec<bool> {
        null_bitmap_to_bools(self.data(), self.null_bitmap)
    }
}

impl<'f> From<NullableNumberDataRef<'f>> for FeatureDataRef<'f> {
    fn from(data_ref: NullableNumberDataRef<'f>) -> Self {
        Self::NullableNumber(data_ref)
    }
}

impl<'f> NullableNumberDataRef<'f> {
    pub fn new(
        buffer: arrow::buffer::Buffer,
        null_bitmap: &'f Option<arrow::bitmap::Bitmap>,
    ) -> Self {
        Self {
            buffer,
            null_bitmap,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DecimalDataRef {
    buffer: arrow::buffer::Buffer,
}

impl DataRef<i64> for DecimalDataRef {
    fn data(&self) -> &[i64] {
        self.buffer.typed_data()
    }
}

impl From<DecimalDataRef> for FeatureDataRef<'_> {
    fn from(data_ref: DecimalDataRef) -> Self {
        Self::Decimal(data_ref)
    }
}

impl DecimalDataRef {
    pub fn new(buffer: arrow::buffer::Buffer) -> Self {
        Self { buffer }
    }
}

#[derive(Clone, Debug)]
pub struct NullableDecimalDataRef<'f> {
    buffer: arrow::buffer::Buffer,
    null_bitmap: &'f Option<arrow::bitmap::Bitmap>,
}

impl DataRef<i64> for NullableDecimalDataRef<'_> {
    fn data(&self) -> &[i64] {
        self.buffer.typed_data()
    }
}

impl<'f> NullableDataRef for NullableDecimalDataRef<'f> {
    fn nulls(&self) -> Vec<bool> {
        null_bitmap_to_bools(self.data(), self.null_bitmap)
    }
}

fn null_bitmap_to_bools<T>(data: &[T], null_bitmap: &Option<Bitmap>) -> Vec<bool> {
    if let Some(nulls) = null_bitmap {
        (0..data.len()).map(|i| !nulls.is_set(i)).collect()
    } else {
        vec![false; data.len()]
    }
}

impl<'f> From<NullableDecimalDataRef<'f>> for FeatureDataRef<'f> {
    fn from(data_ref: NullableDecimalDataRef<'f>) -> Self {
        Self::NullableDecimal(data_ref)
    }
}

impl<'f> NullableDecimalDataRef<'f> {
    pub fn new(
        buffer: arrow::buffer::Buffer,
        null_bitmap: &'f Option<arrow::bitmap::Bitmap>,
    ) -> Self {
        Self {
            buffer,
            null_bitmap,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CategoricalDataRef {
    buffer: arrow::buffer::Buffer,
}

impl DataRef<u8> for CategoricalDataRef {
    fn data(&self) -> &[u8] {
        self.buffer.typed_data()
    }
}

impl From<CategoricalDataRef> for FeatureDataRef<'_> {
    fn from(data_ref: CategoricalDataRef) -> Self {
        Self::Categorical(data_ref)
    }
}

impl CategoricalDataRef {
    pub fn new(buffer: arrow::buffer::Buffer) -> Self {
        Self { buffer }
    }
}

#[derive(Clone, Debug)]
pub struct NullableCategoricalDataRef<'f> {
    buffer: arrow::buffer::Buffer,
    null_bitmap: &'f Option<arrow::bitmap::Bitmap>,
}

impl DataRef<u8> for NullableCategoricalDataRef<'_> {
    fn data(&self) -> &[u8] {
        self.buffer.typed_data()
    }
}

impl<'f> NullableDataRef for NullableCategoricalDataRef<'f> {
    fn nulls(&self) -> Vec<bool> {
        null_bitmap_to_bools(self.data(), self.null_bitmap)
    }
}

impl<'f> From<NullableCategoricalDataRef<'f>> for FeatureDataRef<'f> {
    fn from(data_ref: NullableCategoricalDataRef<'f>) -> Self {
        Self::NullableCategorical(data_ref)
    }
}

impl<'f> NullableCategoricalDataRef<'f> {
    pub fn new(
        buffer: arrow::buffer::Buffer,
        null_bitmap: &'f Option<arrow::bitmap::Bitmap>,
    ) -> Self {
        Self {
            buffer,
            null_bitmap,
        }
    }
}

/// A reference to text data
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
///     builder.append_value("foo");
///     builder.append_value("bar");
///     builder.finish()
/// };
///
/// assert_eq!(string_array.len(), 3);
///
/// let text_data_ref = TextDataRef::new(string_array.value_data(), string_array.value_offsets());
///
/// assert_eq!(text_data_ref.data().len(), 12);
/// assert_eq!(text_data_ref.offsets().len(), 4);
///
/// assert_eq!(text_data_ref.text_at(0), Ok("foobar"));
/// assert_eq!(text_data_ref.text_at(1), Ok("foo"));
/// assert_eq!(text_data_ref.text_at(2), Ok("bar"));
/// assert!(text_data_ref.text_at(3).is_err());
/// ```
///
#[derive(Clone, Debug)]
pub struct TextDataRef {
    data_buffer: arrow::buffer::Buffer,
    offsets_buffer: arrow::buffer::Buffer,
}

impl DataRef<u8> for TextDataRef {
    fn data(&self) -> &[u8] {
        self.data_buffer.data()
    }
}

impl From<TextDataRef> for FeatureDataRef<'_> {
    fn from(data_ref: TextDataRef) -> Self {
        Self::Text(data_ref)
    }
}

impl TextDataRef {
    pub fn new(data_buffer: arrow::buffer::Buffer, offsets_buffer: arrow::buffer::Buffer) -> Self {
        Self {
            data_buffer,
            offsets_buffer,
        }
    }

    pub fn offsets(&self) -> &[i32] {
        self.offsets_buffer.typed_data()
    }

    pub fn text_at(&self, pos: usize) -> Result<&str> {
        ensure!(
            pos < (self.offsets().len() - 1),
            error::FeatureData {
                details: "Position must be in data range"
            }
        );

        let start = self.offsets()[pos];
        let end = self.offsets()[pos + 1];

        let text = unsafe {
            byte_ptr_to_str(
                self.data_buffer.slice(start as usize).raw_data(),
                (end - start) as usize,
            )
        };

        Ok(text)
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
/// use geoengine_datatypes::primitives::{NullableTextDataRef, DataRef};
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
/// let text_data_ref = NullableTextDataRef::new(string_array.value_data(), string_array.value_offsets());
///
/// assert_eq!(text_data_ref.data().len(), 9);
/// assert_eq!(text_data_ref.offsets().len(), 4);
///
/// assert_eq!(text_data_ref.text_at(0), Ok(Some("foobar")));
/// assert_eq!(text_data_ref.text_at(1), Ok(None));
/// assert_eq!(text_data_ref.text_at(2), Ok(Some("bar")));
/// assert!(text_data_ref.text_at(3).is_err());
/// ```
///
#[derive(Clone, Debug)]
pub struct NullableTextDataRef {
    data_buffer: arrow::buffer::Buffer,
    offsets_buffer: arrow::buffer::Buffer,
}

impl DataRef<u8> for NullableTextDataRef {
    fn data(&self) -> &[u8] {
        self.data_buffer.data()
    }
}

impl From<NullableTextDataRef> for FeatureDataRef<'_> {
    fn from(data_ref: NullableTextDataRef) -> Self {
        Self::NullableText(data_ref)
    }
}

impl NullableTextDataRef {
    pub fn new(data_buffer: arrow::buffer::Buffer, offsets_buffer: arrow::buffer::Buffer) -> Self {
        Self {
            data_buffer,
            offsets_buffer,
        }
    }

    pub fn offsets(&self) -> &[i32] {
        self.offsets_buffer.typed_data()
    }

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

impl NullableDataRef for NullableTextDataRef {
    /// A null vector for text data
    ///
    /// # Examples
    ///
    /// ```rust
    /// use geoengine_datatypes::primitives::{NullableTextDataRef, DataRef, NullableDataRef};
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
    /// let text_data_ref = NullableTextDataRef::new(string_array.value_data(), string_array.value_offsets());
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
}

impl FeatureDataType {
    pub fn arrow_data_type(self) -> arrow::datatypes::DataType {
        match self {
            Self::Text | Self::NullableText => arrow::datatypes::DataType::Utf8,
            Self::Number | Self::NullableNumber => arrow::datatypes::DataType::Float64,
            Self::Decimal | Self::NullableDecimal => arrow::datatypes::DataType::Int64,
            Self::Categorical | Self::NullableCategorical => arrow::datatypes::DataType::UInt8,
        }
    }

    pub fn nullable(self) -> bool {
        match self {
            Self::Text | Self::Number | Self::Decimal | Self::Categorical => false,
            Self::NullableText
            | Self::NullableNumber
            | Self::NullableDecimal
            | Self::NullableCategorical => true,
        }
    }

    pub fn arrow_builder(self, len: usize) -> Box<dyn arrow::array::ArrayBuilder> {
        match self {
            Self::Text | Self::NullableText => Box::new(arrow::array::StringBuilder::new(len)),
            Self::Number | Self::NullableNumber => Box::new(arrow::array::Float64Builder::new(len)),
            Self::Decimal | Self::NullableDecimal => Box::new(arrow::array::Int64Builder::new(len)),
            Self::Categorical | Self::NullableCategorical => {
                Box::new(arrow::array::UInt8Builder::new(len))
            }
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

    pub fn arrow_builder(&self) -> Result<Box<dyn arrow::array::ArrayBuilder>> {
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
            FeatureData::Text(_) => Self::Text,
            FeatureData::NullableText(_) => Self::NullableText,
            FeatureData::Number(_) => Self::Number,
            FeatureData::NullableNumber(_) => Self::NullableNumber,
            FeatureData::Decimal(_) => Self::Decimal,
            FeatureData::NullableDecimal(_) => Self::NullableDecimal,
            FeatureData::Categorical(_) => Self::Categorical,
            FeatureData::NullableCategorical(_) => Self::NullableCategorical,
        }
    }
}

impl From<&FeatureDataValue> for FeatureDataType {
    fn from(value: &FeatureDataValue) -> Self {
        match value {
            FeatureDataValue::Text(_) => Self::Text,
            FeatureDataValue::NullableText(_) => Self::NullableText,
            FeatureDataValue::Number(_) => Self::Number,
            FeatureDataValue::NullableNumber(_) => Self::NullableNumber,
            FeatureDataValue::Decimal(_) => Self::Decimal,
            FeatureDataValue::NullableDecimal(_) => Self::NullableDecimal,
            FeatureDataValue::Categorical(_) => Self::Categorical,
            FeatureDataValue::NullableCategorical(_) => Self::NullableCategorical,
        }
    }
}

impl<'f> From<&'f FeatureDataRef<'f>> for FeatureDataType {
    fn from(value: &FeatureDataRef) -> Self {
        match value {
            FeatureDataRef::Text(_) => Self::Text,
            FeatureDataRef::NullableText(_) => Self::NullableText,
            FeatureDataRef::Number(..) => Self::Number,
            FeatureDataRef::NullableNumber(_) => Self::NullableNumber,
            FeatureDataRef::Decimal(_) => Self::Decimal,
            FeatureDataRef::NullableDecimal(_) => Self::NullableDecimal,
            FeatureDataRef::Categorical(_) => Self::Categorical,
            FeatureDataRef::NullableCategorical(_) => Self::NullableCategorical,
        }
    }
}
