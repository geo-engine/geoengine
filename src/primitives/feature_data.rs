use crate::util::Result;
use arrow;

pub enum FeatureData {
    Text(Vec<String>),
    NullableText(Vec<Option<String>>),
    Number(Vec<f64>),
    NullableNumber(Vec<Option<f64>>),
    Decimal(Vec<i64>),
    NullableDecimal(Vec<Option<i64>>),
    Categorical(Vec<u8>),
    NullableCategorical(Vec<Option<u8>>),
}

impl FeatureData {
    pub fn arrow_data_type(&self) -> arrow::datatypes::DataType {
        match self {
            Self::Text(..) | Self::NullableText(..) => arrow::datatypes::DataType::Utf8,
            Self::Number(..) | Self::NullableNumber(..) => arrow::datatypes::DataType::Float64,
            Self::Decimal(..) | Self::NullableDecimal(..) => arrow::datatypes::DataType::Int64,
            Self::Categorical(..) | Self::NullableCategorical(..) => {
                arrow::datatypes::DataType::UInt8
            }
        }
    }

    pub fn nullable(&self) -> bool {
        match self {
            Self::Text(..) | Self::Number(..) | Self::Decimal(..) | Self::Categorical(..) => false,
            Self::NullableText(..)
            | Self::NullableNumber(..)
            | Self::NullableDecimal(..)
            | Self::NullableCategorical(..) => true,
        }
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
