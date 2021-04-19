use std::convert::TryFrom;

use crate::error;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A type that allows use inputs to be either strings or numbers
#[derive(Debug, Clone, PartialEq)]
pub enum StringOrNumber {
    String(String),
    Float(f64),
    Int(i64),
}

impl Serialize for StringOrNumber {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        match self {
            StringOrNumber::String(v) => serializer.serialize_str(v),
            StringOrNumber::Float(v) => serializer.serialize_f64(*v),
            StringOrNumber::Int(v) => serializer.serialize_i64(*v),
        }
    }
}

impl<'de> Deserialize<'de> for StringOrNumber {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StringOrNumberDeserializer)
    }
}

struct StringOrNumberDeserializer;
impl<'de> Visitor<'de> for StringOrNumberDeserializer {
    type Value = StringOrNumber;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an integer, float or string")
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(StringOrNumber::Int(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_i64(v as i64)
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(StringOrNumber::Float(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_string(v.to_string())
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(StringOrNumber::String(v))
    }
}

impl From<f64> for StringOrNumber {
    fn from(v: f64) -> Self {
        StringOrNumber::Float(v)
    }
}

impl From<i64> for StringOrNumber {
    fn from(v: i64) -> Self {
        StringOrNumber::Int(v)
    }
}

impl From<String> for StringOrNumber {
    fn from(v: String) -> Self {
        StringOrNumber::String(v)
    }
}

impl From<&str> for StringOrNumber {
    fn from(v: &str) -> Self {
        StringOrNumber::from(v.to_string())
    }
}

impl TryFrom<StringOrNumber> for f64 {
    type Error = error::Error;

    fn try_from(value: StringOrNumber) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl TryFrom<&StringOrNumber> for f64 {
    type Error = error::Error;

    fn try_from(value: &StringOrNumber) -> Result<Self, Self::Error> {
        match value {
            StringOrNumber::String(_) => Err(error::Error::InvalidType {
                expected: "number".to_string(),
                found: "string".to_string(),
            }),
            StringOrNumber::Float(v) => Ok(*v),
            StringOrNumber::Int(v) => Ok(*v as f64),
        }
    }
}

impl TryFrom<StringOrNumber> for i64 {
    type Error = error::Error;

    fn try_from(value: StringOrNumber) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl TryFrom<&StringOrNumber> for i64 {
    type Error = error::Error;

    fn try_from(value: &StringOrNumber) -> Result<Self, Self::Error> {
        match value {
            StringOrNumber::String(_) => Err(error::Error::InvalidType {
                expected: "number".to_string(),
                found: "string".to_string(),
            }),
            StringOrNumber::Float(v) => Ok(*v as i64),
            StringOrNumber::Int(v) => Ok(*v),
        }
    }
}

impl TryFrom<StringOrNumber> for String {
    type Error = error::Error;

    fn try_from(value: StringOrNumber) -> Result<Self, Self::Error> {
        match value {
            StringOrNumber::String(v) => Ok(v),
            StringOrNumber::Float(_) | StringOrNumber::Int(_) => Err(error::Error::InvalidType {
                expected: "string".to_string(),
                found: "number".to_string(),
            }),
        }
    }
}

impl TryFrom<&StringOrNumber> for String {
    type Error = error::Error;

    fn try_from(value: &StringOrNumber) -> Result<Self, Self::Error> {
        Self::try_from(value.clone())
    }
}

impl<'v> TryFrom<&'v StringOrNumber> for &'v str {
    type Error = error::Error;

    fn try_from(value: &'v StringOrNumber) -> Result<Self, Self::Error> {
        match value {
            StringOrNumber::String(v) => Ok(v),
            StringOrNumber::Float(_) | StringOrNumber::Int(_) => Err(error::Error::InvalidType {
                expected: "string".to_string(),
                found: "number".to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;

    #[test]
    fn serialize() {
        assert_eq!(
            serde_json::to_string(&StringOrNumber::String("foobar".to_string())).unwrap(),
            "\"foobar\""
        );

        assert_eq!(
            serde_json::to_string(&StringOrNumber::Float(1337.)).unwrap(),
            "1337.0"
        );

        assert_eq!(
            serde_json::to_string(&StringOrNumber::Int(42)).unwrap(),
            "42"
        );
    }

    #[test]
    fn deserialize() {
        assert_eq!(
            serde_json::from_str::<StringOrNumber>("\"foobar\"").unwrap(),
            StringOrNumber::String("foobar".to_string())
        );

        assert_eq!(
            serde_json::from_str::<StringOrNumber>("1337.0").unwrap(),
            StringOrNumber::Float(1337.)
        );

        assert_eq!(
            serde_json::from_str::<StringOrNumber>("42").unwrap(),
            StringOrNumber::Int(42)
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn try_into() {
        assert_eq!(
            String::try_from(StringOrNumber::String("foobar".to_string())).unwrap(),
            "foobar".to_string()
        );
        assert_eq!(
            String::try_from(&StringOrNumber::String("foobar".to_string())).unwrap(),
            "foobar"
        );

        assert_eq!(f64::try_from(StringOrNumber::Float(1337.)).unwrap(), 1337.);
        assert_eq!(f64::try_from(&StringOrNumber::Float(1337.)).unwrap(), 1337.);

        assert_eq!(i64::try_from(StringOrNumber::Float(1337.)).unwrap(), 1337);
        assert_eq!(i64::try_from(&StringOrNumber::Float(1337.)).unwrap(), 1337);

        assert_eq!(i64::try_from(StringOrNumber::Int(42)).unwrap(), 42);

        assert_eq!(f64::try_from(StringOrNumber::Int(42)).unwrap(), 42.);

        assert!(String::try_from(StringOrNumber::Float(1337.)).is_err());
        assert!(String::try_from(StringOrNumber::Int(42)).is_err());

        assert!(i64::try_from(StringOrNumber::String("foobar".to_string())).is_err());
        assert!(f64::try_from(StringOrNumber::String("foobar".to_string())).is_err());
    }
}
