use crate::error;
use failure::_core::convert::TryFrom;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A type that allows use inputs to be either strings or numbers
#[derive(Debug, Clone, PartialEq)]
pub enum StringOrNumber {
    String(String),
    Number(f64),
    Decimal(i64),
}

impl Serialize for StringOrNumber {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        match self {
            StringOrNumber::String(v) => serializer.serialize_str(v),
            StringOrNumber::Number(v) => serializer.serialize_f64(*v),
            StringOrNumber::Decimal(v) => serializer.serialize_i64(*v),
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
        Ok(StringOrNumber::Decimal(v))
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
        Ok(StringOrNumber::Number(v))
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

impl TryFrom<StringOrNumber> for f64 {
    type Error = error::Error;

    fn try_from(value: StringOrNumber) -> Result<f64, Self::Error> {
        match value {
            StringOrNumber::String(_) => Err(error::Error::InvalidType {
                expected: "number".to_string(),
                found: "string".to_string(),
            }),
            StringOrNumber::Number(v) => Ok(v),
            StringOrNumber::Decimal(v) => Ok(v as f64),
        }
    }
}

impl TryFrom<StringOrNumber> for i64 {
    type Error = error::Error;

    fn try_from(value: StringOrNumber) -> Result<i64, Self::Error> {
        match value {
            StringOrNumber::String(_) => Err(error::Error::InvalidType {
                expected: "number".to_string(),
                found: "string".to_string(),
            }),
            StringOrNumber::Number(v) => Ok(v as i64),
            StringOrNumber::Decimal(v) => Ok(v),
        }
    }
}

impl TryFrom<StringOrNumber> for String {
    type Error = error::Error;

    fn try_from(value: StringOrNumber) -> Result<String, Self::Error> {
        match value {
            StringOrNumber::String(v) => Ok(v),
            StringOrNumber::Number(_) | StringOrNumber::Decimal(_) => {
                Err(error::Error::InvalidType {
                    expected: "string".to_string(),
                    found: "number".to_string(),
                })
            }
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
            serde_json::to_string(&StringOrNumber::Number(1337.)).unwrap(),
            "1337.0"
        );

        assert_eq!(
            serde_json::to_string(&StringOrNumber::Decimal(42)).unwrap(),
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
            StringOrNumber::Number(1337.)
        );

        assert_eq!(
            serde_json::from_str::<StringOrNumber>("42").unwrap(),
            StringOrNumber::Decimal(42)
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn try_into() {
        assert_eq!(
            String::try_from(StringOrNumber::String("foobar".to_string())).unwrap(),
            "foobar".to_string()
        );

        assert_eq!(f64::try_from(StringOrNumber::Number(1337.)).unwrap(), 1337.);

        assert_eq!(i64::try_from(StringOrNumber::Number(1337.)).unwrap(), 1337);

        assert_eq!(i64::try_from(StringOrNumber::Decimal(42)).unwrap(), 42);

        assert_eq!(f64::try_from(StringOrNumber::Decimal(42)).unwrap(), 42.);

        assert!(String::try_from(StringOrNumber::Number(1337.)).is_err());
        assert!(String::try_from(StringOrNumber::Decimal(42)).is_err());

        assert!(i64::try_from(StringOrNumber::String("foobar".to_string())).is_err());
        assert!(f64::try_from(StringOrNumber::String("foobar".to_string())).is_err());
    }
}
