use crate::error;
use crate::util::input::StringOrNumber;
use crate::util::Result;
use failure::_core::convert::TryFrom;
use failure::_core::ops::RangeInclusive;
use geoengine_datatypes::primitives::FeatureDataValue;
use num_traits::AsPrimitive;
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A type that allows use inputs to be either ranges of strings or numbers.
/// The range is inclusive.
/// TODO: generify for `RangeBounds`
#[derive(Debug, Clone, PartialEq)]
pub enum StringOrNumberRange {
    String(RangeInclusive<String>),
    Number(RangeInclusive<f64>),
    Decimal(RangeInclusive<i64>),
}

impl StringOrNumberRange {
    pub fn into_number_range(self) -> Result<Self> {
        RangeInclusive::<f64>::try_from(self).map(Into::into)
    }

    pub fn into_decimal_range(self) -> Result<Self> {
        RangeInclusive::<i64>::try_from(self).map(Into::into)
    }

    pub fn into_string_range(self) -> Result<Self> {
        RangeInclusive::<String>::try_from(self).map(Into::into)
    }
}

impl Serialize for StringOrNumberRange {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut tuple_serializer = serializer.serialize_tuple(2)?;

        match self {
            Self::String(range) => {
                tuple_serializer.serialize_element(range.start())?;
                tuple_serializer.serialize_element(range.end())?;
            }
            Self::Number(range) => {
                tuple_serializer.serialize_element(range.start())?;
                tuple_serializer.serialize_element(range.end())?;
            }
            Self::Decimal(range) => {
                tuple_serializer.serialize_element(range.start())?;
                tuple_serializer.serialize_element(range.end())?;
            }
        }

        tuple_serializer.end()
    }
}

impl<'de> Deserialize<'de> for StringOrNumberRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_tuple(2, StringOrNumberRangeDeserializer)
    }
}

struct StringOrNumberRangeDeserializer;
impl<'de> Visitor<'de> for StringOrNumberRangeDeserializer {
    type Value = StringOrNumberRange;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a 2-tuple of integers, floats or strings")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut elements: Vec<StringOrNumber> = Vec::with_capacity(seq.size_hint().unwrap_or(2));

        while let Some(element) = seq.next_element()? {
            elements.push(element);
        }

        if elements.len() != 2 {
            return Err(A::Error::invalid_length(elements.len(), &Self));
        }

        let mut elements = elements.into_iter();
        let (from, to) = (
            elements.next().expect("checked"),
            elements.next().expect("checked"),
        );

        Ok(match (from, to) {
            (StringOrNumber::String(v1), StringOrNumber::String(v2)) => {
                StringOrNumberRange::String(v1..=v2)
            }
            (StringOrNumber::Number(v1), StringOrNumber::Number(v2)) => {
                StringOrNumberRange::Number(v1..=v2)
            }
            (StringOrNumber::Decimal(v1), StringOrNumber::Decimal(v2)) => {
                StringOrNumberRange::Decimal(v1..=v2)
            }
            (StringOrNumber::Number(v1), StringOrNumber::Decimal(v2)) => {
                StringOrNumberRange::Number(v1..=v2.as_())
            }
            (StringOrNumber::Decimal(v1), StringOrNumber::Number(v2)) => {
                StringOrNumberRange::Number(v1.as_()..=v2)
            }
            _ => {
                return Err(A::Error::invalid_type(
                    serde::de::Unexpected::Other("mismatched type"),
                    &Self,
                ));
            }
        })
    }
}

impl From<RangeInclusive<f64>> for StringOrNumberRange {
    fn from(v: RangeInclusive<f64>) -> Self {
        StringOrNumberRange::Number(v)
    }
}

impl From<RangeInclusive<i64>> for StringOrNumberRange {
    fn from(v: RangeInclusive<i64>) -> Self {
        StringOrNumberRange::Decimal(v)
    }
}

impl From<RangeInclusive<String>> for StringOrNumberRange {
    fn from(v: RangeInclusive<String>) -> Self {
        StringOrNumberRange::String(v)
    }
}

impl From<RangeInclusive<&str>> for StringOrNumberRange {
    fn from(v: RangeInclusive<&str>) -> Self {
        StringOrNumberRange::from((*v.start()).to_string()..=(*v.end()).to_string())
    }
}

impl TryFrom<StringOrNumberRange> for RangeInclusive<f64> {
    type Error = error::Error;

    fn try_from(value: StringOrNumberRange) -> Result<Self, Self::Error> {
        match value {
            StringOrNumberRange::String(_) => Err(error::Error::InvalidType {
                expected: "number".to_string(),
                found: "string".to_string(),
            }),
            StringOrNumberRange::Number(v) => Ok(v),
            StringOrNumberRange::Decimal(v) => Ok(v.start().as_()..=v.end().as_()),
        }
    }
}

impl TryFrom<&StringOrNumberRange> for RangeInclusive<f64> {
    type Error = error::Error;

    fn try_from(value: &StringOrNumberRange) -> Result<Self, Self::Error> {
        Self::try_from(value.clone())
    }
}

impl TryFrom<StringOrNumberRange> for RangeInclusive<i64> {
    type Error = error::Error;

    fn try_from(value: StringOrNumberRange) -> Result<Self, Self::Error> {
        match value {
            StringOrNumberRange::String(_) => Err(error::Error::InvalidType {
                expected: "number".to_string(),
                found: "string".to_string(),
            }),
            StringOrNumberRange::Number(v) => Ok(v.start().as_()..=v.end().as_()),
            StringOrNumberRange::Decimal(v) => Ok(v),
        }
    }
}

impl TryFrom<&StringOrNumberRange> for RangeInclusive<i64> {
    type Error = error::Error;

    fn try_from(value: &StringOrNumberRange) -> Result<Self, Self::Error> {
        Self::try_from(value.clone())
    }
}

impl TryFrom<StringOrNumberRange> for RangeInclusive<String> {
    type Error = error::Error;

    fn try_from(value: StringOrNumberRange) -> Result<Self, Self::Error> {
        match value {
            StringOrNumberRange::String(v) => Ok(v),
            StringOrNumberRange::Number(_) | StringOrNumberRange::Decimal(_) => {
                Err(error::Error::InvalidType {
                    expected: "string".to_string(),
                    found: "number".to_string(),
                })
            }
        }
    }
}

impl TryFrom<&StringOrNumberRange> for RangeInclusive<String> {
    type Error = error::Error;

    fn try_from(value: &StringOrNumberRange) -> Result<Self, Self::Error> {
        Self::try_from(value.clone())
    }
}

impl From<StringOrNumberRange> for RangeInclusive<FeatureDataValue> {
    fn from(value: StringOrNumberRange) -> Self {
        match value {
            StringOrNumberRange::String(v) => {
                let (start, end) = v.into_inner();
                FeatureDataValue::Text(start)..=FeatureDataValue::Text(end)
            }
            StringOrNumberRange::Number(v) => {
                let (start, end) = v.into_inner();
                FeatureDataValue::Number(start)..=FeatureDataValue::Number(end)
            }
            StringOrNumberRange::Decimal(v) => {
                let (start, end) = v.into_inner();
                FeatureDataValue::Decimal(start)..=FeatureDataValue::Decimal(end)
            }
        }
    }
}

impl From<&StringOrNumberRange> for RangeInclusive<FeatureDataValue> {
    fn from(value: &StringOrNumberRange) -> Self {
        Self::from(value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize() {
        assert_eq!(
            serde_json::to_string(&StringOrNumberRange::String(
                "foo".to_string()..="bar".to_string()
            ))
            .unwrap(),
            "[\"foo\",\"bar\"]"
        );

        assert_eq!(
            serde_json::to_string(&StringOrNumberRange::Number(1337. ..=1338.)).unwrap(),
            "[1337.0,1338.0]"
        );

        assert_eq!(
            serde_json::to_string(&StringOrNumberRange::Decimal(42..=43)).unwrap(),
            "[42,43]"
        );
    }

    #[test]
    fn deserialize() {
        assert_eq!(
            serde_json::from_str::<StringOrNumberRange>("[\"foo\",\"bar\"]").unwrap(),
            StringOrNumberRange::String("foo".to_string()..="bar".to_string())
        );

        assert_eq!(
            serde_json::from_str::<StringOrNumberRange>("[1337.0,1338.0]").unwrap(),
            StringOrNumberRange::Number(1337. ..=1338.)
        );

        assert_eq!(
            serde_json::from_str::<StringOrNumberRange>("[42,43]").unwrap(),
            StringOrNumberRange::Decimal(42..=43)
        );

        assert_eq!(
            serde_json::from_str::<StringOrNumberRange>("[1337.0,1338]").unwrap(),
            StringOrNumberRange::Number(1337. ..=1338.)
        );

        assert_eq!(
            serde_json::from_str::<StringOrNumberRange>("[1337,1338.0]").unwrap(),
            StringOrNumberRange::Number(1337. ..=1338.)
        );

        assert!(serde_json::from_str::<StringOrNumberRange>("[\"foo\",42]").is_err());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn try_into() {
        assert_eq!(
            RangeInclusive::<String>::try_from(StringOrNumberRange::String(
                "foo".to_string()..="bar".to_string()
            ))
            .unwrap(),
            "foo".to_string()..="bar".to_string()
        );
        assert_eq!(
            RangeInclusive::<String>::try_from(&StringOrNumberRange::String(
                "foo".to_string()..="bar".to_string()
            ))
            .unwrap(),
            "foo".to_string()..="bar".to_string()
        );
        assert_eq!(
            RangeInclusive::<String>::try_from(StringOrNumberRange::from("foo"..="bar")).unwrap(),
            "foo".to_string()..="bar".to_string()
        );

        assert_eq!(
            RangeInclusive::<f64>::try_from(StringOrNumberRange::Number(1337. ..=1338.)).unwrap(),
            1337. ..=1338.
        );
        assert_eq!(
            RangeInclusive::<f64>::try_from(&StringOrNumberRange::Number(1337. ..=1338.)).unwrap(),
            1337. ..=1338.
        );

        assert_eq!(
            RangeInclusive::<f64>::try_from(StringOrNumberRange::Decimal(1337..=1338)).unwrap(),
            1337. ..=1338.
        );
        assert_eq!(
            RangeInclusive::<f64>::try_from(&StringOrNumberRange::Decimal(1337..=1338)).unwrap(),
            1337. ..=1338.
        );

        assert_eq!(
            RangeInclusive::<i64>::try_from(StringOrNumberRange::Decimal(42..=43)).unwrap(),
            42..=43
        );
        assert_eq!(
            RangeInclusive::<i64>::try_from(&StringOrNumberRange::Decimal(42..=43)).unwrap(),
            42..=43
        );

        assert_eq!(
            RangeInclusive::<i64>::try_from(StringOrNumberRange::Number(42. ..=43.)).unwrap(),
            42..=43
        );
        assert_eq!(
            RangeInclusive::<i64>::try_from(&StringOrNumberRange::Number(42. ..=43.)).unwrap(),
            42..=43
        );

        assert!(
            RangeInclusive::<String>::try_from(StringOrNumberRange::Number(1337. ..=1338.))
                .is_err()
        );
        assert!(RangeInclusive::<String>::try_from(StringOrNumberRange::Decimal(42..=43)).is_err());

        assert!(RangeInclusive::<i64>::try_from(StringOrNumberRange::String(
            "foo".to_string()..="bar".to_string()
        ))
        .is_err());
        assert!(RangeInclusive::<f64>::try_from(StringOrNumberRange::String(
            "foo".to_string()..="bar".to_string()
        ))
        .is_err());
    }
}
