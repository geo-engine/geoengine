use serde::de::{self, Visitor};
use serde::Serializer;
use std::fmt;

/// Serialize and deserialize floats with special treatment of NaN
pub mod float {

    use super::*;

    pub(super) struct FloatOrStringVisitor;

    impl<'de> Visitor<'de> for FloatOrStringVisitor {
        type Value = f64;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a float value or NaN")
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_f64(v as f64)
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_f64(v as f64)
        }

        fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(v)
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if v.to_lowercase() == "nan" {
                Ok(f64::NAN)
            } else {
                Err(de::Error::invalid_value(de::Unexpected::Str(v), &self))
            }
        }
    }

    /// Parse no data from either number or `"nan"`
    pub fn deserialize<'de, D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(FloatOrStringVisitor)
    }

    /// write no data as either number or "nan"
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(x: &f64, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if x.is_nan() {
            s.serialize_str("nan")
        } else {
            s.serialize_f64(*x)
        }
    }
}

/// Serialize and deserialize float options with special treatment of NaN
pub mod float_option {
    use super::*;

    struct FloatOrStringOptionVisitor;

    impl<'de> Visitor<'de> for FloatOrStringOptionVisitor {
        type Value = Option<f64>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an optional float value or NaN")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer
                .deserialize_any(float::FloatOrStringVisitor)
                .map(Some)
        }
    }

    /// Parse no data from either number or `"nan"`
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_option(FloatOrStringOptionVisitor)
    }

    /// write no data as either number or "nan"
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(x: &Option<f64>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(x) = x {
            float::serialize(x, s)
        } else {
            s.serialize_none()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde::{Deserialize, Serialize};

    #[test]
    fn serde_f64() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Foo {
            #[serde(with = "float")]
            bar: f64,
        }

        // serialize

        assert_eq!(
            serde_json::to_value(Foo { bar: f64::NAN }).unwrap(),
            serde_json::json!({
                "bar": "nan",
            })
        );

        assert_eq!(
            serde_json::to_value(Foo { bar: 4.2 }).unwrap(),
            serde_json::json!({
                "bar": 4.2,
            })
        );

        // deserialize

        assert!(serde_json::from_value::<Foo>(serde_json::json!({
            "bar": "nan",
        }))
        .unwrap()
        .bar
        .is_nan());

        assert_eq!(
            serde_json::from_value::<Foo>(serde_json::json!({
                "bar": 4.2,
            }))
            .unwrap(),
            Foo { bar: 4.2 }
        );
    }

    #[test]
    fn serde_f64_option() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Foo {
            #[serde(with = "float_option")]
            bar: Option<f64>,
        }

        // serialize

        assert_eq!(
            serde_json::to_value(Foo {
                bar: Some(f64::NAN)
            })
            .unwrap(),
            serde_json::json!({
                "bar": "nan",
            })
        );

        assert_eq!(
            serde_json::to_value(Foo { bar: Some(4.2) }).unwrap(),
            serde_json::json!({
                "bar": 4.2,
            })
        );

        assert_eq!(
            serde_json::to_value(Foo { bar: None }).unwrap(),
            serde_json::json!({
                "bar": null,
            })
        );

        // deserialize

        assert!(serde_json::from_value::<Foo>(serde_json::json!({
            "bar": "nan",
        }))
        .unwrap()
        .bar
        .unwrap()
        .is_nan());

        assert_eq!(
            serde_json::from_value::<Foo>(serde_json::json!({
                "bar": 4.2,
            }))
            .unwrap(),
            Foo { bar: Some(4.2) }
        );

        assert_eq!(
            serde_json::from_value::<Foo>(serde_json::json!({
                "bar": null,
            }))
            .unwrap(),
            Foo { bar: None }
        );
    }
}
