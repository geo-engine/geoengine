use geoengine_datatypes::primitives::SpatialResolution;
use serde::de;
use serde::de::Error;
use serde::Deserialize;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

/// Parse the `SpatialResolution` of a request by parsing `x,y`, e.g. `0.1,0.1`.
pub fn parse_spatial_resolution<'de, D>(deserializer: D) -> Result<SpatialResolution, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    let split: Result<Vec<f64>, <f64 as FromStr>::Err> = s.split(',').map(f64::from_str).collect();

    match split.as_ref().map(Vec::as_slice) {
        Ok(&[x, y]) => SpatialResolution::new(x, y).map_err(D::Error::custom),
        Err(error) => Err(D::Error::custom(error)),
        Ok(..) => Err(D::Error::custom("Invalid spatial resolution")),
    }
}

/// Parse a field as a string or array of strings. Always returns a `Vec<String>`.
pub fn string_or_string_array<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct StringOrVec(PhantomData<Vec<String>>);

    impl<'de> de::Visitor<'de> for StringOrVec {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or array of strings")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(vec![value.to_owned()])
        }

        fn visit_seq<S>(self, visitor: S) -> Result<Self::Value, S::Error>
        where
            S: de::SeqAccess<'de>,
        {
            Deserialize::deserialize(de::value::SeqAccessDeserializer::new(visitor))
        }
    }

    deserializer.deserialize_any(StringOrVec(PhantomData))
}
