use crate::api::model::datatypes::BandSelection;
use geoengine_datatypes::primitives::{Coordinate2D, SpatialPartition2D, SpatialResolution};
use serde::Deserialize;
use serde::de;
use serde::de::Error;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;
use url::Url;

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

/// Parse spatial partition, format is: "x1,y1,x2,y2"
pub fn parse_spatial_partition<'de, D>(deserializer: D) -> Result<SpatialPartition2D, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    let split: Vec<Result<f64, std::num::ParseFloatError>> =
        s.splitn(4, ',').map(str::parse).collect();

    if let [Ok(x1), Ok(y1), Ok(x2), Ok(y2)] = *split.as_slice() {
        SpatialPartition2D::new(Coordinate2D::new(x1, y2), Coordinate2D::new(x2, y1))
            .map_err(D::Error::custom)
    } else {
        Err(D::Error::custom("Invalid spatial partition"))
    }
}

/// Parse a list of elements that implement `FromStr` that is separated by commas.
pub fn parse_list_from_str<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    let s = String::deserialize(deserializer)?;

    let split: Result<Vec<T>, <T as FromStr>::Err> = s.split(',').map(T::from_str).collect();

    split.map_err(D::Error::custom)
}

pub fn parse_band_selection<'de, D>(deserializer: D) -> Result<BandSelection, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let bands: Vec<usize> = parse_list_from_str(deserializer)?;

    Ok(BandSelection(bands))
}

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

/// Deserialize a base URL by enforcing a trailing slash and then parsing it as a URL.
pub fn deserialize_base_url<'de, D>(deserializer: D) -> Result<Url, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let mut url_string = String::deserialize(deserializer)?;

    if !url_string.ends_with('/') {
        url_string.push('/');
    }

    Url::parse(&url_string).map_err(D::Error::custom)
}

/// Deserialize an optional base URL by enforcing a trailing slash and then parsing it as a URL.
pub fn deserialize_base_url_option<'de, D>(deserializer: D) -> Result<Option<Url>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(mut url_string) = Option::<String>::deserialize(deserializer)? else {
        return Ok(None);
    };

    if !url_string.ends_with('/') {
        url_string.push('/');
    }

    Url::parse(&url_string)
        .map(Option::Some)
        .map_err(D::Error::custom)
}

/// Deserialize an API prefix that is prepended to all services.
/// Must only consist of characters nad numbers, underscores and dashes, and slashes.
/// If it does not start with a slash, it is added.
/// If it ends with a slash, it is removed.
pub fn deserialize_api_prefix<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let mut api_prefix = String::deserialize(deserializer)?;

    if !api_prefix.starts_with('/') {
        api_prefix.insert(0, '/');
    }

    let mut prev_c = None;
    for c in api_prefix.chars() {
        if !c.is_ascii_alphanumeric() && c != '_' && c != '-' && c != '/' {
            return Err(D::Error::custom(format!(
                "Invalid character '{c}' in API prefix"
            )));
        }

        if let Some(prev_c) = prev_c {
            if prev_c == '/' && c == '/' {
                return Err(D::Error::custom(
                    "API prefix must not contain consecutive slashes",
                ));
            }

            if prev_c == '-' && c == '-' {
                return Err(D::Error::custom(
                    "API prefix must not contain consecutive dashes",
                ));
            }

            if prev_c == '_' && c == '_' {
                return Err(D::Error::custom(
                    "API prefix must not contain consecutive underscores",
                ));
            }
        }

        prev_c = Some(c);
    }

    if api_prefix.ends_with('/') {
        api_prefix.pop();
    }

    Ok(api_prefix)
}

#[cfg(test)]
mod tests {
    use std::fmt::Display;

    use super::*;

    #[test]
    fn test_deserialize_base_url() {
        #[derive(Deserialize)]
        struct Test {
            #[serde(deserialize_with = "deserialize_base_url")]
            base_url: Url,
        }

        impl Display for Test {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.base_url.as_ref())
            }
        }

        assert_eq!(
            serde_json::from_str::<Test>(r#"{"base_url": "https://www.endpoint.de/"}"#)
                .unwrap()
                .to_string(),
            "https://www.endpoint.de/"
        );
        assert_eq!(
            serde_json::from_str::<Test>(r#"{"base_url": "https://www.endpoint.de"}"#)
                .unwrap()
                .to_string(),
            "https://www.endpoint.de/"
        );
        assert!(serde_json::from_str::<Test>(r#"{"base_url": "foo"}"#).is_err());
    }

    #[test]
    fn test_deserialize_base_url_option() {
        #[derive(Deserialize)]
        struct Test {
            #[serde(deserialize_with = "deserialize_base_url_option")]
            base_url: Option<Url>,
        }

        impl Display for Test {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match &self.base_url {
                    Some(base_url) => f.write_str(base_url.as_ref()),
                    None => f.write_str(""),
                }
            }
        }

        assert_eq!(
            serde_json::from_str::<Test>(r#"{"base_url": "https://www.endpoint.de/"}"#)
                .unwrap()
                .to_string(),
            "https://www.endpoint.de/"
        );
        assert_eq!(
            serde_json::from_str::<Test>(r#"{"base_url": "https://www.endpoint.de"}"#)
                .unwrap()
                .to_string(),
            "https://www.endpoint.de/"
        );
        assert!(serde_json::from_str::<Test>(r#"{"base_url": "foo"}"#).is_err());

        assert_eq!(
            serde_json::from_str::<Test>(r#"{"base_url": null}"#)
                .unwrap()
                .to_string(),
            ""
        );
    }

    #[test]
    fn test_deserialize_api_prefix() {
        #[derive(Deserialize)]
        struct ApiPrefix(#[serde(deserialize_with = "deserialize_api_prefix")] String);

        assert_eq!(
            serde_json::from_str::<ApiPrefix>(r#""/api/""#).unwrap().0,
            "/api"
        );

        assert_eq!(
            serde_json::from_str::<ApiPrefix>(r#""/api""#).unwrap().0,
            "/api"
        );

        assert_eq!(serde_json::from_str::<ApiPrefix>(r#""/""#).unwrap().0, "");

        assert_eq!(serde_json::from_str::<ApiPrefix>(r#""""#).unwrap().0, "");

        assert_eq!(
            serde_json::from_str::<ApiPrefix>(r#""/a/b/c_d/e-f/""#)
                .unwrap()
                .0,
            "/a/b/c_d/e-f"
        );

        assert!(serde_json::from_str::<ApiPrefix>(r#""//""#).is_err());
        assert!(serde_json::from_str::<ApiPrefix>(r#""hello?""#).is_err());
        assert!(serde_json::from_str::<ApiPrefix>(r#""foo=bar""#).is_err());
    }

    #[test]
    fn test_parse_spatial_partition() {
        #[derive(Deserialize)]
        struct MySpatialPartition {
            #[serde(deserialize_with = "parse_spatial_partition")]
            spatial_partition: SpatialPartition2D,
        }

        assert_eq!(
            serde_json::from_str::<MySpatialPartition>(
                r#"{"spatial_partition": "-180,-90,180,90"}"#
            )
            .unwrap()
            .spatial_partition,
            SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap(),
        );
    }
}
