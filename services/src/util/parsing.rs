use geoengine_datatypes::primitives::SpatialResolution;
use serde::de;
use serde::de::Error;
use serde::Deserialize;
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
    let mut url_string = if let Some(url_string) = Option::<String>::deserialize(deserializer)? {
        url_string
    } else {
        return Ok(None);
    };

    if !url_string.ends_with('/') {
        url_string.push('/');
    }

    Url::parse(&url_string)
        .map(Option::Some)
        .map_err(D::Error::custom)
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
                f.write_str(&self.base_url.to_string())
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
}
