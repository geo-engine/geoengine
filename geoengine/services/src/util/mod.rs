use crate::error::{Error, Result};
use bytes::BytesMut;
use postgres_types::{FromSql, ToSql, to_sql_checked};
use serde::{Deserialize, Serialize, Serializer};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use url::Url;
use utoipa::ToSchema;

pub use geoengine_datatypes::util::Identifier;
pub use geoengine_operators::util::{spawn, spawn_blocking, spawn_blocking_with_thread_pool};

pub mod apidoc;
pub mod encryption;
pub mod extractors;
pub mod identifiers;
pub mod middleware;
pub mod oidc;
#[cfg(test)]
pub mod openapi_examples;
pub mod openapi_visitor;
pub mod openapi_visitors;
pub mod operators;
pub mod parsing;
pub mod postgres;
pub mod sentinel_2_utm_zones;
pub mod server;
// TODO: refactor to be gated by `#[cfg(test)]`
pub mod tests;
#[cfg(test)]
pub mod websocket_tests;
pub mod workflows;

/// Serde deserializer <https://docs.rs/serde_qs/0.6.0/serde_qs/index.html#flatten-workaround>
pub fn from_str<'de, D, S>(deserializer: D) -> Result<S, D::Error>
where
    D: serde::Deserializer<'de>,
    S: std::str::FromStr,
{
    use serde::de::Error;

    let s = <&str as serde::Deserialize>::deserialize(deserializer)?;
    S::from_str(s).map_err(|_error| D::Error::custom("could not parse string"))
}

/// Serde deserializer <https://docs.rs/serde_qs/0.6.0/serde_qs/index.html#flatten-workaround>
pub fn from_str_option<'de, D, S>(deserializer: D) -> Result<Option<S>, D::Error>
where
    D: serde::Deserializer<'de>,
    S: std::str::FromStr,
{
    use serde::de::Error;

    let s = <&str as serde::Deserialize>::deserialize(deserializer)?;
    if s.is_empty() {
        Ok(None)
    } else {
        S::from_str(s)
            .map(Some)
            .map_err(|_error| D::Error::custom("could not parse string"))
    }
}

/// Serde deserializer for booleans with case insensitive strings
pub fn bool_option_case_insensitive<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let s = <&str as serde::Deserialize>::deserialize(deserializer)?;
    if s.is_empty() {
        Ok(None)
    } else {
        bool::from_str(&s.to_lowercase())
            .map(Some)
            .map_err(|_error| {
                D::Error::custom(format_args!("could not parse string as boolean: {s}"))
            })
    }
}

/// Join `base` and `sub_path` and ensure the `sub_path` doesn't escape the `base`
/// returns an error if the `sub_path` escapes the `base`
///
/// If the resulting path must exist, use [`canonicalize_subpath`] instead.
///
pub fn path_with_base_path(base: &Path, sub_path: &Path) -> Result<PathBuf> {
    for component in base.components() {
        if let std::path::Component::CurDir | std::path::Component::ParentDir = component {
            return Err(Error::PathMustNotContainParentReferences {
                base: base.into(),
                sub_path: sub_path.into(),
            });
        }
    }
    for component in sub_path.components() {
        if let std::path::Component::CurDir
        | std::path::Component::ParentDir
        | std::path::Component::RootDir
        | std::path::Component::Prefix(_) = component
        {
            return Err(Error::PathMustNotContainParentReferences {
                base: base.into(),
                sub_path: sub_path.into(),
            });
        }
    }

    let path = base.join(sub_path);

    if !path.starts_with(base) {
        return Err(crate::error::Error::SubPathMustNotEscapeBasePath {
            base: base.into(),
            sub_path: sub_path.into(),
        });
    }

    Ok(path)
}

pub fn join_base_url_and_path(base_url: &Url, path: &str) -> Result<Url, url::ParseError> {
    let mut url = base_url.to_string();

    if !url.ends_with('/') {
        url.push('/');
    }

    if let Some(stripped) = path.strip_prefix('/') {
        url.push_str(stripped);
    } else {
        url.push_str(path);
    }

    url.parse()
}

/// A wrapper type that serializes to "*****" and can be deserialized from any string.
/// If the inner value is "*****", it is considered unknown and `as_option` returns `None`.
/// This is useful for secrets that should not be exposed in API responses, but can be set in API requests.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(transparent)]
#[schema(value_type = String)]
pub struct Secret<T>(pub T);

const SECRET_STR: &str = "*****";

impl<T> Secret<T>
where
    T: AsRef<str>,
{
    pub fn new(inner: T) -> Self {
        Self(inner)
    }

    pub fn is_unknown(&self) -> bool {
        self.0.as_ref() == SECRET_STR
    }

    pub fn as_option(&self) -> Option<&T> {
        if self.is_unknown() {
            return None;
        }
        Some(&self.0)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> Serialize for Secret<T>
where
    T: AsRef<str>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(SECRET_STR)
    }
}

impl<T> Deref for Secret<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> AsRef<T> for Secret<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T: PartialEq> PartialEq for Secret<T> {
    fn eq(&self, other: &Secret<T>) -> bool {
        self.0 == other.0
    }
}

impl<T> std::fmt::Display for Secret<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{SECRET_STR}")
    }
}

impl<'t, T> FromSql<'t> for Secret<T>
where
    T: FromSql<'t>,
{
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'t [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let inner = <T as FromSql>::from_sql(ty, raw)?;
        Ok(Self(inner))
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <T as FromSql>::accepts(ty)
    }
}

impl<T> ToSql for Secret<T>
where
    T: ToSql,
{
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        <T as ToSql>::to_sql(&self.0, ty, out)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <T as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

#[cfg(test)]
mod mod_tests {
    use super::*;

    #[test]
    fn bool() {
        assert!(
            bool_option_case_insensitive(&mut serde_json::Deserializer::from_str("\"\""))
                .unwrap()
                .is_none()
        );
        assert!(
            bool_option_case_insensitive(&mut serde_json::Deserializer::from_str("\"true\""))
                .unwrap()
                .unwrap()
        );
        assert!(
            !bool_option_case_insensitive(&mut serde_json::Deserializer::from_str("\"false\""))
                .unwrap()
                .unwrap()
        );
        assert!(
            bool_option_case_insensitive(&mut serde_json::Deserializer::from_str("\"TRUE\""))
                .unwrap()
                .unwrap()
        );
        assert!(
            !bool_option_case_insensitive(&mut serde_json::Deserializer::from_str("\"False\""))
                .unwrap()
                .unwrap()
        );
    }

    #[test]
    fn it_doesnt_escape_base_path_too() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path();

        assert_eq!(
            path_with_base_path(&tmp_path.join("foo/bar"), Path::new("foobar"))
                .unwrap()
                .to_string_lossy(),
            tmp_path.join("foo/bar/foobar").to_string_lossy()
        );

        assert!(path_with_base_path(&tmp_path.join("foo/bar"), Path::new("../barfoo")).is_err());
    }

    #[test]
    fn it_joins_url_base_with_path() {
        assert_eq!(
            join_base_url_and_path(&Url::parse("https://example.com/foo").unwrap(), "bar/baz")
                .unwrap()
                .to_string(),
            "https://example.com/foo/bar/baz"
        );

        assert_eq!(
            join_base_url_and_path(&Url::parse("https://example.com/foo/").unwrap(), "bar/baz")
                .unwrap()
                .to_string(),
            "https://example.com/foo/bar/baz"
        );
    }
}
