use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use utoipa::ToSchema;

pub use geoengine_datatypes::util::Identifier;
pub use geoengine_operators::util::{spawn, spawn_blocking, spawn_blocking_with_thread_pool};

pub mod config;
pub mod identifiers;
pub mod operators;
pub mod parsing;
pub mod retry;
pub mod server;
pub mod tests;
pub mod user_input;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, ToSchema)]
pub struct IdResponse<T> {
    #[schema(value_type = String)]
    pub id: T,
}

impl<T> From<T> for IdResponse<T> {
    fn from(id: T) -> Self {
        Self { id }
    }
}

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
                D::Error::custom(format_args!("could not parse string as boolean: {}", s))
            })
    }
}

/// Canonicalize `base`/`sub_path` and ensure the `sub_path` doesn't escape the `base`
/// returns an error if the `sub_path` escapes the `base`
///
/// This only works if the `Path` you are referring to actually exists.
///
pub fn canonicalize_subpath(base: &Path, sub_path: &Path) -> crate::error::Result<PathBuf> {
    let base = base.canonicalize()?;
    let path = base.join(sub_path).canonicalize()?;

    if path.starts_with(&base) {
        Ok(path)
    } else {
        Err(crate::error::Error::SubPathMustNotEscapeBasePath {
            base,
            sub_path: sub_path.into(),
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

    if !path.starts_with(&base) {
        return Err(crate::error::Error::SubPathMustNotEscapeBasePath {
            base: base.into(),
            sub_path: sub_path.into(),
        });
    }

    Ok(path)
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
    fn it_doesnt_escape_base_path() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path();
        std::fs::create_dir_all(tmp_path.join("foo/bar/foobar")).unwrap();
        std::fs::create_dir_all(tmp_path.join("foo/barfoo")).unwrap();

        assert_eq!(
            canonicalize_subpath(&tmp_path.join("foo/bar"), Path::new("foobar"))
                .unwrap()
                .to_string_lossy(),
            tmp_path.join("foo/bar/foobar").to_string_lossy()
        );

        assert!(canonicalize_subpath(&tmp_path.join("foo/bar"), Path::new("../barfoo")).is_err());
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
}
