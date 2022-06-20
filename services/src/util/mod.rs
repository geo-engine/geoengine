use serde::de::Error;
use serde::{Deserialize, Serialize};

use std::str::FromStr;

pub use geoengine_datatypes::util::Identifier;
pub use geoengine_operators::util::{spawn, spawn_blocking, spawn_blocking_with_thread_pool};

pub mod config;
pub mod parsing;
pub mod retry;
pub mod tests;
pub mod user_input;

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct IdResponse<T> {
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
    let s = <&str as serde::Deserialize>::deserialize(deserializer)?;
    S::from_str(s).map_err(|_error| D::Error::custom("could not parse string"))
}

/// Serde deserializer <https://docs.rs/serde_qs/0.6.0/serde_qs/index.html#flatten-workaround>
pub fn from_str_option<'de, D, S>(deserializer: D) -> Result<Option<S>, D::Error>
where
    D: serde::Deserializer<'de>,
    S: std::str::FromStr,
{
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
}
