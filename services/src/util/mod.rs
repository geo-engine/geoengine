pub use geoengine_datatypes::util::Identifier;
use serde::de::Error;
use serde::{Deserialize, Serialize};

pub mod config;
mod string_token;
pub mod user_input;

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct IdResponse<T: Identifier> {
    pub id: T,
}

impl<T> From<T> for IdResponse<T>
where
    T: Identifier,
{
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
    S::from_str(&s).map_err(|_error| D::Error::custom("could not parse string"))
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
        S::from_str(&s)
            .map(Some)
            .map_err(|_error| D::Error::custom("could not parse string"))
    }
}
