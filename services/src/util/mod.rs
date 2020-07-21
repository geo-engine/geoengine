use serde::de::Error;

#[macro_use]
pub mod identifiers;
pub mod user_input;

/// Serde deserializer <https://docs.rs/serde_qs/0.6.0/serde_qs/index.html#flatten-workaround>
pub fn from_str<'de, D, S>(deserializer: D) -> Result<S, D::Error>
where
    D: serde::Deserializer<'de>,
    S: std::str::FromStr,
{
    let s = <&str as serde::Deserialize>::deserialize(deserializer)?;
    S::from_str(&s).map_err(|_| D::Error::custom("could not parse string"))
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
            .map_err(|_| D::Error::custom("could not parse string"))
    }
}
