use serde::de::Error;

/// Serde deserializer <https://docs.rs/serde_qs/0.6.0/serde_qs/index.html#flatten-workaround>
pub fn from_str<'de, D, S>(deserializer: D) -> Result<S, D::Error>
    where D: serde::Deserializer<'de>,
          S: std::str::FromStr
{
    let s = <&str as serde::Deserialize>::deserialize(deserializer)?;
    S::from_str(&s).map_err(|_| D::Error::custom("could not parse string"))
}
