use crate::error;
use serde::de::Visitor;
use serde::export::Formatter;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use snafu::ResultExt;
use std::str::FromStr;

/// A spatial reference authority that is part of a spatial reference definition
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum SpatialReferenceAuthority {
    Epsg,
    SrOrg,
    Iau2000,
    Esri,
}

impl std::fmt::Display for SpatialReferenceAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                SpatialReferenceAuthority::Epsg => "EPSG",
                SpatialReferenceAuthority::SrOrg => "SR-ORG",
                SpatialReferenceAuthority::Iau2000 => "IAU2000",
                SpatialReferenceAuthority::Esri => "ESRI",
            }
        )
    }
}

/// A spatial reference consists of an authority and a code
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct SpatialReference {
    authority: SpatialReferenceAuthority,
    code: u32,
}

impl SpatialReference {
    pub fn new(authority: SpatialReferenceAuthority, code: u32) -> Self {
        Self { authority, code }
    }

    /// the WGS 84 spatial reference system
    pub fn wgs84() -> Self {
        Self::new(SpatialReferenceAuthority::Epsg, 4326)
    }
}

impl std::fmt::Display for SpatialReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.authority, self.code)
    }
}

impl Serialize for SpatialReference {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// Helper struct for deserializing a `SpatialReferencce`
struct SpatialReferenceDeserializeVisitor;

impl<'de> Visitor<'de> for SpatialReferenceDeserializeVisitor {
    type Value = SpatialReference;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a spatial reference in the form authority:code")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        v.parse().map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for SpatialReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(SpatialReferenceDeserializeVisitor)
    }
}

impl FromStr for SpatialReferenceAuthority {
    type Err = error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "EPSG" => SpatialReferenceAuthority::Epsg,
            "SR-ORG" => SpatialReferenceAuthority::SrOrg,
            "IAU2000" => SpatialReferenceAuthority::Iau2000,
            "ESRI" => SpatialReferenceAuthority::Esri,
            _ => {
                return Err(error::Error::InvalidSpatialReferenceString {
                    spatial_reference_string: s.into(),
                })
            }
        })
    }
}

impl FromStr for SpatialReference {
    type Err = error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split = s.split(':');

        match (split.next(), split.next(), split.next()) {
            (Some(authority), Some(code), None) => Ok(Self::new(
                authority.parse()?,
                code.parse::<u32>().context(error::ParseU32)?,
            )),
            _ => Err(error::Error::InvalidSpatialReferenceString {
                spatial_reference_string: s.into(),
            }),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SpatialReferenceOption {
    SpatialReference(SpatialReference),
    Unreferenced,
}

impl std::fmt::Display for SpatialReferenceOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpatialReferenceOption::SpatialReference(p) => write!(f, "{}", p),
            SpatialReferenceOption::Unreferenced => Ok(()),
        }
    }
}

impl Into<SpatialReferenceOption> for SpatialReference {
    fn into(self) -> SpatialReferenceOption {
        SpatialReferenceOption::SpatialReference(self)
    }
}

impl From<Option<SpatialReference>> for SpatialReferenceOption {
    fn from(option: Option<SpatialReference>) -> Self {
        match option {
            Some(p) => SpatialReferenceOption::SpatialReference(p),
            None => SpatialReferenceOption::Unreferenced,
        }
    }
}

impl Serialize for SpatialReferenceOption {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// Helper struct for deserializing a `SpatialReferenceOption`
struct SpatialReferenceOptionDeserializeVisitor;

impl<'de> Visitor<'de> for SpatialReferenceOptionDeserializeVisitor {
    type Value = SpatialReferenceOption;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a spatial reference in the form authority:code")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.is_empty() {
            return Ok(SpatialReferenceOption::Unreferenced);
        }

        let spatial_reference: SpatialReference = v.parse().map_err(serde::de::Error::custom)?;

        Ok(spatial_reference.into())
    }
}

impl<'de> Deserialize<'de> for SpatialReferenceOption {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(SpatialReferenceOptionDeserializeVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display() {
        assert_eq!(SpatialReferenceAuthority::Epsg.to_string(), "EPSG");
        assert_eq!(SpatialReferenceAuthority::SrOrg.to_string(), "SR-ORG");
        assert_eq!(SpatialReferenceAuthority::Iau2000.to_string(), "IAU2000");
        assert_eq!(SpatialReferenceAuthority::Esri.to_string(), "ESRI");

        assert_eq!(
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 4326).to_string(),
            "EPSG:4326"
        );
        assert_eq!(
            SpatialReference::new(SpatialReferenceAuthority::SrOrg, 1).to_string(),
            "SR-ORG:1"
        );
        assert_eq!(
            SpatialReference::new(SpatialReferenceAuthority::Iau2000, 4711).to_string(),
            "IAU2000:4711"
        );
        assert_eq!(
            SpatialReference::new(SpatialReferenceAuthority::Esri, 42).to_string(),
            "ESRI:42"
        );
    }

    #[test]
    fn serialize_json() {
        assert_eq!(
            serde_json::to_string(&SpatialReference::new(
                SpatialReferenceAuthority::Epsg,
                4326
            ))
            .unwrap(),
            "\"EPSG:4326\""
        );
        assert_eq!(
            serde_json::to_string(&SpatialReference::new(SpatialReferenceAuthority::SrOrg, 1))
                .unwrap(),
            "\"SR-ORG:1\""
        );
        assert_eq!(
            serde_json::to_string(&SpatialReference::new(
                SpatialReferenceAuthority::Iau2000,
                4711
            ))
            .unwrap(),
            "\"IAU2000:4711\""
        );
        assert_eq!(
            serde_json::to_string(&SpatialReference::new(SpatialReferenceAuthority::Esri, 42))
                .unwrap(),
            "\"ESRI:42\""
        );
    }

    #[test]
    fn deserialize_json() {
        assert_eq!(
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 4326),
            serde_json::from_str("\"EPSG:4326\"").unwrap()
        );
        assert_eq!(
            SpatialReference::new(SpatialReferenceAuthority::SrOrg, 1),
            serde_json::from_str("\"SR-ORG:1\"").unwrap()
        );
        assert_eq!(
            SpatialReference::new(SpatialReferenceAuthority::Iau2000, 4711),
            serde_json::from_str("\"IAU2000:4711\"").unwrap()
        );
        assert_eq!(
            SpatialReference::new(SpatialReferenceAuthority::Esri, 42),
            serde_json::from_str("\"ESRI:42\"").unwrap()
        );

        assert!(serde_json::from_str::<SpatialReference>("\"foo:bar\"").is_err());
    }

    #[test]
    fn spatial_reference_option_serde() {
        assert_eq!(
            serde_json::to_string(&SpatialReferenceOption::SpatialReference(
                SpatialReference::new(SpatialReferenceAuthority::Epsg, 4326)
            ))
            .unwrap(),
            "\"EPSG:4326\""
        );

        assert_eq!(
            serde_json::to_string(&SpatialReferenceOption::Unreferenced).unwrap(),
            "\"\""
        );

        assert_eq!(
            SpatialReferenceOption::SpatialReference(SpatialReference::new(
                SpatialReferenceAuthority::Epsg,
                4326
            )),
            serde_json::from_str("\"EPSG:4326\"").unwrap()
        );

        assert_eq!(
            SpatialReferenceOption::Unreferenced,
            serde_json::from_str("\"\"").unwrap()
        );

        assert!(serde_json::from_str::<SpatialReferenceOption>("\"foo:bar\"").is_err());
    }
}
