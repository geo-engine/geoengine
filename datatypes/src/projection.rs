use crate::error;
use serde::de::Visitor;
use serde::export::Formatter;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use snafu::ResultExt;
use std::str::FromStr;

/// A projection authority that is part of a projection definition
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum ProjectionAuthority {
    Epsg,
    SrOrg,
    Iau2000,
    Esri,
}

impl std::fmt::Display for ProjectionAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ProjectionAuthority::Epsg => "EPSG",
                ProjectionAuthority::SrOrg => "SR-ORG",
                ProjectionAuthority::Iau2000 => "IAU2000",
                ProjectionAuthority::Esri => "ESRI",
            }
        )
    }
}

/// A projection consists of an authority and a code
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Projection {
    authority: ProjectionAuthority,
    code: u32,
}

impl Projection {
    pub fn new(authority: ProjectionAuthority, code: u32) -> Self {
        Self { authority, code }
    }

    /// the WGS 84 projection
    pub fn wgs84() -> Self {
        Self::new(ProjectionAuthority::Epsg, 4326)
    }
}

impl std::fmt::Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.authority, self.code)
    }
}

impl Serialize for Projection {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// Helper struct for deserializing a `Projection`
struct ProjectionDeserializeVisitor;

impl<'de> Visitor<'de> for ProjectionDeserializeVisitor {
    type Value = Projection;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a projection in the form authority:code")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        v.parse().map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for Projection {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ProjectionDeserializeVisitor)
    }
}

impl FromStr for ProjectionAuthority {
    type Err = error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "EPSG" => ProjectionAuthority::Epsg,
            "SR-ORG" => ProjectionAuthority::SrOrg,
            "IAU2000" => ProjectionAuthority::Iau2000,
            "ESRI" => ProjectionAuthority::Esri,
            _ => {
                return Err(error::Error::InvalidProjectionString {
                    projection_string: s.into(),
                })
            }
        })
    }
}

impl FromStr for Projection {
    type Err = error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split = s.split(':');

        match (split.next(), split.next(), split.next()) {
            (Some(authority), Some(code), None) => Ok(Self::new(
                authority.parse()?,
                code.parse::<u32>().context(error::ParseU32)?,
            )),
            _ => Err(error::Error::InvalidProjectionString {
                projection_string: s.into(),
            }),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum ProjectionOption {
    Projection(Projection),
    None,
}

impl std::fmt::Display for ProjectionOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProjectionOption::Projection(p) => write!(f, "{}", p),
            ProjectionOption::None => Ok(()),
        }
    }
}

impl Into<ProjectionOption> for Projection {
    fn into(self) -> ProjectionOption {
        ProjectionOption::Projection(self)
    }
}

impl From<Option<Projection>> for ProjectionOption {
    fn from(option: Option<Projection>) -> Self {
        match option {
            Some(p) => ProjectionOption::Projection(p),
            None => ProjectionOption::None,
        }
    }
}

impl Serialize for ProjectionOption {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// Helper struct for deserializing a `ProjectionOption`
struct ProjectionOptionDeserializeVisitor;

impl<'de> Visitor<'de> for ProjectionOptionDeserializeVisitor {
    type Value = ProjectionOption;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a projection in the form authority:code")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.is_empty() {
            return Ok(ProjectionOption::None);
        }

        let projection: Projection = v.parse().map_err(serde::de::Error::custom)?;

        Ok(projection.into())
    }
}

impl<'de> Deserialize<'de> for ProjectionOption {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ProjectionOptionDeserializeVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display() {
        assert_eq!(ProjectionAuthority::Epsg.to_string(), "EPSG");
        assert_eq!(ProjectionAuthority::SrOrg.to_string(), "SR-ORG");
        assert_eq!(ProjectionAuthority::Iau2000.to_string(), "IAU2000");
        assert_eq!(ProjectionAuthority::Esri.to_string(), "ESRI");

        assert_eq!(
            Projection::new(ProjectionAuthority::Epsg, 4326).to_string(),
            "EPSG:4326"
        );
        assert_eq!(
            Projection::new(ProjectionAuthority::SrOrg, 1).to_string(),
            "SR-ORG:1"
        );
        assert_eq!(
            Projection::new(ProjectionAuthority::Iau2000, 4711).to_string(),
            "IAU2000:4711"
        );
        assert_eq!(
            Projection::new(ProjectionAuthority::Esri, 42).to_string(),
            "ESRI:42"
        );
    }

    #[test]
    fn serialize_json() {
        assert_eq!(
            serde_json::to_string(&Projection::new(ProjectionAuthority::Epsg, 4326)).unwrap(),
            "\"EPSG:4326\""
        );
        assert_eq!(
            serde_json::to_string(&Projection::new(ProjectionAuthority::SrOrg, 1)).unwrap(),
            "\"SR-ORG:1\""
        );
        assert_eq!(
            serde_json::to_string(&Projection::new(ProjectionAuthority::Iau2000, 4711)).unwrap(),
            "\"IAU2000:4711\""
        );
        assert_eq!(
            serde_json::to_string(&Projection::new(ProjectionAuthority::Esri, 42)).unwrap(),
            "\"ESRI:42\""
        );
    }

    #[test]
    fn deserialize_json() {
        assert_eq!(
            Projection::new(ProjectionAuthority::Epsg, 4326),
            serde_json::from_str("\"EPSG:4326\"").unwrap()
        );
        assert_eq!(
            Projection::new(ProjectionAuthority::SrOrg, 1),
            serde_json::from_str("\"SR-ORG:1\"").unwrap()
        );
        assert_eq!(
            Projection::new(ProjectionAuthority::Iau2000, 4711),
            serde_json::from_str("\"IAU2000:4711\"").unwrap()
        );
        assert_eq!(
            Projection::new(ProjectionAuthority::Esri, 42),
            serde_json::from_str("\"ESRI:42\"").unwrap()
        );

        assert!(serde_json::from_str::<Projection>("\"foo:bar\"").is_err());
    }

    #[test]
    fn projection_option_serde() {
        assert_eq!(
            serde_json::to_string(&ProjectionOption::Projection(Projection::new(
                ProjectionAuthority::Epsg,
                4326
            )))
            .unwrap(),
            "\"EPSG:4326\""
        );

        assert_eq!(
            serde_json::to_string(&ProjectionOption::None).unwrap(),
            "\"\""
        );

        assert_eq!(
            ProjectionOption::Projection(Projection::new(ProjectionAuthority::Epsg, 4326)),
            serde_json::from_str("\"EPSG:4326\"").unwrap()
        );

        assert_eq!(
            ProjectionOption::None,
            serde_json::from_str("\"\"").unwrap()
        );

        assert!(serde_json::from_str::<ProjectionOption>("\"foo:bar\"").is_err());
    }
}
