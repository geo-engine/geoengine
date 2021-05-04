use crate::{error, primitives::BoundingBox2D, util::Result};
use gdal::spatial_ref::SpatialRef;
#[cfg(feature = "postgres")]
use postgres_types::private::BytesMut;
#[cfg(feature = "postgres")]
use postgres_types::{FromSql, IsNull, ToSql, Type};
use proj::Proj;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
#[cfg(feature = "postgres")]
use snafu::Error;
use snafu::ResultExt;
use std::str::FromStr;
use std::{convert::TryFrom, fmt::Formatter};

/// A spatial reference authority that is part of a spatial reference definition
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
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
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
pub struct SpatialReference {
    authority: SpatialReferenceAuthority,
    code: u32,
}

impl SpatialReference {
    pub fn new(authority: SpatialReferenceAuthority, code: u32) -> Self {
        Self { authority, code }
    }

    /// the WGS 84 spatial reference system
    pub fn epsg_4326() -> Self {
        Self::new(SpatialReferenceAuthority::Epsg, 4326)
    }

    pub fn proj_string(self) -> Result<String> {
        match self.authority {
            SpatialReferenceAuthority::Epsg | SpatialReferenceAuthority::Iau2000 => {
                Ok(format!("{}:{}", self.authority, self.code))
            }
            // poor-mans integration of Meteosat Second Generation 
            SpatialReferenceAuthority::SrOrg if self.code == 81 => Ok("+proj=geos +lon_0=0 +h=35785831 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs +type=crs".to_owned()),
            SpatialReferenceAuthority::SrOrg | SpatialReferenceAuthority::Esri => {
                Err(error::Error::ProjStringUnresolvable { spatial_ref: self })
                //TODO: we might need to look them up somehow! Best solution would be a registry where we can store user definexd srs strings.
            }
        }
    }

    pub fn area_of_use(self) -> Result<BoundingBox2D> {
        let proj = Proj::new(&self.proj_string()?).ok_or(error::Error::InvalidProjDefinition)?;
        let area = proj
            .area_of_use()
            .context(error::ProjInternal)?
            .0
            .ok_or(error::Error::NoAreaOfUseDefined)?;
        BoundingBox2D::new(
            (area.west, area.south).into(),
            (area.east, area.north).into(),
        )
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

impl TryFrom<SpatialRef> for SpatialReference {
    type Error = error::Error;

    fn try_from(value: SpatialRef) -> Result<Self, Self::Error> {
        Ok(SpatialReference::new(
            SpatialReferenceAuthority::from_str(&value.auth_name()?)?,
            value.auth_code()? as u32,
        ))
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum SpatialReferenceOption {
    SpatialReference(SpatialReference),
    Unreferenced,
}

impl SpatialReferenceOption {
    pub fn is_spatial_ref(self) -> bool {
        match self {
            SpatialReferenceOption::SpatialReference(_) => true,
            SpatialReferenceOption::Unreferenced => false,
        }
    }

    pub fn is_unreferenced(self) -> bool {
        !self.is_spatial_ref()
    }
}

#[cfg(feature = "postgres")]
impl ToSql for SpatialReferenceOption {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            SpatialReferenceOption::SpatialReference(sref) => sref.to_sql(ty, out),
            SpatialReferenceOption::Unreferenced => Ok(IsNull::Yes),
        }
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        <SpatialReference as ToSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self {
            SpatialReferenceOption::SpatialReference(sref) => sref.to_sql_checked(ty, out),
            SpatialReferenceOption::Unreferenced => Ok(IsNull::Yes),
        }
    }
}

#[cfg(feature = "postgres")]
impl<'a> FromSql<'a> for SpatialReferenceOption {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(SpatialReferenceOption::SpatialReference(
            SpatialReference::from_sql(ty, raw)?,
        ))
    }

    fn from_sql_null(_: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(SpatialReferenceOption::Unreferenced)
    }

    fn accepts(ty: &Type) -> bool {
        <SpatialReference as FromSql>::accepts(ty)
    }
}

impl std::fmt::Display for SpatialReferenceOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpatialReferenceOption::SpatialReference(p) => write!(f, "{}", p),
            SpatialReferenceOption::Unreferenced => Ok(()),
        }
    }
}

impl From<SpatialReference> for SpatialReferenceOption {
    fn from(spatial_reference: SpatialReference) -> Self {
        Self::SpatialReference(spatial_reference)
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

impl From<SpatialReferenceOption> for Option<SpatialReference> {
    fn from(s_ref: SpatialReferenceOption) -> Self {
        match s_ref {
            SpatialReferenceOption::SpatialReference(s) => Some(s),
            SpatialReferenceOption::Unreferenced => None,
        }
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

    #[test]
    fn is_spatial_ref() {
        let s_ref = SpatialReferenceOption::from(SpatialReference::epsg_4326());
        assert!(s_ref.is_spatial_ref());
        assert!(!s_ref.is_unreferenced());
    }

    #[test]
    fn is_unreferenced() {
        let s_ref = SpatialReferenceOption::Unreferenced;
        assert!(s_ref.is_unreferenced());
        assert!(!s_ref.is_spatial_ref());
    }

    #[test]
    fn from_option_some() {
        let s_ref: SpatialReferenceOption = Some(SpatialReference::epsg_4326()).into();
        assert_eq!(
            s_ref,
            SpatialReferenceOption::SpatialReference(SpatialReference::epsg_4326())
        );
    }

    #[test]
    fn from_option_none() {
        let s_ref: SpatialReferenceOption = None.into();
        assert_eq!(s_ref, SpatialReferenceOption::Unreferenced);
    }

    #[test]
    fn into_option_some() {
        let s_ref: Option<SpatialReference> =
            SpatialReferenceOption::SpatialReference(SpatialReference::epsg_4326()).into();
        assert_eq!(s_ref, Some(SpatialReference::epsg_4326()));
    }

    #[test]
    fn into_option_none() {
        let s_ref: Option<SpatialReference> = SpatialReferenceOption::Unreferenced.into();
        assert_eq!(s_ref, None);
    }

    #[test]
    fn proj_string() {
        assert_eq!(
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 4326)
                .proj_string()
                .unwrap(),
            "EPSG:4326"
        );
        assert_eq!(
            SpatialReference::new(SpatialReferenceAuthority::SrOrg, 81).proj_string().unwrap(),
            "+proj=geos +lon_0=0 +h=35785831 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs +type=crs"
        );
        assert_eq!(
            SpatialReference::new(SpatialReferenceAuthority::Iau2000, 4711)
                .proj_string()
                .unwrap(),
            "IAU2000:4711"
        );
        assert!(SpatialReference::new(SpatialReferenceAuthority::Esri, 42)
            .proj_string()
            .is_err());
        assert!(SpatialReference::new(SpatialReferenceAuthority::SrOrg, 1)
            .proj_string()
            .is_err());
    }
}
