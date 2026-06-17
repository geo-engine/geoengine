use crate::{
    error::{self, BoxedResultExt},
    operations::reproject::{CoordinateProjection, CoordinateProjector, Reproject},
    primitives::AxisAlignedRectangle,
    util::Result,
};
use gdal::spatial_ref::SpatialRef;

use postgres_types::private::BytesMut;

use postgres_types::{FromSql, IsNull, ToSql, Type};
use proj::Proj;
use proj_sys::{
    proj_context_create, proj_context_destroy, proj_create, proj_destroy,
    proj_ellipsoid_get_parameters, proj_get_ellipsoid,
};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::ffi::CString;

use snafu::Error;
use snafu::ResultExt;
use std::str::FromStr;
use std::{convert::TryFrom, fmt::Formatter};

/// A spatial reference authority that is part of a spatial reference definition
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, ToSql, FromSql,
)]
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
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, ToSql, FromSql)]
pub struct SpatialReference {
    authority: SpatialReferenceAuthority,
    code: u32,
}

impl SpatialReference {
    pub fn new(authority: SpatialReferenceAuthority, code: u32) -> Self {
        Self { authority, code }
    }

    pub fn authority(&self) -> &SpatialReferenceAuthority {
        &self.authority
    }

    pub fn code(self) -> u32 {
        self.code
    }

    /// the WGS 84 spatial reference system
    pub fn epsg_4326() -> Self {
        Self::new(SpatialReferenceAuthority::Epsg, 4326)
    }

    pub fn web_mercator() -> Self {
        Self::new(SpatialReferenceAuthority::Epsg, 3857)
    }

    pub fn proj_string(self) -> Result<String> {
        match self.authority {
            SpatialReferenceAuthority::Epsg | SpatialReferenceAuthority::Iau2000 | SpatialReferenceAuthority::Esri => {
                Ok(format!("{}:{}", self.authority, self.code))
            }
            // poor-mans integration of Meteosat Second Generation 
            SpatialReferenceAuthority::SrOrg if self.code == 81 => Ok("+proj=geos +lon_0=0 +h=35785831 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs +type=crs".to_owned()),
            SpatialReferenceAuthority::SrOrg => {
                Err(error::Error::ProjStringUnresolvable { spatial_ref: self })
                //TODO: we might need to look them up somehow! Best solution would be a registry where we can store user definexd srs strings.
            }
        }
    }

    /// Return the area of use in EPSG:4326 projection
    pub fn area_of_use<A: AxisAlignedRectangle>(self) -> Result<A> {
        let proj_string = self.proj_string()?;

        let proj = Proj::new(&proj_string).map_err(|_| error::Error::InvalidProjDefinition {
            proj_definition: proj_string.clone(),
        })?;
        let area = proj
            .area_of_use()
            .context(error::ProjInternal)?
            .0
            .ok_or(error::Error::NoAreaOfUseDefined { proj_string })?;
        A::from_min_max(
            (area.west, area.south).into(),
            (area.east, area.north).into(),
        )
    }

    /// Return the area of use in current projection
    pub fn area_of_use_projected<A: AxisAlignedRectangle>(self) -> Result<A> {
        if self == Self::epsg_4326() {
            return self.area_of_use();
        }
        let p = CoordinateProjector::from_known_srs(Self::epsg_4326(), self)?;
        self.area_of_use::<A>()?.reproject(&p)
    }

    /// Return the srs-string "authority:code"
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn srs_string(&self) -> String {
        format!("{}:{}", self.authority, self.code)
    }

    /// Compute the bounding box of this spatial reference that is also valid in the `other` spatial reference. Might be None.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn area_of_use_intersection<T>(&self, other: &SpatialReference) -> Result<Option<T>>
    where
        T: AxisAlignedRectangle,
    {
        // generate a projector which transforms wgs84 into the projection we want to produce.
        let valid_bounds_proj =
            CoordinateProjector::from_known_srs(SpatialReference::epsg_4326(), *self)?;

        // transform the bounds of the input srs (coordinates are in wgs84) into the output projection.
        // TODO check if  there is a better / smarter way to check if the coordinates are valid.
        let area_out = self.area_of_use::<T>()?;
        let area_other = other.area_of_use::<T>()?;

        area_out
            .intersection(&area_other)
            .map(|x| x.reproject(&valid_bounds_proj))
            .transpose()
    }

    /// Computes the equatorial radius of the ellipsoid associated with this spatial reference in meters.
    /// This is a helper function for calculating the meters per unit for projections that use degrees as their unit of measurement.
    pub fn meters_per_unit(self) -> Result<f64> {
        if self.uses_meters()? {
            return Ok(1.0);
        }

        let proj_string = CString::new(self.proj_string()?).boxed_context(error::ProjInternal2)?;
        let mut meters_per_degree = None;

        unsafe {
            // 1. Initialize the PROJ context and instantiate the CRS
            let ctx = proj_context_create();
            let crs = proj_create(ctx, proj_string.as_ptr());

            if crs.is_null() {
                proj_context_destroy(ctx);
                return Err(error::Error::ProjStringUnresolvable { spatial_ref: self });
            }

            // 2. Fetch the underlying ellipsoid object from the CRS
            let ellipsoid = proj_get_ellipsoid(ctx, crs);

            if ellipsoid.is_null() {
                proj_destroy(crs);
                proj_context_destroy(ctx);
                return Err(error::Error::ProjStringUnresolvable { spatial_ref: self });
            }

            let mut semi_major: f64 = 0.0;
            let mut semi_minor: f64 = 0.0;
            let mut is_semi_minor_computed: i32 = 0;
            let mut inv_flattening: f64 = 0.0;

            // 3. Extract the semi-major axis (Equatorial Radius)
            let success = proj_ellipsoid_get_parameters(
                ctx,
                ellipsoid,
                &raw mut semi_major,
                &raw mut semi_minor,
                &raw mut is_semi_minor_computed,
                &raw mut inv_flattening,
            );

            if success == 1 {
                // 4. Calculate the Equatorial Perimeter divided by 360 degrees
                // WGS84 Semi-major axis (semi_major) = 6378137.0 meters
                let equatorial_perimeter = semi_major * 2.0 * std::f64::consts::PI;
                meters_per_degree = Some(equatorial_perimeter / 360.0);
            }

            // Clean up the main context, CRS and ellipsoid structures
            proj_destroy(ellipsoid);
            proj_destroy(crs);
            proj_context_destroy(ctx);
        }

        if let Some(meters) = meters_per_degree {
            Ok(meters)
        } else {
            Err(error::Error::ProjStringUnresolvable { spatial_ref: self })
        }
    }

    /// Checks if the spatial reference uses meters as its unit of measurement.
    /// This is a heuristic check and may not be 100% accurate for all projections.
    pub fn uses_meters(self) -> Result<bool> {
        let proj_string = self.proj_string()?;
        let proj = Proj::new_known_crs("EPSG:4326", &proj_string, None).map_err(|_| {
            error::Error::InvalidProjDefinition {
                proj_definition: proj_string.clone(),
            }
        })?;

        // Using 500,000 Easting (UTM Center) and 100,000 Northing (just North of the Equator/Origin)
        let (Ok(coord0), Ok(coord1)) = (
            proj.project((500_000.0, 100_000.0), true),
            proj.project((500_001.0, 100_000.0), true),
        ) else {
            // If the projection cannot handle these coordinates, it's likely not in meters
            return Ok(false);
        };

        // If it handles meters, moving 1 meter changes the output degrees by a microscopic amount
        let delta = f64::abs(coord1.0 - coord0.0);
        Ok(delta < 0.1)
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

impl Visitor<'_> for SpatialReferenceDeserializeVisitor {
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
                });
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
        let auth_name = value.auth_name().map_or_else(|| value.authority(), Ok)?;
        Ok(SpatialReference::new(
            SpatialReferenceAuthority::from_str(&auth_name)?,
            value.auth_code()? as u32,
        ))
    }
}

impl TryFrom<SpatialReference> for SpatialRef {
    type Error = error::Error;

    fn try_from(value: SpatialReference) -> Result<Self, Self::Error> {
        if value.authority == SpatialReferenceAuthority::Epsg {
            return SpatialRef::from_epsg(value.code).context(error::Gdal);
        }

        // TODO: support other projections reliably

        SpatialRef::from_proj4(&value.proj_string()?).context(error::Gdal)
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

    pub fn as_option(self) -> Option<SpatialReference> {
        self.into()
    }
}

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
            SpatialReferenceOption::SpatialReference(p) => write!(f, "{p}"),
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

impl Visitor<'_> for SpatialReferenceOptionDeserializeVisitor {
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
    use core::f64;
    use std::convert::TryInto;

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
            SpatialReference::new(SpatialReferenceAuthority::SrOrg, 81)
                .proj_string()
                .unwrap(),
            "+proj=geos +lon_0=0 +h=35785831 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs +type=crs"
        );
        assert_eq!(
            SpatialReference::new(SpatialReferenceAuthority::Iau2000, 4711)
                .proj_string()
                .unwrap(),
            "IAU2000:4711"
        );
        assert_eq!(
            SpatialReference::new(SpatialReferenceAuthority::Esri, 42)
                .proj_string()
                .unwrap(),
            "ESRI:42"
        );
        assert!(
            SpatialReference::new(SpatialReferenceAuthority::SrOrg, 1)
                .proj_string()
                .is_err()
        );
    }

    #[test]
    fn spatial_reference_to_gdal_spatial_ref_epsg() {
        let spatial_reference = SpatialReference::epsg_4326();
        let gdal_sref: SpatialRef = spatial_reference.try_into().unwrap();

        assert_eq!(gdal_sref.auth_name().unwrap(), "EPSG");
        assert_eq!(gdal_sref.auth_code().unwrap(), 4326);
    }

    #[test]
    fn it_knows_if_it_is_in_meters() {
        for (epsg, should_be_in_meters) in &[
            (4326, false), // WGS 84
            (3857, true),  // Web Mercator
            (25832, true), // ETRS89 / UTM zone 32N
        ] {
            let spatial_ref = SpatialReference::new(SpatialReferenceAuthority::Epsg, *epsg);
            assert_eq!(
                spatial_ref.uses_meters().unwrap(),
                *should_be_in_meters,
                "EPSG:{epsg} should be in meters: {should_be_in_meters}",
            );
        }
    }

    #[test]
    fn it_calculates_the_perimeter_in_meters() {
        use float_cmp::assert_approx_eq;

        let wgs84 = SpatialReference::new(SpatialReferenceAuthority::Epsg, 4326);
        let web_mercator = SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857);

        // cf. <https://docs.ogc.org/is/17-083r4/17-083r4.html#6-1-1-1-%C2%A0-tile-matrix-in-a-two-dimensional-space>
        assert_approx_eq!(
            f64,
            wgs84.meters_per_unit().unwrap(),
            111_319.490_8,
            epsilon = 0.000_1
        );

        assert_approx_eq!(f64, web_mercator.meters_per_unit().unwrap(), 1.0);
    }
}
