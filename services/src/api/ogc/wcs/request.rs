use crate::api::model::datatypes::SpatialReference;
use crate::api::model::datatypes::TimeInterval;
use crate::api::ogc::util::{
    parse_time_option, parse_wcs_bbox, parse_wcs_crs, rectangle_from_ogc_params,
    tuple_from_ogc_params,
};
use crate::error::{self, Result};
use crate::util::from_str_option;
use geoengine_datatypes::primitives::{Coordinate2D, SpatialPartition2D, SpatialResolution};
use serde::de::Error;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
pub enum WcsService {
    #[serde(rename = "WCS")]
    Wcs,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
pub enum WcsVersion {
    #[serde(rename = "1.1.0")]
    V1_1_0,
    #[serde(rename = "1.1.1")]
    V1_1_1,
}

// sample: SERVICE=WCS&request=GetCapabilities&VERSION=1.0.0
#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, IntoParams)]
pub struct GetCapabilities {
    #[serde(alias = "VERSION")]
    pub version: Option<WcsVersion>,
    #[serde(alias = "SERVICE")]
    pub service: WcsService,
    #[serde(alias = "REQUEST")]
    pub request: GetCapabilitiesRequest,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
pub enum GetCapabilitiesRequest {
    GetCapabilities,
}
// sample: SERVICE=WCS&request=DescribeCoverage&VERSION=1.1.1&IDENTIFIERS=nurc:Arc_Sample
#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, IntoParams)]
pub struct DescribeCoverage {
    #[serde(alias = "VERSION")]
    pub version: WcsVersion,
    #[serde(alias = "SERVICE")]
    pub service: WcsService,
    #[serde(alias = "REQUEST")]
    pub request: DescribeCoverageRequest,
    #[serde(alias = "IDENTIFIERS")]
    #[param(example = "<Workflow Id>")]
    pub identifiers: String,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
pub enum DescribeCoverageRequest {
    DescribeCoverage,
}

// sample: SERVICE=WCS&VERSION=1.1.1&request=GetCoverage&FORMAT=image/tiff&IDENTIFIER=nurc:Arc_Sample&BOUNDINGBOX=-81,-162,81,162,urn:ogc:def:crs:EPSG::4326&GRIDBASECRS=urn:ogc:def:crs:EPSG::4326&GRIDCS=urn:ogc:def:cs:OGC:0.0:Grid2dSquareCS&GRIDTYPE=urn:ogc:def:method:WCS:1.1:2dSimpleGrid&GRIDORIGIN=81,-162&GRIDOFFSETS=-18,36
#[derive(PartialEq, Debug, Deserialize, Serialize, IntoParams)]
pub struct GetCoverage {
    #[serde(alias = "VERSION")]
    pub version: WcsVersion,
    #[serde(alias = "SERVICE")]
    pub service: WcsService,
    #[serde(alias = "REQUEST")]
    pub request: GetCoverageRequest,
    #[serde(alias = "FORMAT")]
    pub format: GetCoverageFormat,
    #[serde(alias = "IDENTIFIER")]
    #[param(example = "<Workflow Id>")]
    pub identifier: String,
    #[serde(alias = "BOUNDINGBOX")]
    #[serde(deserialize_with = "parse_wcs_bbox")]
    #[param(value_type = String, example = "-90,-180,90,180,urn:ogc:def:crs:EPSG::4326")]
    pub boundingbox: WcsBoundingbox, // TODO: optional?
    #[serde(
        alias = "GRIDBASECRS",
        alias = "GridBaseCRS",
        alias = "CRS",
        alias = "crs"
    )]
    #[serde(deserialize_with = "parse_wcs_crs")]
    #[param(example = "urn:ogc:def:crs:EPSG::4326", value_type = String)]
    pub gridbasecrs: SpatialReference,
    #[serde(default)]
    #[serde(alias = "GRIDORIGIN", alias = "GridOrigin")]
    #[serde(deserialize_with = "parse_grid_origin_option")]
    #[param(value_type = String, example="90,-180")]
    pub gridorigin: Option<GridOrigin>,
    #[serde(alias = "GRIDOFFSETS", alias = "GridOffsets")]
    #[serde(default)]
    #[serde(deserialize_with = "parse_grid_offset_option")]
    #[param(value_type = String, example="-0.1,0.1")]
    pub gridoffsets: Option<GridOffsets>,

    // ignored for now:
    // GRIDCS=crs: The grid CRS (URN).
    // GridType=urn:ogc:def:method:WCS:1.1:2dGridIn2dCrs:
    // RangeSubset=selection: e.g. bands
    #[serde(default)]
    #[serde(deserialize_with = "parse_time_option")]
    #[serde(alias = "timesequence")] // owsLib sends it like this
    #[param(value_type = String)]
    pub time: Option<TimeInterval>,

    // fallback (to support clients using some weird mixture of 1.0 and 1.1)
    #[serde(default)]
    #[serde(deserialize_with = "from_str_option")]
    resx: Option<f64>,
    #[serde(default)]
    #[serde(deserialize_with = "from_str_option")]
    resy: Option<f64>,

    // Geo Engine specific
    #[serde(default)]
    #[serde(deserialize_with = "from_str_option")]
    pub nodatavalue: Option<f64>,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
pub enum GetCoverageRequest {
    GetCoverage,
}

impl GetCoverage {
    pub fn spatial_resolution(&self) -> Option<Result<SpatialResolution>> {
        if let Some(grid_offsets) = self.gridoffsets {
            return Some(grid_offsets.spatial_resolution(self.gridbasecrs));
        }

        match (self.resx, self.resy) {
            (Some(xres), Some(yres)) => match tuple_from_ogc_params(xres, yres, self.gridbasecrs) {
                Ok((x, y)) => Some(SpatialResolution::new(x.abs(), y.abs()).map_err(Into::into)),
                Err(e) => Some(Err(e)),
            },
            (Some(_), None) | (None, Some(_)) => Some(Err(error::Error::WcsInvalidGridOffsets)),
            (None, None) => None,
        }
    }

    pub fn spatial_partition(&self) -> Result<SpatialPartition2D> {
        let spatial_reference = self
            .boundingbox
            .spatial_reference
            .unwrap_or(self.gridbasecrs);

        rectangle_from_ogc_params(self.boundingbox.bbox, spatial_reference)
    }
}

#[derive(PartialEq, Debug, Deserialize, Serialize, ToSchema)]
pub struct WcsBoundingbox {
    pub bbox: [f64; 4],
    #[schema(value_type = Option<String>)]
    pub spatial_reference: Option<SpatialReference>,
}

#[derive(PartialEq, Debug, Deserialize, Serialize, Clone, Copy)]
pub struct GridOffsets {
    x_step: f64,
    y_step: f64,
}

impl GridOffsets {
    fn spatial_resolution(&self, spatial_reference: SpatialReference) -> Result<SpatialResolution> {
        let (x, y) = tuple_from_ogc_params(self.x_step, self.y_step, spatial_reference)?;
        SpatialResolution::new(x.abs(), y.abs()).map_err(Into::into)
    }
}

#[derive(PartialEq, Debug, Deserialize, Serialize, Clone, Copy)]
pub struct GridOrigin {
    x: f64,
    y: f64,
}

impl GridOrigin {
    pub fn coordinate(&self, spatial_reference: SpatialReference) -> Result<Coordinate2D> {
        tuple_from_ogc_params(self.x, self.y, spatial_reference).map(Into::into)
    }
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
pub enum GetCoverageFormat {
    #[serde(rename = "image/tiff")]
    ImageTiff,
}

/// parse coordinate, format is "x,y"
pub fn parse_grid_origin_option<'de, D>(deserializer: D) -> Result<Option<GridOrigin>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if s.is_empty() {
        return Ok(None);
    }

    let split: Vec<Result<f64, std::num::ParseFloatError>> = s.split(',').map(str::parse).collect();

    match *split.as_slice() {
        [Ok(x), Ok(y)] => Ok(Some(GridOrigin { x, y })),
        _ => Err(D::Error::custom("Invalid gridorigin")),
    }
}

/// Parse grid offset, format is `x_step,y_step`
pub fn parse_grid_offset_option<'de, D>(deserializer: D) -> Result<Option<GridOffsets>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if s.is_empty() {
        return Ok(None);
    }

    let split: Vec<Result<f64, std::num::ParseFloatError>> = s.split(',').map(str::parse).collect();

    let grid_offset = match *split.as_slice() {
        [Ok(x_step), Ok(y_step)] => GridOffsets { x_step, y_step },
        _ => return Err(D::Error::custom("Invalid grid offset")),
    };

    Ok(Some(grid_offset))
}

#[cfg(test)]
mod tests {
    use serde::de::{IntoDeserializer, value::StringDeserializer};

    use crate::api::model::datatypes::SpatialReferenceAuthority;

    use super::*;

    fn to_deserializer(s: &str) -> StringDeserializer<serde::de::value::Error> {
        s.to_owned().into_deserializer()
    }

    #[test]
    fn deserialize() {
        let params = &[
            ("service", "WCS"),
            ("request", "GetCoverage"),
            ("version", "1.1.1"),
            ("identifier", "nurc:Arc_Sample"),
            ("boundingbox", "-81,-162,81,162,urn:ogc:def:crs:EPSG::4326"),
            ("format", "image/tiff"),
            ("gridbasecrs", "urn:ogc:def:crs:EPSG::4326"),
            ("gridcs", "urn:ogc:def:cs:OGC:0.0:Grid2dSquareCS"),
            ("gridtype", "urn:ogc:def:method:WCS:1.1:2dSimpleGrid"),
            ("gridorigin", "81,-162"),
            ("gridoffsets", "-18,36"),
            ("time", "2014-01-01T00:00:00.0Z"),
        ];
        let string = serde_urlencoded::to_string(params).unwrap();

        let coverage: GetCoverage = serde_urlencoded::from_str(&string).unwrap();

        assert_eq!(
            GetCoverage {
                version: WcsVersion::V1_1_1,
                service: WcsService::Wcs,
                request: GetCoverageRequest::GetCoverage,
                format: GetCoverageFormat::ImageTiff,
                identifier: "nurc:Arc_Sample".to_owned(),
                boundingbox: WcsBoundingbox {
                    bbox: [-81., -162., 81., 162.],
                    spatial_reference: Some(SpatialReference::new(
                        SpatialReferenceAuthority::Epsg,
                        4326
                    )),
                },
                gridbasecrs: SpatialReference::new(SpatialReferenceAuthority::Epsg, 4326),
                gridorigin: Some(GridOrigin { x: 81., y: -162. }),
                gridoffsets: Some(GridOffsets {
                    x_step: -18.,
                    y_step: 36.
                }),
                time: Some(
                    geoengine_datatypes::primitives::TimeInterval::new_instant(1_388_534_400_000)
                        .unwrap()
                        .into()
                ),
                resx: None,
                resy: None,
                nodatavalue: None,
            },
            coverage
        );

        assert_eq!(
            coverage.spatial_partition().unwrap(),
            SpatialPartition2D::new_unchecked((-162., 81.).into(), (162., -81.).into())
        );

        assert_eq!(
            coverage.spatial_resolution().unwrap().unwrap(),
            SpatialResolution::new_unchecked(36., 18.)
        );
    }

    #[test]
    fn it_parses_grid_offset() {
        let s = "-8,5";

        assert_eq!(
            parse_grid_offset_option(to_deserializer(s)).unwrap(),
            Some(GridOffsets {
                x_step: -8.,
                y_step: 5.
            })
        );
    }
}
