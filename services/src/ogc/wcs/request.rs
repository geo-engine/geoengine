use crate::ogc::util::{
    parse_coordinate, parse_grid_offset_option, parse_time_option, parse_wcs_bbox, parse_wcs_crs,
};
use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D, SpatialResolution};
use geoengine_datatypes::{primitives::TimeInterval, spatial_reference::SpatialReference};
use serde::{Deserialize, Serialize};

// TODO: ignore case for field names

/// WCS 1.1.x
#[derive(PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "request")]
pub enum WcsRequest {
    GetCapabilities(GetCapabilities),
    DescribeCoverage(DescribeCoverage),
    GetCoverage(GetCoverage),
}

// sample: SERVICE=WCS&request=GetCapabilities&VERSION=1.0.0
#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct GetCapabilities {
    #[serde(alias = "VERSION")]
    pub version: Option<String>,
}

// sample: SERVICE=WCS&request=DescribeCoverage&VERSION=1.1.1&IDENTIFIERS=nurc:Arc_Sample
#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct DescribeCoverage {
    #[serde(alias = "VERSION")]
    pub version: String,
    #[serde(alias = "IDENTIFIERS")]
    pub identifiers: String,
}

// sample: SERVICE=WCS&VERSION=1.1.1&request=GetCoverage&FORMAT=image/tiff&IDENTIFIER=nurc:Arc_Sample&BOUNDINGBOX=-81,-162,81,162,urn:ogc:def:crs:EPSG::4326&GRIDBASECRS=urn:ogc:def:crs:EPSG::4326&GRIDCS=urn:ogc:def:cs:OGC:0.0:Grid2dSquareCS&GRIDTYPE=urn:ogc:def:method:WCS:1.1:2dSimpleGrid&GRIDORIGIN=81,-162&GRIDOFFSETS=-18,36
#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct GetCoverage {
    #[serde(alias = "VERSION")]
    pub version: String,
    #[serde(alias = "FORMAT")]
    pub format: GetCoverageFormat,
    #[serde(alias = "IDENTIFIER")]
    pub identifier: String,
    #[serde(alias = "BOUNDINGBOX")]
    #[serde(deserialize_with = "parse_wcs_bbox")]
    pub boundingbox: WcsBoundingbox, // TODO: optional?
    #[serde(alias = "GRIDBASECRS")]
    #[serde(deserialize_with = "parse_wcs_crs")]
    pub gridbasecrs: SpatialReference,
    #[serde(alias = "GRIDORIGIN")]
    #[serde(deserialize_with = "parse_coordinate")]
    pub gridorigin: Coordinate2D,
    #[serde(alias = "GRIDOFFSETS")]
    #[serde(default)]
    #[serde(deserialize_with = "parse_grid_offset_option")]
    pub gridoffsets: Option<GridOffset>,

    // ignored for now:
    // GRIDCS=crs: The grid CRS (URN).
    // GridType=urn:ogc:def:method:WCS:1.1:2dGridIn2dCrs:
    // RangeSubset=selection: e.g. bands

    // vendor specific
    #[serde(default)]
    #[serde(deserialize_with = "parse_time_option")]
    pub time: Option<TimeInterval>,
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct WcsBoundingbox {
    pub bbox: BoundingBox2D,
    pub spatial_reference: SpatialReference,
}

#[derive(PartialEq, Debug, Deserialize, Serialize, Clone, Copy)]
pub struct GridOffset {
    pub x_step: f64,
    pub y_step: f64,
}

impl From<GridOffset> for SpatialResolution {
    fn from(value: GridOffset) -> Self {
        SpatialResolution {
            x: value.x_step.abs(),
            y: value.y_step.abs(),
        }
    }
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub enum GetCoverageFormat {
    #[serde(rename = "image/tiff")]
    ImageTiff,
}
