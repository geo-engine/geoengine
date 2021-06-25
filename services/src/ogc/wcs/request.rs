use crate::ogc::util::{parse_subset_option, parse_time_option};
use crate::util::from_str;
use geoengine_datatypes::{primitives::TimeInterval, spatial_reference::SpatialReference};
use serde::{Deserialize, Serialize};

// TODO: ignore case for field names

/// towards an implementation of WCS 2.0.1
#[derive(PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "request")]
pub enum WcsRequest {
    GetCapabilities(GetCapabilities),
    DescribeCoverage(DescribeCoverage),
    GetCoverage(GetCoverage),
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct GetCapabilities {
    pub version: Option<String>,
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct DescribeCoverage {
    pub version: String,
    #[serde(rename = "coverageid")]
    pub coverage_id: String,
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct GetCoverage {
    pub version: String,
    pub format: GetCoverageFormat,
    pub coverageid: String,
    #[serde(default)]
    #[serde(deserialize_with = "parse_time_option")]
    pub time: Option<TimeInterval>,
    #[serde(deserialize_with = "parse_subset_option")]
    pub subset_x: Option<(f64, f64)>, // TODO: actually `&subset=x(min,max)`
    #[serde(deserialize_with = "parse_subset_option")]
    pub subset_y: Option<(f64, f64)>, // TODO: actually `&subset=y(min,max)`
    #[serde(deserialize_with = "from_str")]
    pub size_x: u32, // TODO: actually `&size=x(pixels)` and optional(?)
    #[serde(deserialize_with = "from_str")]
    pub size_y: u32, // TODO: actually `&size=y(pixels)` and optional(?)
    pub srsname: Option<SpatialReference>,
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub enum GetCoverageFormat {
    #[serde(rename = "image/tiff")]
    ImageTiff,
}
