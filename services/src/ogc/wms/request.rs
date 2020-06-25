use serde::{Deserialize, Serialize};
use crate::util::{from_str, from_str_option};
use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D, TimeInterval};
use serde::de::Error;

// TODO: ignore case for field names

#[derive(PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "request")]
#[allow(clippy::pub_enum_variant_names, clippy::large_enum_variant)]
pub enum WMSRequest {
    GetCapabilities(GetCapabilities),
    GetMap(GetMap),
    GetFeatureInfo(GetFeatureInfo),
    GetStyles(GetStyles),
    GetLegendGraphic(GetLegendGraphic),
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct GetCapabilities {
    pub version: Option<String>,
    pub service: String,
    pub format: Option<String>, // TODO: Option<GetCapabilitiesFormat>,
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub enum GetCapabilitiesFormat {
    TextXml //, ...
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct GetMap {
    pub version: String,
    #[serde(deserialize_with = "from_str")]
    pub width: u32,
    #[serde(deserialize_with = "from_str")]
    pub height: u32,
    #[serde(deserialize_with = "parse_bbox")]
    pub bbox: BoundingBox2D,
    pub format: GetMapFormat,
    pub layer: String,
    pub crs: String,  // TODO: parse CRS
    pub styles: String,
    #[serde(default)]
    #[serde(deserialize_with = "parse_time")]
    pub time: Option<TimeInterval>,
    #[serde(default)]
    #[serde(deserialize_with = "from_str_option")]
    pub transparent: Option<bool>,
    pub bgcolor: Option<String>,
    pub sld: Option<String>,
    pub sld_body: Option<String>,
    pub elevation: Option<String>,
    pub exceptions: Option<String>, // TODO: parse Option<GetMapExceptionFormat>
    // DIM_<name>
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub enum GetMapExceptionFormat {
    TextXML //, ...
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub enum GetMapFormat {
    #[serde(rename = "image/png")]
    ImagePng //, ...
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct GetFeatureInfo {
    pub version: String,
    pub query_layers: String,
    pub info_format: Option<String>, // TODO: parse Option<GetFeatureInfoFormat>,
    // ...
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub enum GetFeatureInfoFormat {
    TextXML //, ...
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct GetStyles {
    pub version: String,
    pub layer: String,
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct GetLegendGraphic {
    pub version: String,
    pub layer: String,
    // ...
}

pub fn parse_bbox<'de, D>(deserializer: D) -> Result<BoundingBox2D, D::Error>
    where D: serde::Deserializer<'de>
{
    // format is : "x1,y1,x2,y2"

    let s = <&str as serde::Deserialize>::deserialize(deserializer)?;

    let split: Vec<Result<f64, std::num::ParseFloatError>> = s.split(',')
        .map(str::parse).collect();

    if let [Ok(x1), Ok(y1), Ok(x2), Ok(y2)] = *split.as_slice() {
        BoundingBox2D::new(
            Coordinate2D::new(x1, y1),
            Coordinate2D::new(x2, y2))
            .map_err(|_| D::Error::custom("Invalid bbox"))
    } else {
        Err(D::Error::custom("Invalid bbox"))
    }
}

/// Parse the time string of a WMS request
/// time is specified in ISO8601, it can either be an instant (single datetime) or an interval
/// An interval is separated by "/". "Either the start value or the end value can be omitted to
/// indicate no restriction on time in that direction."
/// sources: - <http://docs.geoserver.org/2.8.x/en/user/services/wms/time.html#wms-time>
///          - <http://www.ogcnetwork.net/node/178>
pub fn parse_time<'de, D>(deserializer: D) -> Result<Option<TimeInterval>, D::Error>
    where D: serde::Deserializer<'de>
{
    // TODO: support relative time intervals

    let s = <&str as serde::Deserialize>::deserialize(deserializer)?;

    let split: Vec<_> = s.split('/').map(|s| chrono::DateTime::parse_from_rfc3339(s)).collect();

    match *split.as_slice() {
        [Ok(time)] => TimeInterval::new(time.timestamp(), time.timestamp())
            .map(Some)
            .map_err(|_| D::Error::custom("Invalid time")),
        [Ok(start), Ok(end)]  => TimeInterval::new(start.timestamp(), end.timestamp())
            .map(Some)
            .map_err(|_| D::Error::custom("Invalid time")),
        _ => Err(D::Error::custom("Invalid time"))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D};

    #[test]
    fn deserialize_get_map() {
        let query = "request=GetMap&service=WMS&version=1.3.0&layer=test&bbox=1,2,3,4&width=2&height=2&crs=foo&styles=ssss&format=image/png&time=2000-01-01T00:00:00.0Z/2000-01-02T00:00:00.0Z&transparent=true&bgcolor=#000000&sld=sld_spec&sld_body=sld_body&elevation=elevation&exceptions=exceptions";
        let parsed: WMSRequest = serde_urlencoded::from_str(query).unwrap();

        let request = WMSRequest::GetMap(GetMap {
            version: "1.3.0".into(),
            width: 2,
            layer: "test".into(),
            crs: "foo".into(),
            styles: "ssss".into(),
            time: Some(TimeInterval::new(946_684_800, 946_771_200).unwrap()),
            transparent: Some(true),
            bgcolor: Some("#000000".into()),
            sld: Some("sld_spec".into()),
            sld_body: Some("sld_body".into()),
            elevation: Some("elevation".into()),
            bbox: BoundingBox2D::new(Coordinate2D::new(1., 2.),
                                     Coordinate2D::new(3., 4.)).unwrap(),
            height: 2,
            format: GetMapFormat::ImagePng,
            exceptions: Some("exceptions".into()),
        });

        assert_eq!(parsed, request);
    }

    #[test]
    fn deserialize_get_map_not_time() {
        let query = "request=GetMap&service=WMS&version=1.3.0&layer=test&bbox=1,2,3,4&width=2&height=2&crs=foo&styles=ssss&format=image/png";
        let parsed: WMSRequest = serde_urlencoded::from_str(query).unwrap();

        let request = WMSRequest::GetMap(GetMap {
            version: "1.3.0".into(),
            width: 2,
            layer: "test".into(),
            crs: "foo".to_string(),
            styles: "ssss".into(),
            time: None,
            transparent: None,
            bgcolor: None,
            sld: None,
            sld_body: None,
            elevation: None,
            bbox: BoundingBox2D::new(Coordinate2D::new(1., 2.),
                                     Coordinate2D::new(3., 4.)).unwrap(),
            height: 2,
            format: GetMapFormat::ImagePng,
            exceptions: None,
        });

        assert_eq!(parsed, request);
    }
}