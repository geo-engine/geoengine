use crate::ogc::util::{parse_ogc_bbox, parse_time_option, OgcBoundingBox};
use crate::util::{bool_option_case_insensitive, from_str};
use geoengine_datatypes::primitives::TimeInterval;
use geoengine_datatypes::spatial_reference::SpatialReference;
use serde::{Deserialize, Serialize};

// TODO: ignore case for field names

#[derive(PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "request")]
// TODO: evaluate overhead of large enum variant and maybe refactor it
#[allow(clippy::large_enum_variant, clippy::enum_variant_names)]
pub enum WmsRequest {
    GetCapabilities(GetCapabilities),
    GetMap(GetMap),
    GetFeatureInfo(GetFeatureInfo),
    GetStyles(GetStyles),
    GetLegendGraphic(GetLegendGraphic),
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct GetCapabilities {
    pub version: Option<String>,
    pub service: String,
    pub format: Option<String>, // TODO: Option<GetCapabilitiesFormat>,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub enum GetCapabilitiesFormat {
    TextXml, // TODO: remaining formats
}

// TODO: remove serde aliases and use serde-aux and case insensitive keys
#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct GetMap {
    #[serde(alias = "VERSION")]
    pub version: String,
    #[serde(alias = "WIDTH")]
    #[serde(deserialize_with = "from_str")]
    pub width: u32,
    #[serde(alias = "HEIGHT")]
    #[serde(deserialize_with = "from_str")]
    pub height: u32,
    #[serde(alias = "BBOX")]
    #[serde(deserialize_with = "parse_ogc_bbox")]
    pub bbox: OgcBoundingBox,
    #[serde(alias = "FORMAT")]
    pub format: GetMapFormat,
    #[serde(alias = "LAYERS")]
    pub layers: String,
    #[serde(alias = "CRS")]
    pub crs: Option<SpatialReference>,
    #[serde(alias = "STYLES")]
    pub styles: String,
    #[serde(default)]
    #[serde(alias = "TIME")]
    #[serde(deserialize_with = "parse_time_option")]
    pub time: Option<TimeInterval>,
    #[serde(alias = "TRANSPARENT")]
    #[serde(default)]
    #[serde(deserialize_with = "bool_option_case_insensitive")]
    pub transparent: Option<bool>,
    #[serde(alias = "BGCOLOR")]
    pub bgcolor: Option<String>,
    #[serde(alias = "SLD")]
    pub sld: Option<String>,
    #[serde(alias = "SLD_BODY")]
    pub sld_body: Option<String>,
    #[serde(alias = "ELEVATION")]
    pub elevation: Option<String>,
    #[serde(alias = "EXCEPTIONS")]
    pub exceptions: Option<String>, // TODO: parse Option<GetMapExceptionFormat>
                                    // TODO: DIM_<name>
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub enum GetMapExceptionFormat {
    TextXml, // TODO: remaining formats
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub enum GetMapFormat {
    #[serde(rename = "image/png")]
    ImagePng, // TODO: remaining formats
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct GetFeatureInfo {
    pub version: String,
    pub query_layers: String,
    pub info_format: Option<String>, // TODO: parse Option<GetFeatureInfoFormat>,
                                     // TODO: remaining fields
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub enum GetFeatureInfoFormat {
    TextXml, // TODO: remaining formats
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct GetStyles {
    pub version: String,
    pub layer: String,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct GetLegendGraphic {
    pub version: String,
    pub layer: String,
    // TODO: remaining fields
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::spatial_reference::SpatialReference;

    #[test]
    fn deserialize_get_map() {
        let query = "request=GetMap&service=WMS&version=1.3.0&layers=modis_ndvi&bbox=1,2,3,4&width=2&height=2&crs=EPSG:4326&styles=ssss&format=image/png&time=2000-01-01T00:00:00.0Z/2000-01-02T00:00:00.0Z&transparent=true&bgcolor=#000000&sld=sld_spec&sld_body=sld_body&elevation=elevation&exceptions=exceptions";
        let parsed: WmsRequest = serde_urlencoded::from_str(query).unwrap();

        let request = WmsRequest::GetMap(GetMap {
            version: "1.3.0".into(),
            width: 2,
            layers: "modis_ndvi".into(),
            crs: Some(SpatialReference::epsg_4326()),
            styles: "ssss".into(),
            time: Some(TimeInterval::new(946_684_800_000, 946_771_200_000).unwrap()),
            transparent: Some(true),
            bgcolor: Some("#000000".into()),
            sld: Some("sld_spec".into()),
            sld_body: Some("sld_body".into()),
            elevation: Some("elevation".into()),
            bbox: OgcBoundingBox::new(1., 2., 3., 4.),
            height: 2,
            format: GetMapFormat::ImagePng,
            exceptions: Some("exceptions".into()),
        });

        assert_eq!(parsed, request);
    }

    #[test]
    fn deserialize_get_map_not_time() {
        let query = "request=GetMap&service=WMS&version=1.3.0&layers=modis_ndvi&bbox=1,2,3,4&width=2&height=2&crs=EPSG:4326&styles=ssss&format=image/png";
        let parsed: WmsRequest = serde_urlencoded::from_str(query).unwrap();

        let request = WmsRequest::GetMap(GetMap {
            version: "1.3.0".into(),
            width: 2,
            layers: "modis_ndvi".into(),
            crs: SpatialReference::epsg_4326().into(),
            styles: "ssss".into(),
            time: None,
            transparent: None,
            bgcolor: None,
            sld: None,
            sld_body: None,
            elevation: None,
            bbox: OgcBoundingBox::new(1., 2., 3., 4.),
            height: 2,
            format: GetMapFormat::ImagePng,
            exceptions: None,
        });

        assert_eq!(parsed, request);
    }
}
