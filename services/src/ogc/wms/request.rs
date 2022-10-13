use crate::api::model::datatypes::{SpatialReference, TimeInterval};
use crate::ogc::util::{parse_ogc_bbox, parse_time_option, OgcBoundingBox};
use crate::util::{bool_option_case_insensitive, from_str};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
pub enum WmsService {
    #[serde(rename = "WMS")]
    Wms,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
pub enum WmsVersion {
    #[serde(rename = "1.3.0")]
    V1_3_0,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, IntoParams, ToSchema)]
pub struct GetCapabilities {
    #[serde(alias = "VERSION")]
    pub version: Option<WmsVersion>,
    #[serde(alias = "SERVICE")]
    pub service: WmsService,
    #[serde(alias = "REQUEST")]
    pub request: GetCapabilitiesRequest,
    #[serde(alias = "FORMAT")]
    pub format: Option<GetCapabilitiesFormat>,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
pub enum GetCapabilitiesRequest {
    GetCapabilities,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
pub enum GetCapabilitiesFormat {
    #[serde(rename = "text/xml")]
    TextXml, // TODO: remaining formats
}

// TODO: remove serde aliases and use serde-aux and case insensitive keys
#[derive(PartialEq, Debug, Deserialize, Serialize, IntoParams, ToSchema)]
pub struct GetMap {
    #[serde(alias = "VERSION")]
    pub version: WmsVersion,
    #[serde(alias = "SERVICE")]
    pub service: WmsService,
    #[serde(alias = "WIDTH")]
    #[serde(deserialize_with = "from_str")]
    #[param(example = 256)]
    pub width: u32,
    #[serde(alias = "HEIGHT")]
    #[serde(deserialize_with = "from_str")]
    #[param(example = 256)]
    pub height: u32,
    #[serde(alias = "BBOX")]
    #[serde(deserialize_with = "parse_ogc_bbox")]
    #[param(example = "-90,-180,90,180")]
    pub bbox: OgcBoundingBox,
    #[serde(alias = "FORMAT")]
    pub format: GetMapFormat,
    #[serde(alias = "LAYERS")]
    pub layers: String,
    #[serde(alias = "CRS")]
    #[param(example = "EPSG:4326")]
    pub crs: Option<SpatialReference>,
    #[serde(alias = "STYLES")]
    #[param(
        example = r#"custom:{"type":"linearGradient","breakpoints":[{"value":1,"color":[0,0,0,255]},{"value":255,"color":[255,255,255,255]}],"noDataColor":[0,0,0,0],"defaultColor":[0,0,0,0]}"#
    )]
    pub styles: String,
    #[serde(default)]
    #[serde(alias = "TIME")]
    #[serde(deserialize_with = "parse_time_option")]
    #[param(example = "2014-04-01T12:00:00.000Z")]
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
    pub exceptions: Option<GetMapExceptionFormat>, // TODO: parse Option<GetMapExceptionFormat>
                                                   // TODO: DIM_<name>
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
pub enum GetMapExceptionFormat {
    #[serde(rename = "application/json")]
    ApplicationJson, // TODO: remaining formats
}

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, ToSchema)]
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

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, IntoParams, ToSchema)]
pub struct GetLegendGraphic {
    pub version: Option<WmsVersion>,
    pub layer: String,
    // TODO: remaining fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_get_map() {
        let query = "request=GetMap&service=WMS&version=1.3.0&layers=modis_ndvi&bbox=1,2,3,4&width=2&height=2&crs=EPSG:4326&styles=ssss&format=image/png&time=2000-01-01T00:00:00.0Z/2000-01-02T00:00:00.0Z&transparent=true&bgcolor=#000000&sld=sld_spec&sld_body=sld_body&elevation=elevation&exceptions=application/json";
        let parsed: GetMap = serde_urlencoded::from_str(query).unwrap();

        let request = GetMap {
            service: WmsService::Wms,
            version: WmsVersion::V1_3_0,
            width: 2,
            layers: "modis_ndvi".into(),
            crs: Some(geoengine_datatypes::spatial_reference::SpatialReference::epsg_4326().into()),
            styles: "ssss".into(),
            time: Some(
                geoengine_datatypes::primitives::TimeInterval::new(
                    946_684_800_000,
                    946_771_200_000,
                )
                .unwrap()
                .into(),
            ),
            transparent: Some(true),
            bgcolor: Some("#000000".into()),
            sld: Some("sld_spec".into()),
            sld_body: Some("sld_body".into()),
            elevation: Some("elevation".into()),
            bbox: OgcBoundingBox::new(1., 2., 3., 4.),
            height: 2,
            format: GetMapFormat::ImagePng,
            exceptions: Some(GetMapExceptionFormat::ApplicationJson),
        };

        assert_eq!(parsed, request);
    }

    #[test]
    fn deserialize_get_map_not_time() {
        let query = "request=GetMap&service=WMS&version=1.3.0&layers=modis_ndvi&bbox=1,2,3,4&width=2&height=2&crs=EPSG:4326&styles=ssss&format=image/png";
        let parsed: GetMap = serde_urlencoded::from_str(query).unwrap();

        let request = GetMap {
            service: WmsService::Wms,
            version: WmsVersion::V1_3_0,
            width: 2,
            layers: "modis_ndvi".into(),
            crs: Some(geoengine_datatypes::spatial_reference::SpatialReference::epsg_4326().into()),
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
        };

        assert_eq!(parsed, request);
    }
}
