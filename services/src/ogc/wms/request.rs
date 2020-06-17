use serde::{Deserialize, Serialize};

// TODO: ignore case for field names

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "request")]
#[allow(clippy::pub_enum_variant_names)]
pub enum WMSRequest {
    GetCapabilities(GetCapabilities),
    GetMap(GetMap),
    GetFeatureInfo(GetFeatureInfo),
    GetStyles(GetStyles),
    GetLegendGraphic(GetLegendGraphic),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetCapabilities {
    pub version: Option<String>,
    pub service: String,
    pub format: Option<String>, // TODO: Option<GetCapabilitiesFormat>,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum GetCapabilitiesFormat {
    TextXml //, ...
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetMap {
    pub version: String,
    pub width: String, // TODO: parse u32
    pub height: String, // TODO: parse u32
    pub bbox: String, // TODO: parse BoundingBox2D,
    pub format: String, // TODO: parse GetMapFormat,
    pub layer: String,
    pub crs: String, // TODO: parse CRS
    pub styles: String,
    pub time: Option<String>, // TODO: parse Option<TimeInterval>,
    pub transparent: Option<bool>,
    pub bgcolor: Option<String>,
    pub sld: Option<String>,
    pub sld_body: Option<String>,
    pub elevation: Option<String>,
    pub exceptions: Option<String>, // TODO: parse Option<GetMapExceptionFormat>
    // DIM_<name>
}

#[derive(Debug, Deserialize, Serialize)]
pub enum GetMapExceptionFormat {
    TextXML //, ...
}

#[derive(Debug, Deserialize, Serialize)]
pub enum GetMapFormat {
    ImagePng //, ...
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetFeatureInfo {
    pub version: String,
    pub query_layers: String,
    pub info_format: Option<String>, // TODO: parse Option<GetFeatureInfoFormat>,
    // ...
}

#[derive(Debug, Deserialize, Serialize)]
pub enum GetFeatureInfoFormat {
    TextXML //, ...
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetStyles {
    pub version: String,
    pub layer: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetLegendGraphic {
    pub version: String,
    pub layer: String,
    // ...
}
