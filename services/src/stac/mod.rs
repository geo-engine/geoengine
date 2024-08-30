use crate::util::parsing::{deserialize_as_f64_opt, deserialize_as_string};
use geo::Rect;
use geoengine_datatypes::primitives::DateTime;
use serde::{de::value::MapDeserializer, de::Error, Deserialize, Deserializer};
use serde_with::with_prefix;
use snafu::Snafu;
use std::{collections::HashMap, convert::TryFrom};

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct FeatureCollection {
    pub stac_version: String,
    pub stac_extensions: Option<Vec<String>>,
    pub context: Option<Context>,
    pub features: Vec<Feature>,
    pub links: Vec<Link>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct Context {
    pub page: u64,
    pub limit: u64,
    pub matched: u64,
    pub returned: u64,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct Link {
    pub rel: String,
    pub title: Option<String>,
    pub method: Option<String>,
    pub r#type: Option<String>,
    pub href: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Feature {
    // geo_type: String,
    pub stac_version: String,
    pub stac_extensions: Vec<String>,
    pub id: String,
    pub bbox: geo::Rect<f64>,
    pub geometry: geo::Geometry<f64>,
    pub properties: Properties,
    pub assets: HashMap<String, StacAsset>,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct Properties {
    pub datetime: DateTime,
    pub platform: String,
    pub constellation: Option<String>,
    pub instruments: Option<Vec<String>>,
    pub gsd: Option<f64>,
    #[serde(rename = "view:off_nadir")]
    pub view_off_nadir: Option<f64>,
    #[serde(rename = "proj:epsg")]
    pub proj_epsg: Option<u32>,
    #[serde(flatten, with = "prefix_sentinel")]
    pub sentinel: Option<SentinelProperties>,
    #[serde(
        rename = "eo:cloud_cover",
        default,
        deserialize_with = "deserialize_as_f64_opt"
    )]
    pub eo_cloud_cover: Option<f64>,
    pub created: DateTime,
    pub updated: DateTime,
    pub collection: Option<String>,

    #[serde(alias = "raster:data_type")]
    pub data_type: Option<String>,
    #[serde(alias = "raster:nodata")]
    pub nodata: Option<String>,
}

with_prefix!(prefix_sentinel "sentinel:");

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct SentinelProperties {
    utm_zone: u32,
    latitude_band: String,
    grid_square: String,
    sequence: String,
    data_coverage: Option<f32>, // TODO: is this really optional? Element84 S2 COGS seems to not always have this
    valid_cloud_cover: bool,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct EoBand {
    #[serde(deserialize_with = "deserialize_as_string")]
    name: String,
    common_name: Option<String>,
    center_wavelenght: Option<f64>,
    full_width_half_max: Option<f64>,

    #[serde(alias = "enmap:gain_of_band")]
    gain_of_band: Option<f64>,
    #[serde(alias = "enmap:offset_of_band")]
    offset_of_band: Option<f64>,
}

/// Cf. [`https://github.com/stac-extensions/raster`]
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct StacRasterBand {
    data_type: String,
    scale: f64,
    offset: f64,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct StacAsset {
    pub href: String,
    pub title: String,
    #[serde(rename = "type")]
    pub mime_type: String,
    pub roles: Option<Vec<String>>,

    pub gsd: Option<f64>,

    #[serde(rename = "eo:bands")]
    pub eo_bands: Option<Vec<EoBand>>,
    #[serde(rename = "raster:bands")]
    pub raster_bands: Option<Vec<StacRasterBand>>,

    #[serde(rename = "proj:shape")]
    pub proj_shape: Option<[u32; 2]>,
    #[serde(rename = "proj:transform")]
    pub proj_transform: Option<[f64; 9]>,
    #[serde(rename = "proj:epsg")]
    pub proj_epsg: Option<u32>,
}

impl StacAsset {
    pub fn gdal_geotransform(&self) -> Option<[f64; 6]> {
        self.proj_transform
            .map(|t| [t[2], t[0], t[1], t[5], t[3], t[4]])
    }

    pub fn native_bbox(&self) -> Option<Rect<f64>> {
        match (self.proj_shape, self.gdal_geotransform()) {
            (Some([shape_x, shape_y]), Some(transform)) => {
                let ul_x = transform[0];
                let ul_y = transform[3];
                let lr_x = transform[0]
                    + f64::from(shape_x) * transform[1]
                    + f64::from(shape_y) * transform[2];
                let lr_y = transform[3]
                    + f64::from(shape_x) * transform[4]
                    + f64::from(shape_y) * transform[5];
                Some(Rect::new((ul_x, lr_y), (lr_x, ul_y)))
            }
            (_, _) => None,
        }
    }
}

impl<'de> Deserialize<'de> for Feature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        geojson::Feature::deserialize(deserializer)
            .map_err(|_| StacError::Deserialization)
            .and_then(Feature::try_from)
            .map_err(D::Error::custom)
    }
}

impl TryFrom<serde_json::Value> for Feature {
    type Error = StacError;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        geojson::Feature::from_json_value(value)
            .map_err(StacError::from)
            .and_then(Feature::try_from)
    }
}

impl TryFrom<geojson::Feature> for Feature {
    type Error = StacError;

    fn try_from(value: geojson::Feature) -> Result<Self, Self::Error> {
        let id = value.id.ok_or(StacError::MissingRequiredField {
            field_name: "id".to_owned(),
        })?;

        let id = match id {
            geojson::feature::Id::String(s) => s,
            geojson::feature::Id::Number(n) => n.to_string(),
        }; // TODO: maybe hava a real id field.

        let bbox: geo::Rect<f64> = value
            .bbox
            .ok_or(StacError::MissingRequiredField {
                field_name: "bbox".to_owned(),
            })
            .and_then(|b| {
                if b.len() == 4 {
                    Ok(geo::Rect::new((b[0], b[1]), (b[2], b[3])))
                } else {
                    Err(StacError::InvalidBoundingBox)
                }
            })?;

        let geometry: geo::Geometry<f64> = value
            .geometry
            .ok_or(StacError::MissingRequiredField {
                field_name: "geometry".to_owned(),
            })
            .and_then(|g| geo::Geometry::try_from(g).map_err(Into::into))?;

        let props = value.properties.ok_or(StacError::MissingRequiredField {
            field_name: "properties".to_owned(),
        })?;
        let properties: Properties =
            Properties::deserialize(MapDeserializer::new(props.into_iter()))?;

        if let Some(foreign_members) = value.foreign_members {
            let ass = foreign_members.get("assets").map(Clone::clone).ok_or(
                StacError::MissingRequiredField {
                    field_name: "assets".to_owned(),
                },
            )?;
            let assets: HashMap<String, StacAsset> = serde_json::from_value(ass)?;

            let stac_version = foreign_members
                .get("stac_version")
                .map(Clone::clone)
                .ok_or(StacError::MissingRequiredField {
                    field_name: "stac_version".to_owned(),
                })?;

            let stac_version: String = serde_json::from_value(stac_version)?;

            let stac_extensions = foreign_members
                .get("stac_extensions")
                .map(Clone::clone)
                .ok_or(StacError::MissingRequiredField {
                    field_name: "stac_extensions".to_owned(),
                })?;

            let stac_extensions: Vec<String> = serde_json::from_value(stac_extensions)?;

            return Ok(Feature {
                stac_version,
                stac_extensions,
                id,
                bbox,
                geometry,
                properties,
                assets,
            });
        }

        Err(StacError::MissingRequiredField {
            field_name: "foreign_members".to_string(),
        })
    }
}

#[derive(Debug, Snafu)]
pub enum StacError {
    MissingRequiredField {
        field_name: String,
    },
    InvalidBoundingBox,
    GeoJsonError {
        source: Box<geojson::Error>,
    },
    #[snafu(display("Error parsing json response: {source}"))]
    SerdeJsonError {
        source: serde_json::Error,
    },
    Deserialization,
}

impl From<geojson::Error> for StacError {
    fn from(e: geojson::Error) -> Self {
        StacError::GeoJsonError {
            source: Box::new(e),
        }
    }
}

impl From<serde_json::Error> for StacError {
    fn from(e: serde_json::Error) -> Self {
        StacError::SerdeJsonError { source: e }
    }
}
