//! This module provides tools to load
//! and parse meta-data for a pangaea dataset

use crate::error::Error;
use futures::StreamExt;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, Coordinate2D, FeatureDataType, Measurement, MultiPoint,
    MultiPolygon, TypedGeometry, VectorQueryRectangle,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{StaticMetaData, VectorColumnInfo, VectorResultDescriptor};
use geoengine_operators::source::{
    CsvHeader, FormatSpecifics, OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType,
    OgrSourceErrorSpec,
};
use reqwest::Client;
use serde::de;
use serde::de::Unexpected;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::io::ErrorKind;
use std::path::PathBuf;
use url::Url;

/// Holds all relevant meta-data for a pangaea dataset
#[derive(Deserialize, Debug, Clone)]
#[serde(from = "MetaInternal")]
pub struct PangaeaMetaData {
    pub title: String,
    pub authors: Vec<Author>,
    pub license: Option<String>,
    pub url: Url,
    pub feature_info: FeatureInfo,
    pub columns: Vec<PangeaParam>,
    pub distributions: Vec<Distribution>,
}

impl PangaeaMetaData {
    /// Retrieves the link to the TSV file for the dataset
    fn get_tsv_file(&self) -> Option<&Distribution> {
        self.distributions
            .iter()
            .find(|&d| d.encoding == "text/tab-separated-values")
    }

    fn get_result_descriptor(&self) -> VectorResultDescriptor {
        let feature_type = match &self.feature_info {
            FeatureInfo::None => VectorDataType::Data,
            FeatureInfo::DefaultPolygon { .. } => VectorDataType::MultiPolygon,
            _ => VectorDataType::MultiPoint,
        };

        let column_map: HashMap<String, VectorColumnInfo> = self
            .columns
            .iter()
            .map(|param| match param {
                PangeaParam::Numeric { .. } => (
                    param.full_name(),
                    VectorColumnInfo {
                        data_type: FeatureDataType::Float,
                        measurement: Measurement::Unitless, // TOOD: get measurement
                    },
                ),
                PangeaParam::String { .. } => (
                    param.full_name(),
                    VectorColumnInfo {
                        data_type: FeatureDataType::Text,
                        measurement: Measurement::Unitless,
                    },
                ),
            })
            .collect();

        VectorResultDescriptor {
            spatial_reference: SpatialReference::epsg_4326().into(),
            data_type: feature_type,
            columns: column_map,
            time: None, // TODO: determine time
            bbox: None, // TODO: determine bbox
        }
    }

    fn get_column_spec(&self) -> OgrSourceColumnSpec {
        let (x, y) = match &self.feature_info {
            FeatureInfo::Point {
                lat_column: y,
                lon_column: x,
            } => (x.clone(), Some(y.clone())),
            _ => (String::new(), None),
        };

        // Create name mapping
        let rename: HashMap<String, String> = self
            .columns
            .iter()
            .enumerate()
            .map(|(idx, p)| (format!("field_{}", (idx + 1)), p.full_name()))
            .collect();

        OgrSourceColumnSpec {
            format_specifics: Some(FormatSpecifics::Csv {
                header: CsvHeader::No,
            }),
            x,
            y,
            rename: Some(rename),
            float: self
                .columns
                .iter()
                .enumerate()
                .filter(|(_, p)| matches!(p, PangeaParam::Numeric { .. }))
                .map(|(idx, _)| format!("field_{}", (idx + 1)))
                .collect(),
            int: vec![],
            text: self
                .columns
                .iter()
                .enumerate()
                .filter(|(_, p)| matches!(p, PangeaParam::String { .. }))
                .map(|(idx, _)| format!("field_{}", (idx + 1)))
                .collect(),
            bool: vec![],
            datetime: vec![],
        }
    }

    fn get_ogr_source_ds(&self, url: &str) -> OgrSourceDataset {
        let default_geometry = match &self.feature_info {
            FeatureInfo::DefaultPolygon(p) => Some(TypedGeometry::MultiPolygon(p.clone())),
            FeatureInfo::DefaultPoint(p) => Some(TypedGeometry::MultiPoint(p.clone())),
            _ => None,
        };

        OgrSourceDataset {
            file_name: PathBuf::from(url),
            layer_name: "PANGAEA".into(),
            data_type: match &self.feature_info {
                FeatureInfo::None => None,
                FeatureInfo::DefaultPolygon { .. } => Some(VectorDataType::MultiPolygon),
                _ => Some(VectorDataType::MultiPoint),
            },
            time: OgrSourceDatasetTimeType::None,
            default_geometry,
            columns: Some(self.get_column_spec()),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Abort,
            sql_query: None,
            attribute_query: None,
        }
    }

    async fn begin_of_data(client: &Client, url: &str) -> Result<usize, Error> {
        let mut stream = client.get(url).send().await?.bytes_stream();
        let mut state = TSVParseState::Initial;
        let mut buf = Vec::new();

        loop {
            state = match state.proceed(&mut buf) {
                TSVParseState::MoreData(s) => match stream.next().await {
                    Some(bytes) => {
                        buf.append(&mut bytes?.to_vec());
                        s.proceed(&mut buf)
                    }
                    _ => {
                        return Err(Error::Io {
                            source: std::io::Error::new(
                                ErrorKind::UnexpectedEof,
                                "End of Pangea TSV reached unexpectedly.",
                            ),
                        })
                    }
                },
                TSVParseState::DataStart(idx) => {
                    return Ok(idx);
                }
                s => s,
            }
        }
    }

    /// Retrieves the meta data required by the ogr source.
    pub async fn get_ogr_metadata(
        &self,
        client: &Client,
    ) -> Result<StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>, Error>
    {
        let url = self.get_tsv_file().ok_or(Error::PangaeaNoTsv)?.url.as_str();
        let offset = PangaeaMetaData::begin_of_data(client, url).await?;
        let url = format!("CSV:/vsisubfile/{offset},/vsicurl_streaming/{url}");

        let omd = self.get_ogr_source_ds(url.as_str());
        Ok(StaticMetaData {
            loading_info: omd,
            result_descriptor: self.get_result_descriptor(),
            phantom: Default::default(),
        })
    }
}

enum TSVParseState {
    Initial,
    CommentStart,
    CommentEnd(usize),
    HeaderStart(usize),
    DataStart(usize),
    MoreData(Box<TSVParseState>),
}

impl TSVParseState {
    fn proceed(self, buf: &mut Vec<u8>) -> TSVParseState {
        match self {
            TSVParseState::Initial => match buf.windows(2).position(|chars| chars == b"/*") {
                Some(0) => TSVParseState::CommentStart,
                None if buf.len() >= 2 => TSVParseState::HeaderStart(0),
                _ => TSVParseState::MoreData(Box::new(self)),
            },
            TSVParseState::CommentStart => match buf.windows(2).position(|chars| chars == b"*/") {
                Some(idx) => TSVParseState::CommentEnd(idx),
                _ => TSVParseState::MoreData(Box::new(self)),
            },
            TSVParseState::CommentEnd(ce) => {
                match buf.as_slice()[ce..].iter().position(|&char| char == b'\n') {
                    Some(idx) => TSVParseState::HeaderStart(ce + idx + 1),
                    _ => TSVParseState::MoreData(Box::new(self)),
                }
            }
            TSVParseState::HeaderStart(h) => {
                match buf.as_slice()[h..].iter().position(|&char| char == b'\n') {
                    Some(idx) => TSVParseState::DataStart(h + idx + 1),
                    _ => TSVParseState::MoreData(Box::new(self)),
                }
            }
            TSVParseState::DataStart(_) => self,
            TSVParseState::MoreData(inner) => inner.proceed(buf),
        }
    }
}

/// This corresponds to one attribute in a dataset
#[derive(Deserialize, Debug, Clone)]
#[serde(untagged, rename_all = "camelCase")]
pub enum PangeaParam {
    Numeric {
        name: String,
        description: Option<String>,
        #[serde(rename = "unitText")]
        unit: String,
    },
    String {
        name: String,
        description: Option<String>,
    },
}

impl PangeaParam {
    pub fn full_name(&self) -> String {
        match self {
            PangeaParam::Numeric {
                name: n,
                description: Some(d),
                unit: u,
            } => {
                format!("{} ({}) [{}]", n.as_str(), d.as_str(), u.as_str())
            }
            PangeaParam::Numeric {
                name: n,
                description: None,
                unit: u,
            } => {
                format!("{} [{}]", n.as_str(), u.as_str())
            }
            PangeaParam::String {
                name: n,
                description: Some(d),
            } => {
                format!("{} ({})", n.as_str(), d.as_str())
            }
            PangeaParam::String {
                name: n,
                description: None,
            } => n.clone(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            PangeaParam::Numeric { name: n, .. } | PangeaParam::String { name: n, .. } => {
                n.as_str()
            }
        }
    }

    /// Consume this parameter and returns an identical one with
    /// the name composed of the original name and the given suffix.
    fn with_new_name(self, suffix: &str) -> Self {
        match self {
            PangeaParam::Numeric {
                name,
                description,
                unit,
            } => PangeaParam::Numeric {
                name: format!("{name} {suffix}"),
                description,
                unit,
            },
            PangeaParam::String { name, description } => PangeaParam::String {
                name: format!("{name} {suffix}"),
                description,
            },
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Distribution {
    #[serde(rename = "encodingFormat")]
    pub encoding: String,
    #[serde(rename = "contentUrl")]
    pub url: String,
}

/// Represents one author of a dataset
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Author {
    pub family_name: String,
    pub given_name: String,
}

/// Describes the spatial property of a dataset
#[derive(Debug, Clone)]
pub enum FeatureInfo {
    None,
    Point {
        lat_column: String,
        lon_column: String,
    },
    DefaultPoint(MultiPoint),
    DefaultPolygon(MultiPolygon),
}

// Helper structs start here

#[derive(Deserialize)]
#[serde(untagged)]
enum ObjectOrArray<T> {
    Object(T),
    Array(Vec<T>),
}

impl<T> From<ObjectOrArray<T>> for Vec<T> {
    fn from(x: ObjectOrArray<T>) -> Self {
        match x {
            ObjectOrArray::Object(v) => vec![v],
            ObjectOrArray::Array(v) => v,
        }
    }
}

/// This helper is used to map the pangaea
/// data structure to our internal representation
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "@type")]
enum FeatureHelper {
    #[serde(rename = "GeoShape")]
    Box {
        #[serde(rename = "box", deserialize_with = "FeatureHelper::parse_coords")]
        bbox: BoundingBox2D,
    },
    #[serde(rename = "GeoCoordinates")]
    Point { longitude: f64, latitude: f64 },
    #[serde(other)]
    None,
}

impl FeatureHelper {
    fn parse_coords<'de, D>(deserializer: D) -> Result<BoundingBox2D, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct V;

        impl<'de> de::Visitor<'de> for V {
            type Value = BoundingBox2D;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str(
                    "4 whitespace separated floating point values: \"y1:f64 x1:f64 y2:f64 x2:f64\"",
                )
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let mut floats = Vec::with_capacity(4);

                for v in value.split_ascii_whitespace() {
                    floats
                        .push(v.parse::<f64>().map_err(|_| {
                            de::Error::invalid_value(Unexpected::Str(value), &self)
                        })?);
                }

                if floats.len() == 4 {
                    BoundingBox2D::new(
                        Coordinate2D {
                            x: floats[1],
                            y: floats[0],
                        },
                        Coordinate2D {
                            x: floats[3],
                            y: floats[2],
                        },
                    )
                    .map_err(|_| de::Error::invalid_value(Unexpected::Str(value), &self))
                } else {
                    Err(de::Error::invalid_value(Unexpected::Str(value), &self))
                }
            }
        }
        deserializer.deserialize_any(V)
    }

    fn to_feature_info(&self) -> FeatureInfo {
        match self {
            FeatureHelper::None => FeatureInfo::None,
            FeatureHelper::Point {
                longitude,
                latitude,
            } => FeatureInfo::DefaultPoint(
                MultiPoint::new(vec![Coordinate2D::new(*longitude, *latitude)])
                    .expect("Impossible"),
            ),
            FeatureHelper::Box { bbox } => FeatureInfo::DefaultPolygon(
                MultiPolygon::new(vec![vec![vec![
                    bbox.lower_left(),
                    bbox.lower_right(),
                    bbox.upper_right(),
                    bbox.upper_left(),
                    bbox.lower_left(),
                ]]])
                .expect("Impossible"),
            ),
        }
    }
}

/// This helper is used to map the pangaea
/// data structure to our internal representation
#[derive(Serialize, Deserialize, Debug)]
struct CoverageHelper {
    geo: FeatureHelper,
}

impl Default for CoverageHelper {
    fn default() -> Self {
        CoverageHelper {
            geo: FeatureHelper::None,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetaInternal {
    #[serde(rename = "name")]
    title: String,

    #[serde(rename = "creator")]
    authors: ObjectOrArray<Author>,

    license: Option<String>,
    url: Url,

    #[serde(default)]
    spatial_coverage: CoverageHelper,

    #[serde(rename = "variableMeasured")]
    params: Vec<PangeaParam>,

    #[serde(rename = "distribution")]
    distributions: ObjectOrArray<Distribution>,
}

impl From<MetaInternal> for PangaeaMetaData {
    fn from(meta: MetaInternal) -> Self {
        // Detect coordinate columns
        let lat = meta
            .params
            .iter()
            .enumerate()
            .find(|(_, param)| param.name().to_ascii_lowercase().contains("latitude"))
            .map(|(idx, _)| idx);

        let lon = meta
            .params
            .iter()
            .enumerate()
            .find(|(_, param)| param.name().to_ascii_lowercase().contains("longitude"))
            .map(|(idx, _)| idx);

        let feature = if let (Some(lat), Some(lon)) = (lat, lon) {
            FeatureInfo::Point {
                lat_column: format!("field_{}", (lat + 1)),
                lon_column: format!("field_{}", (lon + 1)),
            }
        } else {
            meta.spatial_coverage.geo.to_feature_info()
        };

        // handle name clashes
        let mut columns: Vec<PangeaParam> = vec![];

        let mut m = HashMap::new();
        for p in meta.params {
            let k = p.name();
            let count = m.entry(k.to_string()).or_insert_with(|| 0_u32);
            if *count == 0 {
                columns.push(p);
            } else {
                columns.push(p.with_new_name(count.to_string().as_str()));
            }
            *count += 1;
        }

        PangaeaMetaData {
            title: meta.title,
            authors: meta.authors.into(),
            license: meta.license,
            url: meta.url,
            feature_info: feature,
            columns,
            distributions: meta.distributions.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::datasets::external::pangaea::meta::{FeatureInfo, PangaeaMetaData};
    use crate::datasets::external::pangaea::tests::test_data_path;
    use std::fs::OpenOptions;

    fn load_meta(file_name: &str) -> serde_json::error::Result<PangaeaMetaData> {
        let f = OpenOptions::new()
            .read(true)
            .open(test_data_path(file_name).as_path())
            .unwrap();
        serde_json::from_reader::<_, PangaeaMetaData>(f)
    }

    #[test]
    fn test_meta_point() {
        let md = load_meta("pangaea_geo_point_meta.json").unwrap();
        match &md.feature_info {
            FeatureInfo::DefaultPoint { .. } => {}
            _ => panic!("Expected to find FeatureInfo::DefaultPoint"),
        }
    }

    #[test]
    fn test_meta_none() {
        let md = load_meta("pangaea_geo_none_meta.json").unwrap();
        match &md.feature_info {
            FeatureInfo::None => {}
            _ => panic!("Expected to find FeatureInfo::None"),
        }
    }

    #[test]
    fn test_meta_box() {
        let md = load_meta("pangaea_geo_box_meta.json").unwrap();
        match &md.feature_info {
            FeatureInfo::DefaultPolygon { .. } => {}
            _ => panic!("Expected to find FeatureInfo::DefaultPolygon"),
        }
    }

    #[test]
    fn test_meta_box_invalid_coord() {
        assert!(load_meta("pangaea_geo_box_meta_invalid_coord.json").is_err());
    }

    #[test]
    fn test_meta_box_missing_coord() {
        assert!(load_meta("pangaea_geo_box_meta_missing_coord.json").is_err());
    }

    #[test]
    fn test_meta_lat_lon() {
        let md = load_meta("pangaea_geo_lat_lon_meta.json").unwrap();
        match &md.feature_info {
            FeatureInfo::Point { .. } => {}
            _ => panic!("Expected to find FeatureInfo::Point"),
        }
    }

    #[test]
    fn test_meta_single_creator() {
        let md = load_meta("pangaea_single_creator_meta.json").unwrap();
        assert_eq!(1, md.authors.len());
    }
}
