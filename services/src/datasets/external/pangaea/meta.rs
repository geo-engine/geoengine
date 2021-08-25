//! This module provides tools to load
//! and parse meta-data for a pangaea dataset

use crate::error::Error;
use async_trait::async_trait;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, Coordinate2D, FeatureDataType,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{
    PreLoadHook, StaticMetaDataWithHook, VectorQueryRectangle, VectorResultDescriptor,
};
use geoengine_operators::source::{
    OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceErrorSpec,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use url::Url;

const DEFAULT_FEATURE_COLUMN_NAME: &str = "spatial_coverage";

/// Holds all relevant meta-data for a pangaea dataset
#[derive(Deserialize, Debug, Clone)]
#[serde(from = "MetaInternal")]
pub struct PangeaMetaData {
    pub title: String,
    pub authors: Vec<Author>,
    pub license: Option<String>,
    pub url: Url,
    pub feature_info: FeatureInfo,
    pub columns: Vec<PangeaParam>,
    pub distributions: Vec<Distribution>,
}

impl PangeaMetaData {
    /// Retrieves the link to the TSV file for the dataset
    fn get_tsv_file(&self) -> Option<&Distribution> {
        for d in &self.distributions {
            if d.encoding == "text/tab-separated-values" {
                return Some(d);
            }
        }
        None
    }

    /// Creates a new tsv header matching the actual column names.
    /// The new header also includes a column for the default geometry,
    /// if applicable.
    fn tsv_header(&self) -> String {
        let mut result = String::new();

        for (idx, p) in self.columns.iter().enumerate() {
            if idx > 0 {
                result.push('\t');
            }
            result.push_str(p.full_name().as_str());
        }

        // Default geometry
        match &self.feature_info {
            FeatureInfo::DefaultPoint { .. } | FeatureInfo::DefaultPolygon { .. } => {
                result.push('\t');
                result.push_str(DEFAULT_FEATURE_COLUMN_NAME);
            }
            _ => {}
        };
        result
    }

    fn get_result_descriptor(&self) -> VectorResultDescriptor {
        let feature_type = match &self.feature_info {
            FeatureInfo::None => VectorDataType::Data,
            FeatureInfo::DefaultPolygon { .. } => VectorDataType::MultiPolygon,
            _ => VectorDataType::MultiPoint,
        };

        let column_map: HashMap<String, FeatureDataType> = self
            .columns
            .iter()
            .map(|param| match param {
                PangeaParam::Numeric { name: n, .. } => (n.clone(), FeatureDataType::Float),
                PangeaParam::String { name: n, .. } => (n.clone(), FeatureDataType::Text),
            })
            .collect();

        VectorResultDescriptor {
            spatial_reference: SpatialReference::epsg_4326().into(),
            data_type: feature_type,
            columns: column_map,
        }
    }

    fn get_column_spec(&self) -> OgrSourceColumnSpec {
        let (x, y) = match &self.feature_info {
            FeatureInfo::Point {
                lat_column: y,
                lon_column: x,
            } => (x.clone(), Some(y.clone())),
            FeatureInfo::DefaultPoint { .. } | FeatureInfo::DefaultPolygon { .. } => {
                (DEFAULT_FEATURE_COLUMN_NAME.to_owned(), None)
            }
            FeatureInfo::None => ("".to_string(), None),
        };

        OgrSourceColumnSpec {
            x,
            y,
            rename: None,
            float: self
                .columns
                .iter()
                .filter(|&c| matches!(c, PangeaParam::Numeric { .. }))
                .map(PangeaParam::full_name)
                .collect(),
            int: vec![],
            text: self
                .columns
                .iter()
                .filter(|&c| matches!(c, PangeaParam::String { .. }))
                .map(PangeaParam::full_name)
                .collect(),
        }
    }

    fn get_ogr_source_ds(&self, memfile_name: &str) -> OgrSourceDataset {
        let path: PathBuf = PathBuf::from(memfile_name);
        let layer = path
            .file_stem()
            .expect("Memfile name requires a file stem")
            .to_str()
            .expect("Memfile name must be valid unicode")
            .to_string();

        OgrSourceDataset {
            file_name: memfile_name.into(),
            layer_name: layer,
            data_type: match &self.feature_info {
                FeatureInfo::None => None,
                FeatureInfo::DefaultPolygon { .. } => Some(VectorDataType::MultiPolygon),
                _ => Some(VectorDataType::MultiPoint),
            },
            time: OgrSourceDatasetTimeType::None,
            columns: Some(self.get_column_spec()),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Abort,
            sql_query: None,
            attribute_query: None,
        }
    }

    /// Retrieves the meta data required by the ogr source.
    pub fn get_ogr_metadata(
        &self,
        client: Client,
    ) -> Result<
        StaticMetaDataWithHook<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        Error,
    > {
        let memfile_name = format!("/vsimem/pangaea{}.tsv", self.url.path());
        let url = self.get_tsv_file().ok_or(Error::PangaeaNoTsv)?.url.clone();

        let omd = self.get_ogr_source_ds(memfile_name.as_str());
        Ok(StaticMetaDataWithHook {
            loading_info: omd,
            result_descriptor: self.get_result_descriptor(),
            phantom: Default::default(),
            pre_load_hook: Box::new(LoadHook::new(url, client, memfile_name, self.clone())),
        })
    }
}

/// This `PreLoadHook` downloads the entire tsv, strips the leading comment section,
/// replaces the header and stores all content in a gdal memfile.
/// The memfile lives as long as a clone of this hook exists.
#[derive(Clone, Debug)]
struct LoadHook {
    handle: Arc<Mutex<Option<MemfileHandle>>>,
    url: String,
    client: Client,
    memfile_name: String,
    meta: PangeaMetaData,
}

impl LoadHook {
    fn new(url: String, client: Client, memfile_name: String, meta: PangeaMetaData) -> LoadHook {
        LoadHook {
            handle: Arc::new(Mutex::new(None)),
            url,
            client,
            memfile_name,
            meta,
        }
    }

    /// Processes tsv data received from Pangaea.
    /// First, comments are stripped. Then the csv header
    /// is replaced with a generated header that matches the meta-data's field
    /// names. Finally, if a default geometry is provided, its WKT is appended
    /// to each line of the tsv.
    fn process_tsv(&self, mut input: String) -> String {
        // Strip comment
        if let Some(idx) = input.find("*/\n") {
            input.replace_range(0..idx + 3, "");
        }

        // Get WKT of default geometries, if any
        let wkt = match &self.meta.feature_info {
            FeatureInfo::DefaultPolygon { wkt } | FeatureInfo::DefaultPoint { wkt } => {
                Some(wkt.clone())
            }
            _ => None,
        };

        // Generate new tsv header
        let header = self.meta.tsv_header();

        let output = match wkt {
            // No or included geometry
            None => {
                // Replace with generated csv header
                if let Some(idx) = input.find('\n') {
                    input.replace_range(0..idx, header.as_str());
                }
                input
            }
            // Append default geometry
            Some(wkt) => {
                let mut output: String = String::with_capacity(input.len());
                // Write output header
                output.push_str(header.as_str());
                output.push('\n');

                let mut i = input.lines();
                // Discard source header
                if i.next().is_some() {
                    // Write data
                    for line in i {
                        output.push_str(line);
                        output.push('\t');
                        output.push_str(wkt.as_str());
                        output.push('\n');
                    }
                }
                output
            }
        };
        output
    }
}

#[async_trait]
impl PreLoadHook for LoadHook {
    async fn execute(&self) -> Result<(), geoengine_operators::error::Error> {
        if self.handle.lock().unwrap().is_some() {
            return Ok(());
        }

        // Execute request, read source tsv and process it.
        let tsv = self.process_tsv(
            self.client
                .get(&self.url)
                .send()
                .await
                .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                })?
                .text()
                .await
                .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                })?,
        );

        // Create the mem-file
        let mut lock = self.handle.lock().unwrap();
        if lock.is_none() {
            lock.replace(MemfileHandle::new(
                self.memfile_name.clone(),
                tsv.into_bytes(),
            )?);
        }
        Ok(())
    }

    fn box_clone(&self) -> Box<dyn PreLoadHook> {
        Box::new(self.clone())
    }
}

/// This struct simply holds the name of a generated gdal memfile and releases
/// the file on drop.
#[derive(Debug)]
struct MemfileHandle {
    memfile_name: String,
}

impl MemfileHandle {
    fn new(
        memfile_name: String,
        data: Vec<u8>,
    ) -> Result<MemfileHandle, geoengine_operators::error::Error> {
        gdal::vsi::create_mem_file(memfile_name.as_str(), data)?;
        Ok(MemfileHandle { memfile_name })
    }
}

impl Drop for MemfileHandle {
    fn drop(&mut self) {
        if gdal::vsi::unlink_mem_file(self.memfile_name.as_str()).is_err() {
            // TODO: Silently swallow or log somewhere?
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
                name: format!("{} {}", name, suffix),
                description,
                unit,
            },
            PangeaParam::String { name, description } => PangeaParam::String {
                name: format!("{} {}", name, suffix),
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
    family_name: String,
    given_name: String,
}

/// Describes the spatial property of a dataset
#[derive(Debug, Clone)]
pub enum FeatureInfo {
    None,
    Point {
        lat_column: String,
        lon_column: String,
    },
    DefaultPoint {
        wkt: String,
    },
    DefaultPolygon {
        wkt: String,
    },
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

use serde::de;
use serde::de::Unexpected;
use std::fmt;

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
            } => FeatureInfo::DefaultPoint {
                wkt: format!("POINT ( {} {} )", longitude, latitude),
            },
            FeatureHelper::Box { bbox } => FeatureInfo::DefaultPolygon {
                wkt: format!(
                    "POLYGON (( {} {}, {} {}, {} {}, {} {}, {} {} ))",
                    bbox.lower_left().x,
                    bbox.lower_left().y,
                    bbox.lower_right().x,
                    bbox.lower_right().y,
                    bbox.upper_right().x,
                    bbox.upper_right().y,
                    bbox.upper_left().x,
                    bbox.upper_left().y,
                    bbox.lower_left().x,
                    bbox.lower_left().y,
                ),
            },
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

impl From<MetaInternal> for PangeaMetaData {
    fn from(meta: MetaInternal) -> Self {
        // Detect coordinate columns
        let lat = meta
            .params
            .iter()
            .find(|&param| param.name().to_ascii_lowercase().contains("latitude"));

        let lon = meta
            .params
            .iter()
            .find(|&param| param.name().to_ascii_lowercase().contains("longitude"));

        let feature = if let (Some(lat), Some(lon)) = (lat, lon) {
            FeatureInfo::Point {
                lat_column: lat.full_name(),
                lon_column: lon.full_name(),
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

        PangeaMetaData {
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
    use crate::datasets::external::pangaea::meta::{FeatureInfo, PangeaMetaData};
    use crate::datasets::external::pangaea::tests::test_data_path;
    use std::fs::OpenOptions;

    fn load_meta(file_name: &str) -> serde_json::error::Result<PangeaMetaData> {
        let f = OpenOptions::new()
            .read(true)
            .open(test_data_path(file_name).as_path())
            .unwrap();
        serde_json::from_reader::<_, PangeaMetaData>(f)
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
