use crate::datasets::listing::Provenance;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::primitives::{
    FeatureDataType, Measurement, TimeGranularity, TimeInterval,
};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::source::GdalDatasetGeoTransform;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

pub const METADATA_KEY: &str = "geoengine";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GEMetadata {
    pub crs: Option<SpatialReference>,
    pub data_type: DataType,
    pub provenance: Option<Vec<Provenance>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum DataType {
    SingleVectorFile(VectorInfo),
    SingleRasterFile(RasterInfo),
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::SingleVectorFile(_) => "SingleVectorFile",
            Self::SingleRasterFile(_) => "SingleRasterFile",
        };
        write!(f, "{name}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum TemporalExtend {
    Instant {
        attribute: String,
    },
    Interval {
        attribute_start: String,
        attribute_end: String,
    },
    Duration {
        attribute_start: String,
        attribute_duration: String,
        unit: TimeGranularity,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VectorInfo {
    pub vector_type: VectorDataType,
    pub layer_name: String,
    pub attributes: Vec<Attribute>,
    pub temporal_extend: Option<TemporalExtend>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Attribute {
    pub name: String,
    pub r#type: FeatureDataType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RasterInfo {
    pub data_type: RasterDataType,
    pub measurement: Option<Measurement>,
    pub no_data_value: Option<f64>,
    #[serde(default)]
    pub time_interval: TimeInterval,
    pub rasterband_channel: usize,
    pub width: usize,
    pub height: usize,
    pub geo_transform: GdalDatasetGeoTransform,
}

#[cfg(test)]
mod tests {
    use crate::datasets::external::nfdi::metadata::{
        Attribute, DataType, GEMetadata, RasterInfo, VectorInfo,
    };
    use crate::datasets::listing::Provenance;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::primitives::{Coordinate2D, FeatureDataType};
    use geoengine_datatypes::raster::RasterDataType;
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_operators::source::GdalDatasetGeoTransform;

    #[test]
    fn test_ser_vector() {
        let md = GEMetadata {
            crs: Some(SpatialReference::epsg_4326()),
            data_type: DataType::SingleVectorFile(VectorInfo {
                vector_type: VectorDataType::MultiPoint,
                layer_name: "test".to_string(),
                attributes: vec![Attribute {
                    name: "name".to_string(),
                    r#type: FeatureDataType::Text,
                }],
                temporal_extend: None,
            }),
            provenance: Some(vec![Provenance {
                citation: "Test".to_string(),
                license: "MIT".to_string(),
                uri: "http://geoengine.io".to_string(),
            }]),
        };

        let json = serde_json::json!({
            "crs":"EPSG:4326",
            "dataType":{
                "singleVectorFile":{
                    "vectorType":"MultiPoint",
                    "layerName":"test",
                    "attributes":[{
                        "name":"name",
                        "type":"text"
                    }],
                    "temporalExtend":null
                }
            },
            "provenance":{
                "citation":"Test",
                "license":"MIT",
                "uri":"http://geoengine.io"
            }
        });

        let des = serde_json::from_value::<GEMetadata>(serde_json::to_value(&md).unwrap()).unwrap();

        assert_eq!(json, serde_json::to_value(&md).unwrap());
        assert_eq!(md, des);
    }

    #[test]
    fn test_ser_raster() {
        let md = GEMetadata {
            crs: Some(SpatialReference::epsg_4326()),
            data_type: DataType::SingleRasterFile(RasterInfo {
                data_type: RasterDataType::U8,
                measurement: None,
                no_data_value: Some(0.0),
                time_interval: Default::default(),
                rasterband_channel: 1,
                width: 3600,
                height: 1800,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: Coordinate2D::new(-180.0, 90.0),
                    x_pixel_size: 0.1,
                    y_pixel_size: -0.1,
                },
            }),
            provenance: Some(vec![Provenance {
                citation: "Test".to_string(),
                license: "MIT".to_string(),
                uri: "http://geoengine.io".to_string(),
            }]),
        };

        let json = serde_json::json!({
            "crs":"EPSG:4326",
            "dataType":{
                "singleRasterFile":{
                    "dataType":"U8",
                    "noDataValue":0.0,
                    "rasterbandChannel":1,
                    "width":3600,
                    "height":1800,
                    "geoTransform":{
                        "originCoordinate":{
                            "x":-180.0,
                            "y":90.0
                        },
                        "xPixelSize":0.1,
                        "yPixelSize":-0.1
                    }
                }
            },
            "provenance":{
                "citation":"Test",
                "license":"MIT",
                "uri":"http://geoengine.io"
            }
        });

        let des = serde_json::from_value::<GEMetadata>(serde_json::to_value(&md).unwrap()).unwrap();
        assert_eq!(md, des);
        let des2 = serde_json::from_value::<GEMetadata>(json).unwrap();
        assert_eq!(md, des2);
    }
}
