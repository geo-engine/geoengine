use crate::source::CsvSourceParameters;
use serde::{Deserialize, Serialize};
use geoengine_datatypes::primitives::Coordinate2D;
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Operator {
    Projection {
        params: ProjectionParameters,
        sources: AllSources,
    },
    GdalSource {
        params: GdalSourceParameters,
        #[serde(default)]
        sources: NoSources,
    },
    CsvSource {
        params: CsvSourceParameters,
        #[serde(default)]
        sources: NoSources,
    },
    MockPointSource {
        params: MockPointSourceParameters,
        sources: NoSources,
    },
    MockDelay {
        params: MockDelayParameters,
        sources: AllSources,
    },
    MockRasterSource {
        params: MockRasterSourceParameters,
        sources: NoSources
    },
    MockRasterPoints {
        params: MockRasterPointsParameters,
        sources: AllSources // TODO: only raster/points
    }
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoSources {}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RasterSources {
    pub rasters: Vec<Operator>,
}

impl Into<AllSources> for RasterSources {
    fn into(self) -> AllSources {
        AllSources {
            points: Default::default(),
            lines: Default::default(),
            polygons: Default::default(),
            rasters: self.rasters,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AllSources {
    #[serde(default)]
    pub points: Vec<Operator>,
    #[serde(default)]
    pub lines: Vec<Operator>,
    #[serde(default)]
    pub polygons: Vec<Operator>,
    #[serde(default)]
    pub rasters: Vec<Operator>,
}

/// Parameters for the Projection Operator
///
/// # Examples
///
/// ```rust
/// use serde_json::{Result, Value};
/// use geoengine_operators::Operator;
/// use geoengine_operators::operators::{ProjectionParameters, GdalSourceParameters, NoSources, RasterSources};
///
/// let json_string = r#"
///     {
///         "type": "projection",
///         "params": {
///             "src_crs": "EPSG:4326",
///             "dest_crs" : "EPSG:3857"
///         },
///         "sources": {
///             "rasters": [
///                 {
///                     "type": "gdal_source",
///                     "params": {
///                         "source_name": "test",
///                         "channel": 0
///                     }
///                 }
///             ]
///         }
///     }"#;
///
/// let operator: Operator = serde_json::from_str(json_string).unwrap();
///
/// assert_eq!(operator, Operator::Projection {
///     params: ProjectionParameters {
///        src_crs: "EPSG:4326".into(),
///        dest_crs: "EPSG:3857".into(),
///     },
///     sources: RasterSources {
///         rasters: vec![Operator::GdalSource {
///             params: GdalSourceParameters {
///                 source_name: "test".into(),
///                 channel: 0,
///             },
///             sources: NoSources {},
///         }],
///     }.into()
/// });
/// ```
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionParameters {
    pub src_crs: String,
    pub dest_crs: String,
}

/// Parameters for the GDAL Source Operator
///
/// # Examples
///
/// ```rust
/// use serde_json::{Result, Value};
/// use geoengine_operators::Operator;
/// use geoengine_operators::operators::{GdalSourceParameters, NoSources, RasterSources};
///
/// let json_string = r#"
///     {
///         "type": "gdal_source",
///         "params": {
///             "source_name": "test",
///             "channel": 3
///         }
///     }"#;
///
/// let operator: Operator = serde_json::from_str(json_string).unwrap();
///
/// assert_eq!(operator, Operator::GdalSource {
///     params: GdalSourceParameters {
///         source_name: "test".into(),
///         channel: 3,
///     },
///     sources: Default::default(),
/// });
/// ```
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GdalSourceParameters {
    pub source_name: String,
    pub channel: u8,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct MockPointSourceParameters {
    pub points: Vec<Coordinate2D>,
}

impl Eq for MockPointSourceParameters {}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MockDelayParameters {
    pub seconds: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct MockRasterSourceParameters {
    pub data: Vec<f64>,
    pub dim: [usize; 2],
    pub geo_transform: [f64; 6]
}

impl Eq for MockRasterSourceParameters {}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MockRasterPointsParameters {
    pub coords: [usize; 2]
}