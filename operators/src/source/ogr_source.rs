use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::PathBuf;

use futures::stream::BoxStream;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use geoengine_datatypes::collections::{FeatureCollection, VectorDataType};
use geoengine_datatypes::primitives::Geometry;
use geoengine_datatypes::provenance::ProvenanceInformation;
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_datatypes::util::arrow::ArrowTyped;

use crate::engine::{
    InitializedOperator, InitializedOperatorImpl, QueryContext, QueryProcessor, QueryRectangle,
    SourceOperator, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
    VectorResultDescriptor,
};
use crate::error::Error;
use crate::util::Result;
use gdal::vector::{Dataset, OGRwkbGeometryType};
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct OgrSourceParameters {
    pub layer_name: String,
    pub attribute_projection: Option<Vec<String>>,
}

pub type OgrSource = SourceOperator<OgrSourceParameters>;

lazy_static! {
    static ref DATASET_INFORMATION_PROVIDER: HashMap<String, OgrSourceDataset> = {
        let mut map = HashMap::new();
        map.insert(
            "foo".to_string(),
            OgrSourceDataset {
                filename: "foobar.csv".into(),
                layer_name: "foobar".to_string(),
                data_type: None,
                time: OgrSourceDatasetTimeType::StartDuration,
                duration: Some(42),
                time1_format: Some(OgrSourceTimeFormat::Custom {
                    custom_format: "YYYY-MM-DD".to_string(),
                }),
                time2_format: None,
                columns: Some(OgrSourceColumnSpec {
                    x: "x".to_string(),
                    y: Some("y".to_string()),
                    time1: Some("start".to_string()),
                    time2: None,
                    numeric: vec!["num".to_string()],
                    decimal: vec!["dec1".to_string(), "dec2".to_string()],
                    textual: vec!["text".to_string()],
                }),
                default: Some("POINT(0, 0)".to_string()),
                force_ogr_time_filter: false,
                on_error: OgrSourceErrorSpec::Skip,
                provenance: Some(ProvenanceInformation {
                    citation: "Foo Bar".to_string(),
                    license: "CC".to_string(),
                    uri: "foo:bar".to_string(),
                }),
            },
        );
        map.insert(
            "ne_10m_ports".to_string(),
            OgrSourceDataset {
                filename: "/home/beilschmidt/CLionProjects/geoengine/operators/test-data/vector/ne_10m_ports/ne_10m_ports.shp".into(),
                layer_name: "ne_10m_ports".to_string(),
                data_type: None,
                time: OgrSourceDatasetTimeType::None,
                duration: None,
                time1_format: None,
                time2_format: None,
                columns: None,
                default: None,
                force_ogr_time_filter: false,
                on_error: OgrSourceErrorSpec::Skip,
                provenance: None,
            },
        );

        map
    };
}

///  - `filename`: path to the input file
///  - `layer_name`: name of the layer to load
///  - `time`: the type of the time attribute(s)
///  - `duration`: the duration of the time validity for all features in the file [if time == "duration"]
///  - `time1_format`: a mapping of a column to the start time (cf. `OgrSourceDatasetTimeType`) [if time != "none"]
///  - `time2_format`: a mapping of a columns to the end time (cf. `time1_format`) [if time == "start+end" || "start+duration"]
///  - `columns`: a mapping of the columns to data, time, space. Columns that are not listed are skipped when parsing.
///  - `default`: wkt definition of the default point/line/polygon as a string [optional]
///  - `force_ogr_time_filter`: bool. force external time filter via ogr layer, even though data types don't match. Might not work
///    (result: empty collection), but has better performance for wfs requests [optional, false if not provided]
///  - `on_error`: specify the type of error handling
///  - `provenance`: specify the provenance of a file
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct OgrSourceDataset {
    filename: PathBuf,
    layer_name: String,
    data_type: Option<VectorDataType>,
    time: OgrSourceDatasetTimeType,
    duration: Option<u64>,
    time1_format: Option<OgrSourceTimeFormat>,
    time2_format: Option<OgrSourceTimeFormat>,
    columns: Option<OgrSourceColumnSpec>,
    default: Option<String>,
    force_ogr_time_filter: bool,
    on_error: OgrSourceErrorSpec,
    provenance: Option<ProvenanceInformation>,
}

/// The type of the time attribute(s):
///  - "none": no time information is mapped
///  - "start": only start information is mapped. duration has to specified in the duration attribute
///  - "start+end": start and end information is mapped
///  - "start+duration": start and duration information is mapped
#[serde(rename_all = "lowercase")]
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum OgrSourceDatasetTimeType {
    None,
    Start,
    #[serde(rename = "start+end")]
    StartEnd,
    #[serde(rename = "start+duration")]
    StartDuration,
}

///  A mapping for a column to the start time [if time != "none"]
///   - format: define the format of the column
///   - "custom": define a custom format in the attribute `custom_format`
///   - "seconds": time column is numeric and contains seconds as UNIX timestamp
///   - "iso": time column contains string with ISO8601
#[serde(tag = "format")]
#[serde(rename_all = "lowercase")]
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum OgrSourceTimeFormat {
    Custom { custom_format: String },
    Seconds,
    Iso,
}

/// A mapping of the columns to data, time, space. Columns that are not listed are skipped when parsing.
///  - x: the name of the column containing the x coordinate (or the wkt string) [if CSV file]
///  - y: the name of the column containing the y coordinate [if CSV file with y column]
///  - time1: the name of the first time column [if time != "none"]
///  - time2: the name of the second time column [if time == "start+end" || "start+duration"]
///  - numeric: an array of column names containing numeric values
///  - textual: an array of column names containing alpha-numeric values
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct OgrSourceColumnSpec {
    x: String,
    y: Option<String>,
    time1: Option<String>,
    time2: Option<String>,
    numeric: Vec<String>,
    decimal: Vec<String>,
    textual: Vec<String>,
}

/// Specify the type of error handling
///  - "skip"
///  - "abort"
///  - "keep"
#[serde(rename_all = "lowercase")]
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum OgrSourceErrorSpec {
    Skip,
    Abort,
    Keep,
}

pub type InitializedOgrSource =
    InitializedOperatorImpl<OgrSourceParameters, VectorResultDescriptor, OgrSourceDataset>;

#[typetag::serde]
impl VectorOperator for OgrSource {
    fn initialize(
        self: Box<Self>,
        context: crate::engine::ExecutionContext,
    ) -> Result<Box<crate::engine::InitializedVectorOperator>> {
        let dataset_information = DATASET_INFORMATION_PROVIDER
            .get(&self.params.layer_name)
            .ok_or_else(|| Error::UnknownDataset {
                name: self.params.layer_name.clone(),
            })?;

        // TODO: get metadata
        let mut dataset = Dataset::open(&dataset_information.filename)?;
        let layer = dataset.layer_by_name(&dataset_information.layer_name)?;

        let spatial_reference = match layer
            .spatial_reference()
            .and_then(|gdal_srs| gdal_srs.authority())
        {
            Ok(authority) => SpatialReference::from_str(&authority)?,
            Err(_) => {
                // TODO: is this a reasonable fallback type?
                SpatialReference::wgs84()
            }
        }
        .into();

        let data_type = if let Some(data_type) = dataset_information.data_type {
            data_type
        } else if dataset_information
            .columns
            .as_ref()
            .map_or(false, |columns| columns.y.is_some())
        {
            // if there is a `y` column, there is no WKT in a single column
            VectorDataType::MultiPoint
        } else {
            // TODO: is *data* a reasonable fallback type for an empty layer?
            layer
                .features()
                .next()
                .map_or(VectorDataType::Data, |feature| {
                    match feature.geometry().geometry_type() {
                        OGRwkbGeometryType::wkbPoint | OGRwkbGeometryType::wkbMultiPoint => {
                            VectorDataType::MultiPoint
                        }
                        OGRwkbGeometryType::wkbLineString
                        | OGRwkbGeometryType::wkbMultiLineString => VectorDataType::MultiLineString,
                        OGRwkbGeometryType::wkbPolygon | OGRwkbGeometryType::wkbMultiPolygon => {
                            VectorDataType::MultiPolygon
                        }
                        _ => {
                            // TODO: is *data* a reasonable fallback type? or throw an error?
                            VectorDataType::Data
                        }
                    }
                })
        };

        let result_descriptor = VectorResultDescriptor {
            spatial_reference,
            data_type,
        };

        Ok(InitializedOgrSource::new(
            self.params,
            context,
            result_descriptor,
            vec![],
            vec![],
            dataset_information.clone(),
        )
        .boxed())
    }
}

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedOgrSource
{
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        // TODO: simplify with macro
        Ok(match self.result_descriptor.data_type {
            VectorDataType::Data => TypedVectorQueryProcessor::Data(
                OgrSourceProcessor::new(self.params.clone()).boxed(),
            ),
            VectorDataType::MultiPoint => TypedVectorQueryProcessor::MultiPoint(
                OgrSourceProcessor::new(self.params.clone()).boxed(),
            ),
            VectorDataType::MultiLineString => TypedVectorQueryProcessor::MultiLineString(
                OgrSourceProcessor::new(self.params.clone()).boxed(),
            ),
            VectorDataType::MultiPolygon => TypedVectorQueryProcessor::MultiPolygon(
                OgrSourceProcessor::new(self.params.clone()).boxed(),
            ),
        })
    }
}

pub struct OgrSourceProcessor<G>
where
    G: Geometry + ArrowTyped,
{
    params: OgrSourceParameters,
    _collection_type: PhantomData<FeatureCollection<G>>,
}

impl<G> OgrSourceProcessor<G>
where
    G: Geometry + ArrowTyped,
{
    pub fn new(params: OgrSourceParameters) -> Self {
        Self {
            params,
            _collection_type: Default::default(),
        }
    }
}

impl<G> QueryProcessor for OgrSourceProcessor<G>
where
    G: Geometry + ArrowTyped,
{
    type Output = FeatureCollection<G>;
    fn query(&self, query: QueryRectangle, ctx: QueryContext) -> BoxStream<Result<Self::Output>> {
        todo!("implement")
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::engine::ExecutionContext;

    #[test]
    fn specification_serde() {
        let spec = OgrSourceDataset {
            filename: "foobar.csv".into(),
            layer_name: "foobar".to_string(),
            data_type: Some(VectorDataType::MultiPoint),
            time: OgrSourceDatasetTimeType::StartDuration,
            duration: Some(42),
            time1_format: Some(OgrSourceTimeFormat::Custom {
                custom_format: "YYYY-MM-DD".to_string(),
            }),
            time2_format: None,
            columns: Some(OgrSourceColumnSpec {
                x: "x".to_string(),
                y: Some("y".to_string()),
                time1: Some("start".to_string()),
                time2: None,
                numeric: vec!["num".to_string()],
                decimal: vec!["dec1".to_string(), "dec2".to_string()],
                textual: vec!["text".to_string()],
            }),
            default: Some("POINT(0, 0)".to_string()),
            force_ogr_time_filter: false,
            on_error: OgrSourceErrorSpec::Skip,
            provenance: Some(ProvenanceInformation {
                citation: "Foo Bar".to_string(),
                license: "CC".to_string(),
                uri: "foo:bar".to_string(),
            }),
        };

        let serialized_spec = serde_json::to_string(&spec).unwrap();

        assert_eq!(
            serialized_spec,
            json!({
                "filename": "foobar.csv",
                "layer_name": "foobar",
                "data_type": "MultiPoint",
                "time": "start+duration",
                "duration": 42,
                "time1_format": {
                    "format": "custom",
                    "custom_format": "YYYY-MM-DD"
                },
                "time2_format": null,
                "columns": {
                    "x": "x",
                    "y": "y",
                    "time1": "start",
                    "time2": null,
                    "numeric": ["num"],
                    "decimal": ["dec1", "dec2"],
                    "textual": ["text"]
                },
                "default": "POINT(0, 0)",
                "force_ogr_time_filter": false,
                "on_error": "skip",
                "provenance": {
                    "citation": "Foo Bar",
                    "license": "CC",
                    "uri": "foo:bar"
                }
            })
            .to_string()
        );

        let deserialized_spec: OgrSourceDataset = serde_json::from_str(
            &json!({
                "filename": "foobar.csv",
                "layer_name": "foobar",
                "data_type": "MultiPoint",
                "time": "start+duration",
                "duration": 42,
                "time1_format": {
                    "format": "custom",
                    "custom_format": "YYYY-MM-DD"
                },
                "columns": {
                    "x": "x",
                    "y": "y",
                    "time1": "start",
                    "numeric": ["num"],
                    "decimal": ["dec1", "dec2"],
                    "textual": ["text"]
                },
                "default": "POINT(0, 0)",
                "force_ogr_time_filter": false,
                "on_error": "skip",
                "provenance": {
                    "citation": "Foo Bar",
                    "license": "CC",
                    "uri": "foo:bar"
                }
            })
            .to_string(),
        )
        .unwrap();

        assert_eq!(deserialized_spec, spec);
    }

    #[test]
    fn ne_10m_ports() {
        let source = OgrSource {
            params: OgrSourceParameters {
                layer_name: "ne_10m_ports".to_string(),
                attribute_projection: None,
            },
        }
        .boxed()
        .initialize(ExecutionContext)
        .unwrap();

        assert_eq!(
            source.result_descriptor().data_type,
            VectorDataType::MultiPoint
        );
        assert_eq!(
            source.result_descriptor().spatial_reference,
            SpatialReference::wgs84().into()
        );
    }
}
