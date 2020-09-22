use std::marker::PhantomData;

use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

use geoengine_datatypes::collections::{FeatureCollection, VectorDataType};
use geoengine_datatypes::primitives::Geometry;
use geoengine_datatypes::projection::Projection;
use geoengine_datatypes::util::arrow::ArrowTyped;

use crate::engine::{
    InitializedOperator, InitializedOperatorImpl, QueryContext, QueryProcessor, QueryRectangle,
    SourceOperator, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
    VectorResultDescriptor,
};
use crate::util::Result;
use geoengine_datatypes::provenance::ProvenanceInformation;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct OgrSourceParameters {
    pub layer_name: String,
    pub attribute_projection: Option<Vec<String>>,
}

pub type OgrSource = SourceOperator<OgrSourceParameters>;

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
    time: OgrSourceDatasetTimeType,
    duration: Option<u64>,
    time1_format: Option<OgrSourceTimeFormat>,
    time2_format: Option<OgrSourceTimeFormat>,
    columns: OgrSourceColumnSpec,
    default: Option<String>,
    force_ogr_time_filter: bool,
    on_error: OgrSourceErrorSpec,
    provenance: ProvenanceInformation,
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
    InitializedOperatorImpl<OgrSourceParameters, VectorResultDescriptor, ()>;

#[typetag::serde]
impl VectorOperator for OgrSource {
    fn initialize(
        self: Box<Self>,
        context: crate::engine::ExecutionContext,
    ) -> Result<Box<crate::engine::InitializedVectorOperator>> {
        let projection = Projection::wgs84().into();
        let data_type = VectorDataType::MultiPoint;

        let result_descriptor = VectorResultDescriptor {
            projection,
            data_type,
        };

        // TODO: get metadata
        // let mut dataset = Dataset::open(Path::new("fixtures/roads.geojson")).unwrap();
        // let layer = dataset.layer(0).unwrap();
        // for feature in layer.features() {
        //     let highway_field = feature.field("highway").unwrap();
        //     let geometry = feature.geometry();
        //     println!(
        //         "{} {}",
        //         highway_field.to_string().unwrap(),
        //         geometry.wkt().unwrap()
        //     );
        // }

        Ok(
            InitializedOgrSource::new(self.params, context, result_descriptor, vec![], vec![], ())
                .boxed(),
        )
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
    use super::*;

    use serde_json::json;

    #[test]
    fn specification_serde() {
        let spec = OgrSourceDataset {
            filename: "foobar.csv".into(),
            layer_name: "foobar".to_string(),
            time: OgrSourceDatasetTimeType::StartDuration,
            duration: Some(42),
            time1_format: Some(OgrSourceTimeFormat::Custom {
                custom_format: "YYYY-MM-DD".to_string(),
            }),
            time2_format: None,
            columns: OgrSourceColumnSpec {
                x: "x".to_string(),
                y: Some("y".to_string()),
                time1: Some("start".to_string()),
                time2: None,
                numeric: vec!["num".to_string()],
                decimal: vec!["dec1".to_string(), "dec2".to_string()],
                textual: vec!["text".to_string()],
            },
            default: Some("POINT(0, 0)".to_string()),
            force_ogr_time_filter: false,
            on_error: OgrSourceErrorSpec::Skip,
            provenance: ProvenanceInformation {
                citation: "Foo Bar".to_string(),
                license: "CC".to_string(),
                uri: "foo:bar".to_string(),
            },
        };

        let serialized_spec = serde_json::to_string(&spec).unwrap();

        assert_eq!(
            serialized_spec,
            json!({
                "filename": "foobar.csv",
                "layer_name": "foobar",
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
}
