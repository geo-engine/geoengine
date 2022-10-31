use crate::api::model::datatypes::{
    Coordinate2D, DateTimeParseFormat, MultiLineString, MultiPoint, MultiPolygon, NoGeometry,
    RasterPropertiesEntryType, RasterPropertiesKey, TimeInstance, TimeStep, VectorQueryRectangle,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;
use utoipa::openapi::Schema;
use utoipa::ToSchema;

use super::datatypes::{
    BoundingBox2D, FeatureDataType, Measurement, RasterDataType, SpatialPartition2D,
    SpatialReferenceOption, TimeInterval, VectorDataType,
};

/// A `ResultDescriptor` for raster queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RasterResultDescriptor {
    pub data_type: RasterDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub measurement: Measurement,
    pub time: Option<TimeInterval>,
    pub bbox: Option<SpatialPartition2D>,
}

/// An enum to differentiate between `Operator` variants
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "operator")]
pub enum TypedOperator {
    Vector(Box<dyn geoengine_operators::engine::VectorOperator>),
    Raster(Box<dyn geoengine_operators::engine::RasterOperator>),
    Plot(Box<dyn geoengine_operators::engine::PlotOperator>),
}

impl ToSchema for TypedOperator {
    fn schema() -> utoipa::openapi::Schema {
        use utoipa::openapi::*;
        ObjectBuilder::new()
            .property(
                "type",
                ObjectBuilder::new()
                    .schema_type(SchemaType::String)
                    .enum_values(Some(vec!["Vector", "Raster", "Plot"]))
            )
            .required("type")
            .property(
                "operator",
                ObjectBuilder::new()
                    .property(
                        "type",
                        Object::with_type(SchemaType::String)
                    )
                    .required("type")
                    .property(
                        "params",
                        Object::with_type(SchemaType::Object)
                    )
                    .property(
                        "sources",
                        Object::with_type(SchemaType::Object)
                    )
            )
            .required("operator")
            .example(Some(serde_json::json!(
                {"type": "MockPointSource", "params": {"points": [{"x": 0.0, "y": 0.1}, {"x": 1.0, "y": 1.1}]}
            })))
            .description(Some("An enum to differentiate between `Operator` variants"))
            .into()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VectorResultDescriptor {
    pub data_type: VectorDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub columns: HashMap<String, VectorColumnInfo>,
    pub time: Option<TimeInterval>,
    pub bbox: Option<BoundingBox2D>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VectorColumnInfo {
    pub data_type: FeatureDataType,
    pub measurement: Measurement,
}

/// A `ResultDescriptor` for plot queries
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlotResultDescriptor {
    pub spatial_reference: SpatialReferenceOption,
    pub time: Option<TimeInterval>,
    pub bbox: Option<BoundingBox2D>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum TypedResultDescriptor {
    Plot(PlotResultDescriptor),
    Raster(RasterResultDescriptor),
    Vector(VectorResultDescriptor),
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MockDatasetDataSourceLoadingInfo {
    pub points: Vec<Coordinate2D>,
}

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticMetaData<L, R, Q> {
    pub loading_info: L,
    pub result_descriptor: R,
    #[serde(skip)]
    pub phantom: PhantomData<Q>,
}

pub type MockMetaData =
    StaticMetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>;
pub type OgrMetaData =
    StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>;

impl ToSchema for MockMetaData {
    fn schema() -> Schema {
        use utoipa::openapi::*;
        ObjectBuilder::new()
            .property(
                "loading_info",
                Ref::from_schema_name("MockDatasetDataSourceLoadingInfo"),
            )
            .required("loading_info")
            .property(
                "result_descriptor",
                Ref::from_schema_name("VectorResultDescriptor"),
            )
            .required("result_descriptor")
            .into()
    }
}

impl ToSchema for OgrMetaData {
    fn schema() -> Schema {
        use utoipa::openapi::*;
        ObjectBuilder::new()
            .property("loading_info", Ref::from_schema_name("OgrSourceDataset"))
            .required("loading_info")
            .property(
                "result_descriptor",
                Ref::from_schema_name("VectorResultDescriptor"),
            )
            .required("result_descriptor")
            .into()
    }
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataStatic {
    pub time: Option<TimeInterval>,
    pub params: GdalDatasetParameters,
    pub result_descriptor: RasterResultDescriptor,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OgrSourceDataset {
    pub file_name: String,
    pub layer_name: String,
    pub data_type: Option<VectorDataType>,
    #[serde(default)]
    pub time: OgrSourceDatasetTimeType,
    pub default_geometry: Option<TypedGeometry>,
    pub columns: Option<OgrSourceColumnSpec>,
    #[serde(default)]
    pub force_ogr_time_filter: bool,
    #[serde(default)]
    pub force_ogr_spatial_filter: bool,
    pub on_error: OgrSourceErrorSpec,
    pub sql_query: Option<String>,
    pub attribute_query: Option<String>,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(tag = "format")]
#[serde(rename_all = "camelCase")]
pub enum OgrSourceTimeFormat {
    #[serde(rename_all = "camelCase")]
    Custom {
        custom_format: DateTimeParseFormat,
    },
    #[serde(rename_all = "camelCase")]
    UnixTimeStamp {
        timestamp_type: UnixTimeStampType,
        #[serde(skip)]
        #[serde(default = "DateTimeParseFormat::unix")]
        fmt: DateTimeParseFormat,
    },
    Auto,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum UnixTimeStampType {
    EpochSeconds,
    EpochMilliseconds,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum OgrSourceErrorSpec {
    Ignore,
    Abort,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum OgrSourceDatasetTimeType {
    None,
    #[serde(rename_all = "camelCase")]
    Start {
        start_field: String,
        start_format: OgrSourceTimeFormat,
        duration: OgrSourceDurationSpec,
    },
    #[serde(rename = "start+end")]
    #[serde(rename_all = "camelCase")]
    StartEnd {
        start_field: String,
        start_format: OgrSourceTimeFormat,
        end_field: String,
        end_format: OgrSourceTimeFormat,
    },
    #[serde(rename = "start+duration")]
    #[serde(rename_all = "camelCase")]
    StartDuration {
        start_field: String,
        start_format: OgrSourceTimeFormat,
        duration_field: String,
    },
}

/// If no time is specified, expect to parse none
impl Default for OgrSourceDatasetTimeType {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum OgrSourceDurationSpec {
    Infinite,
    Zero,
    Value(TimeStep),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum TypedGeometry {
    Data(NoGeometry),
    MultiPoint(MultiPoint),
    MultiLineString(MultiLineString),
    MultiPolygon(MultiPolygon),
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OgrSourceColumnSpec {
    pub format_specifics: Option<FormatSpecifics>,
    pub x: String,
    pub y: Option<String>,
    #[serde(default)]
    pub int: Vec<String>,
    #[serde(default)]
    pub float: Vec<String>,
    #[serde(default)]
    pub text: Vec<String>,
    #[serde(default)]
    pub bool: Vec<String>,
    #[serde(default)]
    pub datetime: Vec<String>,
    pub rename: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataRegular {
    pub result_descriptor: RasterResultDescriptor,
    pub params: GdalDatasetParameters,
    pub time_placeholders: HashMap<String, GdalSourceTimePlaceholder>,
    pub data_time: TimeInterval,
    pub step: TimeStep,
}

/// Parameters for loading data using Gdal
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalDatasetParameters {
    pub file_path: String,
    pub rasterband_channel: usize,
    pub geo_transform: GdalDatasetGeoTransform, // TODO: discuss if we need this at all
    pub width: usize,
    pub height: usize,
    pub file_not_found_handling: FileNotFoundHandling,
    #[serde(default)]
    //#[serde(with = "float_option_with_nan")]
    pub no_data_value: Option<f64>,
    pub properties_mapping: Option<Vec<GdalMetadataMapping>>,
    // Dataset open option as strings, e.g. `vec!["UserPwd=geoengine:pwd".to_owned(), "HttpAuth=BASIC".to_owned()]`
    pub gdal_open_options: Option<Vec<String>>,
    // Configs as key, value pairs that will be set as thread local config options, e.g.
    // `vec!["AWS_REGION".to_owned(), "eu-central-1".to_owned()]` and unset afterwards
    // TODO: validate the config options: only allow specific keys and specific values
    pub gdal_config_options: Option<Vec<(String, String)>>, // TODO: does not compile https://github.com/juhaku/utoipa/issues/330
    #[serde(default)]
    pub allow_alphaband_as_mask: bool,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct GdalSourceTimePlaceholder {
    pub format: DateTimeParseFormat,
    pub reference: TimeReference,
}

#[derive(Copy, Clone, PartialEq, Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalDatasetGeoTransform {
    pub origin_coordinate: Coordinate2D,
    pub x_pixel_size: f64,
    pub y_pixel_size: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, ToSchema)]
pub enum FileNotFoundHandling {
    NoData, // output tiles filled with nodata
    Error,  // return error tile
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct GdalMetadataMapping {
    pub source_key: RasterPropertiesKey,
    pub target_key: RasterPropertiesKey,
    pub target_type: RasterPropertiesEntryType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum TimeReference {
    Start,
    End,
}

/// Meta data for 4D `NetCDF` CF datasets
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetadataNetCdfCf {
    pub result_descriptor: RasterResultDescriptor,
    pub params: GdalDatasetParameters,
    pub start: TimeInstance,
    /// We use the end to specify the last, non-inclusive valid time point.
    /// Queries behind this point return no data.
    /// TODO: Alternatively, we could think about using the number of possible time steps in the future.
    pub end: TimeInstance,
    pub step: TimeStep,
    /// A band offset specifies the first band index to use for the first point in time.
    /// All other time steps are added to this offset.
    pub band_offset: usize,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataList {
    pub result_descriptor: RasterResultDescriptor,
    pub params: Vec<GdalLoadingInfoTemporalSlice>,
}

/// one temporal slice of the dataset that requires reading from exactly one Gdal dataset
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalLoadingInfoTemporalSlice {
    pub time: TimeInterval,
    pub params: Option<GdalDatasetParameters>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum CsvHeader {
    Yes,
    No,
    Auto,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum FormatSpecifics {
    Csv { header: CsvHeader },
}
