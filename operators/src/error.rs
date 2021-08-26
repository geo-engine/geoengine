use chrono::ParseError;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::primitives::FeatureDataType;
use snafu::Snafu;
use std::ops::Range;
use std::path::PathBuf;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    UnsupportedRasterValue,

    InvalidMeteosatSatellite,

    InvalidUTCTimestamp,

    #[snafu(display("InvalidChannel Error (requested channel: {})", channel))]
    InvalidChannel {
        channel: usize,
    },

    #[snafu(display("InvalidMeasurement Error; expected {}, found: {}", expected, found))]
    InvalidMeasurement {
        expected: String,
        found: String,
    },

    #[snafu(display("CsvSource Error: {}", source))]
    CsvSourceReader {
        source: csv::Error,
    },

    #[snafu(display("CsvSource Error: {}", details))]
    CsvSource {
        details: String,
    },

    #[snafu(display("DataTypeError: {}", source))]
    DataType {
        source: geoengine_datatypes::error::Error,
    },
    QueryProcessor,

    #[snafu(display(
        "InvalidSpatialReferenceError: expected \"{}\" found \"{}\"",
        expected,
        found
    ))]
    InvalidSpatialReference {
        expected: geoengine_datatypes::spatial_reference::SpatialReferenceOption,
        found: geoengine_datatypes::spatial_reference::SpatialReferenceOption,
    },

    InvalidOperatorSpec {
        reason: String,
    },

    // TODO: use something more general than `Range`, e.g. `dyn RangeBounds` that can, however not be made into an object
    #[snafu(display("InvalidNumberOfRasterInputsError: expected \"[{} .. {}]\" found \"{}\"", expected.start, expected.end, found))]
    InvalidNumberOfRasterInputs {
        expected: Range<usize>,
        found: usize,
    },

    #[snafu(display("InvalidNumberOfVectorInputsError: expected \"[{} .. {}]\" found \"{}\"", expected.start, expected.end, found))]
    InvalidNumberOfVectorInputs {
        expected: Range<usize>,
        found: usize,
    },

    #[snafu(display("InvalidNumberOfVectorInputsError: expected \"[{} .. {}]\" found \"{}\"", expected.start, expected.end, found))]
    InvalidNumberOfInputs {
        expected: Range<usize>,
        found: usize,
    },

    #[snafu(display("Column {} does not exist", column))]
    ColumnDoesNotExist {
        column: String,
    },

    #[snafu(display("GdalError: {}", source))]
    Gdal {
        source: gdal::errors::GdalError,
    },

    #[snafu(display("IOError: {}", source))]
    Io {
        source: std::io::Error,
    },

    #[snafu(display("SerdeJsonError: {}", source))]
    SerdeJson {
        source: serde_json::Error,
    },

    Ocl {
        ocl_error: ocl::error::Error,
    },

    ClProgramInvalidRasterIndex,

    ClProgramInvalidRasterDataType,

    ClProgramInvalidFeaturesIndex,

    ClProgramInvalidVectorDataType,

    ClProgramInvalidGenericIndex,

    ClProgramInvalidGenericDataType,

    ClProgramUnspecifiedRaster,

    ClProgramUnspecifiedFeatures,

    ClProgramUnspecifiedGenericBuffer,

    ClProgramInvalidColumn,

    ClInvalidInputsForIterationType,

    InvalidExpression,

    InvalidNumberOfExpressionInputs,

    InvalidNoDataValueValueForOutputDataType,

    InvalidType {
        expected: String,
        found: String,
    },

    InvalidOperatorType,

    #[snafu(display("Column types do not match: {:?} - {:?}", left, right))]
    ColumnTypeMismatch {
        left: FeatureDataType,
        right: FeatureDataType,
    },

    UnknownDataset {
        name: String,
        source: std::io::Error,
    },

    InvalidDatasetSpec {
        name: String,
        source: serde_json::Error,
    },

    WorkerThread {
        reason: String,
    },

    TimeIntervalColumnNameMissing,

    TimeIntervalDurationMissing,

    TimeParse {
        source: chrono::format::ParseError,
    },

    TimeInstanceNotDisplayable,

    DatasetMetaData {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    Arrow {
        source: arrow::error::ArrowError,
    },

    NoDatasetWithGivenId {
        id: DatasetId,
    },

    RasterRootPathNotConfigured, // TODO: remove when GdalSource uses LoadingInfo

    InvalidDatasetId,
    DatasetLoadingInfoProviderMismatch,
    UnknownDatasetId,

    // TODO: this error should not be propagated to user
    #[snafu(display("Could not open gdal dataset for file path {:?}", file_path))]
    CouldNotOpenGdalDataset {
        file_path: String,
    },

    FilePathNotRepresentableAsString,

    TokioJoin {
        source: tokio::task::JoinError,
    },

    OgrSourceColumnsSpecMissing,

    EmptyInput,

    OgrFieldValueIsNotDateTime,
    OgrFieldValueIsNotString,
    OgrFieldValueIsNotValidForSeconds,
    OgrColumnFieldTypeMismatch {
        expected: String,
        field_value: gdal::vector::FieldValue,
    },

    FeatureDataValueMustNotBeNull,
    InvalidFeatureDataType,
    InvalidRasterDataType,

    #[snafu(display("No candidate source resolutions were produced."))]
    NoSourceResolution,

    WindowSizeMustNotBeZero,

    NotYetImplemented,

    TemporalRasterAggregationLastValidRequiresNoData,
    TemporalRasterAggregationFirstValidRequiresNoData,

    NoSpatialBoundsAvailable,

    ChannelSend,
    #[snafu(display("LoadingInfoError: {}", source))]
    LoadingInfo {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    NotImplemented,

    TileLimitExceeded {
        limit: usize,
    },

    FeatureDataNotAggregatable,

    FeatureDataLengthMismatch,

    OgrSqlQuery,

    GdalRasterDataTypeNotSupported,

    InvalidGdalFilePath {
        file_path: PathBuf,
    },
}

impl From<geoengine_datatypes::error::Error> for Error {
    fn from(datatype_error: geoengine_datatypes::error::Error) -> Self {
        Self::DataType {
            source: datatype_error,
        }
    }
}

impl From<gdal::errors::GdalError> for Error {
    fn from(gdal_error: gdal::errors::GdalError) -> Self {
        Self::Gdal { source: gdal_error }
    }
}

impl From<std::io::Error> for Error {
    fn from(io_error: std::io::Error) -> Self {
        Self::Io { source: io_error }
    }
}

impl From<serde_json::Error> for Error {
    fn from(serde_json_error: serde_json::Error) -> Self {
        Self::SerdeJson {
            source: serde_json_error,
        }
    }
}

impl From<chrono::format::ParseError> for Error {
    fn from(source: ParseError) -> Self {
        Self::TimeParse { source }
    }
}

impl From<ocl::Error> for Error {
    fn from(ocl_error: ocl::Error) -> Self {
        Self::Ocl { ocl_error }
    }
}

impl From<arrow::error::ArrowError> for Error {
    fn from(source: arrow::error::ArrowError) -> Self {
        Error::Arrow { source }
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(source: tokio::task::JoinError) -> Self {
        Error::TokioJoin { source }
    }
}
