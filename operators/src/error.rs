use crate::util::statistics::StatisticsError;
use bb8_postgres::bb8;
use geoengine_datatypes::dataset::{DataId, NamedData};
use geoengine_datatypes::error::ErrorSource;
use geoengine_datatypes::primitives::{FeatureDataType, TimeInterval};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use ordered_float::FloatIsNan;
use snafu::prelude::*;
use std::ops::Range;
use std::path::PathBuf;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
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

    AllSourcesMustHaveSameSpatialReference,

    #[snafu(display("InvalidOperatorSpec: {}", reason))]
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

    InvalidExpression,

    #[snafu(display(
        "The expression operator only supports inputs with up to 8 bands. Found {found} bands.",
    ))]
    InvalidNumberOfExpressionInputBands {
        found: usize,
    },
    InvalidNumberOfRasterStackerInputs,

    InvalidNoDataValueValueForOutputDataType,

    #[snafu(display("Invalid type: expected {} found {}", expected, found))]
    InvalidType {
        expected: String,
        found: String,
    },

    #[snafu(display("Invalid operator type: expected {} found {}", expected, found))]
    InvalidOperatorType {
        expected: String,
        found: String,
    },

    #[snafu(display("Invalid vector type: expected {} found {}", expected, found))]
    InvalidVectorType {
        expected: String,
        found: String,
    },

    #[snafu(display("NotNan error: {}", source))]
    InvalidNotNanFloatKey {
        source: ordered_float::FloatIsNan,
    },

    #[snafu(display("Column types do not match: {:?} - {:?}", left, right))]
    ColumnTypeMismatch {
        left: FeatureDataType,
        right: FeatureDataType,
    },

    UnknownDataset {
        name: String,
        source: std::io::Error,
    },

    UnknownDatasetName {
        name: NamedData,
    },

    CannotResolveDatasetName {
        name: NamedData,
        source: Box<dyn ErrorSource>,
    },

    #[snafu(display("There is no Model with id {:?} avaiable.", id))]
    UnknownModelId {
        id: String,
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
        source: Box<dyn ErrorSource>,
    },

    TimeInstanceNotDisplayable,

    InvalidTimeStringPlaceholder {
        name: String,
    },

    DatasetMetaData {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    Arrow {
        source: arrow::error::ArrowError,
    },

    NoDataWithGivenId {
        id: DataId,
    },

    RasterRootPathNotConfigured, // TODO: remove when GdalSource uses LoadingInfo

    InvalidDataId,
    InvalidMetaDataType,
    UnknownDataId,
    DataIdTypeMissMatch,

    UnknownDatasetId,

    // Error during loading of meta data. The source error typically comes from the services crate
    #[snafu(display("Could not load meta data: {}", source))]
    MetaData {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

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
    OgrFieldValueIsNotValidForTimestamp,
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
    TemporalRasterAggregationMeanRequiresNoData,

    NoSpatialBoundsAvailable,

    ChannelSend,
    #[snafu(display("LoadingInfoError: {}", source))]
    LoadingInfo {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("CreatingProcessorFailed: {}", source))]
    CreatingProcessorFailed {
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

    DynamicGdalSourceSpecHasEmptyTimePlaceholders,

    #[snafu(display("Input `{}` must be greater than zero at `{}`", name, scope))]
    InputMustBeGreaterThanZero {
        scope: &'static str,
        name: &'static str,
    },

    #[snafu(display("Input `{}` must be zero or positive at `{}`", name, scope))]
    InputMustBeZeroOrPositive {
        scope: &'static str,
        name: &'static str,
    },

    DuplicateOutputColumns,

    #[snafu(display("Column name conflict: {} already exists", name))]
    ColumnNameConflict {
        name: String,
    },

    #[snafu(display("Input column `{:}` is missing", name))]
    MissingInputColumn {
        name: String,
    },

    InvalidGdalFilePath {
        file_path: PathBuf,
    },

    #[snafu(display(
        "Raster data sets with a different origin than upper left are currently not supported"
    ))]
    GeoTransformOrigin,

    #[snafu(display("Statistics error: {}", source))]
    Statistics {
        source: crate::util::statistics::StatisticsError,
    },

    #[snafu(display("SparseTilesFillAdapter error: {}", source))]
    SparseTilesFillAdapter {
        source: crate::adapters::SparseTilesFillAdapterError,
    },
    #[snafu(context(false))]
    ExpressionOperator {
        source: crate::processing::RasterExpressionError,
    },

    #[snafu(context(false), display("VectorExpression: {}", source))]
    VectorExpressionOperator {
        source: crate::processing::VectorExpressionError,
    },

    #[snafu(context(false))]
    TimeProjectionOperator {
        source: crate::processing::TimeProjectionError,
    },
    #[snafu(display("MockRasterSource error: {}", source))]
    MockRasterSource {
        source: crate::mock::MockRasterSourceError,
    },
    #[snafu(context(false))]
    InterpolationOperator {
        source: crate::processing::InterpolationError,
    },
    #[snafu(context(false))]
    TimeShift {
        source: crate::processing::TimeShiftError,
    },

    AlphaBandAsMaskNotAllowed,

    SpatialReferenceMustNotBeUnreferenced,

    #[snafu(context(false))]
    RasterKernel {
        source: crate::processing::NeighborhoodAggregateError,
    },

    #[snafu(context(false))]
    GdalSource {
        source: crate::source::GdalSourceError,
    },

    QueryCanceled,

    AbortTriggerAlreadyUsed,

    SubPathMustNotEscapeBasePath {
        base: PathBuf,
        sub_path: PathBuf,
    },

    InvalidDataProviderConfig,

    InvalidMachineLearningConfig,

    MachineLearningFeatureDataNotAvailable,
    MachineLearningFeaturesNotAvailable,
    MachineLearningModelNotFound,
    MachineLearningMustHaveAtLeastTwoFeatures,

    CouldNotCreateMlModelFilePath,
    CouldNotGetMlLabelKeyName,
    CouldNotGetMlModelDirectory,
    CouldNotGetMlModelFileName,
    CouldNotStoreMlModelInDb,
    InvalidMlModelPath,

    #[snafu(display("Valid filetypes: 'json'"))]
    NoValidMlModelFileType,

    #[cfg(feature = "pro")]
    #[snafu(context(false))]
    XGBoost {
        source: crate::pro::xg_error::XGBoostModuleError,
    },

    #[snafu(context(false), display("PieChart: {}", source))]
    PieChart {
        source: crate::plot::PieChartError,
    },

    #[snafu(display(
        "InvalidNumberOfTimeStepsError: expected \"{}\" found \"{}\"",
        expected,
        found
    ))]
    InvalidNumberOfTimeSteps {
        expected: usize,
        found: usize,
    },

    QueryingProcessorFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    // TODO: wrap this somehow, because it's pro
    PermissionDenied,

    #[snafu(context(false))]
    #[snafu(display("LineSimplification error: {}", source))]
    LineSimplification {
        source: crate::processing::LineSimplificationError,
    },

    #[snafu(context(false))]
    #[snafu(display("RgbOperator error: {source}"))]
    RgbOperator {
        source: crate::processing::RgbOperatorError,
    },

    #[snafu(context(false))]
    #[snafu(display("Cache can't produce the promissed result error: {source}"))]
    CacheCantProduceResult {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Input stream {stream_index} is not temporally aligned. Expected {expected:?}, found {found:?}."))]
    InputStreamsMustBeTemporallyAligned {
        stream_index: usize,
        expected: TimeInterval,
        found: TimeInterval,
    },

    AtLeastOneStreamRequired,

    #[snafu(display("Operator {operator:?} does not support sources with multiple bands yet."))]
    OperatorDoesNotSupportMultiBandsSourcesYet {
        operator: &'static str,
    },

    #[snafu(display("Operator {operator:?} does not support sources with multiple bands."))]
    OperatorDoesNotSupportMultiBandsSources {
        operator: &'static str,
    },

    #[snafu(display("Operation {operation:?} does not support queries with multiple bands."))]
    OperationDoesNotSupportMultiBandQueriesYet {
        operation: &'static str,
    },

    #[snafu(display("Invalid band count. Expected {}, found {}", expected, found))]
    InvalidBandCount {
        expected: u32,
        found: u32,
    },

    RasterInputsMustHaveSameSpatialReferenceAndDatatype {
        datatypes: Vec<RasterDataType>,
        spatial_references: Vec<SpatialReferenceOption>,
    },

    GdalSourceDoesNotSupportQueryingOtherBandsThanTheFirstOneYet,

    #[snafu(display("Raster band names must be unique. Found {duplicate_key} more than once.",))]
    RasterBandNamesMustBeUnique {
        duplicate_key: String,
    },
    #[snafu(display("Raster band names must not be empty"))]
    RasterBandNameMustNotBeEmpty,
    #[snafu(display("Raster band names must not be longer than 256 bytes"))]
    RasterBandNameTooLong,

    AtLeastOneRasterBandDescriptorRequired,

    #[snafu(display("Band {band_idx} does not exist."))]
    BandDoesNotExist {
        band_idx: u32,
    },

    #[snafu(display("PostgresError: {}", source))]
    Postgres {
        source: tokio_postgres::Error,
    },

    #[snafu(display("Bb8PostgresError: {}", source))]
    Bb8Postgres {
        source: bb8::RunError<tokio_postgres::Error>,
    },
}

impl From<crate::adapters::SparseTilesFillAdapterError> for Error {
    fn from(source: crate::adapters::SparseTilesFillAdapterError) -> Self {
        Error::SparseTilesFillAdapter { source }
    }
}

impl From<crate::mock::MockRasterSourceError> for Error {
    fn from(source: crate::mock::MockRasterSourceError) -> Self {
        Error::MockRasterSource { source }
    }
}

/// The error requires to be `Send`.
/// This inner modules tries to enforce this.
mod requirements {
    use super::*;

    trait RequiresSend: Send {}

    impl RequiresSend for Error {}
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

impl From<crate::util::statistics::StatisticsError> for Error {
    fn from(source: StatisticsError) -> Self {
        Error::Statistics { source }
    }
}

impl From<ordered_float::FloatIsNan> for Error {
    fn from(source: FloatIsNan) -> Self {
        Error::InvalidNotNanFloatKey { source }
    }
}
