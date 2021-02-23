use chrono::ParseError;
use geoengine_datatypes::dataset::DataSetId;
use geoengine_datatypes::primitives::FeatureDataType;
use snafu::Snafu;
use std::ops::Range;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
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
    #[snafu(display(
    "InvalidNumberOfRasterInputsError: expected \"[{} .. {}]\" found \"{}\"",
    expected.start, expected.end,
    found
    ))]
    InvalidNumberOfRasterInputs {
        expected: Range<usize>,
        found: usize,
    },

    #[snafu(display(
    "InvalidNumberOfVectorInputsError: expected \"[{} .. {}]\" found \"{}\"",
    expected.start, expected.end,
    found
    ))]
    InvalidNumberOfVectorInputs {
        expected: Range<usize>,
        found: usize,
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

    DataSetMetaData {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    Arrow {
        source: arrow::error::ArrowError,
    },

    NoDataSetWithGivenId {
        id: DataSetId,
    },

    RasterRootPathNotConfigured, // TODO: remove when GdalSource uses LoadingInfo

    InvalidDataSetId,
    DataSetLoadingInfoProviderMismatch,
    UnknownDataSetId,

    // TODO: this error should not be propagated to user
    #[snafu(display("Could not open gdal data set for file path {:?}", file_path))]
    CouldNotOpenGdalDataSet {
        file_path: String,
    },

    FilePathNotRepresentableAsString,
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
