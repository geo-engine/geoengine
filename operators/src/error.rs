use failure::Fail; // TODO: replace failure in gdal and then remove
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
        #[snafu(source(from(gdal::errors::Error, failure::Fail::compat)))]
        source: failure::Compat<gdal::errors::Error>,
    },
    #[snafu(display("IOError: {}", source))]
    IO {
        source: std::io::Error,
    },
    #[snafu(display("SerdeJsonError: {}", source))]
    SerdeJson {
        source: serde_json::Error,
    },

    InvalidType {
        expected: String,
        found: String,
    },
    InvalidOperatorType,

    UnknownDataset {
        name: String,
    },

    WorkerThread {
        reason: String,
    },
}

impl From<geoengine_datatypes::error::Error> for Error {
    fn from(datatype_error: geoengine_datatypes::error::Error) -> Self {
        Self::DataType {
            source: datatype_error,
        }
    }
}

impl From<gdal::errors::Error> for Error {
    fn from(gdal_error: gdal::errors::Error) -> Self {
        Self::Gdal {
            source: gdal_error.compat(),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(io_error: std::io::Error) -> Self {
        Self::IO { source: io_error }
    }
}

impl From<serde_json::Error> for Error {
    fn from(serde_json_error: serde_json::Error) -> Self {
        Self::SerdeJson {
            source: serde_json_error,
        }
    }
}
