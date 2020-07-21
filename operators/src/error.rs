use failure::Fail;
use snafu::Snafu; // TODO: replace failure in gdal and then remove

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
    #[snafu(display("GdalError: {}", source))]
    Gdal {
        #[snafu(source(from(gdal::errors::Error, failure::Fail::compat)))]
        source: failure::Compat<gdal::errors::Error>,
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
