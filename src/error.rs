use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("CsvSource Error: {}", source))]
    CsvSourceReaderError { source: csv::Error },
    #[snafu(display("CsvSource Error: {}", details))]
    CsvSourceError { details: String },
}
