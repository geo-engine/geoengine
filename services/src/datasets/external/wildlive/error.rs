use geoengine_datatypes::error::ErrorSource;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub enum WildliveError {
    #[snafu(display("Unable to parse collection id"))]
    UnableToSerializeCollectionId {
        source: Box<dyn ErrorSource>,
    },
    #[snafu(display("Unable to parse JSON: {source}"), context(false))]
    InvalidJSON {
        source: serde_json::Error,
    },
    #[snafu(display("Unable to parse URL: {source}"), context(false))]
    InvalidUrl {
        source: url::ParseError,
    },
    #[snafu(display("Unable to make web request: {source}"), context(false))]
    InvalidRequest {
        source: reqwest::Error,
    },
    #[snafu(display("Unable to get bounds for project: {source}"))]
    InvalidProjectBounds {
        source: geoengine_datatypes::error::Error,
    },
    #[snafu(display("Empty project bounds for project: {project}"))]
    EmptyProjectBounds {
        project: String,
    },
    #[snafu(display("Unexpected execution error: Please contact the system administrator"))]
    UnexpectedExecution {
        source: Box<dyn ErrorSource>,
    },
    #[snafu(display("Unable to create temporary directory: {source}"))]
    TempDirCreation {
        source: std::io::Error,
    },
    UnableToCreateDatasetFilename {
        source: std::fmt::Error,
    },
    UnableToWriteDataset {
        source: std::io::Error,
    },

    InvalidCaptureTimeStamp {
        source: Box<dyn ErrorSource>,
    },

    UnableToLookupStation,

    #[snafu(display("Invalid authentication key"))]
    AuthKeyInvalid {
        source: Box<dyn ErrorSource>,
    },

    #[snafu(display("Missing configuration for Wildlive OIDC on server"))]
    MissingConfiguration {
        source: Box<dyn ErrorSource>,
    },

    #[snafu(display("The provided connector is not of type Wildlive"))]
    WrongConnectorType,

    #[snafu(display("OIDC Error: {info}"))]
    Oidc {
        info: &'static str,
        source: Box<dyn ErrorSource>,
    },
}
