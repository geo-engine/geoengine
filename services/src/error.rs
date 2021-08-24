use crate::handlers::ErrorResponse;
use actix_web::http::StatusCode;
use actix_web::HttpResponse;
use geoengine_datatypes::{dataset::DatasetProviderId, spatial_reference::SpatialReferenceOption};
use snafu::Snafu;
use strum::IntoStaticStr;

pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Debug, Snafu, IntoStaticStr)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    DataType {
        source: geoengine_datatypes::error::Error,
    },
    Operator {
        source: geoengine_operators::error::Error,
    },
    Uuid {
        source: uuid::Error,
    },
    SerdeJson {
        source: serde_json::Error,
    },
    Io {
        source: std::io::Error,
    },
    TokioJoin {
        source: tokio::task::JoinError,
    },

    TokioSignal {
        source: std::io::Error,
    },

    Reqwest {
        source: reqwest::Error,
    },

    #[cfg(feature = "xml")]
    QuickXml {
        source: quick_xml::Error,
    },

    TokioChannelSend,

    #[snafu(display("Unable to parse query string: {}", source))]
    UnableToParseQueryString {
        source: serde_urlencoded::de::Error,
    },

    ServerStartup,

    #[snafu(display("Registration failed: {}", reason))]
    RegistrationFailed {
        reason: String,
    },
    #[snafu(display("Tried to create duplicate: {}", reason))]
    Duplicate {
        reason: String,
    },
    #[snafu(display("User does not exist or password is wrong."))]
    LoginFailed,
    LogoutFailed,
    #[snafu(display("The session id is invalid."))]
    InvalidSession,
    #[snafu(display("Header with authorization token not provided."))]
    MissingAuthorizationHeader,
    #[snafu(display("Authentication scheme must be Bearer."))]
    InvalidAuthorizationScheme,

    #[snafu(display("Authorization error: {:?}", source))]
    Authorization {
        source: Box<Error>,
    },
    #[snafu(display("Failed to create the project."))]
    ProjectCreateFailed,
    #[snafu(display("Failed to list projects."))]
    ProjectListFailed,
    #[snafu(display("The project failed to load."))]
    ProjectLoadFailed,
    #[snafu(display("Failed to update the project."))]
    ProjectUpdateFailed,
    #[snafu(display("Failed to delete the project."))]
    ProjectDeleteFailed,
    PermissionFailed,
    ProjectDbUnauthorized,

    InvalidNamespace,

    InvalidSpatialReference,
    #[snafu(display("SpatialReferenceMissmatch: Found {}, expected: {}", found, expected))]
    SpatialReferenceMissmatch {
        found: SpatialReferenceOption,
        expected: SpatialReferenceOption,
    },

    InvalidWfsTypeNames,

    NoWorkflowForGivenId,

    #[cfg(feature = "postgres")]
    TokioPostgres {
        source: bb8_postgres::tokio_postgres::Error,
    },

    TokioPostgresTimeout,

    #[snafu(display("Identifier does not have the right format."))]
    InvalidUuid,
    SessionNotInitialized,

    ConfigLockFailed,

    Config {
        source: config::ConfigError,
    },

    AddrParse {
        source: std::net::AddrParseError,
    },

    MissingWorkingDirectory {
        source: std::io::Error,
    },

    MissingSettingsDirectory,

    DatasetIdTypeMissMatch,
    UnknownDatasetId,
    UnknownProviderId,
    MissingDatasetId,

    #[snafu(display("Parameter {} must have length between {} and {}", parameter, min, max))]
    InvalidStringLength {
        parameter: String,
        min: usize,
        max: usize,
    },

    #[snafu(display("Limit must be <= {}", limit))]
    InvalidListLimit {
        limit: usize,
    },

    UploadFieldMissingFileName,
    UnknownUploadId,
    PathIsNotAFile,
    Multipart {
        source: actix_multipart::MultipartError,
    },
    InvalidUploadFileName,
    InvalidDatasetName,
    DatasetHasNoAutoImportableLayer,
    #[snafu(display("GdalError: {}", source))]
    Gdal {
        source: gdal::errors::GdalError,
    },
    EmptyDatasetCannotBeImported,
    NoMainFileCandidateFound,
    NoFeatureDataTypeForColumnDataType,

    UnknownSpatialReference {
        srs_string: String,
    },

    NotYetImplemented,

    StacNoSuchBand {
        band_name: String,
    },
    StacInvalidGeoTransform,
    StacInvalidBbox,
    StacJsonResponse {
        url: String,
        response: String,
        error: serde_json::Error,
    },
    RasterDataTypeNotSupportByGdal,

    ExternalAddressNotConfigured,

    MissingSpatialReference,

    WcsVersionNotSupported,
    WcsGridOriginMustEqualBoundingboxUpperLeft,
    WcsBoundingboxCrsMustEqualGridBaseCrs,
    WcsInvalidGridOffsets,

    InvalidDatasetId,

    GfbioMissingAbcdField,
    ExpectedExternalDatasetId,
    InvalidExternalDatasetId {
        provider: DatasetProviderId,
    },

    #[cfg(feature = "nature40")]
    Nature40UnknownRasterDbname,
    #[cfg(feature = "nature40")]
    Nature40WcsDatasetMissingLabelInMetadata,

    Logger {
        source: flexi_logger::FlexiLoggerError,
    },

    #[cfg(feature = "odm")]
    Odm {
        reason: String,
    },
    #[cfg(feature = "odm")]
    OdmInvalidResponse {
        reason: String,
    },
    #[cfg(feature = "odm")]
    OdmMissingContentTypeHeader,
}

impl actix_web::error::ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        let (error, message) = match self {
            Error::Authorization { source } => (
                Into::<&str>::into(source.as_ref()).to_string(),
                source.to_string(),
            ),
            _ => (Into::<&str>::into(self).to_string(), self.to_string()),
        };

        HttpResponse::build(self.status_code()).json(ErrorResponse { error, message })
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::Authorization { source: _ } => StatusCode::UNAUTHORIZED,
            Error::Duplicate { reason: _ } => StatusCode::CONFLICT,
            _ => StatusCode::BAD_REQUEST,
        }
    }
}

impl From<geoengine_datatypes::error::Error> for Error {
    fn from(e: geoengine_datatypes::error::Error) -> Self {
        Self::DataType { source: e }
    }
}

impl From<geoengine_operators::error::Error> for Error {
    fn from(e: geoengine_operators::error::Error) -> Self {
        Self::Operator { source: e }
    }
}

#[cfg(feature = "postgres")]
impl From<bb8_postgres::bb8::RunError<<bb8_postgres::PostgresConnectionManager<bb8_postgres::tokio_postgres::NoTls> as bb8_postgres::bb8::ManageConnection>::Error>> for Error {
    fn from(e: bb8_postgres::bb8::RunError<<bb8_postgres::PostgresConnectionManager<bb8_postgres::tokio_postgres::NoTls> as bb8_postgres::bb8::ManageConnection>::Error>) -> Self {
        match e {
            bb8_postgres::bb8::RunError::User(e) => Self::TokioPostgres { source: e },
            bb8_postgres::bb8::RunError::TimedOut => Self::TokioPostgresTimeout,
        }
    }
}

#[cfg(feature = "postgres")]
impl From<bb8_postgres::tokio_postgres::error::Error> for Error {
    fn from(e: bb8_postgres::tokio_postgres::error::Error) -> Self {
        Self::TokioPostgres { source: e }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::SerdeJson { source: e }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io { source: e }
    }
}

impl From<gdal::errors::GdalError> for Error {
    fn from(gdal_error: gdal::errors::GdalError) -> Self {
        Self::Gdal { source: gdal_error }
    }
}

impl From<reqwest::Error> for Error {
    fn from(source: reqwest::Error) -> Self {
        Self::Reqwest { source }
    }
}

impl From<actix_multipart::MultipartError> for Error {
    fn from(source: actix_multipart::MultipartError) -> Self {
        Self::Multipart { source }
    }
}

#[cfg(feature = "xml")]
impl From<quick_xml::Error> for Error {
    fn from(source: quick_xml::Error) -> Self {
        Self::QuickXml { source }
    }
}

impl From<flexi_logger::FlexiLoggerError> for Error {
    fn from(source: flexi_logger::FlexiLoggerError) -> Self {
        Self::Logger { source }
    }
}
