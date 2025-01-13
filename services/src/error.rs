use crate::api::model::datatypes::{
    DatasetId, SpatialReference, SpatialReferenceOption, TimeInstance,
};
use crate::api::model::responses::ErrorResponse;
use crate::datasets::external::aruna::error::ArunaProviderError;
use crate::datasets::external::netcdfcf::NetCdfCf4DProviderError;
use crate::{layers::listing::LayerCollectionId, workflows::workflow::WorkflowId};
use actix_web::http::StatusCode;
use actix_web::HttpResponse;
use geoengine_datatypes::dataset::LayerId;
use geoengine_datatypes::error::ErrorSource;
use geoengine_datatypes::util::helpers::ge_report;
use ordered_float::FloatIsNan;
use snafu::prelude::*;
use std::fmt;
use std::path::PathBuf;
use strum::IntoStaticStr;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, IntoStaticStr)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum Error {
    #[snafu(transparent)]
    DataType {
        source: geoengine_datatypes::error::Error,
    },
    #[snafu(transparent)]
    Operator {
        source: geoengine_operators::error::Error,
    },
    #[snafu(display("Uuid error: {source}"))]
    Uuid {
        source: uuid::Error,
    },
    #[snafu(display("Serde json error: {source}"))]
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
    #[snafu(display("Unable to parse url: {source}"))]
    Url {
        source: url::ParseError,
    },
    Proj {
        source: proj::ProjError,
    },

    #[snafu(context(false))]
    Trace {
        source: opentelemetry::trace::TraceError,
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
    #[snafu(display("Invalid admin token"))]
    InvalidAdminToken,
    #[snafu(display("Header with authorization token not provided."))]
    MissingAuthorizationHeader,
    #[snafu(display("Authentication scheme must be Bearer."))]
    InvalidAuthorizationScheme,

    #[snafu(display("Authorization error: {}", source))]
    Unauthorized {
        source: Box<Error>,
    },
    AccessDenied,
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
    #[snafu(display("A permission error occured: {source}."))]
    PermissionDb {
        source: Box<dyn ErrorSource>,
    },
    #[snafu(display("A role error occured: {source}."))]
    RoleDb {
        source: Box<dyn ErrorSource>,
    },
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

    TokioPostgres {
        source: bb8_postgres::tokio_postgres::Error,
    },

    TokioPostgresTimeout,

    #[snafu(display(
        "Database cannot be cleared on startup because it was started without that setting before."
    ))]
    ClearDatabaseOnStartupNotAllowed,

    #[snafu(display("Database schema must not be `public`."))]
    InvalidDatabaseSchema,

    #[snafu(display("Identifier does not have the right format."))]
    InvalidUuid,
    SessionNotInitialized,

    ConfigLockFailed,

    Config {
        source: config::ConfigError,
    },

    #[snafu(display("Unable to parse IP address: {source}"))]
    AddrParse {
        source: std::net::AddrParseError,
    },

    #[snafu(display("Missing working directory"))]
    MissingWorkingDirectory {
        source: std::io::Error,
    },

    MissingSettingsDirectory,

    DataIdTypeMissMatch,
    UnknownDataId,
    UnknownProviderId,
    MissingDatasetId,

    UnknownDatasetId,

    OperationRequiresAdminPrivilige,
    OperationRequiresOwnerPermission,

    #[snafu(display("Permission denied for dataset with id {:?}", dataset))]
    DatasetPermissionDenied {
        dataset: DatasetId,
    },

    #[snafu(display("Updating permission ({}, {:?}, {}) denied", role, dataset, permission))]
    UpdateDatasetPermission {
        role: String,
        dataset: DatasetId,
        permission: String,
    },

    #[snafu(display("Permission ({}, {:?}, {}) already exists", role, dataset, permission))]
    DuplicateDatasetPermission {
        role: String,
        dataset: DatasetId,
        permission: String,
    },

    // TODO: move to pro folder, because permissions are pro only
    #[snafu(display("Permission denied"))]
    PermissionDenied,

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
    #[snafu(display("Failed loading multipart body: {reason}"))]
    Multipart {
        // TODO: this error is not send, so this does not work
        // source: actix_multipart::MultipartError,
        reason: String,
    },
    InvalidUploadFileName,
    InvalidDatasetIdNamespace,
    DuplicateDatasetId,
    #[snafu(display("Dataset name '{}' already exists", dataset_name))]
    DatasetNameAlreadyExists {
        dataset_name: String,
        dataset_id: DatasetId,
    },
    #[snafu(display("Dataset name '{}' does not exist", dataset_name))]
    UnknownDatasetName {
        dataset_name: String,
    },
    InvalidDatasetName,
    #[snafu(display("Layer name '{layer_name}' is invalid"))]
    DatasetInvalidLayerName {
        layer_name: String,
    },
    DatasetHasNoAutoImportableLayer,
    #[snafu(display("GdalError: {}", source))]
    Gdal {
        source: gdal::errors::GdalError,
    },
    EmptyDatasetCannotBeImported,
    NoMainFileCandidateFound,
    NoFeatureDataTypeForColumnDataType,

    #[snafu(display("Spatial reference '{srs_string}' is unknown"))]
    UnknownSpatialReference {
        srs_string: String,
    },

    NotYetImplemented,

    #[snafu(display("Band '{band_name}' does not exist"))]
    StacNoSuchBand {
        band_name: String,
    },
    StacInvalidGeoTransform,
    StacInvalidBbox,
    #[snafu(display(
        "Failed to parse stac response from '{url}'. Error: {error}\nOriginal Response: {response}"
    ))]
    StacJsonResponse {
        url: String,
        response: String,
        error: serde_json::Error,
    },
    RasterDataTypeNotSupportByGdal,

    MissingSpatialReference,

    WcsVersionNotSupported,
    WcsGridOriginMustEqualBoundingboxUpperLeft,
    WcsBoundingboxCrsMustEqualGridBaseCrs,
    WcsInvalidGridOffsets,

    InvalidDatasetId,

    PangaeaNoTsv,
    GfbioMissingAbcdField,
    #[snafu(display("The response from the EDR server does not match the expected format."))]
    EdrInvalidMetadataFormat,
    ExpectedExternalDataId,
    InvalidDataId,

    Nature40UnknownRasterDbname,
    Nature40WcsDatasetMissingLabelInMetadata,

    #[snafu(display("FlexiLogger initialization error"))]
    Logger {
        source: flexi_logger::FlexiLoggerError,
    },

    #[snafu(display("Spatial reference system '{srs_string}' is unknown"))]
    UnknownSrsString {
        srs_string: String,
    },

    #[snafu(display("Axis ordering is unknown for SRS '{srs_string}'"))]
    AxisOrderingNotKnownForSrs {
        srs_string: String,
    },

    #[snafu(display("Anonymous access is disabled, please log in"))]
    AnonymousAccessDisabled,

    #[snafu(display("User registration is disabled"))]
    UserRegistrationDisabled,

    UserDoesNotExist,
    RoleDoesNotExist,
    RoleWithNameAlreadyExists,
    RoleAlreadyAssigned,
    RoleNotAssigned,

    #[snafu(display(
        "WCS request endpoint {} must match identifier {}",
        endpoint,
        identifier
    ))]
    WCSEndpointIdentifierMissmatch {
        endpoint: WorkflowId,
        identifier: WorkflowId,
    },
    #[snafu(display(
        "WCS request endpoint {} must match identifiers {}",
        endpoint,
        identifiers
    ))]
    WCSEndpointIdentifiersMissmatch {
        endpoint: WorkflowId,
        identifiers: WorkflowId,
    },
    #[snafu(display("WMS request endpoint {} must match layer {}", endpoint, layer))]
    WMSEndpointLayerMissmatch {
        endpoint: WorkflowId,
        layer: WorkflowId,
    },
    #[snafu(display(
        "WFS request endpoint {} must match type_names {}",
        endpoint,
        type_names
    ))]
    WFSEndpointTypeNamesMissmatch {
        endpoint: WorkflowId,
        type_names: WorkflowId,
    },

    #[snafu(context(false))]
    ArunaProvider {
        source: ArunaProviderError,
    },

    #[snafu(context(false))]
    NetCdfCf4DProvider {
        source: NetCdfCf4DProviderError,
    },

    AbcdUnitIdColumnMissingInDatabase,

    BaseUrlMustEndWithSlash,

    #[snafu(context(false))]
    LayerDb {
        source: crate::layers::LayerDbError,
    },

    #[snafu(display("Operator '{operator}' is unknown"))]
    UnknownOperator {
        operator: String,
    },

    #[snafu(display("The id is expected to be an uuid, but it is '{found}'."))]
    IdStringMustBeUuid {
        found: String,
    },

    #[snafu(context(false), display("TaskError: {}", source))]
    Task {
        source: crate::tasks::TaskError,
    },

    #[snafu(display("'{id}' is not a known layer collection id"))]
    UnknownLayerCollectionId {
        id: LayerCollectionId,
    },
    #[snafu(display("'{id}' is not a known layer id"))]
    UnknownLayerId {
        id: LayerId,
    },
    InvalidLayerCollectionId,
    InvalidLayerId,

    #[snafu(context(false))]
    WorkflowApi {
        source: crate::api::handlers::workflows::WorkflowApiError,
    },

    #[snafu(display("The sub path '{}' escapes the base path '{}'", sub_path.display(), base.display()))]
    SubPathMustNotEscapeBasePath {
        base: PathBuf,
        sub_path: PathBuf,
    },

    #[snafu(display("The sub path '{}' contains references to the parent '{}'", sub_path.display(), base.display()))]
    PathMustNotContainParentReferences {
        base: PathBuf,
        sub_path: PathBuf,
    },

    #[snafu(display("Time instance must be between {} and {}, but is {}", min.inner(), max.inner(), is))]
    InvalidTimeInstance {
        min: TimeInstance,
        max: TimeInstance,
        is: i64,
    },

    #[snafu(display("ParseU32: {}", source))]
    ParseU32 {
        source: <u32 as std::str::FromStr>::Err,
    },
    #[snafu(display("InvalidSpatialReferenceString: {}", spatial_reference_string))]
    InvalidSpatialReferenceString {
        spatial_reference_string: String,
    },

    #[snafu(context(false), display("OidcError: {}", source))]
    Oidc {
        source: crate::pro::users::OidcError,
    },

    #[snafu(display(
        "Could not resolve a Proj string for this SpatialReference: {}",
        spatial_ref
    ))]
    ProjStringUnresolvable {
        spatial_ref: SpatialReference,
    },

    #[snafu(display(
        "Cannot resolve the query's BBOX ({:?}) in the selected SRS ({})",
        query_bbox,
        query_srs
    ))]
    UnresolvableQueryBoundingBox2DInSrs {
        query_srs: SpatialReference,
        query_bbox: crate::api::model::datatypes::BoundingBox2D,
    },

    #[snafu(display("Result Descriptor field '{}' {}", field, cause))]
    LayerResultDescriptorMissingFields {
        field: String,
        cause: String,
    },

    ProviderDoesNotSupportBrowsing,

    InvalidPath,

    InvalidWorkflowOutputType,

    #[snafu(display("Functionality is not implemented: '{}'", message))]
    NotImplemented {
        message: String,
    },

    #[snafu(display("NotNan error: {}", source))]
    InvalidNotNanFloatKey {
        source: ordered_float::FloatIsNan,
    },

    UnexpectedInvalidDbTypeConversion,

    #[snafu(display(
        "Unexpected database version during migration, expected `{}` but found `{}`",
        expected,
        found
    ))]
    UnexpectedDatabaseVersionDuringMigration {
        expected: String,
        found: String,
    },

    #[snafu(display("Raster band names must be unique. Found {duplicate_key} more than once."))]
    RasterBandNamesMustBeUnique {
        duplicate_key: String,
    },
    #[snafu(display("Raster band names must not be empty"))]
    RasterBandNameMustNotBeEmpty,
    #[snafu(display("Raster band names must not be longer than 256 bytes"))]
    RasterBandNameTooLong,

    #[snafu(display("Resource id is invalid: type: {}, id: {}", resource_type, resource_id))]
    InvalidResourceId {
        resource_type: String,
        resource_id: String,
    },

    #[snafu(display("Unknown volume name: {}", volume_name))]
    UnknownVolumeName {
        volume_name: String,
    },

    #[snafu(display("Cannot access path of volume with name {}", volume_name))]
    CannotAccessVolumePath {
        volume_name: String,
    },

    #[snafu(display("Unknown resource name {} of kind {}", name, kind))]
    UnknownResource {
        kind: String,
        name: String,
    },

    #[snafu(display("MachineLearning error: {}", source))]
    MachineLearning {
        // TODO: make `source: MachineLearningError`, once pro features is removed
        source: Box<dyn ErrorSource>,
    },
}

impl actix_web::error::ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(ErrorResponse::from_service_error(self))
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::Unauthorized { source: _ } => StatusCode::UNAUTHORIZED,
            Error::Duplicate { reason: _ } => StatusCode::CONFLICT,
            _ => StatusCode::BAD_REQUEST,
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ge_report(self))
    }
}

impl From<bb8_postgres::bb8::RunError<<bb8_postgres::PostgresConnectionManager<bb8_postgres::tokio_postgres::NoTls> as bb8_postgres::bb8::ManageConnection>::Error>> for Error {
    fn from(e: bb8_postgres::bb8::RunError<<bb8_postgres::PostgresConnectionManager<bb8_postgres::tokio_postgres::NoTls> as bb8_postgres::bb8::ManageConnection>::Error>) -> Self {
        match e {
            bb8_postgres::bb8::RunError::User(e) => Self::TokioPostgres { source: e },
            bb8_postgres::bb8::RunError::TimedOut => Self::TokioPostgresTimeout,
        }
    }
}

// TODO: remove automatic conversion to our Error because we do not want to leak database internals in the API

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
        Self::Multipart {
            reason: source.to_string(),
        }
    }
}

impl From<url::ParseError> for Error {
    fn from(source: url::ParseError) -> Self {
        Self::Url { source }
    }
}

impl From<flexi_logger::FlexiLoggerError> for Error {
    fn from(source: flexi_logger::FlexiLoggerError) -> Self {
        Self::Logger { source }
    }
}

impl From<proj::ProjError> for Error {
    fn from(source: proj::ProjError) -> Self {
        Self::Proj { source }
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(source: tokio::task::JoinError) -> Self {
        Error::TokioJoin { source }
    }
}

impl From<ordered_float::FloatIsNan> for Error {
    fn from(source: FloatIsNan) -> Self {
        Error::InvalidNotNanFloatKey { source }
    }
}
