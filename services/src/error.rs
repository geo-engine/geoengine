use snafu::Snafu;
use warp::reject::Reject;

pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    DataType {
        source: geoengine_datatypes::error::Error,
    },
    Operator {
        source: geoengine_operators::error::Error,
    },
    HTTP {
        source: warp::http::Error,
    },
    Uuid {
        source: uuid::Error,
    },
    SerdeJson {
        source: serde_json::Error,
    },
    IO {
        source: std::io::Error,
    },
    TokioJoin {
        source: tokio::task::JoinError,
    },
    UnableToParseQueryString {
        source: serde_urlencoded::de::Error,
    },

    #[snafu(display("Registration failed: {:?}", reason))]
    RegistrationFailed {
        reason: String,
    },
    LoginFailed,
    LogoutFailed,
    SessionDoesNotExist,
    InvalidSessionToken,

    ProjectCreateFailed,
    ProjectListFailed,
    ProjectLoadFailed,
    ProjectUpdateFailed,
    ProjectDeleteFailed,
    PermissionFailed,

    InvalidNamespace,

    InvalidWFSTypeNames,

    NoWorkflowForGivenId,
}

impl Reject for Error {}

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

impl From<Error> for warp::Rejection {
    fn from(e: Error) -> Self {
        warp::reject::custom(e)
    }
}
