use bb8_postgres::bb8::{ManageConnection, RunError};
use snafu::Snafu;
use strum::IntoStaticStr;
use warp::reject::Reject;

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

    TokioSignal {
        source: std::io::Error,
    },

    TokioChannelSend,

    UnableToParseQueryString {
        source: serde_urlencoded::de::Error,
    },

    ServerStartup,

    #[snafu(display("Registration failed: {:?}", reason))]
    RegistrationFailed {
        reason: String,
    },
    LoginFailed,
    LogoutFailed,
    SessionDoesNotExist,
    InvalidSession,
    MissingAuthorizationHeader,

    #[snafu(display("Authorization error {:?}", source))]
    Authorization {
        source: Box<Error>,
    },

    ProjectCreateFailed,
    ProjectListFailed,
    ProjectLoadFailed,
    ProjectUpdateFailed,
    ProjectDeleteFailed,
    PermissionFailed,
    ProjectDBUnauthorized,

    InvalidNamespace,

    InvalidWFSTypeNames,

    NoWorkflowForGivenId,

    TokioPostgres {
        source: bb8_postgres::tokio_postgres::Error,
    },

    TokioPostgresTimeout,

    InvalidUuid,
    SessionNotInitialized,

    ConfigLockFailed,

    Config {
        source: config::ConfigError,
    },

    AddrParse {
        source: std::net::AddrParseError,
    },
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

impl From<RunError<<bb8_postgres::PostgresConnectionManager<bb8_postgres::tokio_postgres::NoTls> as ManageConnection>::Error>> for Error {
    fn from(e: RunError<<bb8_postgres::PostgresConnectionManager<bb8_postgres::tokio_postgres::NoTls> as ManageConnection>::Error>) -> Self {
        match e {
            RunError::User(e) => Self::TokioPostgres { source: e },
            RunError::TimedOut => Self::TokioPostgresTimeout,
        }
    }
}

impl From<bb8_postgres::tokio_postgres::error::Error> for Error {
    fn from(e: bb8_postgres::tokio_postgres::error::Error) -> Self {
        Self::TokioPostgres { source: e }
    }
}
