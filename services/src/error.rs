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
}

impl Reject for Error {}
