use snafu::Snafu;
use warp::reject::Reject;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    DataType {
        source: geoengine_datatypes::error::Error
    },
    HTTP {
        source: warp::http::Error
    },

    #[snafu(display("Registration failed: {:?}", reason))]
    RegistrationFailed { reason : String },
    LoginFailed,
    LogoutFailed,
    SessionDoesNotExist,
    InvalidSessionToken,

    ProjectCreateFailed,
    ProjectListFailed,
    ProjectLoadFailed,
    ProjectUpdateFailed,
    ProjectDeleteFailed,
    PermissionFailed
}

impl Reject for Error {}

impl From<geoengine_datatypes::error::Error> for Error {
    fn from(datatypes_error: geoengine_datatypes::error::Error) -> Self {
        Error::DataType {
            source: datatypes_error,
        }
    }
}

// TODO: generic way to wrap external errors
impl From<warp::http::Error> for Error {
    fn from(http_error: warp::http::Error) -> Self {
        Error::HTTP {
            source: http_error,
        }
    }
}
