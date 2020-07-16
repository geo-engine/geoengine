use snafu::Snafu;
use warp::reject::Reject;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, PartialEq, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    DataType {
        error: geoengine_datatypes::error::Error,
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

impl From<geoengine_datatypes::error::Error> for Error {
    fn from(datatypes_error: geoengine_datatypes::error::Error) -> Self {
        Error::DataType {
            error: datatypes_error,
        }
    }
}
