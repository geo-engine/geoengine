use snafu::Snafu;
use warp::reject::Reject;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, PartialEq, Snafu, Clone)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("Registration failed: {:?}", reason))]
    RegistrationFailed { reason : String },
    LoginFailed,
    LogoutFailed,
    SessionDoesNotExist,
    InvalidSessionToken,
}

impl Reject for Error {}
