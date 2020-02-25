use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, PartialEq, Snafu, Clone)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    RegistrationFailed,
    LoginFailed,
    LogoutFailed,
    SessionDoesNotExist,
    InvalidSessionToken,
}
