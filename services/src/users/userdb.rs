use crate::error::Result;
use crate::users::session::{Session, SessionToken};
use crate::users::user::{UserCredentials, UserIdentification, UserRegistration, Validated};

pub trait UserDB: Send + Sync {
    /// Registers a user by providing `UserRegistration` parameters
    ///
    /// # Errors
    ///
    /// This call fails if the `UserRegistration` is invalid.
    ///
    fn register(&mut self, user: Validated<UserRegistration>) -> Result<UserIdentification>;

    /// Creates a `Session` by providing `UserCredentials`
    ///
    /// # Errors
    ///
    /// This call fails if the `UserCredentials` are invalid.
    ///
    fn login(&mut self, user: UserCredentials) -> Result<Session>;

    /// Removes a session from the `UserDB`
    ///
    /// # Errors
    ///
    /// This call fails if the token is invalid.
    ///
    fn logout(&mut self, session: SessionToken) -> Result<()>;

    /// Creates a new `UserDB` session
    ///
    /// # Errors
    ///
    /// This call fails if the token is invalid.
    ///
    fn session(&self, token: SessionToken) -> Result<Session>;
}
