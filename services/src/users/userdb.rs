use crate::error::Result;
use crate::users::session::{Session, SessionId};
use crate::users::user::{UserCredentials, UserId, UserRegistration};
use crate::util::user_input::Validated;
use async_trait::async_trait;

#[async_trait]
pub trait UserDB: Send + Sync {
    /// Registers a user by providing `UserRegistration` parameters
    ///
    /// # Errors
    ///
    /// This call fails if the `UserRegistration` is invalid.
    ///
    async fn register(&mut self, user: Validated<UserRegistration>) -> Result<UserId>;

    /// Creates a `Session` by providing `UserCredentials`
    ///
    /// # Errors
    ///
    /// This call fails if the `UserCredentials` are invalid.
    ///
    async fn login(&mut self, user: UserCredentials) -> Result<Session>;

    /// Removes a session from the `UserDB`
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid.
    ///
    async fn logout(&mut self, session: SessionId) -> Result<()>;

    /// Creates a new `UserDB` session
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid.
    ///
    async fn session(&self, session: SessionId) -> Result<Session>;
}
