use crate::contexts::SessionId;
use crate::error::Result;
use crate::pro::users::{UserCredentials, UserId, UserRegistration, UserSession};
use crate::projects::{ProjectId, STRectangle};
use crate::util::user_input::Validated;
use async_trait::async_trait;

#[async_trait]
pub trait UserDb: Send + Sync {
    /// Registers a user by providing `UserRegistration` parameters
    ///
    /// # Errors
    ///
    /// This call fails if the `UserRegistration` is invalid.
    ///
    async fn register(&mut self, user: Validated<UserRegistration>) -> Result<UserId>;

    /// Creates session for anonymous user
    ///
    /// # Errors
    ///
    /// This call fails if the `UserRegistration` is invalid.
    ///
    async fn anonymous(&mut self) -> Result<UserSession>;

    /// Creates a `Session` by providing `UserCredentials`
    ///
    /// # Errors
    ///
    /// This call fails if the `UserCredentials` are invalid.
    ///
    async fn login(&mut self, user: UserCredentials) -> Result<UserSession>;

    /// Removes a session from the `UserDB`
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid.
    ///
    async fn logout(&mut self, session: SessionId) -> Result<()>;

    /// Get session by id
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid.
    ///
    async fn session(&self, session: SessionId) -> Result<UserSession>;

    /// Sets the session project
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid
    ///
    async fn set_session_project(
        &mut self,
        session: &UserSession,
        project: ProjectId,
    ) -> Result<()>;

    /// Sets the session view
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid
    ///
    async fn set_session_view(&mut self, session: &UserSession, view: STRectangle) -> Result<()>;
}
