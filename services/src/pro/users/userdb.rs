use crate::contexts::SessionId;
use crate::error::Result;
use crate::pro::users::oidc::ExternalUserClaims;
use crate::pro::users::{UserCredentials, UserId, UserRegistration, UserSession};
use crate::projects::{ProjectId, STRectangle};
use crate::util::user_input::Validated;
use async_trait::async_trait;
use geoengine_datatypes::primitives::Duration;

#[async_trait]
pub trait UserDb: Send + Sync {
    /// Registers a user by providing `UserRegistration` parameters
    ///
    /// # Errors
    ///
    /// This call fails if the `UserRegistration` is invalid.
    ///
    async fn register(&self, user: Validated<UserRegistration>) -> Result<UserId>;

    /// Creates session for anonymous user
    ///
    /// # Errors
    ///
    /// This call fails if the `UserRegistration` is invalid.
    ///
    async fn anonymous(&self) -> Result<UserSession>;

    /// Creates a `Session` by providing `UserCredentials`
    ///
    /// # Errors
    ///
    /// This call fails if the `UserCredentials` are invalid.
    ///
    async fn login(&self, user: UserCredentials) -> Result<UserSession>;

    /// Creates a `Session` for authorized user by providing `ExternalUserClaims`.
    /// If external user is unknown to the internal system, a new user id is created.
    ///
    /// # Errors
    ///
    /// This call fails if the `ExternalUserClaims` are invalid.
    ///
    async fn login_external(
        &self,
        user: ExternalUserClaims,
        duration: Duration,
    ) -> Result<UserSession>;

    /// Removes a session from the `UserDB`
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid.
    ///
    async fn logout(&self, session: SessionId) -> Result<()>;

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
    async fn set_session_project(&self, session: &UserSession, project: ProjectId) -> Result<()>;

    /// Sets the session view
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid
    ///
    async fn set_session_view(&self, session: &UserSession, view: STRectangle) -> Result<()>;

    /// Increments the users quota by the given amount
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid
    ///
    // TODO: move this method to some AdminDb?
    async fn increment_quota_used(&self, user: &UserId, quota_used: u64) -> Result<()>;

    /// Gets the current users used quota
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid
    ///
    async fn quota_used_by_session(&self, session: &UserSession) -> Result<u64>;

    /// Gets the current users used quota
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid
    ///
    async fn quota_used_by_user(&self, user: &UserId) -> Result<u64>;
}
