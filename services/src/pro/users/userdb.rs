use crate::contexts::SessionId;
use crate::error::Result;
use crate::pro::permissions::RoleId;
use crate::pro::users::oidc::ExternalUserClaims;
use crate::pro::users::{UserCredentials, UserId, UserRegistration, UserSession};
use crate::projects::{ProjectId, STRectangle};
use crate::util::user_input::Validated;
use async_trait::async_trait;
use geoengine_datatypes::primitives::Duration;

#[async_trait]
pub trait UserAuth {
    /// Registers a user by providing `UserRegistration` parameters
    ///
    /// # Errors
    ///
    /// This call fails if the `UserRegistration` is invalid.
    ///
    async fn register_user(&self, user: Validated<UserRegistration>) -> Result<UserId>;

    /// Creates session for anonymous user
    ///
    /// # Errors
    ///
    /// This call fails if the `UserRegistration` is invalid.
    ///
    async fn create_anonymous_session(&self) -> Result<UserSession>;

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

    /// Get session by id
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid.
    ///
    async fn user_session_by_id(&self, session: SessionId) -> Result<UserSession>;
}

#[async_trait]
pub trait UserDb: Send + Sync {
    /// Removes the session from the `UserDB`
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid.
    ///
    async fn logout(&self) -> Result<()>;

    /// Sets the session project
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid
    ///
    async fn set_session_project(&self, project: ProjectId) -> Result<()>;

    /// Sets the session view
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid
    ///
    async fn set_session_view(&self, view: STRectangle) -> Result<()>;

    /// Gets the current users total used quota. `session` is used to identify the user.
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid
    ///
    async fn quota_used(&self) -> Result<u64>;

    /// Gets the current users available quota. `session` is used to identify the user.
    ///
    /// # Errors
    ///
    /// This call fails if the session is invalid
    ///
    async fn quota_available(&self) -> Result<i64>;

    /// Increments a users quota by the given amount
    ///
    /// # Errors
    ///
    /// This call fails if the user is unknown
    ///
    // TODO: move this method to some AdminDb?
    async fn increment_quota_used(&self, user: &UserId, quota_used: u64) -> Result<()>;

    /// Gets a specific users used quota
    ///
    /// # Errors
    ///
    /// This call fails if the user is unknown
    ///
    /// // TODO: move this method to some AdminDb?
    async fn quota_used_by_user(&self, user: &UserId) -> Result<u64>;

    /// Gets a specific users available quota
    ///
    /// # Errors
    ///
    /// This call fails if the user is unknown
    ///
    /// // TODO: move this method to some AdminDb?
    async fn quota_available_by_user(&self, user: &UserId) -> Result<i64>;

    /// Updates a specific users available quota
    ///
    /// # Errors
    ///
    /// This call fails if the user is unknown
    ///
    /// // TODO: move this method to some AdminDb?
    async fn update_quota_available_by_user(
        &self,
        user: &UserId,
        new_available_quota: i64,
    ) -> Result<()>;

    /// Add a new role
    ///// TODO: move this method to some AdminDb?
    async fn add_role(&self, role_name: &str) -> Result<RoleId>;

    /// Remove an existing role
    /// // TODO: move this method to some AdminDb?
    async fn remove_role(&self, role_id: &RoleId) -> Result<()>;

    /// Remove an existing role
    /// // TODO: move this method to some AdminDb?
    async fn assign_role(&self, role_id: &RoleId, user_id: &UserId) -> Result<()>;

    /// Remove an existing role
    /// // TODO: move this method to some AdminDb?
    async fn revoke_role(&self, role_id: &RoleId, user_id: &UserId) -> Result<()>;
}
