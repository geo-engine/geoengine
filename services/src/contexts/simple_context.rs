use crate::projects::{ProjectId, STRectangle};

use super::{error::SimpleApplicationContextError, ApplicationContext, SessionId, SimpleSession};

use async_trait::async_trait;

/// A simple application context that provides a default session and session context.
#[async_trait]
pub trait SimpleApplicationContext: ApplicationContext<Session = SimpleSession> {
    async fn default_session_id(&self) -> SessionId;
    async fn default_session(&self) -> Result<SimpleSession, SimpleApplicationContextError>;

    async fn update_default_session_project(
        &self,
        project: ProjectId,
    ) -> Result<(), SimpleApplicationContextError>;
    async fn update_default_session_view(
        &self,
        view: STRectangle,
    ) -> Result<(), SimpleApplicationContextError>;

    /// Get a session context for the default session.
    async fn default_session_context(
        &self,
    ) -> Result<Self::SessionContext, SimpleApplicationContextError>;
}
