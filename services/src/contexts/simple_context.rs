use crate::error::Result;
use crate::projects::{ProjectId, STRectangle};

use super::{ApplicationContext, SessionId, SimpleSession};

use async_trait::async_trait;

/// A simple application context that provides a default session and session context.
#[async_trait]
pub trait SimpleApplicationContext: ApplicationContext<Session = SimpleSession> {
    async fn default_session_id(&self) -> SessionId;
    async fn default_session(&self) -> Result<SimpleSession>;

    async fn update_default_session_project(&self, project: ProjectId) -> Result<()>;
    async fn update_default_session_view(&self, view: STRectangle) -> Result<()>;

    /// Get a session context for the default session.
    async fn default_session_context(&self) -> Result<Self::SessionContext>;
}
