use super::{ApplicationContext, Db, SimpleSession};

use async_trait::async_trait;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

/// A simple application context that provides a default session and session context.
#[async_trait]
pub trait SimpleApplicationContext: ApplicationContext<Session = SimpleSession> {
    fn default_session(&self) -> Db<SimpleSession>;
    async fn default_session_ref(&self) -> RwLockReadGuard<SimpleSession>;
    async fn default_session_ref_mut(&self) -> RwLockWriteGuard<SimpleSession>;

    /// Get a session context for the default session.
    async fn default_session_context(&self) -> Self::SessionContext {
        self.session_context(self.default_session_ref().await.clone())
    }
}
