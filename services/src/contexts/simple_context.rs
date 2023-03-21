use super::{ApplicationContext, Db, SimpleSession};

use async_trait::async_trait;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

// TODO: rename SimpleAppContext
#[async_trait]
pub trait SimpleContext: ApplicationContext<Session = SimpleSession> {
    fn default_session(&self) -> Db<SimpleSession>;
    async fn default_session_ref(&self) -> RwLockReadGuard<SimpleSession>;
    async fn default_session_ref_mut(&self) -> RwLockWriteGuard<SimpleSession>;

    async fn default_session_context(&self) -> Self::Context {
        self.session_context(self.default_session_ref().await.clone())
    }
}
