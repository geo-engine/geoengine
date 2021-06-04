use super::{Context, Db, SimpleSession};

use async_trait::async_trait;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

#[async_trait]
pub trait SimpleContext: Context<Session = SimpleSession> {
    // async fn default_session(&self) -> &SimpleSession;

    // fn set_default_session(&mut self, session: SimpleSession);

    fn default_session(&self) -> Db<SimpleSession>;
    async fn default_session_ref(&self) -> RwLockReadGuard<SimpleSession>;
    async fn default_session_ref_mut(&self) -> RwLockWriteGuard<SimpleSession>;
}
