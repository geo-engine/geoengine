use super::{Context, SimpleSession};

use async_trait::async_trait;

#[async_trait]
pub trait SimpleContext: Context<Session = SimpleSession> {
    async fn default_session(&self) -> &SimpleSession;

    fn set_default_session(&mut self, session: SimpleSession);
}
