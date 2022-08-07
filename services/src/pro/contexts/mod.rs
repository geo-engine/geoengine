mod in_memory;

#[cfg(feature = "postgres")]
mod postgres;

use std::sync::Arc;

pub use in_memory::ProInMemoryContext;
#[cfg(feature = "postgres")]
pub use postgres::PostgresContext;

use crate::contexts::Context;
use crate::pro::users::{UserDb, UserSession};

use async_trait::async_trait;
use crate::pro::users::oidc::OIDCRequestsDB;

/// A pro contexts that extends the default context.
// TODO: avoid locking the individual DBs here IF they are already thread safe (e.g. guaranteed by postgres)
#[async_trait]
pub trait ProContext: Context<Session = UserSession> {
    type UserDB: UserDb;

    fn user_db(&self) -> Arc<Self::UserDB>;
    fn user_db_ref(&self) -> &Self::UserDB;
    fn oidc_request_db(&self) -> &OIDCRequestsDB;
}
