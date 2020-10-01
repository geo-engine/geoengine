use crate::error;
use crate::error::Error;
use crate::error::Result;
use crate::projects::hashmap_projectdb::HashMapProjectDB;
use crate::projects::projectdb::ProjectDB;
use crate::users::hashmap_userdb::HashMapUserDB;
use crate::users::session::{Session, SessionToken};
use crate::users::userdb::UserDB;
use crate::workflows::registry::{HashMapRegistry, WorkflowRegistry};
use async_trait::async_trait;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use warp::Filter;
use warp::{Rejection, Reply};

pub mod projects;
pub mod users;
pub mod wfs;
pub mod wms;
pub mod workflows;

type DB<T> = Arc<RwLock<T>>;

/// A handler for custom rejections
///
/// # Errors
///
/// Fails if the rejection is not custom
///
pub async fn handle_rejection(error: Rejection) -> Result<impl Reply, Rejection> {
    // TODO: handle/report serde deserialization error when e.g. a json attribute is missing/malformed
    error.find::<Error>().map_or(Err(warp::reject()), |err| {
        let json = warp::reply::json(&err.to_string());
        Ok(warp::reply::with_status(
            json,
            warp::http::StatusCode::BAD_REQUEST,
        ))
    })
}

fn authenticate<C: Context>(
    ctx: C,
) -> impl warp::Filter<Extract = (C,), Error = warp::Rejection> + Clone {
    async fn do_authenticate<C: Context>(mut ctx: C, token: String) -> Result<C, warp::Rejection> {
        let token = SessionToken::from_str(&token).map_err(|_| warp::reject())?;
        let session = ctx
            .user_db_ref()
            .await
            .session(token)
            .map_err(|_| warp::reject())?;
        ctx.set_session(session);
        Ok(ctx)
    }

    warp::any()
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::header::<String>("authorization"))
        .and_then(do_authenticate)
}

/// A context bundles access to shared resources like databases and session specific information
/// about the user to pass to the services handlers.
#[async_trait]
pub trait Context: 'static + Send + Sync + Clone {
    type UserDB: UserDB;
    type ProjectDB: ProjectDB;
    type WorkflowRegistry: WorkflowRegistry;

    fn user_db(&self) -> DB<Self::UserDB>;
    async fn user_db_ref(&self) -> RwLockReadGuard<Self::UserDB>;
    async fn user_db_ref_mut(&self) -> RwLockWriteGuard<Self::UserDB>;

    fn project_db(&self) -> DB<Self::ProjectDB>;
    async fn project_db_ref(&self) -> RwLockReadGuard<Self::ProjectDB>;
    async fn project_db_ref_mut(&self) -> RwLockWriteGuard<Self::ProjectDB>;

    fn workflow_registry(&self) -> DB<Self::WorkflowRegistry>;
    async fn workflow_registry_ref(&self) -> RwLockReadGuard<Self::WorkflowRegistry>;
    async fn workflow_registry_ref_mut(&self) -> RwLockWriteGuard<Self::WorkflowRegistry>;

    fn session(&self) -> Result<&Session>;

    fn set_session(&mut self, session: Session);
}

/// A context with references to in-memory versions of the individual databases.
#[derive(Clone, Default)]

pub struct InMemoryContext {
    user_db: DB<HashMapUserDB>,
    project_db: DB<HashMapProjectDB>,
    workflow_registry: DB<HashMapRegistry>,
    session: Option<Session>,
}

#[async_trait]
impl Context for InMemoryContext {
    type UserDB = HashMapUserDB;
    type ProjectDB = HashMapProjectDB;
    type WorkflowRegistry = HashMapRegistry;

    fn user_db(&self) -> DB<Self::UserDB> {
        self.user_db.clone()
    }
    async fn user_db_ref(&self) -> RwLockReadGuard<'_, Self::UserDB> {
        self.user_db.read().await
    }
    async fn user_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::UserDB> {
        self.user_db.write().await
    }

    fn project_db(&self) -> DB<Self::ProjectDB> {
        self.project_db.clone()
    }
    async fn project_db_ref(&self) -> RwLockReadGuard<'_, Self::ProjectDB> {
        self.project_db.read().await
    }
    async fn project_db_ref_mut(&self) -> RwLockWriteGuard<'_, Self::ProjectDB> {
        self.project_db.write().await
    }

    fn workflow_registry(&self) -> DB<Self::WorkflowRegistry> {
        self.workflow_registry.clone()
    }
    async fn workflow_registry_ref(&self) -> RwLockReadGuard<'_, Self::WorkflowRegistry> {
        self.workflow_registry.read().await
    }
    async fn workflow_registry_ref_mut(&self) -> RwLockWriteGuard<'_, Self::WorkflowRegistry> {
        self.workflow_registry.write().await
    }

    fn session(&self) -> Result<&Session, Error> {
        self.session
            .as_ref()
            .ok_or(error::Error::SessionNotInitialized)
    }

    fn set_session(&mut self, session: Session) {
        self.session = Some(session)
    }
}
