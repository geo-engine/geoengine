use crate::error;
use crate::error::Error;
use crate::error::Result;
use crate::projects::hashmap_projectdb::HashMapProjectDB;
use crate::projects::projectdb::ProjectDB;
use crate::users::hashmap_userdb::HashMapUserDB;
use crate::users::session::{Session, SessionToken};
use crate::users::userdb::UserDB;
use crate::workflows::registry::{HashMapRegistry, WorkflowRegistry};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
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
        let user_db = ctx.user_db();
        let db = user_db.read().await;
        ctx.set_session(db.session(token).map_err(|_| warp::reject())?);
        Ok(ctx)
    }

    warp::any()
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::header::<String>("authorization"))
        .and_then(do_authenticate)
}

/// A context bundles access to shared resources like databases and session specific information
/// about the user to pass to the services handlers.
pub trait Context: 'static + Send + Sync + Clone {
    type UserDB: UserDB;
    type ProjectDB: ProjectDB;
    type WorkflowRegistry: WorkflowRegistry;

    fn user_db(&self) -> DB<Self::UserDB>;
    fn project_db(&self) -> DB<Self::ProjectDB>;
    fn workflow_registry(&self) -> DB<Self::WorkflowRegistry>;
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

impl Context for InMemoryContext {
    type UserDB = HashMapUserDB;
    type ProjectDB = HashMapProjectDB;
    type WorkflowRegistry = HashMapRegistry;

    fn user_db(&self) -> DB<Self::UserDB> {
        self.user_db.clone()
    }

    fn project_db(&self) -> DB<Self::ProjectDB> {
        self.project_db.clone()
    }

    fn workflow_registry(&self) -> DB<Self::WorkflowRegistry> {
        self.workflow_registry.clone()
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
