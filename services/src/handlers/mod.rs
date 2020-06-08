use warp::{Rejection, Reply};
use crate::error::Error;
use crate::users::userdb::UserDB;
use crate::users::session::{Session, SessionToken};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::str::FromStr;
use warp::Filter;

pub mod projects;
pub mod users;
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
    if let Some(err) = error.find::<Error>() {
        let json = warp::reply::json(&err.to_string());
        Ok(warp::reply::with_status(
            json,
            warp::http::StatusCode::BAD_REQUEST,
        ))
    } else {
        Err(warp::reject())
    }
}

pub fn authenticate<T: UserDB>(user_db: DB<T>) -> impl warp::Filter<Extract=(Session, ), Error=warp::Rejection> + Clone {
    async fn do_authenticate<T: UserDB>(user_db: DB<T>, token: String) -> Result<Session, warp::Rejection> {
        let token = SessionToken::from_str(&token).map_err(|_| warp::reject())?;
        let db = user_db.read().await;
        db.session(token).map_err(|_| warp::reject())
    }

    warp::any()
        .and(warp::any().map(move || Arc::clone(&user_db)))
        .and(warp::header::<String>("authorization"))
        .and_then(do_authenticate)
}
