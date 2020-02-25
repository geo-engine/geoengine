use warp::Filter;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::reply::Reply;
use std::str::FromStr;
use crate::users::user::{UserDB, UserRegistration, UserCredentials, SessionToken, Session};
use crate::error::Result;

type DB<T> = Arc<RwLock<T>>;

pub fn authenticate<T: UserDB>(user_db: DB<T>) -> impl warp::Filter<Extract = (Session,), Error = warp::Rejection> + Clone {
    async fn do_authenticate<T: UserDB>(user_db: DB<T>, token: String) -> Result<Session, warp::Rejection>  {
        let token = SessionToken::from_str(&token).map_err(|_| warp::reject())?;
        let db = user_db.read().await;
        db.session(token).map_err(|_| warp::reject())
    }

    warp::any()
        .and(warp::any().map(move || Arc::clone(&user_db)))
        .and(warp::header::<String>("authorization"))
        .and_then(do_authenticate)
}

pub fn register_user_handler<T: UserDB>(user_db: DB<T>) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone  {
    warp::post()
        .and(warp::path!("user" / "register"))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&user_db)))
        .and_then(register_user)
}

// TODO: move into handler once async closures are available?
async fn register_user<T: UserDB>(user: UserRegistration, user_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let mut db = user_db.write().await;
    match db.register(user) {
        Ok(id) => Ok(warp::reply::json(&id).into_response()),
        Err(_) => Ok(warp::http::StatusCode::BAD_REQUEST.into_response())
    }
}

pub fn login_handler<T: UserDB>(user_db: DB<T>) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone  {
    warp::post()
        .and(warp::path!("user" / "login"))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&user_db.clone())))
        .and_then(login)
}

// TODO: move into handler once async closures are available?
async fn login<T: UserDB>(user: UserCredentials, user_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let mut db = user_db.write().await;
    match db.login(user) {
        Ok(id) => Ok(warp::reply::json(&id).into_response()),
        Err(_) => Ok(warp::http::StatusCode::UNAUTHORIZED.into_response())
    }
}

pub fn logout_handler<T: UserDB>(user_db: DB<T>) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone  {
    warp::post()
        .and(warp::path!("user" / "logout"))
        .and(authenticate(user_db.clone()))
        .and(warp::any().map(move || Arc::clone(&user_db.clone())))
        .and_then(logout)
}

// TODO: move into handler once async closures are available?
async fn logout<T: UserDB>(session: Session, user_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let mut db = user_db.write().await;
    match db.logout(session.token) {
        Ok(_) => Ok(warp::reply().into_response()),
        Err(_) => Ok(warp::http::StatusCode::UNAUTHORIZED.into_response())
    }
}
