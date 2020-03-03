use warp::{Filter, Rejection, Reply};
use std::sync::Arc;
use tokio::sync::RwLock;

use geoengine_services::workflows::registry::HashMapRegistry;
use geoengine_services::handlers;
use geoengine_services::error::Error;
use geoengine_services::users::userdb::HashMapUserDB;

#[tokio::main]
async fn main() {
    let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));
    let workflow_registry = Arc::new(RwLock::new(HashMapRegistry::default()));

    // TODO: hierarchical filters workflow -> (register, load), user -> (register, login, ...)
    warp::serve(
        handlers::workflows::register_workflow_handler(workflow_registry.clone())
            .or(handlers::workflows::load_workflow_handler(workflow_registry.clone()))
            .or(handlers::users::register_user_handler(user_db.clone()))
            .or(handlers::users::login_handler(user_db.clone()))
            .or(handlers::users::logout_handler(user_db.clone()))
            .recover(handle_rejection)
    ).run(([127, 0, 0, 1], 3030)).await
}

async fn handle_rejection(error: Rejection) -> Result<impl Reply, Rejection> {
    if let Some(err) = error.find::<Error>() {
        let json = warp::reply::json(&err.to_string());
        Ok(warp::reply::with_status(json, warp::http::StatusCode::BAD_REQUEST))
    } else {
        Err(warp::reject())
    }
}
