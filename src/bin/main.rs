use warp::Filter;
use std::sync::Arc;
use tokio::sync::RwLock;

use geoengine_services::workflows::registry::HashMapRegistry;
use geoengine_services::handlers;
use geoengine_services::users::userdb::HashMapUserDB;
use geoengine_services::handlers::handle_rejection;

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
