use warp::Filter;
use std::sync::Arc;
use tokio::sync::RwLock;

use geoengine_services::workflows::registry::HashMapRegistry;
use geoengine_services::handlers;
use geoengine_services::users::userdb::HashMapUserDB;
use geoengine_services::handlers::handle_rejection;
use geoengine_services::projects::projectdb::HashMapProjectDB;

#[tokio::main]
async fn main() {
    let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));
    let workflow_registry = Arc::new(RwLock::new(HashMapRegistry::default()));
    let project_db = Arc::new(RwLock::new(HashMapProjectDB::default()));

    // TODO: hierarchical filters workflow -> (register, load), user -> (register, login, ...)
    warp::serve(
        handlers::workflows::register_workflow_handler(workflow_registry.clone())
            .or(handlers::workflows::load_workflow_handler(workflow_registry.clone()))
            .or(handlers::users::register_user_handler(user_db.clone()))
            .or(handlers::users::login_handler(user_db.clone()))
            .or(handlers::users::logout_handler(user_db.clone()))
            .or(handlers::projects::create_project_handler(user_db.clone(), project_db.clone()))
            .or(handlers::projects::list_projects_handler(user_db.clone(), project_db.clone()))
            .or(handlers::projects::update_project_handler(user_db.clone(), project_db.clone()))
            .or(handlers::projects::delete_project_handler(user_db.clone(), project_db.clone()))
            .or(handlers::projects::add_permission_handler(user_db.clone(), project_db.clone()))
            .or(handlers::projects::remove_permission_handler(user_db.clone(), project_db.clone()))
            .or(handlers::projects::list_permissions_handler(user_db.clone(), project_db.clone()))
            .recover(handle_rejection)
    ).run(([127, 0, 0, 1], 3030)).await
}
