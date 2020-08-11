use std::sync::Arc;

use tokio::sync::RwLock;
use warp::Filter;

use geoengine_services::handlers;
use geoengine_services::handlers::handle_rejection;
use geoengine_services::projects::hashmap_projectdb::HashMapProjectDB;
use geoengine_services::users::hashmap_userdb::HashMapUserDB;
use geoengine_services::workflows::registry::HashMapRegistry;

#[tokio::main]
async fn main() {
    let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));
    let workflow_registry = Arc::new(RwLock::new(HashMapRegistry::default()));
    let project_db = Arc::new(RwLock::new(HashMapProjectDB::default()));

    // TODO: hierarchical filters workflow -> (register, load), user -> (register, login, ...)
    warp::serve(
        handlers::workflows::register_workflow_handler(workflow_registry.clone())
            .or(handlers::workflows::load_workflow_handler(
                workflow_registry.clone(),
            ))
            .or(handlers::users::register_user_handler(user_db.clone()))
            .or(handlers::users::login_handler(user_db.clone()))
            .or(handlers::users::logout_handler(user_db.clone()))
            .or(handlers::projects::create_project_handler(
                user_db.clone(),
                project_db.clone(),
            ))
            .or(handlers::projects::list_projects_handler(
                user_db.clone(),
                project_db.clone(),
            ))
            .or(handlers::projects::update_project_handler(
                user_db.clone(),
                project_db.clone(),
            ))
            .or(handlers::projects::delete_project_handler(
                user_db.clone(),
                project_db.clone(),
            ))
            .or(handlers::projects::project_versions_handler(
                user_db.clone(),
                project_db.clone(),
            ))
            .or(handlers::projects::add_permission_handler(
                user_db.clone(),
                project_db.clone(),
            ))
            .or(handlers::projects::remove_permission_handler(
                user_db.clone(),
                project_db.clone(),
            ))
            .or(handlers::projects::list_permissions_handler(
                user_db.clone(),
                project_db.clone(),
            ))
            .or(handlers::wms::wms_handler(workflow_registry.clone()))
            .recover(handle_rejection),
    )
    .run(([127, 0, 0, 1], 3030))
    .await
}
