use warp::Filter;
use std::sync::Arc;
use tokio::sync::RwLock;

use geoengine_services::workflows::registry::HashMapRegistry;
use geoengine_services::handlers;


#[tokio::main]
async fn main() {
    let workflow_registry = Arc::new(RwLock::new(HashMapRegistry::new()));

    warp::serve(
        handlers::workflows::register_workflow_handler(Arc::clone(&workflow_registry))
        .or(handlers::workflows::load_workflow_handler(workflow_registry.clone()))
    ).run(([127, 0, 0, 1], 3030)).await
}
