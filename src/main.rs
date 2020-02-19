mod handlers;
mod workflows;

use warp::Filter;
use std::sync::Arc;
use tokio::sync::RwLock;

use workflows::registry::{HashMapRegistry};

#[tokio::main]
async fn main() {
    let workflow_registry = Arc::new(RwLock::new(HashMapRegistry::new()));

    warp::serve(handlers::workflows::register_workflow_handler(workflow_registry.clone())
        .or(handlers::workflows::load_workflow_handler(workflow_registry.clone()))).run(([127, 0, 0, 1], 3030)).await
}


