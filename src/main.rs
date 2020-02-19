mod workflows;

use warp::Filter;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::reply::Reply;

use workflows::registry::{WorkflowRegistry, HashMapRegistry};
use workflows::Workflow;

#[tokio::main]
async fn main() {
    let workflow_registry = Arc::new(RwLock::new(HashMapRegistry::new()));
    let workflow_registry = warp::any().map(move || Arc::clone(&workflow_registry));

    let register_workflow = warp::post()
        .and(warp::path!("workflow" / "register"))
        .and(warp::body::json())
        .and(workflow_registry.clone())
        .and_then(register_workflow);

    let load_workflow = warp::get()
        .and(warp::path!("workflow" / usize))
        .and(workflow_registry.clone())
        .and_then(load_workflow);

    warp::serve(register_workflow.or(load_workflow)).run(([127, 0, 0, 1], 3030)).await
}

async fn register_workflow(workflow: Workflow, workflow_registry: Arc<RwLock<HashMapRegistry>>) -> Result<impl warp::Reply, warp::Rejection> {
    let mut wr = workflow_registry.write().await;
    let id = wr.register(workflow);
    Ok(warp::reply::json(&id))
}

async fn load_workflow(id: usize, workflow_registry: Arc<RwLock<HashMapRegistry>>) -> Result<impl warp::Reply, warp::Rejection> {
    let wr = workflow_registry.read().await;
    match wr.load(&id) {
        Some(w) => Ok(warp::reply::json(&w).into_response()),
        None => Ok(warp::http::StatusCode::NOT_FOUND.into_response())
    }
}
