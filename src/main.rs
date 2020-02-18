use warp::Filter;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use geoengine_operators::Operator;

#[tokio::main]
async fn main() {
    let workflow_registry = Arc::new(RwLock::new(HashMap::<usize, Operator>::new()));
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

async fn register_workflow(operator: Operator, workflow_registry: Arc<RwLock<HashMap::<usize, Operator>>>) -> Result<impl warp::Reply, warp::Rejection> {
    let mut wr = workflow_registry.write().await;
    let id = wr.len();
    wr.insert(id, operator);
    Ok(id.to_string())
}

async fn load_workflow(id: usize, workflow_registry: Arc<RwLock<HashMap::<usize, Operator>>>) -> Result<impl warp::Reply, warp::Rejection> {
    let wr = workflow_registry.read().await;
    match wr.get(&id) {
        Some(operator) => Ok(serde_json::to_string(operator).unwrap()),
        None => Ok("no".into())
    }
}
