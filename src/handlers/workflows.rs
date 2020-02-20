use warp::Filter;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::reply::Reply;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::Workflow;
use warp::filters::BoxedFilter;

type WR<T> = Arc<RwLock<T>>;

pub fn register_workflow_handler<T: WorkflowRegistry + Send + Sync + 'static>(workflow_registry: WR<T>) -> BoxedFilter<(impl Reply,)>  {
    warp::post()
        .and(warp::path!("workflow" / "register"))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&workflow_registry)))
        .and_then(register_workflow).boxed()
}

pub fn load_workflow_handler<T: WorkflowRegistry + Send + Sync + 'static>(workflow_registry: WR<T>) -> BoxedFilter<(impl Reply,)>  {
    warp::get()
        .and(warp::path!("workflow" / usize))
        .and(warp::any().map(move || Arc::clone(&workflow_registry)))
        .and_then(load_workflow).boxed()
}


// TODO: move into handler once async closures are available?
async fn register_workflow<T: WorkflowRegistry + Send + Sync + 'static>(workflow: Workflow, workflow_registry: WR<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let mut wr = workflow_registry.write().await;
    let id = wr.register(workflow);
    Ok(warp::reply::json(&id))
}

async fn load_workflow<T: WorkflowRegistry + Send + Sync + 'static>(id: usize, workflow_registry: WR<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let wr = workflow_registry.read().await;
    match wr.load(&id) {
        Some(w) => Ok(warp::reply::json(&w).into_response()),
        None => Ok(warp::http::StatusCode::NOT_FOUND.into_response())
    }
}