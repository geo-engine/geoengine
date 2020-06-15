use warp::Filter;
use std::sync::Arc;
use warp::reply::Reply;
use uuid::Uuid;

use crate::workflows::registry::{WorkflowRegistry, WorkflowIdentifier};
use crate::workflows::Workflow;
use crate::handlers::DB;
use crate::util::identifiers::Identifier;


// TODO: require authorized access
pub fn register_workflow_handler<T: WorkflowRegistry>(workflow_registry: DB<T>) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone  {
    warp::post()
        .and(warp::path!("workflow" / "register"))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&workflow_registry)))
        .and_then(register_workflow)
}

pub fn load_workflow_handler<T: WorkflowRegistry>(workflow_registry: DB<T>) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone  {
    warp::get()
        .and(warp::path!("workflow" / Uuid))
        .and(warp::any().map(move || Arc::clone(&workflow_registry)))
        .and_then(load_workflow)
}


// TODO: move into handler once async closures are available?
async fn register_workflow<T: WorkflowRegistry>(workflow: Workflow, workflow_registry: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let mut wr = workflow_registry.write().await;
    let id = wr.register(workflow);
    Ok(warp::reply::json(&id))
}

async fn load_workflow<T: WorkflowRegistry>(id: Uuid, workflow_registry: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let wr = workflow_registry.read().await;
    match wr.load(&WorkflowIdentifier::from_uuid(id)) {
        Some(w) => Ok(warp::reply::json(&w).into_response()),
        None => Ok(warp::http::StatusCode::NOT_FOUND.into_response())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflows::registry::{WorkflowRegistry, HashMapRegistry};
    use geoengine_operators::Operator;
    use geoengine_operators::operators::{ProjectionParameters, RasterSources, GdalSourceParameters, NoSources};
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn register() {
        let workflow_registry = Arc::new(RwLock::new(HashMapRegistry::default()));

        let workflow = Workflow {
            operator: Operator::Projection {
                params: ProjectionParameters {
                    src_crs: "EPSG:4326".into(),
                    dest_crs: "EPSG:3857".into(),
                },
                sources: RasterSources {
                    rasters: vec![Operator::GdalSource {
                        params: GdalSourceParameters {
                            source_name: "test".into(),
                            channel: 0,
                        },
                        sources: NoSources {},
                    }],
                }.into(),
            }
        };

        // insert workflow
        let res = warp::test::request()
            .method("POST")
            .path("/workflow/register")
            .header("Content-Length", "0")
            .json(&workflow)
            .reply(&register_workflow_handler(workflow_registry.clone()))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let _id: WorkflowIdentifier = serde_json::from_str(&body).unwrap();
    }

    #[tokio::test]
    async fn load() {
        let workflow_registry = Arc::new(RwLock::new(HashMapRegistry::default()));

        let workflow = Workflow {
            operator: Operator::Projection {
                params: ProjectionParameters {
                    src_crs: "EPSG:4326".into(),
                    dest_crs: "EPSG:3857".into(),
                },
                sources: RasterSources {
                    rasters: vec![Operator::GdalSource {
                        params: GdalSourceParameters {
                            source_name: "test".into(),
                            channel: 0,
                        },
                        sources: NoSources {},
                    }],
                }.into(),
            }
        };

        let id = workflow_registry.write().await.register(workflow.clone());

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/workflow/{}", id.to_string()))
            .reply(&load_workflow_handler(workflow_registry.clone()))
            .await;

        assert_eq!(res.status(), 200);
        assert_eq!(res.body(), &serde_json::to_string(&workflow).unwrap());
    }


    #[tokio::test]
    async fn load_not_exist() {
        let workflow_registry = Arc::new(RwLock::new(HashMapRegistry::default()));

        let res = warp::test::request()
            .method("GET")
            .path("/workflow/1")
            .reply(&load_workflow_handler(workflow_registry.clone()))
            .await;

        assert_eq!(res.status(), 404);
    }
}
