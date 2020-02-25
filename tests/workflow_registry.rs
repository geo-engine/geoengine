use geoengine_services::handlers::workflows::{load_workflow_handler, register_workflow_handler};
use geoengine_operators::operators::{ProjectionParameters, RasterSources, GdalSourceParameters, NoSources};
use std::sync::Arc;
use tokio::sync::RwLock;
use geoengine_services::workflows::registry::{HashMapRegistry, WorkflowIdentifier};
use geoengine_services::workflows::Workflow;

use geoengine_operators::Operator;

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn register_and_load_workflow() {
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
        let id: WorkflowIdentifier = serde_json::from_str(&body).unwrap();

        // load workflow again
        let res = warp::test::request()
            .method("GET")
            .path(&format!("/workflow/{}", id.to_string()))
            .reply(&load_workflow_handler(workflow_registry.clone()))
            .await;

        assert_eq!(res.status(), 200);
        assert_eq!(res.body(), &serde_json::to_string(&workflow).unwrap());

        // load not existing workflow
        let res = warp::test::request()
            .method("GET")
            .path("/workflow/1")
            .reply(&load_workflow_handler(workflow_registry.clone()))
            .await;

        assert_eq!(res.status(), 404);
    }
}
