use uuid::Uuid;
use warp::reply::Reply;
use warp::Filter;

use crate::handlers::Context;
use crate::util::identifiers::Identifier;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};

// TODO: require authorized access
pub fn register_workflow_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("workflow" / "register"))
        .and(warp::body::json())
        .and(warp::any().map(move || ctx.clone()))
        .and_then(register_workflow)
}

pub fn load_workflow_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path!("workflow" / Uuid))
        .and(warp::any().map(move || ctx.clone()))
        .and_then(load_workflow)
}

// TODO: move into handler once async closures are available?
async fn register_workflow<C: Context>(
    workflow: Workflow,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    let id = ctx.workflow_registry_ref_mut().await.register(workflow)?;
    Ok(warp::reply::json(&id))
}

async fn load_workflow<C: Context>(id: Uuid, ctx: C) -> Result<impl warp::Reply, warp::Rejection> {
    let wf = ctx
        .workflow_registry_ref()
        .await
        .load(&WorkflowId::from_uuid(id))?;
    Ok(warp::reply::json(&wf).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::InMemoryContext;
    use crate::workflows::registry::WorkflowRegistry;
    use geoengine_operators::engine::VectorOperator;
    use geoengine_operators::mock::{MockPointSource, MockPointSourceParams};

    #[tokio::test]
    async fn register() {
        let ctx = InMemoryContext::default();

        let workflow = Workflow {
            operator: MockPointSource {
                params: MockPointSourceParams {
                    points: vec![(0.0, 0.1).into(), (1.0, 1.1).into()],
                },
            }
            .boxed()
            .into(),
        };

        // insert workflow
        let res = warp::test::request()
            .method("POST")
            .path("/workflow/register")
            .header("Content-Length", "0")
            .json(&workflow)
            .reply(&register_workflow_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let _id: WorkflowId = serde_json::from_str(&body).unwrap();
    }

    #[tokio::test]
    async fn load() {
        let ctx = InMemoryContext::default();

        let workflow = Workflow {
            operator: MockPointSource {
                params: MockPointSourceParams {
                    points: vec![(0.0, 0.1).into(), (1.0, 1.1).into()],
                },
            }
            .boxed()
            .into(),
        };

        let id = ctx
            .workflow_registry()
            .write()
            .await
            .register(workflow.clone())
            .unwrap();

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/workflow/{}", id.to_string()))
            .reply(&load_workflow_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);
        assert_eq!(res.body(), &serde_json::to_string(&workflow).unwrap());
    }

    #[tokio::test]
    async fn load_not_exist() {
        let ctx = InMemoryContext::default();

        let res = warp::test::request()
            .method("GET")
            .path("/workflow/1")
            .reply(&load_workflow_handler(ctx))
            .await;

        assert_eq!(res.status(), 404);
    }
}
