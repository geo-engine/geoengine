use uuid::Uuid;
use warp::reply::Reply;
use warp::Filter;

use crate::handlers::{authenticate, Context};
use crate::util::IdResponse;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};

pub(crate) fn register_workflow_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("workflow")
        .and(warp::post())
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(register_workflow)
}

// TODO: move into handler once async closures are available?
async fn register_workflow<C: Context>(
    ctx: C,
    workflow: Workflow,
) -> Result<impl warp::Reply, warp::Rejection> {
    let id = ctx
        .workflow_registry_ref_mut()
        .await
        .register(workflow)
        .await?;
    Ok(warp::reply::json(&IdResponse::from_id(id)))
}

pub(crate) fn load_workflow_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("workflow" / Uuid)
        .and(warp::get())
        .and(authenticate(ctx))
        .and_then(load_workflow)
}

// TODO: move into handler once async closures are available?
async fn load_workflow<C: Context>(id: Uuid, ctx: C) -> Result<impl warp::Reply, warp::Rejection> {
    let wf = ctx
        .workflow_registry_ref()
        .await
        .load(&WorkflowId(id))
        .await?;
    Ok(warp::reply::json(&wf).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::InMemoryContext;
    use crate::handlers::{handle_rejection, ErrorResponse};
    use crate::util::tests::{create_session_helper, register_workflow_helper};
    use crate::util::IdResponse;
    use geoengine_operators::engine::VectorOperator;
    use geoengine_operators::mock::{MockPointSource, MockPointSourceParams};
    use serde_json::json;
    use warp::http::Response;
    use warp::hyper::body::Bytes;

    async fn register_test_helper(method: &str) -> Response<Bytes> {
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

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
        warp::test::request()
            .method(method)
            .path("/workflow")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .json(&workflow)
            .reply(&register_workflow_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn register() {
        let res = register_test_helper("POST").await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let _id: IdResponse<WorkflowId> = serde_json::from_str(&body).unwrap();
    }

    #[tokio::test]
    async fn register_invalid_method() {
        let res = register_test_helper("GET").await;

        ErrorResponse::assert(&res, 405, "MethodNotAllowed", "HTTP method not allowed.");
    }

    #[tokio::test]
    async fn register_missing_header() {
        let ctx = InMemoryContext::default();

        create_session_helper(&ctx).await;

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
            .path("/workflow")
            .header("Content-Length", "0")
            .json(&workflow)
            .reply(&register_workflow_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            &res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        );
    }

    #[tokio::test]
    async fn register_invalid_body() {
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

        // insert workflow
        let res = warp::test::request()
            .method("POST")
            .path("/workflow")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .body("no json")
            .reply(&register_workflow_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            &res,
            400,
            "BodyDeserializeError",
            "expected ident at line 1 column 2",
        );
    }

    #[tokio::test]
    async fn register_missing_fields() {
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

        let workflow = json!({});

        // insert workflow
        let res = warp::test::request()
            .method("POST")
            .path("/workflow")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .json(&workflow)
            .reply(&register_workflow_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            &res,
            400,
            "BodyDeserializeError",
            "missing field `type` at line 1 column 2",
        );
    }

    #[tokio::test]
    async fn load() {
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

        let (workflow, id) = register_workflow_helper(&ctx).await;

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/workflow/{}", id.to_string()))
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&load_workflow_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);
        assert_eq!(res.body(), &serde_json::to_string(&workflow).unwrap());
    }

    #[tokio::test]
    async fn load_missing_header() {
        let ctx = InMemoryContext::default();

        let (_, id) = register_workflow_helper(&ctx).await;

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/workflow/{}", id.to_string()))
            .reply(&load_workflow_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            &res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        );
    }

    #[tokio::test]
    async fn load_not_exist() {
        let ctx = InMemoryContext::default();

        let res = warp::test::request()
            .method("GET")
            .path("/workflow/1")
            .reply(&load_workflow_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(&res, 404, "NotFound", "Not Found");
    }
}
