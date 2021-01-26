use uuid::Uuid;
use warp::reply::Reply;
use warp::Filter;

use crate::handlers::{authenticate, Context};
use crate::util::identifiers::IdResponse;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};

pub(crate) fn register_workflow_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("workflow"))
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
    warp::get()
        .and(warp::path!("workflow" / Uuid))
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
    use crate::handlers::{handle_rejection, ErrorResponse};
    use crate::users::user::{UserCredentials, UserRegistration};
    use crate::users::userdb::UserDB;
    use crate::util::identifiers::IdResponse;
    use crate::util::user_input::UserInput;
    use crate::{contexts::InMemoryContext, workflows::registry::WorkflowRegistry};
    use geoengine_operators::engine::VectorOperator;
    use geoengine_operators::mock::{MockPointSource, MockPointSourceParams};
    use serde_json::json;

    #[tokio::test]
    async fn register() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

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
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .json(&workflow)
            .reply(&register_workflow_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let _id: IdResponse<WorkflowId> = serde_json::from_str(&body).unwrap();
    }

    #[tokio::test]
    async fn register_invalid_method() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

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
            .method("GET")
            .path("/workflow")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .json(&workflow)
            .reply(&register_workflow_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 405);

        let body = std::str::from_utf8(&res.body()).unwrap();
        assert_eq!(
            serde_json::from_str::<ErrorResponse>(body).unwrap(),
            ErrorResponse {
                error: "MethodNotAllowed".to_string(),
                message: "HTTP method not allowed.".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn register_missing_header() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

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

        assert_eq!(res.status(), 401);

        let body = std::str::from_utf8(&res.body()).unwrap();
        assert_eq!(
            serde_json::from_str::<ErrorResponse>(body).unwrap(),
            ErrorResponse {
                error: "MissingAuthorizationHeader".to_string(),
                message: "Header with authorization token not provided.".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn register_invalid_body() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

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

        assert_eq!(res.status(), 400);

        let body = std::str::from_utf8(&res.body()).unwrap();
        assert_eq!(
            serde_json::from_str::<ErrorResponse>(body).unwrap(),
            ErrorResponse {
                error: "BodyDeserializeError".to_string(),
                message: "expected ident at line 1 column 2".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn register_missing_fields() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

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

        assert_eq!(res.status(), 400);

        let body = std::str::from_utf8(&res.body()).unwrap();
        assert_eq!(
            serde_json::from_str::<ErrorResponse>(body).unwrap(),
            ErrorResponse {
                error: "BodyDeserializeError".to_string(),
                message: "missing field `type` at line 1 column 2".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn load() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

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
            .await
            .unwrap();

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
            .await
            .unwrap();

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/workflow/{}", id.to_string()))
            .reply(&load_workflow_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 401);

        let body = std::str::from_utf8(&res.body()).unwrap();
        assert_eq!(
            serde_json::from_str::<ErrorResponse>(body).unwrap(),
            ErrorResponse {
                error: "MissingAuthorizationHeader".to_string(),
                message: "Header with authorization token not provided.".to_string(),
            }
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

        assert_eq!(res.status(), 404);
    }
}
