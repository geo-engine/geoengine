use uuid::Uuid;
use warp::reply::Reply;
use warp::Filter;

use crate::error;
use crate::handlers::{authenticate, Context};
use crate::util::IdResponse;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};
use snafu::ResultExt;

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
    Ok(warp::reply::json(&IdResponse::from(id)))
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

pub(crate) fn get_workflow_metadata_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path!("workflow" / Uuid / "metadata"))
        .and(authenticate(ctx))
        .and_then(get_workflow_metadata)
}

// TODO: move into handler once async closures are available?
async fn get_workflow_metadata<C: Context>(
    id: Uuid,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    let workflow = ctx
        .workflow_registry_ref()
        .await
        .load(&WorkflowId(id))
        .await?;

    let operator = workflow.operator.get_vector().context(error::Operator)?;

    let operator = operator
        .initialize(&ctx.execution_context()?)
        .context(error::Operator)?;

    // TODO: use cache here
    let result_descriptor = operator.result_descriptor();

    Ok(warp::reply::json(result_descriptor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::users::user::{UserCredentials, UserRegistration};
    use crate::users::userdb::UserDB;
    use crate::util::user_input::UserInput;
    use crate::util::IdResponse;
    use crate::{contexts::InMemoryContext, workflows::registry::WorkflowRegistry};
    use geoengine_datatypes::collections::MultiPointCollection;
    use geoengine_datatypes::primitives::{FeatureData, MultiPoint, TimeInterval};
    use geoengine_operators::engine::VectorOperator;
    use geoengine_operators::mock::{
        MockFeatureCollectionSource, MockPointSource, MockPointSourceParams,
    };

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
    async fn load_not_exist() {
        let ctx = InMemoryContext::default();

        let res = warp::test::request()
            .method("GET")
            .path("/workflow/1")
            .reply(&load_workflow_handler(ctx))
            .await;

        assert_eq!(res.status(), 404);
    }

    #[tokio::test]
    async fn metadata() {
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
            operator: MockFeatureCollectionSource::single(
                MultiPointCollection::from_data(
                    MultiPoint::many(vec![(0.0, 0.1)]).unwrap(),
                    vec![TimeInterval::default()],
                    [
                        ("foo".to_string(), FeatureData::Number(vec![42.0])),
                        ("bar".to_string(), FeatureData::Decimal(vec![23])),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                )
                .unwrap(),
            )
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
            .path(&format!("/workflow/{}/metadata", id.to_string()))
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&get_workflow_metadata_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(res.body()).unwrap(),
            serde_json::json!({
                "data_type": "MultiPoint",
                "spatial_reference": "EPSG:4326",
                "columns": {
                    "bar": "Decimal",
                    "foo": "Number"
                }
            })
        );
    }
}
