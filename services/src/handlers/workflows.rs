use uuid::Uuid;
use warp::reply::Reply;
use warp::Filter;

use crate::error;
use crate::handlers::{authenticate, Context};
use crate::users::session::Session;
use crate::util::IdResponse;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};
use geoengine_operators::call_on_typed_operator;
use snafu::ResultExt;

pub(crate) fn register_workflow_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("workflow")
        .and(warp::post())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::body::json())
        .and_then(register_workflow)
}

// TODO: move into handler once async closures are available?
async fn register_workflow<C: Context>(
    _session: Session,
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
    warp::path!("workflow" / Uuid)
        .and(warp::get())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and_then(load_workflow)
}

// TODO: move into handler once async closures are available?
async fn load_workflow<C: Context>(
    id: Uuid,
    _session: Session,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
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
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and_then(get_workflow_metadata)
}

// TODO: move into handler once async closures are available?
async fn get_workflow_metadata<C: Context>(
    id: Uuid,
    session: Session,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    let workflow = ctx
        .workflow_registry_ref()
        .await
        .load(&WorkflowId(id))
        .await?;

    let execution_context = ctx.execution_context(&session)?;

    // TODO: use cache here
    call_on_typed_operator!(
        workflow.operator,
        operator => {
            let operator = operator
                .initialize(&execution_context)
                .context(error::Operator)?;

            let result_descriptor = operator.result_descriptor();

            Ok(warp::reply::json(result_descriptor))
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::InMemoryContext;
    use crate::handlers::{handle_rejection, ErrorResponse};
    use crate::util::tests::{
        check_allowed_http_methods, check_allowed_http_methods2, create_session_helper,
        register_workflow_helper,
    };
    use crate::util::IdResponse;
    use crate::workflows::registry::WorkflowRegistry;
    use geoengine_datatypes::collections::MultiPointCollection;
    use geoengine_datatypes::primitives::{FeatureData, Measurement, MultiPoint, TimeInterval};
    use geoengine_datatypes::raster::RasterDataType;
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_operators::engine::PlotOperator;
    use geoengine_operators::engine::{RasterOperator, RasterResultDescriptor, VectorOperator};
    use geoengine_operators::mock::{
        MockFeatureCollectionSource, MockPointSource, MockPointSourceParams, MockRasterSource,
        MockRasterSourceParams,
    };
    use geoengine_operators::plot::{Statistics, StatisticsParams};
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
        check_allowed_http_methods(register_test_helper, &["POST"]).await;
    }

    #[tokio::test]
    async fn register_missing_header() {
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

    async fn load_test_helper(method: &str) -> (Workflow, Response<Bytes>) {
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

        let (workflow, id) = register_workflow_helper(&ctx).await;

        let res = warp::test::request()
            .method(method)
            .path(&format!("/workflow/{}", id.to_string()))
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&load_workflow_handler(ctx).recover(handle_rejection))
            .await;

        (workflow, res)
    }

    #[tokio::test]
    async fn load() {
        let (workflow, res) = load_test_helper("GET").await;

        assert_eq!(res.status(), 200);
        assert_eq!(res.body(), &serde_json::to_string(&workflow).unwrap());
    }

    #[tokio::test]
    async fn load_invalid_method() {
        check_allowed_http_methods2(load_test_helper, &["GET"], |(_, res)| res).await;
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

    async fn vector_metadata_test_helper(method: &str) -> Response<Bytes> {
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

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

        warp::test::request()
            .method(method)
            .path(&format!("/workflow/{}/metadata", id.to_string()))
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&get_workflow_metadata_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn vector_metadata() {
        let res = vector_metadata_test_helper("GET").await;

        assert_eq!(res.status(), 200, "{:?}", res.body());

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(res.body()).unwrap(),
            json!({
                "data_type": "MultiPoint",
                "spatial_reference": "EPSG:4326",
                "columns": {
                    "bar": "Decimal",
                    "foo": "Number"
                }
            })
        );
    }

    #[tokio::test]
    async fn raster_metadata() {
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

        let workflow = Workflow {
            operator: MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![],
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Continuous {
                            measurement: "radiation".to_string(),
                            unit: None,
                        },
                    },
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
            .path(&format!("/workflow/{}/metadata", id.to_string()))
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&get_workflow_metadata_handler(ctx))
            .await;

        assert_eq!(res.status(), 200, "{:?}", res.body());

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(res.body()).unwrap(),
            serde_json::json!({
                "data_type": "U8",
                "spatial_reference": "EPSG:4326",
                "measurement": {
                    "continuous": {
                        "measurement": "radiation",
                        "unit": null
                    }
                }
            })
        );
    }

    #[tokio::test]
    async fn metadata_invalid_method() {
        check_allowed_http_methods(vector_metadata_test_helper, &["GET"]).await;
    }

    #[tokio::test]
    async fn metadata_missing_header() {
        let ctx = InMemoryContext::default();

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
            .reply(&get_workflow_metadata_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            &res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        );
    }

    #[tokio::test]
    async fn plot_metadata() {
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

        let workflow = Workflow {
            operator: Statistics {
                params: StatisticsParams {},
                raster_sources: vec![],
                vector_sources: vec![],
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
            .path(&format!("/workflow/{}/metadata", id.to_string()))
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&get_workflow_metadata_handler(ctx))
            .await;

        assert_eq!(res.status(), 200, "{:?}", res.body());

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(res.body()).unwrap(),
            serde_json::json!({})
        );
    }
}
