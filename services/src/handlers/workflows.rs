use std::collections::HashSet;

use crate::datasets::provenance::ProvenanceProvider;
use crate::error;
use crate::error::Result;
use crate::handlers::Context;
use crate::util::IdResponse;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};
use actix_web::{web, Responder};
use futures::future::join_all;
use geoengine_operators::call_on_typed_operator;
use geoengine_operators::engine::{OperatorDatasets, TypedResultDescriptor};
use snafu::ResultExt;

/// Registers a new [Workflow].
///
/// # Example
///
/// ```text
/// POST /workflow
/// Authorization: Bearer e9da345c-b1df-464b-901c-0335a0419227
///
/// {
///   "type": "Vector",
///   "operator": {
///     "type": "MockPointSource",
///     "params": {
///       "points": [
///         { "x": 0.0, "y": 0.1 },
///         { "x": 1.0, "y": 1.1 }
///       ]
///     }
///   }
/// }
/// ```
/// Response:
/// ```text
/// {
///   "id": "cee25e8c-18a0-5f1b-a504-0bc30de21e06"
/// }
/// ```
pub(crate) async fn register_workflow_handler<C: Context>(
    _session: C::Session,
    ctx: web::Data<C>,
    workflow: web::Json<Workflow>,
) -> Result<impl Responder> {
    let id = ctx
        .workflow_registry_ref_mut()
        .await
        .register(workflow.into_inner())
        .await?;
    Ok(web::Json(IdResponse::from(id)))
}

/// Retrieves an existing [Workflow] using its id.
///
/// # Example
///
/// ```text
/// GET /workflow/cee25e8c-18a0-5f1b-a504-0bc30de21e06
/// Authorization: Bearer e9da345c-b1df-464b-901c-0335a0419227
/// ```
/// Response:
/// ```text
/// {
///   "type": "Vector",
///   "operator": {
///     "type": "MockPointSource",
///     "params": {
///       "points": [
///         {
///           "x": 0.0,
///           "y": 0.1
///         },
///         {
///           "x": 1.0,
///           "y": 1.1
///         }
///       ]
///     }
///   }
/// }
/// ```
pub(crate) async fn load_workflow_handler<C: Context>(
    id: web::Path<WorkflowId>,
    _session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let wf = ctx
        .workflow_registry_ref()
        .await
        .load(&id.into_inner())
        .await?;
    Ok(web::Json(wf))
}

/// Gets the metadata of a workflow.
///
/// # Example
///
/// ```text
/// GET /workflow/cee25e8c-18a0-5f1b-a504-0bc30de21e06/metadata
/// Authorization: Bearer e9da345c-b1df-464b-901c-0335a0419227
/// ```
/// Response:
/// ```text
/// {
///   "dataType": "MultiPoint",
///   "spatialReference": "EPSG:4326",
///   "columns": {}
/// }
/// ```
pub(crate) async fn get_workflow_metadata_handler<C: Context>(
    id: web::Path<WorkflowId>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let workflow = ctx
        .workflow_registry_ref()
        .await
        .load(&id.into_inner())
        .await?;

    let execution_context = ctx.execution_context(session)?;

    // TODO: use cache here
    let result_descriptor: TypedResultDescriptor = call_on_typed_operator!(
        workflow.operator,
        operator => {
            let operator = operator
                .initialize(&execution_context).await
                .context(error::Operator)?;

            #[allow(clippy::clone_on_copy)]
            operator.result_descriptor().clone().into()
        }
    );

    Ok(web::Json(result_descriptor))
}

/// Gets the provenance of all datasets used in a workflow.
///
/// # Example
///
/// ```text
/// GET /workflow/cee25e8c-18a0-5f1b-a504-0bc30de21e06/provenance
/// Authorization: Bearer e9da345c-b1df-464b-901c-0335a0419227
/// ```
/// Response:
/// ```text
/// [{
///   "id": {
///     "type": "internal",
///     "datasetId": "846a823a-6859-4b94-ab0a-c1de80f593d8"
///   },
///   "citation": "Author, Dataset Tile",
///   "license": "Some license",
///   "uri": "http://example.org/"
/// }, {
///   "id": {
///     "type": "internal",
///     "datasetId": "453cd398-f271-437b-9c3d-7f42213ea30a"
///   },
///   "citation": "Another Author, Another Dataset Tile",
///   "license": "Some other license",
///   "uri": "http://example.org/"
/// }]
/// ```
pub(crate) async fn get_workflow_provenance_handler<C: Context>(
    id: web::Path<WorkflowId>,
    _session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let workflow = ctx
        .workflow_registry_ref()
        .await
        .load(&id.into_inner())
        .await?;

    let datasets = workflow.operator.datasets();

    let db = ctx.dataset_db_ref().await;

    let provenance: Vec<_> = datasets.iter().map(|id| db.provenance(id)).collect();
    let provenance: Result<Vec<_>> = join_all(provenance).await.into_iter().collect();

    // filter duplicates
    let provenance: HashSet<_> = provenance?.into_iter().collect();
    let provenance: Vec<_> = provenance.into_iter().collect();

    Ok(web::Json(provenance))
}

/*#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{InMemoryContext, Session, SimpleContext};
    use crate::handlers::{handle_rejection, ErrorResponse};
    use crate::util::tests::{
        add_ndvi_to_datasets, check_allowed_http_methods, check_allowed_http_methods2,
        register_ndvi_workflow_helper,
    };
    use crate::util::IdResponse;
    use crate::workflows::registry::WorkflowRegistry;
    use geoengine_datatypes::collections::MultiPointCollection;
    use geoengine_datatypes::primitives::{FeatureData, Measurement, MultiPoint, TimeInterval};
    use geoengine_datatypes::raster::RasterDataType;
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_operators::engine::{MultipleRasterSources, PlotOperator, TypedOperator};
    use geoengine_operators::engine::{RasterOperator, RasterResultDescriptor, VectorOperator};
    use geoengine_operators::mock::{
        MockFeatureCollectionSource, MockPointSource, MockPointSourceParams, MockRasterSource,
        MockRasterSourceParams,
    };
    use geoengine_operators::plot::{Statistics, StatisticsParams};
    use geoengine_operators::source::{GdalSource, GdalSourceParameters};
    use serde_json::json;
    use warp::http::Response;
    use warp::hyper::body::Bytes;

    async fn register_test_helper(method: &str) -> Response<Bytes> {
        let ctx = InMemoryContext::default();

        let session = ctx.default_session_ref().await;

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
                format!("Bearer {}", session.id().to_string()),
            )
            .json(&workflow)
            .reply(&register_workflow_handler(ctx.clone()).recover(handle_rejection))
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
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        );
    }

    #[tokio::test]
    async fn register_invalid_body() {
        let ctx = InMemoryContext::default();

        let session_id = ctx.default_session_ref().await.id();

        // insert workflow
        let res = warp::test::request()
            .method("POST")
            .path("/workflow")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session_id.to_string()),
            )
            .body("no json")
            .reply(&register_workflow_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "expected ident at line 1 column 2",
        );
    }

    #[tokio::test]
    async fn register_missing_fields() {
        let ctx = InMemoryContext::default();

        let session_id = ctx.default_session_ref().await.id();

        let workflow = json!({});

        // insert workflow
        let res = warp::test::request()
            .method("POST")
            .path("/workflow")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session_id.to_string()),
            )
            .json(&workflow)
            .reply(&register_workflow_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "missing field `type` at line 1 column 2",
        );
    }

    async fn load_test_helper(method: &str) -> (Workflow, Response<Bytes>) {
        let ctx = InMemoryContext::default();

        let session_id = ctx.default_session_ref().await.id();

        let (workflow, id) = register_ndvi_workflow_helper(&ctx).await;

        let res = warp::test::request()
            .method(method)
            .path(&format!("/workflow/{}", id.to_string()))
            .header(
                "Authorization",
                format!("Bearer {}", session_id.to_string()),
            )
            .reply(&load_workflow_handler(ctx.clone()).recover(handle_rejection))
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

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/workflow/{}", id.to_string()))
            .reply(&load_workflow_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
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

        ErrorResponse::assert(res, 404, "NotFound", "Not Found");
    }

    async fn vector_metadata_test_helper(method: &str) -> Response<Bytes> {
        let ctx = InMemoryContext::default();

        let session = ctx.default_session_ref().await;

        let workflow = Workflow {
            operator: MockFeatureCollectionSource::single(
                MultiPointCollection::from_data(
                    MultiPoint::many(vec![(0.0, 0.1)]).unwrap(),
                    vec![TimeInterval::default()],
                    [
                        ("foo".to_string(), FeatureData::Float(vec![42.0])),
                        ("bar".to_string(), FeatureData::Int(vec![23])),
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
                format!("Bearer {}", session.id().to_string()),
            )
            .reply(&get_workflow_metadata_handler(ctx.clone()).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn vector_metadata() {
        let res = vector_metadata_test_helper("GET").await;

        assert_eq!(res.status(), 200, "{:?}", res.body());

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(res.body()).unwrap(),
            json!({
                "type": "vector",
                "dataType": "MultiPoint",
                "spatialReference": "EPSG:4326",
                "columns": {
                    "bar": "int",
                    "foo": "float"
                }
            })
        );
    }

    #[tokio::test]
    async fn raster_metadata() {
        let ctx = InMemoryContext::default();

        let session_id = ctx.default_session_ref().await.id();

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
                        no_data_value: None,
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
                format!("Bearer {}", session_id.to_string()),
            )
            .reply(&get_workflow_metadata_handler(ctx))
            .await;

        assert_eq!(res.status(), 200, "{:?}", res.body());

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(res.body()).unwrap(),
            serde_json::json!({
                "type": "raster",
                "dataType": "U8",
                "spatialReference": "EPSG:4326",
                "measurement": {
                    "type": "continuous",
                    "measurement": "radiation",
                    "unit": null
                },
                "noDataValue": null
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
                        ("foo".to_string(), FeatureData::Float(vec![42.0])),
                        ("bar".to_string(), FeatureData::Int(vec![23])),
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
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        );
    }

    #[tokio::test]
    async fn plot_metadata() {
        let ctx = InMemoryContext::default();

        let session_id = ctx.default_session_ref().await.id();

        let workflow = Workflow {
            operator: Statistics {
                params: StatisticsParams {},
                sources: MultipleRasterSources { rasters: vec![] },
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
                format!("Bearer {}", session_id.to_string()),
            )
            .reply(&get_workflow_metadata_handler(ctx))
            .await;

        assert_eq!(res.status(), 200, "{:?}", res.body());

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(res.body()).unwrap(),
            serde_json::json!({
                "type": "plot",
            })
        );
    }

    #[tokio::test]
    async fn provenance() {
        let ctx = InMemoryContext::default();

        let session_id = ctx.default_session_ref().await.id();

        let dataset = add_ndvi_to_datasets(&ctx).await;

        let workflow = Workflow {
            operator: TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters {
                        dataset: dataset.clone(),
                    },
                }
                .boxed(),
            ),
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
            .path(&format!("/workflow/{}/provenance", id.to_string()))
            .header(
                "Authorization",
                format!("Bearer {}", session_id.to_string()),
            )
            .reply(&get_workflow_provenance_handler(ctx))
            .await;

        assert_eq!(res.status(), 200, "{:?}", res.body());

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(res.body()).unwrap(),
            serde_json::json!([{
                "dataset": {
                    "type": "internal",
                    "datasetId": dataset.internal().unwrap().to_string()
                },
                "provenance": {
                    "citation": "Sample Citation",
                    "license": "Sample License",
                    "uri": "http://example.org/"
                }
            }])
        );
    }
}*/
