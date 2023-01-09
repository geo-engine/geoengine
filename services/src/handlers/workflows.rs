use std::collections::HashSet;
use std::io::{Cursor, Write};

use crate::api::model::datatypes::DataId;
use crate::datasets::listing::{DatasetProvider, ProvenanceOutput};
use crate::error::Result;
use crate::handlers::Context;
use crate::layers::storage::LayerProviderDb;
use crate::util::IdResponse;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};
use actix_web::{web, FromRequest, HttpResponse, Responder};
use futures::future::join_all;
use geoengine_datatypes::error::{BoxedResultExt, ErrorSource};
use geoengine_operators::call_on_typed_operator;
use geoengine_operators::engine::{OperatorData, TypedOperator, TypedResultDescriptor};

use crate::datasets::{schedule_raster_dataset_from_workflow_task, RasterDatasetFromWorkflow};
use crate::handlers::tasks::TaskResponse;
use crate::util::config::get_config_element;
use snafu::{ResultExt, Snafu};
use zip::{write::FileOptions, ZipWriter};

pub(crate) fn init_workflow_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: Context,
    C::Session: FromRequest,
{
    cfg.service(
        // TODO: rename to plural `workflows`
        web::scope("/workflow")
            .service(web::resource("").route(web::post().to(register_workflow_handler::<C>)))
            .service(
                web::scope("/{id}")
                    .service(web::resource("").route(web::get().to(load_workflow_handler::<C>)))
                    .service(
                        web::resource("/metadata")
                            .route(web::get().to(get_workflow_metadata_handler::<C>)),
                    )
                    .service(
                        web::resource("/provenance")
                            .route(web::get().to(get_workflow_provenance_handler::<C>)),
                    )
                    .service(
                        web::resource("/allMetadata/zip")
                            .route(web::get().to(get_workflow_all_metadata_zip_handler::<C>)),
                    ),
            ),
    )
    .service(
        web::resource("datasetFromWorkflow/{id}")
            .route(web::post().to(dataset_from_workflow_handler::<C>)),
    );
}

/// Registers a new Workflow.
#[utoipa::path(
    tag = "Workflows",
    post,
    path = "/workflow",
    request_body = Workflow,
    responses(
        (status = 200, description = "OK", body = IdResponse,
            example = json!({"id": "cee25e8c-18a0-5f1b-a504-0bc30de21e06"})
        )
    ),
    security(
        ("session_token" = [])
    )
)]
async fn register_workflow_handler<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    workflow: web::Json<Workflow>,
) -> Result<impl Responder> {
    let workflow = workflow.into_inner();

    // ensure the workflow is valid by initializing it
    let execution_context = ctx.execution_context(session)?;
    match workflow.clone().operator {
        TypedOperator::Vector(o) => {
            o.initialize(&execution_context)
                .await
                .context(crate::error::Operator)?;
        }
        TypedOperator::Raster(o) => {
            o.initialize(&execution_context)
                .await
                .context(crate::error::Operator)?;
        }
        TypedOperator::Plot(o) => {
            o.initialize(&execution_context)
                .await
                .context(crate::error::Operator)?;
        }
    }

    let id = ctx.workflow_registry_ref().register(workflow).await?;
    Ok(web::Json(IdResponse::from(id)))
}

/// Retrieves an existing Workflow.
#[utoipa::path(
    tag = "Workflows",
    get,
    path = "/workflow/{id}",
    responses(
        (status = 200, description = "Workflow loaded from database", body = Workflow,
            example = json!({"type": "Vector", "operator": {"type": "MockPointSource", "params": {"points": [{"x": 0.0, "y": 0.1}, {"x": 1.0, "y": 1.1}]}}})
        )
    ),
    params(
        ("id" = WorkflowId, description = "Workflow id")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn load_workflow_handler<C: Context>(
    id: web::Path<WorkflowId>,
    _session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let wf = ctx.workflow_registry_ref().load(&id.into_inner()).await?;
    Ok(web::Json(wf))
}

/// Gets the metadata of a workflow
#[utoipa::path(
    tag = "Workflows",
    get,
    path = "/workflow/{id}/metadata",
    responses(
        (status = 200, description = "Metadata of loaded workflow", body = TypedResultDescriptor,
            example = json!({"type": "vector", "dataType": "MultiPoint", "spatialReference": "EPSG:4326", "columns": {}})
        )
    ),
    params(
        ("id" = WorkflowId, description = "Workflow id")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn get_workflow_metadata_handler<C: Context>(
    id: web::Path<WorkflowId>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let workflow = ctx.workflow_registry_ref().load(&id.into_inner()).await?;

    let execution_context = ctx.execution_context(session)?;

    let result_descriptor = workflow_metadata::<C>(workflow, execution_context).await?;

    Ok(web::Json(result_descriptor))
}

async fn workflow_metadata<C: Context>(
    workflow: Workflow,
    execution_context: C::ExecutionContext,
) -> Result<TypedResultDescriptor> {
    // TODO: use cache here
    let result_descriptor: TypedResultDescriptor = call_on_typed_operator!(
        workflow.operator,
        operator => {
            let operator = operator
                .initialize(&execution_context).await
                .context(crate::error::Operator)?;

            #[allow(clippy::clone_on_copy)]
            operator.result_descriptor().clone().into()
        }
    );

    Ok(result_descriptor)
}

/// Gets the provenance of all datasets used in a workflow.
#[utoipa::path(
    tag = "Workflows",
    get,
    path = "/workflow/{id}/provenance",
    responses(
        (status = 200, description = "Provenance of used datasets", body = [ProvenanceOutput],
            example = json!([{"dataset": {"type": "internal", "datasetId": "846a823a-6859-4b94-ab0a-c1de80f593d8"}, "provenance": {"citation": "Author, Dataset Tile", "license": "Some license", "uri": "http://example.org/"}}, {"dataset": {"type": "internal", "datasetId": "453cd398-f271-437b-9c3d-7f42213ea30a"}, "provenance": {"citation": "Another Author, Another Dataset Tile", "license": "Some other license", "uri": "http://example.org/"}}])
        )
    ),
    params(
        ("id" = WorkflowId, description = "Workflow id")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn get_workflow_provenance_handler<C: Context>(
    id: web::Path<WorkflowId>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let workflow: Workflow = ctx.workflow_registry_ref().load(&id.into_inner()).await?;

    let provenance = workflow_provenance(&workflow, ctx.get_ref(), session).await?;

    Ok(web::Json(provenance))
}

async fn workflow_provenance<C: Context>(
    workflow: &Workflow,
    ctx: &C,
    session: C::Session,
) -> Result<Vec<ProvenanceOutput>> {
    let datasets: Vec<DataId> = workflow
        .operator
        .data_ids()
        .into_iter()
        .map(Into::into)
        .collect();

    let db = ctx.dataset_db_ref();
    let providers = ctx.layer_provider_db_ref();

    let provenance: Vec<_> = datasets
        .iter()
        .map(|id| resolve_provenance::<C>(&session, db, providers, id))
        .collect();
    let provenance: Result<Vec<_>> = join_all(provenance).await.into_iter().collect();

    // filter duplicates
    let provenance: HashSet<_> = provenance?.into_iter().collect();
    let provenance: Vec<_> = provenance.into_iter().collect();

    Ok(provenance)
}

/// Gets a ZIP archive of the worklow, its provenance and the output metadata.
///
/// # Example
///
/// ```text
/// GET /workflow/cee25e8c-18a0-5f1b-a504-0bc30de21e06/all_metadata/zip
/// Authorization: Bearer e9da345c-b1df-464b-901c-0335a0419227
/// ```
/// Response:
/// <zip archive>
/// ```
async fn get_workflow_all_metadata_zip_handler<C: Context>(
    id: web::Path<WorkflowId>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let id = id.into_inner();

    let workflow = ctx.workflow_registry_ref().load(&id).await?;

    let (metadata, provenance) = futures::try_join!(
        workflow_metadata::<C>(workflow.clone(), ctx.execution_context(session.clone())?),
        workflow_provenance(&workflow, ctx.get_ref(), session),
    )?;

    let output = crate::util::spawn_blocking(move || {
        let mut output = Vec::new();

        let zip_options =
            FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        let mut zip_writer = ZipWriter::new(Cursor::new(&mut output));

        let workflow_filename = "workflow.json";
        zip_writer
            .start_file(workflow_filename, zip_options)
            .boxed_context(error::CannotAddDataToZipFile {
                item: workflow_filename,
            })?;
        zip_writer
            .write_all(serde_json::to_string_pretty(&workflow)?.as_bytes())
            .boxed_context(error::CannotAddDataToZipFile {
                item: workflow_filename,
            })?;

        let metadata_filename = "metadata.json";
        zip_writer
            .start_file(metadata_filename, zip_options)
            .boxed_context(error::CannotAddDataToZipFile {
                item: metadata_filename,
            })?;
        zip_writer
            .write_all(serde_json::to_string_pretty(&metadata)?.as_bytes())
            .boxed_context(error::CannotAddDataToZipFile {
                item: metadata_filename,
            })?;

        let citation_filename = "citation.json";
        zip_writer
            .start_file(citation_filename, zip_options)
            .boxed_context(error::CannotAddDataToZipFile {
                item: citation_filename,
            })?;
        zip_writer
            .write_all(serde_json::to_string_pretty(&provenance)?.as_bytes())
            .boxed_context(error::CannotAddDataToZipFile {
                item: citation_filename,
            })?;

        zip_writer
            .finish()
            .boxed_context(error::CannotFinishZipFile)?;
        drop(zip_writer);

        Result::<Vec<u8>>::Ok(output)
    })
    .await??;

    let response = HttpResponse::Ok()
        .content_type("application/zip")
        .insert_header((
            "content-disposition",
            format!("attachment; filename=\"metadata_{id}.zip\""),
        ))
        .body(web::Bytes::from(output));

    Ok(response)
}

async fn resolve_provenance<C: Context>(
    session: &C::Session,
    datasets: &C::DatasetDB,
    providers: &C::LayerProviderDB,
    id: &DataId,
) -> Result<ProvenanceOutput> {
    match id {
        DataId::Internal { dataset_id } => datasets.provenance(session, dataset_id).await,
        DataId::External(e) => {
            providers
                .layer_provider(e.provider_id)
                .await?
                .provenance(id)
                .await
        }
    }
}

/// Create a task for creating a new dataset from the result of the workflow given by its `id` and the dataset parameters in the request body.
/// Returns the id of the created task
#[utoipa::path(
    tag = "Workflows",
    post,
    path = "/datasetFromWorkflow/{id}",
    request_body = RasterDatasetFromWorkflow,
    responses(
        (status = 200, description = "Id of created task", body = TaskResponse,
        example = json!({"task_id": "7f8a4cfe-76ab-4972-b347-b197e5ef0f3c"})
        )
    ),
    params(
        ("id" = WorkflowId, description = "Workflow id")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn dataset_from_workflow_handler<C: Context>(
    id: web::Path<WorkflowId>,
    session: C::Session,
    ctx: web::Data<C>,
    info: web::Json<RasterDatasetFromWorkflow>,
) -> Result<impl Responder> {
    let workflow = ctx.workflow_registry_ref().load(&id.into_inner()).await?;
    let compression_num_threads =
        get_config_element::<crate::util::config::Gdal>()?.compression_num_threads;

    let task_id = schedule_raster_dataset_from_workflow_task(
        workflow,
        session,
        ctx.into_inner(),
        info.into_inner(),
        compression_num_threads,
    )
    .await?;

    Ok(web::Json(TaskResponse::new(task_id)))
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(module(error), context(suffix(false)))] // disables default `Snafu` suffix
pub enum WorkflowApiError {
    #[snafu(display("Adding data to output ZIP file failed"))]
    CannotAddDataToZipFile {
        item: &'static str,
        source: Box<dyn ErrorSource>,
    },
    #[snafu(display("Finishing to output ZIP file failed"))]
    CannotFinishZipFile { source: Box<dyn ErrorSource> },
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::contexts::{InMemoryContext, Session, SimpleContext};
    use crate::datasets::RasterDatasetFromWorkflowResult;
    use crate::handlers::ErrorResponse;
    use crate::tasks::util::test::wait_for_task_to_finish;
    use crate::tasks::{TaskManager, TaskStatus};
    use crate::util::config::get_config_element;
    use crate::util::tests::{
        add_ndvi_to_datasets, check_allowed_http_methods, check_allowed_http_methods2,
        read_body_string, register_ndvi_workflow_helper, send_test_request, TestDataUploads,
    };
    use crate::util::IdResponse;
    use crate::workflows::registry::WorkflowRegistry;
    use actix_web::dev::ServiceResponse;
    use actix_web::{http::header, http::Method, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::collections::MultiPointCollection;
    use geoengine_datatypes::primitives::{
        ContinuousMeasurement, FeatureData, Measurement, MultiPoint, RasterQueryRectangle,
        SpatialPartition2D, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{GridShape, RasterDataType, TilingSpecification};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{
        ExecutionContext, MultipleRasterOrSingleVectorSource, PlotOperator, TypedOperator,
    };
    use geoengine_operators::engine::{RasterOperator, RasterResultDescriptor, VectorOperator};
    use geoengine_operators::mock::{
        MockFeatureCollectionSource, MockPointSource, MockPointSourceParams, MockRasterSource,
        MockRasterSourceParams,
    };
    use geoengine_operators::plot::{Statistics, StatisticsParams};
    use geoengine_operators::source::{GdalSource, GdalSourceParameters};
    use geoengine_operators::util::input::MultiRasterOrVectorOperator::Raster;
    use geoengine_operators::util::raster_stream_to_geotiff::{
        single_timestep_raster_stream_to_geotiff_bytes, GdalGeoTiffDatasetMetadata,
        GdalGeoTiffOptions,
    };
    use serde_json::json;
    use std::io::Read;
    use zip::read::ZipFile;
    use zip::ZipArchive;

    async fn register_test_helper(method: Method) -> ServiceResponse {
        let ctx = InMemoryContext::test_default();

        let session_id = ctx.default_session_ref().await.id();

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
        let req = test::TestRequest::default()
            .method(method)
            .uri("/workflow")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(&workflow);
        send_test_request(req, ctx).await
    }

    #[tokio::test]
    async fn register() {
        let res = register_test_helper(Method::POST).await;

        assert_eq!(res.status(), 200);

        let _id: IdResponse<WorkflowId> = test::read_body_json(res).await;
    }

    #[tokio::test]
    async fn register_invalid_method() {
        check_allowed_http_methods(register_test_helper, &[Method::POST]).await;
    }

    #[tokio::test]
    async fn register_missing_header() {
        let ctx = InMemoryContext::test_default();

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
        let req = test::TestRequest::post()
            .uri("/workflow")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&workflow);
        let res = send_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        )
        .await;
    }

    #[tokio::test]
    async fn register_invalid_body() {
        let ctx = InMemoryContext::test_default();

        let session_id = ctx.default_session_ref().await.id();

        // insert workflow
        let req = test::TestRequest::post()
            .uri("/workflow")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::CONTENT_TYPE, mime::APPLICATION_JSON))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_payload("no json");
        let res = send_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "expected ident at line 1 column 2",
        )
        .await;
    }

    #[tokio::test]
    async fn register_missing_fields() {
        let ctx = InMemoryContext::test_default();

        let session_id = ctx.default_session_ref().await.id();

        let workflow = json!({});

        // insert workflow
        let req = test::TestRequest::post()
            .uri("/workflow")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(&workflow);
        let res = send_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "missing field `type` at line 1 column 2",
        )
        .await;
    }

    async fn load_test_helper(method: Method) -> (Workflow, ServiceResponse) {
        let ctx = InMemoryContext::test_default();

        let session_id = ctx.default_session_ref().await.id();

        let (workflow, id) = register_ndvi_workflow_helper(&ctx).await;

        let req = test::TestRequest::default()
            .method(method)
            .uri(&format!("/workflow/{}", id))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        (workflow, res)
    }

    #[tokio::test]
    async fn load() {
        let (workflow, res) = load_test_helper(Method::GET).await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            read_body_string(res).await,
            serde_json::to_string(&workflow).unwrap()
        );
    }

    #[tokio::test]
    async fn load_invalid_method() {
        check_allowed_http_methods2(load_test_helper, &[Method::GET], |(_, res)| res).await;
    }

    #[tokio::test]
    async fn load_missing_header() {
        let ctx = InMemoryContext::test_default();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        let req = test::TestRequest::get().uri(&format!("/workflow/{}", id));
        let res = send_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        )
        .await;
    }

    #[tokio::test]
    async fn load_not_exist() {
        let ctx = InMemoryContext::test_default();

        let session_id = ctx.default_session_ref().await.id();

        let req = test::TestRequest::get()
            .uri("/workflow/1")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        ErrorResponse::assert(res, 404, "NotFound", "Not Found").await;
    }

    async fn vector_metadata_test_helper(method: Method) -> ServiceResponse {
        let ctx = InMemoryContext::test_default();

        let session_id = ctx.default_session_ref().await.id();

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
            .workflow_registry_ref()
            .register(workflow.clone())
            .await
            .unwrap();

        let req = test::TestRequest::default()
            .method(method)
            .uri(&format!("/workflow/{}/metadata", id))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        send_test_request(req, ctx).await
    }

    #[tokio::test]
    async fn vector_metadata() {
        let res = vector_metadata_test_helper(Method::GET).await;

        let res_status = res.status();
        let res_body = read_body_string(res).await;
        assert_eq!(res_status, 200, "{:?}", res_body);

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&res_body).unwrap(),
            json!({
                "type": "vector",
                "dataType": "MultiPoint",
                "spatialReference": "EPSG:4326",
                "columns": {
                    "bar": {
                        "dataType": "int",
                        "measurement": {
                            "type": "unitless"
                        }
                    },
                    "foo": {
                        "dataType": "float",
                        "measurement": {
                            "type": "unitless"
                        }
                    }
                },
                "time": null,
                "bbox": null
            })
        );
    }

    #[tokio::test]
    async fn raster_metadata() {
        let ctx = InMemoryContext::test_default();

        let session_id = ctx.default_session_ref().await.id();

        let workflow = Workflow {
            operator: MockRasterSource::<u8> {
                params: MockRasterSourceParams::<u8> {
                    data: vec![],
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Continuous(ContinuousMeasurement {
                            measurement: "radiation".to_string(),
                            unit: None,
                        }),
                        time: None,
                        bbox: None,
                        resolution: None,
                    },
                },
            }
            .boxed()
            .into(),
        };

        let id = ctx
            .workflow_registry_ref()
            .register(workflow.clone())
            .await
            .unwrap();

        let req = test::TestRequest::get()
            .uri(&format!("/workflow/{}/metadata", id))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        let res_status = res.status();
        let res_body = read_body_string(res).await;
        assert_eq!(res_status, 200, "{:?}", res_body);

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&res_body).unwrap(),
            serde_json::json!({
                "type": "raster",
                "dataType": "U8",
                "spatialReference": "EPSG:4326",
                "measurement": {
                    "type": "continuous",
                    "measurement": "radiation",
                    "unit": null
                },
                "time": null,
                "bbox": null,
                "resolution": null
            })
        );
    }

    #[tokio::test]
    async fn metadata_invalid_method() {
        check_allowed_http_methods(vector_metadata_test_helper, &[Method::GET]).await;
    }

    #[tokio::test]
    async fn metadata_missing_header() {
        let ctx = InMemoryContext::test_default();

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
            .workflow_registry_ref()
            .register(workflow.clone())
            .await
            .unwrap();

        let req = test::TestRequest::get().uri(&format!("/workflow/{}/metadata", id));
        let res = send_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        )
        .await;
    }

    #[tokio::test]
    async fn plot_metadata() {
        let ctx = InMemoryContext::test_default();

        let session_id = ctx.default_session_ref().await.id();

        let workflow = Workflow {
            operator: Statistics {
                params: StatisticsParams {
                    column_names: vec![],
                },
                sources: MultipleRasterOrSingleVectorSource {
                    source: Raster(vec![]),
                },
            }
            .boxed()
            .into(),
        };

        let id = ctx
            .workflow_registry_ref()
            .register(workflow.clone())
            .await
            .unwrap();

        let req = test::TestRequest::get()
            .uri(&format!("/workflow/{}/metadata", id))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        let res_status = res.status();
        let res_body = read_body_string(res).await;
        assert_eq!(res_status, 200, "{:?}", res_body);

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&res_body).unwrap(),
            serde_json::json!({
                "type": "plot",
                "spatialReference": "",
                "time": null,
                "bbox": null
            })
        );
    }

    #[tokio::test]
    async fn provenance() {
        let ctx = InMemoryContext::test_default();

        let session_id = ctx.default_session_ref().await.id();

        let dataset = add_ndvi_to_datasets(&ctx).await;

        let workflow = Workflow {
            operator: TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters {
                        data: dataset.into(),
                    },
                }
                .boxed(),
            ),
        };

        let id = ctx
            .workflow_registry_ref()
            .register(workflow.clone())
            .await
            .unwrap();

        let req = test::TestRequest::get()
            .uri(&format!("/workflow/{}/provenance", id))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        let res_status = res.status();
        let res_body = read_body_string(res).await;
        assert_eq!(res_status, 200, "{:?}", res_body);

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&res_body).unwrap(),
            serde_json::json!([{
                "data": {
                    "type": "internal",
                    "datasetId": dataset.to_string()
                },
                "provenance": {
                    "citation": "Sample Citation",
                    "license": "Sample License",
                    "uri": "http://example.org/"
                }
            }])
        );
    }

    #[tokio::test]
    async fn it_does_not_register_invalid_workflow() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let workflow = json!({
          "type": "Vector",
          "operator": {
            "type": "Reprojection",
            "params": {
              "targetSpatialReference": "EPSG:4326"
            },
            "sources": {
              "source": {
                "type": "GdalSource",
                "params": {
                  "data": {
                    "type": "internal",
                    "datasetId": "36574dc3-560a-4b09-9d22-d5945f2b8093"
                  }
                }
              }
            }
          }
        });

        let req = test::TestRequest::post()
            .uri("/workflow")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .append_header((header::CONTENT_TYPE, mime::APPLICATION_JSON))
            .set_payload(workflow.to_string());
        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 400);

        let res_body = read_body_string(res).await;
        assert_eq!(
            res_body,
            json!({"error": "Operator", "message": "Operator: Invalid operator type: expected Vector found Raster"}).to_string()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_download_all_metadata_zip() {
        fn zip_file_to_json(mut zip_file: ZipFile) -> serde_json::Value {
            let mut bytes = Vec::new();
            zip_file.read_to_end(&mut bytes).unwrap();

            serde_json::from_slice(&bytes).unwrap()
        }

        let exe_ctx_tiling_spec = TilingSpecification {
            origin_coordinate: (0., 0.).into(),
            tile_size_in_pixels: GridShape::new([600, 600]),
        };

        // override the pixel size since this test was designed for 600 x 600 pixel tiles
        let ctx = InMemoryContext::new_with_context_spec(
            exe_ctx_tiling_spec,
            TestDefault::test_default(),
        );

        let session_id = ctx.default_session_ref().await.id();

        let dataset = add_ndvi_to_datasets(&ctx).await;

        let workflow = Workflow {
            operator: TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters {
                        data: dataset.into(),
                    },
                }
                .boxed(),
            ),
        };

        let workflow_id = ctx
            .workflow_registry_ref()
            .register(workflow)
            .await
            .unwrap();

        // create dataset from workflow
        let req = test::TestRequest::get()
            .uri(&format!("/workflow/{workflow_id}/allMetadata/zip"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let zip_bytes = test::read_body(res).await;

        let mut zip = ZipArchive::new(Cursor::new(zip_bytes)).unwrap();

        assert_eq!(zip.len(), 3);

        assert_eq!(
            zip_file_to_json(zip.by_name("workflow.json").unwrap()),
            serde_json::json!({
                "type": "Raster",
                "operator": {
                    "type": "GdalSource",
                    "params": {
                        "data": {
                            "type": "internal",
                            "datasetId": dataset
                        }
                    }
                }
            })
        );

        assert_eq!(
            zip_file_to_json(zip.by_name("metadata.json").unwrap()),
            serde_json::json!({
                "type": "raster",
                "dataType": "U8",
                "spatialReference": "EPSG:4326",
                "measurement": {
                    "type": "unitless"
                },
                "time": {
                    "start": 1_388_534_400_000_i64,
                    "end": 1_404_172_800_000_i64,
                },
                "bbox": {
                    "upperLeftCoordinate": {
                        "x": -180.0,
                        "y": 90.0,
                    },
                    "lowerRightCoordinate": {
                        "x": 180.0,
                        "y": -90.0
                    }
                },
                "resolution": {
                    "x": 0.1,
                    "y": 0.1
                }
            })
        );

        assert_eq!(
            zip_file_to_json(zip.by_name("citation.json").unwrap()),
            serde_json::json!([{
                "data": {
                    "type": "internal",
                    "datasetId": dataset
                },
                "provenance": {
                    "citation": "Sample Citation",
                    "license": "Sample License",
                    "uri": "http://example.org/"
                }
            }])
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn dataset_from_workflow_task_success() {
        let exe_ctx_tiling_spec = TilingSpecification {
            origin_coordinate: (0., 0.).into(),
            tile_size_in_pixels: GridShape::new([600, 600]),
        };

        // override the pixel size since this test was designed for 600 x 600 pixel tiles
        let ctx = InMemoryContext::new_with_context_spec(
            exe_ctx_tiling_spec,
            TestDefault::test_default(),
        );

        let session_id = ctx.default_session_ref().await.id();

        let dataset = add_ndvi_to_datasets(&ctx).await;

        let workflow = Workflow {
            operator: TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters {
                        data: dataset.into(),
                    },
                }
                .boxed(),
            ),
        };

        let workflow_id = ctx
            .workflow_registry_ref()
            .register(workflow)
            .await
            .unwrap();

        // create dataset from workflow
        let req = test::TestRequest::post()
            .uri(&format!("/datasetFromWorkflow/{}", workflow_id))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .append_header((header::CONTENT_TYPE, mime::APPLICATION_JSON))
            .set_payload(
                r#"{
                "name": "foo",
                "description": null,
                "query": {
                    "spatialBounds": {
                        "upperLeftCoordinate": {
                            "x": -10.0,
                            "y": 80.0
                        },
                        "lowerRightCoordinate": {
                            "x": 50.0,
                            "y": 20.0
                        }
                    },
                    "timeInterval": {
                        "start": 1388534400000,
                        "end": 1388534401000
                    },
                    "spatialResolution": {
                        "x": 0.1,
                        "y": 0.1
                    }
                }
            }"#,
            );
        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        let task_response =
            serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

        wait_for_task_to_finish(ctx.tasks(), task_response.task_id).await;

        let status = ctx.tasks().status(task_response.task_id).await.unwrap();

        let response = if let TaskStatus::Completed { info, .. } = status {
            info.as_any_arc()
                .downcast::<RasterDatasetFromWorkflowResult>()
                .unwrap()
                .as_ref()
                .clone()
        } else {
            panic!("Task must be completed");
        };

        // automatically deletes uploads on drop
        let _test_uploads = TestDataUploads {
            uploads: vec![response.upload],
        };

        let dataset_id: geoengine_datatypes::dataset::DatasetId = response.dataset.into();
        // query the newly created dataset
        let op = GdalSource {
            params: GdalSourceParameters {
                data: dataset_id.into(),
            },
        }
        .boxed();

        let session = ctx.default_session_ref().await.clone();
        let exe_ctx = ctx.execution_context(session.clone()).unwrap();

        let o = op.initialize(&exe_ctx).await.unwrap();

        let query_ctx = ctx.query_context(session.clone()).unwrap();
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap(),
            time_interval: TimeInterval::new_unchecked(1_388_534_400_000, 1_388_534_400_000 + 1000),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };

        let processor = o.query_processor().unwrap().get_u8().unwrap();

        let result = single_timestep_raster_stream_to_geotiff_bytes(
            processor,
            query_rect,
            query_ctx,
            GdalGeoTiffDatasetMetadata {
                no_data_value: Some(0.),
                spatial_reference: SpatialReference::epsg_4326(),
            },
            GdalGeoTiffOptions {
                compression_num_threads: get_config_element::<crate::util::config::Gdal>()
                    .unwrap()
                    .compression_num_threads,
                as_cog: false,
                force_big_tiff: false,
            },
            None,
            Box::pin(futures::future::pending()),
            exe_ctx.tiling_specification(),
        )
        .await
        .unwrap();

        assert_eq!(
            include_bytes!("../../../test_data/raster/geotiff_from_stream_compressed.tiff")
                as &[u8],
            result.as_slice()
        );
    }
}
