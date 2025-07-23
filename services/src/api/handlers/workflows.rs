use crate::api::handlers::tasks::TaskResponse;
use crate::api::model::datatypes::{BandSelection, DataId, TimeInterval};
use crate::api::model::responses::IdResponse;
use crate::api::ogc::util::{parse_bbox, parse_time};
use crate::config::get_config_element;
use crate::contexts::{ApplicationContext, SessionContext};
use crate::datasets::listing::{DatasetProvider, Provenance, ProvenanceOutput};
use crate::datasets::{RasterDatasetFromWorkflow, schedule_raster_dataset_from_workflow_task};
use crate::error::Result;
use crate::layers::storage::LayerProviderDb;
use crate::util::parsing::{
    parse_band_selection, parse_spatial_partition, parse_spatial_resolution,
};
use crate::util::workflows::validate_workflow;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};
use crate::workflows::{WebsocketStreamTask, handle_websocket_message, send_websocket_message};
use actix_web::{FromRequest, HttpRequest, HttpResponse, Responder, web};
use futures::StreamExt;
use futures::future::join_all;
use geoengine_datatypes::error::{BoxedResultExt, ErrorSource};
use geoengine_datatypes::primitives::{
    BoundingBox2D, ColumnSelection, RasterQueryRectangle, SpatialPartition2D, SpatialResolution,
    VectorQueryRectangle,
};
use geoengine_operators::call_on_typed_operator;
use geoengine_operators::engine::{
    ExecutionContext, OperatorData, TypedResultDescriptor, WorkflowOperatorPath,
};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;
use zip::{ZipWriter, write::SimpleFileOptions};

pub(crate) fn init_workflow_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
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
                    )
                    .service(
                        web::resource("/rasterStream")
                            .route(web::get().to(raster_stream_websocket::<C>)),
                    )
                    .service(
                        web::resource("/vectorStream")
                            .route(web::get().to(vector_stream_websocket::<C>)),
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
    request_body(content = Workflow, examples(
        ("MockPointSource" = (value = json!({
            "type": "Vector",
            "operator": {
                "type": "MockPointSource",
                    "params": {
                        "points": [
                            { "x": 0.0, "y": 0.1 },
                            { "x": 1.0, "y": 1.1 }
                        ]
                    }
                }
        }))),
        ("Statistics Plot" = (value = json!({
            "type": "Plot",
            "operator": {
                "type": "Statistics",
                "params": {},
                "sources": {
                    "source": {
                        "type": "OgrSource",
                        "params": {
                            "data": "ne_10m_ports",
                            "attributeProjection": null,
                            "attributeFilters": null
                        }
                    }
                }
            }
        }))))
    ),
    responses(
        (status = 200, response = IdResponse::<WorkflowId>)
    ),
    security(
        ("session_token" = [])
    )
)]
async fn register_workflow_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    workflow: web::Json<Workflow>,
) -> Result<web::Json<IdResponse<WorkflowId>>> {
    let ctx = app_ctx.session_context(session);

    let workflow = workflow.into_inner();

    validate_workflow(&workflow, &ctx.execution_context()?).await?;

    let id = ctx.db().register_workflow(workflow).await?;
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
async fn load_workflow_handler<C: ApplicationContext>(
    id: web::Path<WorkflowId>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    let wf = app_ctx
        .session_context(session)
        .db()
        .load_workflow(&id.into_inner())
        .await?;
    Ok(web::Json(wf))
}

/// Gets the metadata of a workflow
#[utoipa::path(
    tag = "Workflows",
    get,
    path = "/workflow/{id}/metadata",
    responses(
        (status = 200, description = "Metadata of loaded workflow", body = crate::api::model::operators::TypedResultDescriptor,
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
async fn get_workflow_metadata_handler<C: ApplicationContext>(
    id: web::Path<WorkflowId>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    let ctx = app_ctx.session_context(session);

    let workflow = ctx.db().load_workflow(&id.into_inner()).await?;

    let execution_context = ctx.execution_context()?;

    let result_descriptor =
        workflow_metadata::<C::SessionContext>(workflow, execution_context).await?;

    Ok(web::Json(result_descriptor))
}

async fn workflow_metadata<C: SessionContext>(
    workflow: Workflow,
    execution_context: C::ExecutionContext,
) -> Result<TypedResultDescriptor> {
    // TODO: use cache here
    let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

    let result_descriptor: TypedResultDescriptor = call_on_typed_operator!(
        workflow.operator,
        operator => {
            let operator = operator
                .initialize(workflow_operator_path_root, &execution_context).await
                ?;

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
        (status = 200, description = "Provenance of used datasets", body = [ProvenanceEntry],
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
async fn get_workflow_provenance_handler<C: ApplicationContext>(
    id: web::Path<WorkflowId>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    let ctx = app_ctx.session_context(session);

    let workflow: Workflow = ctx.db().load_workflow(&id.into_inner()).await?;

    let provenance = workflow_provenance(&workflow, &ctx).await?;

    Ok(web::Json(provenance))
}

#[derive(Serialize, ToSchema)]
pub struct ProvenanceEntry {
    provenance: Provenance,
    data: Vec<DataId>,
}

async fn workflow_provenance<C: SessionContext>(
    workflow: &Workflow,
    ctx: &C,
) -> Result<Vec<ProvenanceEntry>> {
    let db = ctx.db();
    let execution_ctx = ctx.execution_context()?;

    let data_names = workflow.operator.data_names();
    let mut datasets = Vec::<DataId>::with_capacity(data_names.len());
    for data_name in data_names {
        let data_id = execution_ctx.resolve_named_data(&data_name).await?;
        datasets.push(data_id.into());
    }

    let provenance: Vec<_> = datasets
        .iter()
        .map(|id| resolve_provenance::<C>(&db, id))
        .collect();
    let provenance: Result<Vec<_>> = join_all(provenance).await.into_iter().collect();

    // Filter duplicate DataIds and missing Provenance
    let provenance: HashMap<DataId, Vec<Provenance>> = provenance?
        .into_iter()
        .filter_map(|p| {
            if let Some(provenance) = p.provenance {
                Some((p.data.into(), provenance))
            } else {
                None
            }
        })
        .collect();

    // Group DataIds by Provenance
    let mut result: HashMap<Provenance, Vec<DataId>> = HashMap::new();
    for (data, provenances) in provenance {
        for item in provenances {
            result.entry(item).or_default().push(data.clone());
        }
    }
    let result = result
        .into_iter()
        .map(|(provenance, data)| ProvenanceEntry { provenance, data })
        .collect();

    Ok(result)
}

/// Gets a ZIP archive of the worklow, its provenance and the output metadata.
#[utoipa::path(
    tag = "Workflows",
    get,
    path = "/workflow/{id}/allMetadata/zip",
    responses(
        (status = 200, response = crate::api::model::responses::ZipResponse)
    ),
    params(
        ("id" = WorkflowId, description = "Workflow id")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn get_workflow_all_metadata_zip_handler<C: ApplicationContext>(
    id: web::Path<WorkflowId>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    let ctx = app_ctx.session_context(session);

    let id = id.into_inner();

    let workflow = ctx.db().load_workflow(&id).await?;

    let (metadata, provenance) = futures::try_join!(
        workflow_metadata::<C::SessionContext>(workflow.clone(), ctx.execution_context()?),
        workflow_provenance(&workflow, &ctx),
    )?;

    let output = crate::util::spawn_blocking(move || {
        let mut output = Vec::new();

        let zip_options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
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

async fn resolve_provenance<C: SessionContext>(
    db: &C::GeoEngineDB,
    id: &DataId,
) -> Result<ProvenanceOutput> {
    match id {
        DataId::Internal(internal) => db.load_provenance(&internal.dataset_id.into()).await,
        DataId::External(e) => {
            db.load_layer_provider(e.provider_id.into())
                .await?
                .provenance(&id.into())
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
            example = json!({"taskId": "7f8a4cfe-76ab-4972-b347-b197e5ef0f3c"})
        )
    ),
    params(
        ("id" = WorkflowId, description = "Workflow id")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn dataset_from_workflow_handler<C: ApplicationContext>(
    id: web::Path<WorkflowId>,
    session: C::Session,
    app_ctx: web::Data<C>,
    info: web::Json<RasterDatasetFromWorkflow>,
) -> Result<web::Json<TaskResponse>> {
    let ctx = Arc::new(app_ctx.session_context(session));

    let id = id.into_inner();
    let workflow = ctx.db().load_workflow(&id).await?;
    let compression_num_threads =
        get_config_element::<crate::config::Gdal>()?.compression_num_threads;

    let task_id = schedule_raster_dataset_from_workflow_task(
        format!("workflow {id}"),
        id,
        workflow,
        ctx,
        info.into_inner(),
        compression_num_threads,
    )
    .await?;

    Ok(web::Json(TaskResponse::new(task_id)))
}

/// The query parameters for `raster_stream_websocket`.
#[derive(Clone, Debug, PartialEq, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct RasterStreamWebsocketQuery {
    #[serde(deserialize_with = "parse_spatial_partition")]
    #[param(value_type = crate::api::model::datatypes::SpatialPartition2D)]
    pub spatial_bounds: SpatialPartition2D,
    #[serde(deserialize_with = "parse_time")]
    #[param(value_type = String)]
    pub time_interval: TimeInterval,
    #[serde(deserialize_with = "parse_spatial_resolution")]
    #[param(value_type = crate::api::model::datatypes::SpatialResolution)]
    pub spatial_resolution: SpatialResolution,
    #[serde(deserialize_with = "parse_band_selection")]
    #[param(value_type = String)]
    pub attributes: BandSelection,
    pub result_type: RasterStreamWebsocketResultType,
}

/// The stream result type for `raster_stream_websocket`.
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum RasterStreamWebsocketResultType {
    Arrow,
}

/// Query a workflow raster result as a stream of tiles via a websocket connection.
#[utoipa::path(
    tag = "Workflows",
    get,
    path = "/workflow/{id}/rasterStream",
    responses(
        (
            status = 101,
            description = "Upgrade to websocket connection",
            example = json!({
                "connection": "upgrade",
                "upgrade": "websocket",
                "sec-websocket-accept": "c31quXa8jR1ISUyecRy0pTdNLXk",
                "date": "Mon, 13 Feb 2023 12:10:15 GMT",
            })
        )
    ),
    params(
        ("id" = WorkflowId, description = "Workflow id"),
        RasterStreamWebsocketQuery,
    ),
    security(
        ("session_token" = [])
    )
)]
async fn raster_stream_websocket<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    session: C::Session,
    id: web::Path<WorkflowId>,
    query: web::Query<RasterStreamWebsocketQuery>,
    request: HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse> {
    let ctx = app_ctx.session_context(session);

    let workflow_id = id.into_inner();
    let workflow = ctx.db().load_workflow(&workflow_id).await?;

    let operator = workflow
        .operator
        .get_raster()
        .boxed_context(error::WorkflowMustBeOfTypeRaster)?;

    let query_rectangle = RasterQueryRectangle {
        spatial_bounds: query.spatial_bounds,
        time_interval: query.time_interval.into(),
        spatial_resolution: query.spatial_resolution,
        attributes: query.attributes.clone().try_into()?,
    };

    // this is the only result type for now
    debug_assert!(matches!(
        query.result_type,
        RasterStreamWebsocketResultType::Arrow
    ));

    let mut stream_task = WebsocketStreamTask::new_raster::<C::SessionContext>(
        operator,
        query_rectangle,
        ctx.execution_context()?,
        ctx.query_context(workflow_id.0, Uuid::new_v4())?,
    )
    .await?;

    let (response, mut session, mut msg_stream) = match actix_ws::handle(&request, stream) {
        Ok((response, session, msg_stream)) => (response, session, msg_stream),
        Err(e) => return Ok(e.error_response()),
    };

    actix_web::rt::spawn(async move {
        loop {
            let indicator = tokio::select! {
                Some(Ok(msg)) = msg_stream.next() => {
                    handle_websocket_message(msg, &mut stream_task, &mut session).await
                }

                tile = stream_task.receive_tile() => {
                    send_websocket_message(tile, session.clone()).await
                }

                else => {
                    None
                }
            };

            if indicator.is_none() {
                // the stream ended or session was closed, stop processing
                break;
            }
        }

        stream_task.abort_processing();
        let _ = session.close(None).await;
    });

    Ok(response)
}

/// The query parameters for `raster_stream_websocket`.
#[derive(Copy, Clone, Debug, PartialEq, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct VectorStreamWebsocketQuery {
    #[serde(deserialize_with = "parse_bbox")]
    #[param(value_type = crate::api::model::datatypes::BoundingBox2D)]
    pub spatial_bounds: BoundingBox2D,
    #[serde(deserialize_with = "parse_time")]
    #[param(value_type = String)]
    pub time_interval: TimeInterval,
    #[serde(deserialize_with = "parse_spatial_resolution")]
    #[param(value_type = crate::api::model::datatypes::SpatialResolution)]
    pub spatial_resolution: SpatialResolution,
    pub result_type: RasterStreamWebsocketResultType,
}

/// Query a workflow raster result as a stream of tiles via a websocket connection.
#[utoipa::path(
    tag = "Workflows",
    get,
    path = "/workflow/{id}/vectorStream",
    responses(
        (
            status = 101,
            description = "Upgrade to websocket connection",
            example = json!({
                "connection": "upgrade",
                "upgrade": "websocket",
                "sec-websocket-accept": "c31quXa8jR1ISUyecRy0pTdNLXk",
                "date": "Mon, 13 Feb 2023 12:10:15 GMT",
            })
        )
    ),
    params(
        ("id" = WorkflowId, description = "Workflow id"),
        VectorStreamWebsocketQuery,
    ),
    security(
        ("session_token" = [])
    )
)]
async fn vector_stream_websocket<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    session: C::Session,
    id: web::Path<WorkflowId>,
    query: web::Query<VectorStreamWebsocketQuery>,
    request: HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse> {
    let ctx = app_ctx.session_context(session);

    let workflow_id = id.into_inner();
    let workflow = ctx.db().load_workflow(&workflow_id).await?;

    let operator = workflow
        .operator
        .get_vector()
        .boxed_context(error::WorkflowMustBeOfTypeVector)?;

    let query_rectangle = VectorQueryRectangle {
        spatial_bounds: query.spatial_bounds,
        time_interval: query.time_interval.into(),
        spatial_resolution: query.spatial_resolution,
        attributes: ColumnSelection::all(),
    };

    // this is the only result type for now
    debug_assert!(matches!(
        query.result_type,
        RasterStreamWebsocketResultType::Arrow
    ));

    let mut stream_task = WebsocketStreamTask::new_vector::<C::SessionContext>(
        operator,
        query_rectangle,
        ctx.execution_context()?,
        ctx.query_context(workflow_id.0, Uuid::new_v4())?,
    )
    .await?;

    let (response, mut session, mut msg_stream) = match actix_ws::handle(&request, stream) {
        Ok((response, session, msg_stream)) => (response, session, msg_stream),
        Err(e) => return Ok(e.error_response()),
    };

    actix_web::rt::spawn(async move {
        loop {
            let indicator = tokio::select! {
                Some(Ok(msg)) = msg_stream.next() => {
                    handle_websocket_message(msg, &mut stream_task, &mut session).await
                }

                tile = stream_task.receive_tile() => {
                    send_websocket_message(tile, session.clone()).await
                }

                else => {
                    None
                }
            };

            if indicator.is_none() {
                // the stream ended or session was closed, stop processing
                break;
            }
        }

        stream_task.abort_processing();
        let _ = session.close(None).await;
    });

    Ok(response)
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
    #[snafu(display("You can only query a raster stream for a raster workflow"))]
    WorkflowMustBeOfTypeRaster { source: Box<dyn ErrorSource> },
    #[snafu(display("You can only query a vector stream for a vector workflow"))]
    WorkflowMustBeOfTypeVector { source: Box<dyn ErrorSource> },
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::api::model::responses::ErrorResponse;
    use crate::config::get_config_element;
    use crate::contexts::PostgresContext;
    use crate::contexts::Session;
    use crate::datasets::storage::DatasetStore;
    use crate::datasets::{DatasetName, RasterDatasetFromWorkflowResult};
    use crate::ge_context;
    use crate::tasks::util::test::wait_for_task_to_finish;
    use crate::tasks::{TaskManager, TaskStatus};
    use crate::users::UserAuth;
    use crate::util::tests::add_ports_to_datasets;
    use crate::util::tests::admin_login;
    use crate::util::tests::{
        TestDataUploads, add_ndvi_to_datasets, check_allowed_http_methods,
        check_allowed_http_methods2, read_body_string, register_ndvi_workflow_helper,
        send_test_request,
    };
    use crate::util::websocket_tests;
    use crate::workflows::registry::WorkflowRegistry;
    use actix_web::dev::ServiceResponse;
    use actix_web::{http::Method, http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use futures::StreamExt;
    use geoengine_datatypes::collections::MultiPointCollection;
    use geoengine_datatypes::primitives::CacheHint;
    use geoengine_datatypes::primitives::DateTime;
    use geoengine_datatypes::primitives::{
        ContinuousMeasurement, FeatureData, Measurement, MultiPoint, RasterQueryRectangle,
        SpatialPartition2D, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{GridShape, RasterDataType, TilingSpecification};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::ImageFormat;
    use geoengine_datatypes::util::arrow::arrow_ipc_file_to_record_batches;
    use geoengine_datatypes::util::assert_image_equals_with_format;
    use geoengine_operators::engine::{
        ExecutionContext, MultipleRasterOrSingleVectorSource, PlotOperator, RasterBandDescriptor,
        RasterBandDescriptors, TypedOperator,
    };
    use geoengine_operators::engine::{RasterOperator, RasterResultDescriptor, VectorOperator};
    use geoengine_operators::mock::{
        MockFeatureCollectionSource, MockPointSource, MockPointSourceParams, MockRasterSource,
        MockRasterSourceParams,
    };
    use geoengine_operators::plot::{Statistics, StatisticsParams};
    use geoengine_operators::source::OgrSource;
    use geoengine_operators::source::OgrSourceParameters;
    use geoengine_operators::source::{GdalSource, GdalSourceParameters};
    use geoengine_operators::util::input::MultiRasterOrVectorOperator::Raster;
    use geoengine_operators::util::raster_stream_to_geotiff::{
        GdalGeoTiffDatasetMetadata, GdalGeoTiffOptions,
        single_timestep_raster_stream_to_geotiff_bytes,
    };
    use serde_json::json;
    use std::io::Read;
    use std::sync::Arc;
    use tokio_postgres::NoTls;
    use zip::ZipArchive;
    use zip::read::ZipFile;

    async fn register_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
    ) -> ServiceResponse {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

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
        send_test_request(req, app_ctx).await
    }

    #[ge_context::test]
    async fn register(app_ctx: PostgresContext<NoTls>) {
        let res = register_test_helper(app_ctx, Method::POST).await;

        assert_eq!(res.status(), 200);

        let _id: IdResponse<WorkflowId> = test::read_body_json(res).await;
    }

    #[ge_context::test]
    async fn register_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| register_test_helper(app_ctx.clone(), method),
            &[Method::POST],
        )
        .await;
    }

    #[ge_context::test]
    async fn register_missing_header(app_ctx: PostgresContext<NoTls>) {
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
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "Unauthorized",
            "Authorization error: Header with authorization token not provided.",
        )
        .await;
    }

    #[ge_context::test]
    async fn register_invalid_body(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        // insert workflow
        let req = test::TestRequest::post()
            .uri("/workflow")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::CONTENT_TYPE, mime::APPLICATION_JSON))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_payload("no json");
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "Error in user input: expected ident at line 1 column 2",
        )
        .await;
    }

    #[ge_context::test]
    async fn register_missing_fields(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let workflow = json!({});

        // insert workflow
        let req = test::TestRequest::post()
            .uri("/workflow")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(&workflow);
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "Error in user input: missing field `type` at line 1 column 2",
        )
        .await;
    }

    async fn load_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
    ) -> (Workflow, ServiceResponse) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let (workflow, id) = register_ndvi_workflow_helper(&app_ctx).await;

        let req = test::TestRequest::default()
            .method(method)
            .uri(&format!("/workflow/{id}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        // remove NDVI to allow calling this method again
        ctx.db()
            .delete_dataset(
                ctx.db()
                    .resolve_dataset_name_to_id(&DatasetName::new(None, "NDVI"))
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .await
            .unwrap();

        (workflow, res)
    }

    #[ge_context::test]
    async fn load(app_ctx: PostgresContext<NoTls>) {
        let (workflow, res) = load_test_helper(app_ctx, Method::GET).await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            read_body_string(res).await,
            serde_json::to_string(&workflow).unwrap()
        );
    }

    #[ge_context::test]
    async fn load_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods2(
            |method| load_test_helper(app_ctx.clone(), method),
            &[Method::GET],
            |(_, res)| res,
        )
        .await;
    }

    #[ge_context::test]
    async fn load_missing_header(app_ctx: PostgresContext<NoTls>) {
        let (_, id) = register_ndvi_workflow_helper(&app_ctx).await;

        let req = test::TestRequest::get().uri(&format!("/workflow/{id}"));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "Unauthorized",
            "Authorization error: Header with authorization token not provided.",
        )
        .await;
    }

    #[ge_context::test]
    async fn load_not_exist(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let req = test::TestRequest::get()
            .uri("/workflow/1")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(res, 404, "NotFound", "Not Found").await;
    }

    async fn vector_metadata_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
    ) -> ServiceResponse {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

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
                    CacheHint::default(),
                )
                .unwrap(),
            )
            .boxed()
            .into(),
        };

        let id = ctx.db().register_workflow(workflow.clone()).await.unwrap();

        let req = test::TestRequest::default()
            .method(method)
            .uri(&format!("/workflow/{id}/metadata"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        send_test_request(req, app_ctx).await
    }

    #[ge_context::test]
    async fn vector_metadata(app_ctx: PostgresContext<NoTls>) {
        let res = vector_metadata_test_helper(app_ctx, Method::GET).await;

        let res_status = res.status();
        let res_body = read_body_string(res).await;
        assert_eq!(res_status, 200, "{res_body:?}");

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

    #[ge_context::test]
    async fn raster_metadata(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let workflow = Workflow {
            operator: MockRasterSource::<u8> {
                params: MockRasterSourceParams::<u8> {
                    data: vec![],
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        time: None,
                        bbox: None,
                        resolution: None,
                        bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                            "band".into(),
                            Measurement::Continuous(ContinuousMeasurement {
                                measurement: "radiation".to_string(),
                                unit: None,
                            }),
                        )])
                        .unwrap(),
                    },
                },
            }
            .boxed()
            .into(),
        };

        let id = ctx.db().register_workflow(workflow.clone()).await.unwrap();

        let req = test::TestRequest::get()
            .uri(&format!("/workflow/{id}/metadata"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        let res_body = read_body_string(res).await;
        assert_eq!(res_status, 200, "{res_body:?}");

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&res_body).unwrap(),
            serde_json::json!({
                "type": "raster",
                "dataType": "U8",
                "spatialReference": "EPSG:4326",
                "time": null,
                "bbox": null,
                "resolution": null,
                "bands": [{
                    "name": "band",
                    "measurement": {
                        "type": "continuous",
                        "measurement": "radiation",
                        "unit": null
                    }
                }]
            })
        );
    }

    #[ge_context::test]
    async fn metadata_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| vector_metadata_test_helper(app_ctx.clone(), method),
            &[Method::GET],
        )
        .await;
    }

    #[ge_context::test]
    async fn metadata_missing_header(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

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
                    CacheHint::default(),
                )
                .unwrap(),
            )
            .boxed()
            .into(),
        };

        let id = ctx.db().register_workflow(workflow.clone()).await.unwrap();

        let req = test::TestRequest::get().uri(&format!("/workflow/{id}/metadata"));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "Unauthorized",
            "Authorization error: Header with authorization token not provided.",
        )
        .await;
    }

    #[ge_context::test]
    async fn plot_metadata(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let workflow = Workflow {
            operator: Statistics {
                params: StatisticsParams {
                    column_names: vec![],
                    percentiles: vec![],
                },
                sources: MultipleRasterOrSingleVectorSource {
                    source: Raster(vec![]),
                },
            }
            .boxed()
            .into(),
        };

        let id = ctx.db().register_workflow(workflow.clone()).await.unwrap();

        let req = test::TestRequest::get()
            .uri(&format!("/workflow/{id}/metadata"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        let res_body = read_body_string(res).await;
        assert_eq!(res_status, 200, "{res_body:?}");

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

    #[ge_context::test]
    async fn provenance(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();
        let (dataset_id, dataset) = add_ndvi_to_datasets(&app_ctx).await;

        let workflow = Workflow {
            operator: TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters { data: dataset },
                }
                .boxed(),
            ),
        };

        let id = ctx.db().register_workflow(workflow.clone()).await.unwrap();

        let req = test::TestRequest::get()
            .uri(&format!("/workflow/{id}/provenance"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        let res_body = read_body_string(res).await;
        assert_eq!(res_status, 200, "{res_body:?}");

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&res_body).unwrap(),
            serde_json::json!([
                {
                    "provenance": {
                        "citation": "Sample Citation",
                        "license": "Sample License",
                        "uri": "http://example.org/"
                    },
                    "data": [
                        {
                            "type": "internal",
                            "datasetId": dataset_id.to_string()
                        }
                    ]
                }
            ])
        );
    }

    #[ge_context::test]
    async fn it_does_not_register_invalid_workflow(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());
        let session_id = ctx.session().id();

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
                  "data": "test"
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
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 400);

        let res_body = read_body_string(res).await;
        assert_eq!(
            res_body,
            json!({"error": "InvalidOperatorType", "message": "Invalid operator type: expected Vector found Raster"}).to_string()
        );
    }

    fn test_download_all_metadata_zip_tiling_spec() -> TilingSpecification {
        TilingSpecification {
            origin_coordinate: (0., 0.).into(),
            tile_size_in_pixels: GridShape::new([600, 600]),
        }
    }

    #[ge_context::test(tiling_spec = "test_download_all_metadata_zip_tiling_spec")]
    #[allow(clippy::too_many_lines)]
    async fn test_download_all_metadata_zip(app_ctx: PostgresContext<NoTls>) {
        fn zip_file_to_json<R: std::io::Read>(mut zip_file: ZipFile<R>) -> serde_json::Value {
            let mut bytes = Vec::new();
            zip_file.read_to_end(&mut bytes).unwrap();

            serde_json::from_slice(&bytes).unwrap()
        }

        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let (dataset_id, dataset_name) = add_ndvi_to_datasets(&app_ctx).await;

        let workflow = Workflow {
            operator: TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters {
                        data: dataset_name.clone(),
                    },
                }
                .boxed(),
            ),
        };

        let workflow_id = ctx.db().register_workflow(workflow).await.unwrap();

        // create dataset from workflow
        let req = test::TestRequest::get()
            .uri(&format!("/workflow/{workflow_id}/allMetadata/zip"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx.clone()).await;

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
                        "data": dataset_name
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
                },
                "bands": [{
                        "name": "ndvi",
                        "measurement": {
                            "type": "continuous",
                            "measurement": "vegetation",
                            "unit": null
                        }
                    }]
            })
        );

        assert_eq!(
            zip_file_to_json(zip.by_name("citation.json").unwrap()),
            serde_json::json!([
                {
                    "provenance": {
                        "citation": "Sample Citation",
                        "license": "Sample License",
                        "uri": "http://example.org/"
                    },
                    "data": [
                        {
                            "type": "internal",
                            "datasetId": dataset_id
                        }
                    ]
                }
            ])
        );
    }

    /// override the pixel size since this test was designed for 600 x 600 pixel tiles
    fn dataset_from_workflow_task_success_tiling_spec() -> TilingSpecification {
        TilingSpecification {
            origin_coordinate: (0., 0.).into(),
            tile_size_in_pixels: GridShape::new([600, 600]),
        }
    }

    #[ge_context::test(tiling_spec = "dataset_from_workflow_task_success_tiling_spec")]
    #[allow(clippy::too_many_lines)]
    async fn dataset_from_workflow_task_success(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let (_, dataset) = add_ndvi_to_datasets(&app_ctx).await;

        let workflow = Workflow {
            operator: TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters { data: dataset },
                }
                .boxed(),
            ),
        };

        let workflow_id = ctx.db().register_workflow(workflow).await.unwrap();

        // create dataset from workflow
        let req = test::TestRequest::post()
            .uri(&format!("/datasetFromWorkflow/{workflow_id}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .append_header((header::CONTENT_TYPE, mime::APPLICATION_JSON))
            .set_payload(
                r#"{
                "displayName": "foo",
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
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        let task_response =
            serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

        let tasks = Arc::new(ctx.tasks());

        wait_for_task_to_finish(tasks.clone(), task_response.task_id).await;

        let status = tasks.get_task_status(task_response.task_id).await.unwrap();

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

        // query the newly created dataset
        let op = GdalSource {
            params: GdalSourceParameters {
                data: response.dataset.into(),
            },
        }
        .boxed();

        let exe_ctx = ctx.execution_context().unwrap();

        let o = op
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let query_ctx = ctx.query_context(workflow_id.0, Uuid::new_v4()).unwrap();
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((-10., 80.).into(), (50., 20.).into()).unwrap(),
            time_interval: TimeInterval::new_unchecked(1_388_534_400_000, 1_388_534_400_000 + 1000),
            spatial_resolution: SpatialResolution::zero_point_one(),
            attributes: geoengine_datatypes::primitives::BandSelection::first(),
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
                compression_num_threads: get_config_element::<crate::config::Gdal>()
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

        assert_image_equals_with_format(
            test_data!("raster/geotiff_from_stream_compressed.tiff"),
            result.as_slice(),
            ImageFormat::Tiff,
        );
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_serves_raster_streams_via_websockets(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let (_, dataset) = add_ndvi_to_datasets(&app_ctx).await;

        let workflow = Workflow {
            operator: TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters { data: dataset },
                }
                .boxed(),
            ),
        };

        let workflow_id = ctx.db().register_workflow(workflow).await.unwrap();

        let (req, payload, mut input_tx, send_next_msg_trigger) =
            websocket_tests::test_client().await;

        tokio::task::spawn(async move {
            // Simulate sending messages to the websocket
            for _ in 0..4 {
                websocket_tests::send_text(&mut input_tx, "NEXT").await;
            }
            websocket_tests::send_close(&mut input_tx).await;
        });

        tokio::task::LocalSet::new()
            .run_until(async move {
                let response = raster_stream_websocket(
                    web::Data::new(app_ctx.clone()),
                    session.clone(),
                    web::Path::from(workflow_id),
                    web::Query(RasterStreamWebsocketQuery {
                        spatial_bounds: SpatialPartition2D::new(
                            (-180., 90.).into(),
                            (180., -90.).into(),
                        )
                        .unwrap(),
                        time_interval: TimeInterval::new_instant(DateTime::new_utc(
                            2014, 3, 1, 0, 0, 0,
                        ))
                        .unwrap()
                        .into(),
                        spatial_resolution: SpatialResolution::one(),
                        attributes: geoengine_datatypes::primitives::BandSelection::first().into(),
                        result_type: RasterStreamWebsocketResultType::Arrow,
                    }),
                    req,
                    payload,
                )
                .await
                .unwrap();

                let mut response_stream =
                    websocket_tests::response_messages(response, send_next_msg_trigger)
                        .boxed_local();

                for _ in 0..4 {
                    let tile_bytes = response_stream.next().await.unwrap();

                    let record_batches = arrow_ipc_file_to_record_batches(&tile_bytes).unwrap();
                    assert_eq!(record_batches.len(), 1);
                    let record_batch = record_batches.first().unwrap();
                    let schema = record_batch.schema();

                    assert_eq!(schema.metadata()["spatialReference"], "EPSG:4326");
                }

                assert!(response_stream.next().await.is_none()); // No more messages expected
            })
            .await;
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_serves_vector_streams_via_websockets(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let (_, dataset) = add_ports_to_datasets(&app_ctx, true, true).await;

        let workflow = Workflow {
            operator: TypedOperator::Vector(
                OgrSource {
                    params: OgrSourceParameters {
                        data: dataset,
                        attribute_projection: None,
                        attribute_filters: None,
                    },
                }
                .boxed(),
            ),
        };

        let workflow_id = ctx.db().register_workflow(workflow).await.unwrap();

        let (req, payload, mut input_tx, send_next_msg_trigger) =
            websocket_tests::test_client().await;

        tokio::task::spawn(async move {
            // Simulate sending messages to the websocket
            for _ in 0..1 {
                websocket_tests::send_text(&mut input_tx, "NEXT").await;
            }
            websocket_tests::send_close(&mut input_tx).await;
        });

        tokio::task::LocalSet::new()
            .run_until(async move {
                let response = vector_stream_websocket(
                    web::Data::new(app_ctx.clone()),
                    session.clone(),
                    web::Path::from(workflow_id),
                    web::Query(VectorStreamWebsocketQuery {
                        spatial_bounds: BoundingBox2D::new(
                            (-180., -90.).into(),
                            (180., 90.).into(),
                        )
                        .unwrap(),
                        time_interval: TimeInterval::new_instant(DateTime::new_utc(
                            2014, 3, 1, 0, 0, 0,
                        ))
                        .unwrap()
                        .into(),
                        spatial_resolution: SpatialResolution::one(),
                        result_type: RasterStreamWebsocketResultType::Arrow,
                    }),
                    req,
                    payload,
                )
                .await
                .unwrap();

                let mut response_stream =
                    websocket_tests::response_messages(response, send_next_msg_trigger)
                        .boxed_local();

                for _ in 0..1 {
                    let tile_bytes = response_stream.next().await.unwrap();

                    let record_batches = arrow_ipc_file_to_record_batches(&tile_bytes).unwrap();
                    assert_eq!(record_batches.len(), 1);
                    let record_batch = record_batches.first().unwrap();
                    let schema = record_batch.schema();

                    assert_eq!(schema.metadata()["spatialReference"], "EPSG:4326");
                }

                assert!(response_stream.next().await.is_none()); // No more messages expected
            })
            .await;
    }
}
