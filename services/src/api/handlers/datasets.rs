use crate::{
    api::model::{
        operators::{GdalLoadingInfoTemporalSlice, GdalMetaDataList},
        responses::{
            ErrorResponse,
            datasets::{DatasetNameResponse, errors::*},
        },
        services::{
            AddDataset, CreateDataset, DataPath, DatasetDefinition, MetaDataDefinition,
            MetaDataSuggestion, Provenances, UpdateDataset, Volume,
        },
    },
    config::{Data, get_config_element},
    contexts::{ApplicationContext, SessionContext},
    datasets::{
        DatasetName,
        listing::{DatasetListOptions, DatasetListing, DatasetProvider},
        storage::{AutoCreateDataset, Dataset, DatasetStore, SuggestMetaData},
        upload::{AdjustFilePath, Upload, UploadDb, UploadId, UploadRootPath, VolumeName, Volumes},
    },
    error::{self, Error, Result},
    permissions::{Permission, PermissionDb, Role},
    projects::Symbology,
    util::{
        extractors::{ValidatedJson, ValidatedQuery},
        path_with_base_path,
    },
};
use actix_web::{FromRequest, HttpResponse, HttpResponseBuilder, Responder, web};
use gdal::{
    DatasetOptions,
    vector::{Layer, LayerAccess, OGRFieldType},
};
use geoengine_datatypes::{
    collections::VectorDataType,
    error::BoxedResultExt,
    primitives::{
        CacheTtlSeconds, FeatureDataType, Measurement, TimeInterval, VectorQueryRectangle,
    },
    spatial_reference::{SpatialReference, SpatialReferenceOption},
};
use geoengine_operators::{
    engine::{StaticMetaData, VectorColumnInfo, VectorResultDescriptor},
    source::{
        OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceDurationSpec,
        OgrSourceErrorSpec, OgrSourceTimeFormat,
    },
    util::gdal::{
        gdal_open_dataset, gdal_open_dataset_ex, gdal_parameters_from_dataset,
        raster_descriptor_from_dataset,
    },
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    path::Path,
};
use utoipa::{ToResponse, ToSchema};

pub(crate) fn init_dataset_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(
        web::scope("/dataset")
            .service(
                web::resource("/suggest").route(web::post().to(suggest_meta_data_handler::<C>)),
            )
            .service(web::resource("/auto").route(web::post().to(auto_create_dataset_handler::<C>)))
            .service(
                web::resource("/volumes/{volume_name}/files/{file_name}/layers")
                    .route(web::get().to(list_volume_file_layers_handler::<C>)),
            )
            .service(web::resource("/volumes").route(web::get().to(list_volumes_handler::<C>)))
            .service(
                web::resource("/{dataset}/loadingInfo")
                    .route(web::get().to(get_loading_info_handler::<C>))
                    .route(web::put().to(update_loading_info_handler::<C>)),
            )
            .service(
                web::resource("/{dataset}/symbology")
                    .route(web::put().to(update_dataset_symbology_handler::<C>)),
            )
            .service(
                web::resource("/{dataset}/provenance")
                    .route(web::put().to(update_dataset_provenance_handler::<C>)),
            )
            .service(
                web::resource("/{dataset}")
                    .route(web::get().to(get_dataset_handler::<C>))
                    .route(web::post().to(update_dataset_handler::<C>))
                    .route(web::delete().to(delete_dataset_handler::<C>)),
            )
            .service(web::resource("").route(web::post().to(create_dataset_handler::<C>))), // must come last to not match other routes
    )
    .service(web::resource("/datasets").route(web::get().to(list_datasets_handler::<C>)));
}

/// Lists available volumes.
#[utoipa::path(
    tag = "Datasets",
    get,
    path = "/dataset/volumes",
    responses(
        (status = 200, description = "OK", body = [Volume],
            example = json!([
                {
                    "name": "test_data",
                    "path": "./test_data/"
                }
            ])
        ),
        (status = 401, response = crate::api::model::responses::UnauthorizedAdminResponse)
    ),
    security(
        ("session_token" = [])
    )
)]
#[allow(clippy::unused_async)]
pub async fn list_volumes_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<impl Responder> {
    let volumes = app_ctx.session_context(session).volumes()?;
    Ok(web::Json(volumes))
}

/// Lists available datasets.
#[utoipa::path(
    tag = "Datasets",
    get,
    path = "/datasets",
    responses(
        (status = 200, description = "OK", body = [DatasetListing],
            example = json!([
                {
                    "id": {
                        "internal": "9c874b9e-cea0-4553-b727-a13cb26ae4bb"
                    },
                    "name": "Germany",
                    "description": "Boundaries of Germany",
                    "tags": [],
                    "sourceOperator": "OgrSource",
                    "resultDescriptor": {
                        "vector": {
                            "dataType": "MultiPolygon",
                            "spatialReference": "EPSG:4326",
                            "columns": {}
                        }
                    }
                }
            ])
        ),
        (status = 400, response = crate::api::model::responses::BadRequestQueryResponse),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        DatasetListOptions
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn list_datasets_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    options: ValidatedQuery<DatasetListOptions>,
) -> Result<impl Responder> {
    let options = options.into_inner();
    let list = app_ctx
        .session_context(session)
        .db()
        .list_datasets(options)
        .await?;
    Ok(web::Json(list))
}

/// Retrieves details about a dataset using the internal name.
#[utoipa::path(
    tag = "Datasets",
    get,
    path = "/dataset/{dataset}",
    responses(
        (status = 200, description = "OK", body = Dataset,
            example = json!({
                "id": {
                    "internal": "9c874b9e-cea0-4553-b727-a13cb26ae4bb"
                },
                "name": "Germany",
                "description": "Boundaries of Germany",
                "resultDescriptor": {
                    "vector": {
                        "dataType": "MultiPolygon",
                        "spatialReference": "EPSG:4326",
                        "columns": {}
                    }
                },
                "sourceOperator": "OgrSource"
            })
        ),
        (status = 400, description = "Bad request", body = ErrorResponse, examples(
            ("Referenced an unknown dataset" = (value = json!({
                "error": "CannotLoadDataset",
                "message": "CannotLoadDataset: UnknownDatasetName"
            })))
        )),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset Name")
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn get_dataset_handler<C: ApplicationContext>(
    dataset: web::Path<DatasetName>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder, GetDatasetError> {
    let session_ctx = app_ctx.session_context(session).db();

    let real_dataset = dataset.into_inner();

    let dataset_id = session_ctx
        .resolve_dataset_name_to_id(&real_dataset)
        .await
        .context(CannotLoadDataset)?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id
        .ok_or(error::Error::UnknownDatasetName {
            dataset_name: real_dataset.to_string(),
        })
        .context(CannotLoadDataset)?;

    let dataset = session_ctx
        .load_dataset(&dataset_id)
        .await
        .context(CannotLoadDataset)?;

    Ok(web::Json(dataset))
}

/// Update details about a dataset using the internal name.
#[utoipa::path(
    tag = "Datasets",
    post,
    path = "/dataset/{dataset}",
    request_body = UpdateDataset,
    responses(
        (status = 200, description = "OK" ),
        (status = 400, description = "Bad request", body = ErrorResponse, examples(
            ("Referenced an unknown dataset" = (value = json!({
                "error": "CannotLoadDataset",
                "message": "CannotLoadDataset: UnknownDatasetName"
            })))
        )),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset Name"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn update_dataset_handler<C: ApplicationContext>(
    dataset: web::Path<DatasetName>,
    session: C::Session,
    app_ctx: web::Data<C>,
    update: ValidatedJson<UpdateDataset>,
) -> Result<impl Responder, UpdateDatasetError> {
    let session_ctx = app_ctx.session_context(session).db();

    let real_dataset = dataset.into_inner();

    let dataset_id = session_ctx
        .resolve_dataset_name_to_id(&real_dataset)
        .await
        .context(CannotLoadDatasetForUpdate)?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id
        .ok_or(error::Error::UnknownDatasetName {
            dataset_name: real_dataset.to_string(),
        })
        .context(CannotLoadDatasetForUpdate)?;

    session_ctx
        .update_dataset(dataset_id, update.into_inner())
        .await
        .context(CannotUpdateDataset)?;

    Ok(HttpResponse::Ok())
}

/// Retrieves the loading information of a dataset
#[utoipa::path(
    tag = "Datasets",
    get,
    path = "/dataset/{dataset}/loadingInfo",
    responses(
        (status = 200, description = "OK", body = MetaDataDefinition)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset Name")
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn get_loading_info_handler<C: ApplicationContext>(
    dataset: web::Path<DatasetName>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<web::Json<MetaDataDefinition>> {
    let session_ctx = app_ctx.session_context(session).db();

    let real_dataset = dataset.into_inner();

    let dataset_id = session_ctx
        .resolve_dataset_name_to_id(&real_dataset)
        .await?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id.ok_or(error::Error::UnknownDatasetName {
        dataset_name: real_dataset.to_string(),
    })?;

    let dataset = session_ctx.load_loading_info(&dataset_id).await?;

    Ok(web::Json(dataset.into()))
}

/// Updates the dataset's loading info
#[utoipa::path(
    tag = "Datasets",
    put,
    path = "/dataset/{dataset}/loadingInfo",
    request_body = MetaDataDefinition,
    responses(
        (status = 200, description = "OK"),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset Name"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn update_loading_info_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    dataset: web::Path<DatasetName>,
    meta_data: web::Json<MetaDataDefinition>,
) -> Result<HttpResponse> {
    let session_ctx = app_ctx.session_context(session).db();

    let real_dataset = dataset.into_inner();

    let dataset_id = session_ctx
        .resolve_dataset_name_to_id(&real_dataset)
        .await?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id.ok_or(error::Error::UnknownDatasetName {
        dataset_name: real_dataset.to_string(),
    })?;

    session_ctx
        .update_dataset_loading_info(dataset_id, &meta_data.into_inner().into())
        .await?;

    Ok(HttpResponse::Ok().finish())
}

/// Updates the dataset's symbology
#[utoipa::path(
    tag = "Datasets",
    put,
    path = "/dataset/{dataset}/symbology",
    request_body = Symbology,
    responses(
        (status = 200, description = "OK"),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset Name"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn update_dataset_symbology_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    dataset: web::Path<DatasetName>,
    symbology: web::Json<Symbology>,
) -> Result<impl Responder> {
    let session_ctx = app_ctx.session_context(session).db();

    let real_dataset = dataset.into_inner();

    let dataset_id = session_ctx
        .resolve_dataset_name_to_id(&real_dataset)
        .await?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id.ok_or(error::Error::UnknownDatasetName {
        dataset_name: real_dataset.to_string(),
    })?;

    session_ctx
        .update_dataset_symbology(dataset_id, &symbology.into_inner())
        .await?;

    Ok(HttpResponse::Ok())
}

// Updates the dataset's provenance
#[utoipa::path(
    tag = "Datasets",
    put,
    path = "/dataset/{dataset}/provenance",
    request_body = Provenances,
    responses(
        (status = 200, description = "OK"),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset Name"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn update_dataset_provenance_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    dataset: web::Path<DatasetName>,
    provenance: ValidatedJson<Provenances>,
) -> Result<HttpResponseBuilder> {
    let session_ctx = app_ctx.session_context(session).db();

    let real_dataset = dataset.into_inner();

    let dataset_id = session_ctx
        .resolve_dataset_name_to_id(&real_dataset)
        .await?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id.ok_or(error::Error::UnknownDatasetName {
        dataset_name: real_dataset.to_string(),
    })?;

    let provenance = provenance
        .into_inner()
        .provenances
        .into_iter()
        .map(Into::into)
        .collect::<Vec<_>>();

    session_ctx
        .update_dataset_provenance(dataset_id, &provenance)
        .await?;

    Ok(HttpResponse::Ok())
}

pub async fn create_upload_dataset<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    upload_id: UploadId,
    mut definition: DatasetDefinition,
) -> Result<web::Json<DatasetNameResponse>, CreateDatasetError> {
    let db = app_ctx.session_context(session).db();
    let upload = db.load_upload(upload_id).await.context(UploadNotFound)?;

    add_tag(&mut definition.properties, "upload".to_owned());

    adjust_meta_data_path(&mut definition.meta_data, &upload)
        .context(CannotResolveUploadFilePath)?;

    let result = db
        .add_dataset(definition.properties.into(), definition.meta_data.into())
        .await
        .context(CannotCreateDataset)?;

    Ok(web::Json(result.name.into()))
}

pub fn adjust_meta_data_path<A: AdjustFilePath>(
    meta: &mut MetaDataDefinition,
    adjust: &A,
) -> Result<()> {
    match meta {
        MetaDataDefinition::MockMetaData(_) => {}
        MetaDataDefinition::OgrMetaData(m) => {
            m.loading_info.file_name = adjust.adjust_file_path(&m.loading_info.file_name)?;
        }
        MetaDataDefinition::GdalMetaDataRegular(m) => {
            m.params.file_path = adjust.adjust_file_path(&m.params.file_path)?;
        }
        MetaDataDefinition::GdalStatic(m) => {
            m.params.file_path = adjust.adjust_file_path(&m.params.file_path)?;
        }
        MetaDataDefinition::GdalMetadataNetCdfCf(m) => {
            m.params.file_path = adjust.adjust_file_path(&m.params.file_path)?;
        }
        MetaDataDefinition::GdalMetaDataList(m) => {
            for p in &mut m.params {
                if let Some(ref mut params) = p.params {
                    params.file_path = adjust.adjust_file_path(&params.file_path)?;
                }
            }
        }
    }
    Ok(())
}

/// Add the upload tag to the dataset properties.
/// If the tag already exists, it will not be added again.
pub fn add_tag(properties: &mut AddDataset, tag: String) {
    if let Some(ref mut tags) = properties.tags {
        if !tags.contains(&tag) {
            tags.push(tag);
        }
    } else {
        properties.tags = Some(vec![tag]);
    }
}

/// Creates a new dataset using previously uploaded files.
/// The format of the files will be automatically detected when possible.
#[utoipa::path(
    tag = "Datasets",
    post,
    path = "/dataset/auto",
    request_body = AutoCreateDataset,
    responses(
        (status = 200, body = DatasetNameResponse),
        (status = 400, description = "Bad request", body = ErrorResponse, examples(
            ("Body is invalid json" = (value = json!({
                "error": "BodyDeserializeError",
                "message": "expected `,` or `}` at line 13 column 7"
            }))),
            ("Failed to read body" = (value = json!({
                "error": "Payload",
                "message": "Error that occur during reading payload: Can not decode content-encoding."
            }))),
            ("Referenced an unknown upload" = (value = json!({
                "error": "UnknownUploadId",
                "message": "Unknown upload id"
            }))),
            ("Dataset name is empty" = (value = json!({
                "error": "InvalidDatasetName",
                "message": "Invalid dataset name"
            }))),
            ("Upload filename is invalid" = (value = json!({
                "error": "InvalidUploadFileName",
                "message": "Invalid upload file name"
            }))),
            ("File does not exist" = (value = json!({
                "error": "GdalError",
                "message": "GdalError: GDAL method 'GDALOpenEx' returned a NULL pointer. Error msg: 'upload/0bdd1062-7796-4d44-a655-e548144281a6/asdf: No such file or directory'"
            }))),
            ("Dataset has no auto-importable layer" = (value = json!({
                "error": "DatasetHasNoAutoImportableLayer",
                "message": "Dataset has no auto importable layer"
            })))
        )),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse),
        (status = 413, response = crate::api::model::responses::PayloadTooLargeResponse),
        (status = 415, response = crate::api::model::responses::UnsupportedMediaTypeForJsonResponse)
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn auto_create_dataset_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    create: ValidatedJson<AutoCreateDataset>,
) -> Result<web::Json<DatasetNameResponse>> {
    let db = app_ctx.session_context(session).db();
    let upload = db.load_upload(create.upload).await?;

    let create = create.into_inner();

    let main_file_path = upload.id.root_path()?.join(&create.main_file);
    let meta_data = auto_detect_vector_meta_data_definition(&main_file_path, &create.layer_name)?;
    let meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

    let properties = AddDataset {
        name: None,
        display_name: create.dataset_name,
        description: create.dataset_description,
        source_operator: meta_data.source_operator_type().to_owned(),
        symbology: None,
        provenance: None,
        tags: Some(vec!["upload".to_owned(), "auto".to_owned()]),
    };

    let result = db.add_dataset(properties.into(), meta_data).await?;

    Ok(web::Json(result.name.into()))
}

/// Inspects an upload and suggests metadata that can be used when creating a new dataset based on it.
/// Tries to automatically detect the main file and layer name if not specified.
#[utoipa::path(
    tag = "Datasets",
    post,
    path = "/dataset/suggest",
    request_body = SuggestMetaData,
    responses(
        (status = 200, description = "OK", body = MetaDataSuggestion,
            example = json!({
                "mainFile": "germany_polygon.gpkg",
                "metaData": {
                    "type": "OgrMetaData",
                    "loadingInfo": {
                        "fileName": "upload/23c9ea9e-15d6-453b-a243-1390967a5669/germany_polygon.gpkg",
                        "layerName": "test_germany",
                        "dataType": "MultiPolygon",
                        "time": {
                            "type": "none"
                        },
                        "defaultGeometry": null,
                        "columns": {
                            "formatSpecifics": null,
                            "x": "",
                            "y": null,
                            "int": [],
                            "float": [],
                            "text": [],
                            "bool": [],
                            "datetime": [],
                            "rename": null
                        },
                        "forceOgrTimeFilter": false,
                        "forceOgrSpatialFilter": false,
                        "onError": "ignore",
                        "sqlQuery": null,
                        "attributeQuery": null
                    },
                    "resultDescriptor": {
                        "dataType": "MultiPolygon",
                        "spatialReference": "EPSG:4326",
                        "columns": {},
                        "time": null,
                        "bbox": null
                    }
                }
            })
        ),
        (status = 400, description = "Bad request", body = ErrorResponse, examples(
            ("Missing field in query string" = (value = json!({
                "error": "UnableToParseQueryString",
                "message": "Unable to parse query string: missing field `offset`"
            }))),
            ("Number in query string contains letters" = (value = json!({
                "error": "UnableToParseQueryString",
                "message": "Unable to parse query string: invalid digit found in string"
            }))),
            ("Referenced an unknown upload" = (value = json!({
                "error": "UnknownUploadId",
                "message": "Unknown upload id"
            }))),
            ("No suitable mainfile found" = (value = json!({
                "error": "NoMainFileCandidateFound",
                "message": "No main file candidate found"
            }))),
            ("File does not exist" = (value = json!({
                "error": "GdalError",
                "message": "GdalError: GDAL method 'GDALOpenEx' returned a NULL pointer. Error msg: 'upload/0bdd1062-7796-4d44-a655-e548144281a6/asdf: No such file or directory'"
            }))),
            ("Dataset has no auto-importable layer" = (value = json!({
                "error": "DatasetHasNoAutoImportableLayer",
                "message": "Dataset has no auto importable layer"
            })))
        )),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn suggest_meta_data_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    suggest: web::Json<SuggestMetaData>,
) -> Result<impl Responder> {
    let suggest = suggest.into_inner();

    let (root_path, main_file) = match suggest.data_path {
        DataPath::Upload(upload) => {
            let upload = app_ctx
                .session_context(session)
                .db()
                .load_upload(upload)
                .await?;

            let main_file = suggest
                .main_file
                .or_else(|| suggest_main_file(&upload))
                .ok_or(error::Error::NoMainFileCandidateFound)?;

            let root_path = upload.id.root_path()?;

            (root_path, main_file)
        }
        DataPath::Volume(volume) => {
            let main_file = suggest
                .main_file
                .ok_or(error::Error::NoMainFileCandidateFound)?;

            let volumes = Volumes::default();

            let root_path = volumes.volumes.iter().find(|v| v.name == volume).ok_or(
                crate::error::Error::UnknownVolumeName {
                    volume_name: volume.0,
                },
            )?;

            (root_path.path.clone(), main_file)
        }
    };

    let layer_name = suggest.layer_name;

    let main_file_path = path_with_base_path(&root_path, Path::new(&main_file))?;

    let dataset = gdal_open_dataset(&main_file_path)?;

    if dataset.layer_count() > 0 {
        let meta_data = auto_detect_vector_meta_data_definition(&main_file_path, &layer_name)?;

        let layer_name = meta_data.loading_info.layer_name.clone();

        let meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

        Ok(web::Json(MetaDataSuggestion {
            main_file,
            layer_name,
            meta_data: meta_data.into(),
        }))
    } else {
        let mut gdal_params =
            gdal_parameters_from_dataset(&dataset, 1, &main_file_path, None, None)?;
        if let Ok(relative_path) = gdal_params.file_path.strip_prefix(root_path) {
            gdal_params.file_path = relative_path.to_path_buf();
        }
        let result_descriptor = raster_descriptor_from_dataset(&dataset, 1)?;

        Ok(web::Json(MetaDataSuggestion {
            main_file,
            layer_name: String::new(),
            meta_data: MetaDataDefinition::GdalMetaDataList(GdalMetaDataList {
                r#type: Default::default(),
                result_descriptor: result_descriptor.into(),
                params: vec![GdalLoadingInfoTemporalSlice {
                    time: TimeInterval::default().into(),
                    params: Some(gdal_params.into()),
                    cache_ttl: CacheTtlSeconds::default().into(),
                }],
            }),
        }))
    }
}

fn suggest_main_file(upload: &Upload) -> Option<String> {
    let known_extensions = ["csv", "shp", "json", "geojson", "gpkg", "sqlite"]; // TODO: rasters

    if upload.files.len() == 1 {
        return Some(upload.files[0].name.clone());
    }

    let mut sorted_files = upload.files.clone();
    sorted_files.sort_by(|a, b| b.byte_size.cmp(&a.byte_size));

    for file in sorted_files {
        if known_extensions.iter().any(|ext| file.name.ends_with(ext)) {
            return Some(file.name);
        }
    }
    None
}

#[allow(clippy::ref_option)]
fn select_layer_from_dataset<'a>(
    dataset: &'a gdal::Dataset,
    layer_name: &Option<String>,
) -> Result<Layer<'a>> {
    if let Some(layer_name) = layer_name {
        dataset.layer_by_name(layer_name).map_err(|_| {
            crate::error::Error::DatasetInvalidLayerName {
                layer_name: layer_name.clone(),
            }
        })
    } else {
        dataset
            .layer(0)
            .map_err(|_| crate::error::Error::DatasetHasNoAutoImportableLayer)
    }
}

#[allow(clippy::ref_option)]
fn auto_detect_vector_meta_data_definition(
    main_file_path: &Path,
    layer_name: &Option<String>,
) -> Result<StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>> {
    let dataset = gdal_open_dataset(main_file_path)?;

    auto_detect_vector_meta_data_definition_from_dataset(&dataset, main_file_path, layer_name)
}

#[allow(clippy::ref_option)]
fn auto_detect_vector_meta_data_definition_from_dataset(
    dataset: &gdal::Dataset,
    main_file_path: &Path,
    layer_name: &Option<String>,
) -> Result<StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>> {
    let layer = select_layer_from_dataset(dataset, layer_name)?;

    let columns_map = detect_columns(&layer);
    let columns_vecs = column_map_to_column_vecs(&columns_map);

    let mut geometry = detect_vector_geometry(&layer);
    let mut x = String::new();
    let mut y: Option<String> = None;

    if geometry.data_type == VectorDataType::Data {
        // help Gdal detecting geometry
        if let Some(auto_detect) = gdal_autodetect(main_file_path, &columns_vecs.text) {
            let layer = select_layer_from_dataset(&auto_detect.dataset, layer_name)?;
            geometry = detect_vector_geometry(&layer);
            if geometry.data_type != VectorDataType::Data {
                x = auto_detect.x;
                y = auto_detect.y;
            }
        }
    }

    let time = detect_time_type(&columns_vecs);

    Ok(StaticMetaData::<_, _, VectorQueryRectangle> {
        loading_info: OgrSourceDataset {
            file_name: main_file_path.into(),
            layer_name: geometry.layer_name.unwrap_or_else(|| layer.name()),
            data_type: Some(geometry.data_type),
            time,
            default_geometry: None,
            columns: Some(OgrSourceColumnSpec {
                format_specifics: None,
                x,
                y,
                int: columns_vecs.int,
                float: columns_vecs.float,
                text: columns_vecs.text,
                bool: vec![],
                datetime: columns_vecs.date,
                rename: None,
            }),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Ignore,
            sql_query: None,
            attribute_query: None,
            cache_ttl: CacheTtlSeconds::default(),
        },
        result_descriptor: VectorResultDescriptor {
            data_type: geometry.data_type,
            spatial_reference: geometry.spatial_reference,
            columns: columns_map
                .into_iter()
                .filter_map(|(k, v)| {
                    v.try_into()
                        .map(|v| {
                            (
                                k,
                                VectorColumnInfo {
                                    data_type: v,
                                    measurement: Measurement::Unitless,
                                },
                            )
                        })
                        .ok()
                }) // ignore all columns here that don't have a corresponding type in our collections
                .collect(),
            time: None,
            bbox: None,
        },
        phantom: Default::default(),
    })
}

/// create Gdal dataset with autodetect parameters based on available columns
fn gdal_autodetect(path: &Path, columns: &[String]) -> Option<GdalAutoDetect> {
    let columns_lower = columns.iter().map(|s| s.to_lowercase()).collect::<Vec<_>>();

    // TODO: load candidates from config
    let xy = [("x", "y"), ("lon", "lat"), ("longitude", "latitude")];

    for (x, y) in xy {
        let mut found_x = None;
        let mut found_y = None;

        for (column_lower, column) in columns_lower.iter().zip(columns) {
            if x == column_lower {
                found_x = Some(column);
            }

            if y == column_lower {
                found_y = Some(column);
            }

            if let (Some(x), Some(y)) = (found_x, found_y) {
                let mut dataset_options = DatasetOptions::default();

                let open_opts = &[
                    &format!("X_POSSIBLE_NAMES={x}"),
                    &format!("Y_POSSIBLE_NAMES={y}"),
                    "AUTODETECT_TYPE=YES",
                ];

                dataset_options.open_options = Some(open_opts);

                return gdal_open_dataset_ex(path, dataset_options)
                    .ok()
                    .map(|dataset| GdalAutoDetect {
                        dataset,
                        x: x.clone(),
                        y: Some(y.clone()),
                    });
            }
        }
    }

    // TODO: load candidates from config
    let geoms = ["geom", "wkt"];
    for geom in geoms {
        for (column_lower, column) in columns_lower.iter().zip(columns) {
            if geom == column_lower {
                let mut dataset_options = DatasetOptions::default();

                let open_opts = &[
                    &format!("GEOM_POSSIBLE_NAMES={column}"),
                    "AUTODETECT_TYPE=YES",
                ];

                dataset_options.open_options = Some(open_opts);

                return gdal_open_dataset_ex(path, dataset_options)
                    .ok()
                    .map(|dataset| GdalAutoDetect {
                        dataset,
                        x: geom.to_owned(),
                        y: None,
                    });
            }
        }
    }

    None
}

fn detect_time_type(columns: &Columns) -> OgrSourceDatasetTimeType {
    // TODO: load candidate names from config
    let known_start = [
        "start",
        "time",
        "begin",
        "date",
        "time_start",
        "start time",
        "date_start",
        "start date",
        "datetime",
        "date_time",
        "date time",
        "event",
        "timestamp",
        "time_from",
        "t1",
        "t",
    ];
    let known_end = [
        "end",
        "stop",
        "time2",
        "date2",
        "time_end",
        "time_stop",
        "time end",
        "time stop",
        "end time",
        "stop time",
        "date_end",
        "date_stop",
        "date end",
        "date stop",
        "end date",
        "stop date",
        "time_to",
        "t2",
    ];
    let known_duration = ["duration", "length", "valid for", "valid_for"];

    let mut start = None;
    let mut end = None;
    for column in &columns.date {
        if known_start.contains(&column.as_ref()) && start.is_none() {
            start = Some(column);
        } else if known_end.contains(&column.as_ref()) && end.is_none() {
            end = Some(column);
        }

        if start.is_some() && end.is_some() {
            break;
        }
    }

    let duration = columns
        .int
        .iter()
        .find(|c| known_duration.contains(&c.as_ref()));

    match (start, end, duration) {
        (Some(start), Some(end), _) => OgrSourceDatasetTimeType::StartEnd {
            start_field: start.clone(),
            start_format: OgrSourceTimeFormat::Auto,
            end_field: end.clone(),
            end_format: OgrSourceTimeFormat::Auto,
        },
        (Some(start), None, Some(duration)) => OgrSourceDatasetTimeType::StartDuration {
            start_field: start.clone(),
            start_format: OgrSourceTimeFormat::Auto,
            duration_field: duration.clone(),
        },
        (Some(start), None, None) => OgrSourceDatasetTimeType::Start {
            start_field: start.clone(),
            start_format: OgrSourceTimeFormat::Auto,
            duration: OgrSourceDurationSpec::Zero,
        },
        _ => OgrSourceDatasetTimeType::None,
    }
}

fn detect_vector_geometry(layer: &Layer) -> DetectedGeometry {
    for g in layer.defn().geom_fields() {
        if let Ok(data_type) = VectorDataType::try_from_ogr_type_code(g.field_type()) {
            return DetectedGeometry {
                layer_name: Some(layer.name()),
                data_type,
                spatial_reference: g
                    .spatial_ref()
                    .context(error::Gdal)
                    .and_then(|s| {
                        let s: Result<SpatialReference> = s.try_into().map_err(Into::into);
                        s
                    })
                    .map(Into::into)
                    .unwrap_or(SpatialReferenceOption::Unreferenced),
            };
        }
    }

    // fallback type if no geometry was found
    DetectedGeometry {
        layer_name: Some(layer.name()),
        data_type: VectorDataType::Data,
        spatial_reference: SpatialReferenceOption::Unreferenced,
    }
}

struct GdalAutoDetect {
    dataset: gdal::Dataset,
    x: String,
    y: Option<String>,
}

struct DetectedGeometry {
    layer_name: Option<String>,
    data_type: VectorDataType,
    spatial_reference: SpatialReferenceOption,
}

struct Columns {
    int: Vec<String>,
    float: Vec<String>,
    text: Vec<String>,
    date: Vec<String>,
}

enum ColumnDataType {
    Int,
    Float,
    Text,
    Date,
    Unknown,
}

impl TryFrom<ColumnDataType> for FeatureDataType {
    type Error = error::Error;

    fn try_from(value: ColumnDataType) -> Result<Self, Self::Error> {
        match value {
            ColumnDataType::Int => Ok(Self::Int),
            ColumnDataType::Float => Ok(Self::Float),
            ColumnDataType::Text => Ok(Self::Text),
            ColumnDataType::Date => Ok(Self::DateTime),
            ColumnDataType::Unknown => Err(error::Error::NoFeatureDataTypeForColumnDataType),
        }
    }
}

impl TryFrom<ColumnDataType> for crate::api::model::datatypes::FeatureDataType {
    type Error = error::Error;

    fn try_from(value: ColumnDataType) -> Result<Self, Self::Error> {
        match value {
            ColumnDataType::Int => Ok(Self::Int),
            ColumnDataType::Float => Ok(Self::Float),
            ColumnDataType::Text => Ok(Self::Text),
            ColumnDataType::Date => Ok(Self::DateTime),
            ColumnDataType::Unknown => Err(error::Error::NoFeatureDataTypeForColumnDataType),
        }
    }
}

fn detect_columns(layer: &Layer) -> HashMap<String, ColumnDataType> {
    let mut columns = HashMap::default();

    for field in layer.defn().fields() {
        let field_type = field.field_type();

        let data_type = match field_type {
            OGRFieldType::OFTInteger | OGRFieldType::OFTInteger64 => ColumnDataType::Int,
            OGRFieldType::OFTReal => ColumnDataType::Float,
            OGRFieldType::OFTString => ColumnDataType::Text,
            OGRFieldType::OFTDate | OGRFieldType::OFTDateTime => ColumnDataType::Date,
            _ => ColumnDataType::Unknown,
        };

        columns.insert(field.name(), data_type);
    }

    columns
}

fn column_map_to_column_vecs(columns: &HashMap<String, ColumnDataType>) -> Columns {
    let mut int = Vec::new();
    let mut float = Vec::new();
    let mut text = Vec::new();
    let mut date = Vec::new();

    for (k, v) in columns {
        match v {
            ColumnDataType::Int => int.push(k.clone()),
            ColumnDataType::Float => float.push(k.clone()),
            ColumnDataType::Text => text.push(k.clone()),
            ColumnDataType::Date => date.push(k.clone()),
            ColumnDataType::Unknown => {}
        }
    }

    Columns {
        int,
        float,
        text,
        date,
    }
}

/// Delete a dataset
#[utoipa::path(
    tag = "Datasets",
    delete,
    path = "/dataset/{dataset}",
    responses(
        (status = 200, description = "OK"),
        (status = 400, description = "Bad request", body = ErrorResponse, examples(
            ("Referenced an unknown dataset" = (value = json!({
                "error": "UnknownDatasetName",
                "message": "Unknown dataset name"
            }))),
            ("Given dataset can only be deleted by owner" = (value = json!({
                "error": "OperationRequiresOwnerPermission",
                "message": "Operation requires owner permission"
            })))
        )),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn delete_dataset_handler<C: ApplicationContext>(
    dataset: web::Path<DatasetName>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<HttpResponse> {
    let session_ctx = app_ctx.session_context(session).db();

    let real_dataset = dataset.into_inner();

    let dataset_id = session_ctx
        .resolve_dataset_name_to_id(&real_dataset)
        .await?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id.ok_or(error::Error::UnknownDatasetName {
        dataset_name: real_dataset.to_string(),
    })?;

    session_ctx.delete_dataset(dataset_id).await?;

    Ok(actix_web::HttpResponse::Ok().finish())
}

#[derive(Deserialize, Serialize, ToSchema, ToResponse)]
pub struct VolumeFileLayersResponse {
    layers: Vec<String>,
}

/// List the layers of a file in a volume.
#[utoipa::path(
    tag = "Datasets",
    get,
    path = "/dataset/volumes/{volume_name}/files/{file_name}/layers",
    responses(
        (status = 200, body = VolumeFileLayersResponse,
             example = json!({"layers": ["layer1", "layer2"]}))
    ),
    params(
        ("volume_name" = VolumeName, description = "Volume name"),
        ("file_name" = String, description = "File name")
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn list_volume_file_layers_handler<C: ApplicationContext>(
    path: web::Path<(VolumeName, String)>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    let (volume_name, file_name) = path.into_inner();

    let session_ctx = app_ctx.session_context(session);
    let volumes = session_ctx.volumes()?;

    let volume = volumes.iter().find(|v| v.name == volume_name.0).ok_or(
        crate::error::Error::UnknownVolumeName {
            volume_name: volume_name.0.clone(),
        },
    )?;

    let Some(volume_path) = volume.path.as_ref() else {
        return Err(crate::error::Error::CannotAccessVolumePath {
            volume_name: volume_name.0.clone(),
        });
    };

    let file_path = path_with_base_path(Path::new(volume_path), Path::new(&file_name))?;

    let layers = crate::util::spawn_blocking(move || {
        let dataset = gdal_open_dataset(&file_path)?;

        // TODO: hide system/internal layer like "layer_styles"
        Result::<_, Error>::Ok(dataset.layers().map(|l| l.name()).collect::<Vec<_>>())
    })
    .await??;

    Ok(web::Json(VolumeFileLayersResponse { layers }))
}

/// Creates a new dataset referencing files.
/// Users can reference previously uploaded files.
/// Admins can reference files from a volume.
#[utoipa::path(
    tag = "Datasets",
    post,
    path = "/dataset", 
    request_body = CreateDataset,
    responses(
        (status = 200, body = DatasetNameResponse),
    ),
    security(
        ("session_token" = [])
    )
)]
async fn create_dataset_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    create: web::Json<CreateDataset>,
) -> Result<web::Json<DatasetNameResponse>, CreateDatasetError> {
    let create = create.into_inner();
    match create {
        CreateDataset {
            data_path: DataPath::Volume(upload),
            definition,
        } => create_system_dataset(session, app_ctx, upload, definition).await,
        CreateDataset {
            data_path: DataPath::Upload(volume),
            definition,
        } => create_upload_dataset(session, app_ctx, volume, definition).await,
    }
}

async fn create_system_dataset<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    volume_name: VolumeName,
    mut definition: DatasetDefinition,
) -> Result<web::Json<DatasetNameResponse>, CreateDatasetError> {
    let volumes = get_config_element::<Data>()
        .context(CannotAccessConfig)?
        .volumes;
    let volume_path = volumes
        .get(&volume_name)
        .ok_or(CreateDatasetError::UnknownVolume)?;
    let volume = Volume {
        name: volume_name.to_string(),
        path: Some(volume_path.to_string_lossy().into()),
    };

    adjust_meta_data_path(
        &mut definition.meta_data,
        &crate::datasets::upload::Volume::from(&volume),
    )
    .context(CannotResolveUploadFilePath)?;

    let db = app_ctx.session_context(session).db();

    let dataset = db
        .add_dataset(definition.properties.into(), definition.meta_data.into())
        .await
        .context(CannotCreateDataset)?;

    db.add_permission(
        Role::registered_user_role_id(),
        dataset.id,
        Permission::Read,
    )
    .await
    .boxed_context(crate::error::PermissionDb)
    .context(DatabaseAccess)?;

    db.add_permission(Role::anonymous_role_id(), dataset.id, Permission::Read)
        .await
        .boxed_context(crate::error::PermissionDb)
        .context(DatabaseAccess)?;

    Ok(web::Json(dataset.name.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::model::datatypes::NamedData;
    use crate::api::model::responses::IdResponse;
    use crate::api::model::responses::datasets::DatasetNameResponse;
    use crate::api::model::services::{DatasetDefinition, Provenance};
    use crate::contexts::PostgresContext;
    use crate::contexts::{Session, SessionId};
    use crate::datasets::DatasetIdAndName;
    use crate::datasets::storage::DatasetStore;
    use crate::datasets::upload::{UploadId, VolumeName};
    use crate::error::Result;
    use crate::ge_context;
    use crate::projects::{PointSymbology, RasterSymbology, Symbology};
    use crate::test_data;
    use crate::users::UserAuth;
    use crate::util::tests::admin_login;
    use crate::util::tests::{
        MockQueryContext, SetMultipartBody, TestDataUploads, add_file_definition_to_datasets,
        read_body_json, read_body_string, send_test_request,
    };
    use actix_web;
    use actix_web::http::header;
    use actix_web_httpauth::headers::authorization::Bearer;
    use futures::TryStreamExt;
    use geoengine_datatypes::collections::{
        GeometryCollection, MultiPointCollection, VectorDataType,
    };
    use geoengine_datatypes::operations::image::{RasterColorizer, RgbaColor};
    use geoengine_datatypes::primitives::{BoundingBox2D, ColumnSelection, SpatialResolution};
    use geoengine_datatypes::raster::{GridShape2D, TilingSpecification};
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_operators::engine::{
        ExecutionContext, InitializedVectorOperator, QueryProcessor, StaticMetaData,
        VectorOperator, VectorResultDescriptor, WorkflowOperatorPath,
    };
    use geoengine_operators::source::{
        OgrSource, OgrSourceDataset, OgrSourceErrorSpec, OgrSourceParameters,
    };
    use geoengine_operators::util::gdal::create_ndvi_meta_data;
    use serde_json::{Value, json};
    use tokio_postgres::NoTls;

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn test_list_datasets(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::MultiPoint,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: Some(DatasetName::new(None, "My_Dataset")),
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
        };

        let meta = crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: String::new(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            },
            result_descriptor: descriptor.clone(),
            phantom: Default::default(),
        });

        let db = ctx.db();
        let DatasetIdAndName { id: id1, name: _ } = db.add_dataset(ds.into(), meta).await.unwrap();

        let ds = AddDataset {
            name: Some(DatasetName::new(None, "My_Dataset2")),
            display_name: "OgrDataset2".to_string(),
            description: "My Ogr dataset2".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: Some(Symbology::Point(PointSymbology::default())),
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
        };

        let meta = crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: String::new(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            },
            result_descriptor: descriptor,
            phantom: Default::default(),
        });

        let DatasetIdAndName { id: id2, name: _ } = db.add_dataset(ds.into(), meta).await.unwrap();

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/datasets?{}",
                &serde_urlencoded::to_string([
                    ("order", "NameAsc"),
                    ("offset", "0"),
                    ("limit", "2"),
                ])
                .unwrap()
            ))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            read_body_json(res).await,
            json!([ {
                "id": id1,
                "name": "My_Dataset",
                "displayName": "OgrDataset",
                "description": "My Ogr dataset",
                "tags": ["upload", "test"],
                "sourceOperator": "OgrSource",
                "resultDescriptor": {
                    "type": "vector",
                    "dataType": "MultiPoint",
                    "spatialReference": "",
                    "columns": {},
                    "time": null,
                    "bbox": null
                },
                "symbology": null
            },{
                "id": id2,
                "name": "My_Dataset2",
                "displayName": "OgrDataset2",
                "description": "My Ogr dataset2",
                "tags": ["upload", "test"],
                "sourceOperator": "OgrSource",
                "resultDescriptor": {
                    "type": "vector",
                    "dataType": "MultiPoint",
                    "spatialReference": "",
                    "columns": {},
                    "time": null,
                    "bbox": null
                },
                "symbology": {
                    "type": "point",
                    "radius": {
                        "type": "static",
                        "value": 10
                    },
                    "fillColor": {
                        "type": "static",
                        "color": [255, 255, 255, 255]
                    },
                    "stroke": {
                        "width": {
                            "type": "static",
                            "value": 1
                        },
                        "color": {
                            "type": "static",
                            "color": [0, 0, 0, 255]
                        }
                    },
                    "text": null
                }
            }])
        );
    }

    async fn upload_ne_10m_ports_files(
        app_ctx: PostgresContext<NoTls>,
        session_id: SessionId,
    ) -> Result<UploadId> {
        let files = vec![
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").to_path_buf(),
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.shx").to_path_buf(),
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.prj").to_path_buf(),
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.dbf").to_path_buf(),
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.cpg").to_path_buf(),
        ];

        let req = actix_web::test::TestRequest::post()
            .uri("/upload")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_multipart_files(&files);
        let res = send_test_request(req, app_ctx).await;
        assert_eq!(res.status(), 200);

        let upload: IdResponse<UploadId> = actix_web::test::read_body_json(res).await;
        let root = upload.id.root_path()?;

        for file in files {
            let file_name = file.file_name().unwrap();
            assert!(root.join(file_name).exists());
        }

        Ok(upload.id)
    }

    pub async fn construct_dataset_from_upload(
        app_ctx: PostgresContext<NoTls>,
        upload_id: UploadId,
        session_id: SessionId,
    ) -> DatasetName {
        let s = json!({
            "dataPath": {
                "upload": upload_id
            },
            "definition": {
                "properties": {
                    "name": null,
                    "displayName": "Uploaded Natural Earth 10m Ports",
                    "description": "Ports from Natural Earth",
                    "sourceOperator": "OgrSource"
                },
                "metaData": {
                    "type": "OgrMetaData",
                    "loadingInfo": {
                        "fileName": "ne_10m_ports.shp",
                        "layerName": "ne_10m_ports",
                        "dataType": "MultiPoint",
                        "time": {
                            "type": "none"
                        },
                        "columns": {
                            "x": "",
                            "y": null,
                            "float": ["natlscale"],
                            "int": ["scalerank"],
                            "text": ["featurecla", "name", "website"],
                            "bool": [],
                            "datetime": []
                        },
                        "forceOgrTimeGilter": false,
                        "onError": "ignore",
                        "provenance": null
                    },
                    "resultDescriptor": {
                        "dataType": "MultiPoint",
                        "spatialReference": "EPSG:4326",
                        "columns": {
                            "website": {
                                "dataType": "text",
                                "measurement": {
                                    "type": "unitless"
                                }
                            },
                            "name": {
                                "dataType": "text",
                                "measurement": {
                                    "type": "unitless"
                                }
                            },
                            "natlscale": {
                                "dataType": "float",
                                "measurement": {
                                    "type": "unitless"
                                }
                            },
                            "scalerank": {
                                "dataType": "int",
                                "measurement": {
                                    "type": "unitless"
                                }
                            },
                            "featurecla": {
                                "dataType": "text",
                                "measurement": {
                                    "type": "unitless"
                                }
                            }
                        }
                    }
                }
            }
        });

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(s);
        let res = send_test_request(req, app_ctx).await;
        assert_eq!(res.status(), 200, "response: {res:?}");

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;
        dataset_name
    }

    async fn make_ogr_source<C: ExecutionContext>(
        exe_ctx: &C,
        named_data: NamedData,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        OgrSource {
            params: OgrSourceParameters {
                data: named_data.into(),
                attribute_projection: None,
                attribute_filters: None,
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), exe_ctx)
        .await
        .map_err(Into::into)
    }

    #[ge_context::test]
    async fn it_creates_system_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let volume = VolumeName("test_data".to_string());

        let mut meta_data = create_ndvi_meta_data();

        // make path relative to volume
        meta_data.params.file_path = "raster/modis_ndvi/MOD13A2_M_NDVI_%_START_TIME_%.TIFF".into();

        let create = CreateDataset {
            data_path: DataPath::Volume(volume.clone()),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "ndvi".to_string(),
                    description: "ndvi".to_string(),
                    source_operator: "GdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data: MetaDataDefinition::GdalMetaDataRegular(meta_data.into()),
            },
        };

        // create via admin session
        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);
        let res = send_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200);

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;

        // assert dataset is accessible via regular session
        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);

        let res = send_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200);

        Ok(())
    }

    #[test]
    fn it_auto_detects() {
        let meta_data = auto_detect_vector_meta_data_definition(
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp"),
            &None,
        )
        .unwrap();
        let mut meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

        if let crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data
        {
            if let Some(columns) = &mut meta_data.loading_info.columns {
                columns.text.sort();
            }
        }

        assert_eq!(
            meta_data,
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        int: vec!["scalerank".to_string()],
                        float: vec!["natlscale".to_string()],
                        text: vec![
                            "featurecla".to_string(),
                            "name".to_string(),
                            "website".to_string(),
                        ],
                        bool: vec![],
                        datetime: vec![],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: CacheTtlSeconds::default()
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "name".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "scalerank".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Int,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "website".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "natlscale".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Float,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "featurecla".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            })
        );
    }

    #[test]
    fn it_detects_time_json() {
        let meta_data = auto_detect_vector_meta_data_definition(
            test_data!("vector/data/points_with_iso_time.json"),
            &None,
        )
        .unwrap();

        let mut meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

        if let crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data
        {
            if let Some(columns) = &mut meta_data.loading_info.columns {
                columns.datetime.sort();
            }
        }

        assert_eq!(
            meta_data,
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/points_with_iso_time.json").into(),
                    layer_name: "points_with_iso_time".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::StartEnd {
                        start_field: "time_start".to_owned(),
                        start_format: OgrSourceTimeFormat::Auto,
                        end_field: "time_end".to_owned(),
                        end_format: OgrSourceTimeFormat::Auto,
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        float: vec![],
                        int: vec![],
                        text: vec![],
                        bool: vec![],
                        datetime: vec!["time_end".to_owned(), "time_start".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: CacheTtlSeconds::default()
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "time_start".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "time_end".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default()
            })
        );
    }

    #[test]
    fn it_detects_time_gpkg() {
        let meta_data = auto_detect_vector_meta_data_definition(
            test_data!("vector/data/points_with_time.gpkg"),
            &None,
        )
        .unwrap();

        let mut meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

        if let crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data
        {
            if let Some(columns) = &mut meta_data.loading_info.columns {
                columns.datetime.sort();
            }
        }

        assert_eq!(
            meta_data,
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/points_with_time.gpkg").into(),
                    layer_name: "points_with_time".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::StartEnd {
                        start_field: "time_start".to_owned(),
                        start_format: OgrSourceTimeFormat::Auto,
                        end_field: "time_end".to_owned(),
                        end_format: OgrSourceTimeFormat::Auto,
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        float: vec![],
                        int: vec![],
                        text: vec![],
                        bool: vec![],
                        datetime: vec!["time_end".to_owned(), "time_start".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: CacheTtlSeconds::default()
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "time_start".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "time_end".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            })
        );
    }

    #[test]
    fn it_detects_time_shp() {
        let meta_data = auto_detect_vector_meta_data_definition(
            test_data!("vector/data/points_with_date.shp"),
            &None,
        )
        .unwrap();

        let mut meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

        if let crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data
        {
            if let Some(columns) = &mut meta_data.loading_info.columns {
                columns.datetime.sort();
            }
        }

        assert_eq!(
            meta_data,
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/points_with_date.shp").into(),
                    layer_name: "points_with_date".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::StartEnd {
                        start_field: "time_start".to_owned(),
                        start_format: OgrSourceTimeFormat::Auto,
                        end_field: "time_end".to_owned(),
                        end_format: OgrSourceTimeFormat::Auto,
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        float: vec![],
                        int: vec![],
                        text: vec![],
                        bool: vec![],
                        datetime: vec!["time_end".to_owned(), "time_start".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: CacheTtlSeconds::default()
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "time_end".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "time_start".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            })
        );
    }

    #[test]
    fn it_detects_time_start_duration() {
        let meta_data = auto_detect_vector_meta_data_definition(
            test_data!("vector/data/points_with_iso_start_duration.json"),
            &None,
        )
        .unwrap();

        let meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

        assert_eq!(
            meta_data,
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/points_with_iso_start_duration.json").into(),
                    layer_name: "points_with_iso_start_duration".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::StartDuration {
                        start_field: "time_start".to_owned(),
                        start_format: OgrSourceTimeFormat::Auto,
                        duration_field: "duration".to_owned(),
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        float: vec![],
                        int: vec!["duration".to_owned()],
                        text: vec![],
                        bool: vec![],
                        datetime: vec!["time_start".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: CacheTtlSeconds::default()
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "time_start".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "duration".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Int,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default()
            })
        );
    }

    #[test]
    fn it_detects_csv() {
        let meta_data =
            auto_detect_vector_meta_data_definition(test_data!("vector/data/lonlat.csv"), &None)
                .unwrap();

        let mut meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

        if let crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data
        {
            if let Some(columns) = &mut meta_data.loading_info.columns {
                columns.text.sort();
            }
        }

        assert_eq!(
            meta_data,
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/lonlat.csv").into(),
                    layer_name: "lonlat".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: "Longitude".to_string(),
                        y: Some("Latitude".to_string()),
                        float: vec![],
                        int: vec![],
                        text: vec![
                            "Latitude".to_string(),
                            "Longitude".to_string(),
                            "Name".to_string()
                        ],
                        bool: vec![],
                        datetime: vec![],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: CacheTtlSeconds::default()
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReferenceOption::Unreferenced,
                    columns: [
                        (
                            "Latitude".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "Longitude".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "Name".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default()
            })
        );
    }

    #[ge_context::test]
    async fn get_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
        };

        let meta = crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: String::new(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            },
            result_descriptor: descriptor,
            phantom: Default::default(),
        });

        let db = ctx.db();
        let DatasetIdAndName {
            id,
            name: dataset_name,
        } = db.add_dataset(ds.into(), meta).await?;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        let res_body = serde_json::from_str::<Value>(&read_body_string(res).await).unwrap();
        assert_eq!(res_status, 200, "{res_body}");

        assert_eq!(
            res_body,
            json!({
                "name": dataset_name,
                "id": id,
                "displayName": "OgrDataset",
                "description": "My Ogr dataset",
                "resultDescriptor": {
                    "type": "vector",
                    "dataType": "Data",
                    "spatialReference": "",
                    "columns": {},
                    "time": null,
                    "bbox": null
                },
                "sourceOperator": "OgrSource",
                "symbology": null,
                "provenance": null,
                "tags": ["upload", "test"],
            })
        );

        Ok(())
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_suggests_metadata(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        let session = app_ctx.create_anonymous_session().await.unwrap();

        let body = vec![(
            "test.json",
            r#"{
                "type": "FeatureCollection",
                "features": [
                  {
                    "type": "Feature",
                    "geometry": {
                      "type": "Point",
                      "coordinates": [
                        1,
                        1
                      ]
                    },
                    "properties": {
                      "name": "foo",
                      "id": 1
                    }
                  },
                  {
                    "type": "Feature",
                    "geometry": {
                      "type": "Point",
                      "coordinates": [
                        2,
                        2
                      ]
                    },
                    "properties": {
                      "name": "bar",
                      "id": 2
                    }
                  }
                ]
              }"#,
        )];

        let req = actix_web::test::TestRequest::post()
            .uri("/upload")
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_multipart(body.clone());

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let upload: IdResponse<UploadId> = actix_web::test::read_body_json(res).await;
        test_data.uploads.push(upload.id);

        let upload_content =
            std::fs::read_to_string(upload.id.root_path().unwrap().join("test.json")).unwrap();

        assert_eq!(&upload_content, body[0].1);

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset/suggest")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(SuggestMetaData {
                data_path: DataPath::Upload(upload.id),
                layer_name: None,
                main_file: None,
            });
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        let res_body = read_body_string(res).await;
        assert_eq!(res_status, 200, "{res_body}");

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&res_body).unwrap(),
            json!({
              "mainFile": "test.json",
              "layerName": "test",
              "metaData": {
                "type": "OgrMetaData",
                "loadingInfo": {
                  "fileName": format!("test_upload/{}/test.json", upload.id),
                  "layerName": "test",
                  "dataType": "MultiPoint",
                  "time": {
                    "type": "none"
                  },
                  "defaultGeometry": null,
                  "columns": {
                    "formatSpecifics": null,
                    "x": "",
                    "y": null,
                    "int": [
                      "id"
                    ],
                    "float": [],
                    "text": [
                      "name"
                    ],
                    "bool": [],
                    "datetime": [],
                    "rename": null
                  },
                  "forceOgrTimeFilter": false,
                  "forceOgrSpatialFilter": false,
                  "onError": "ignore",
                  "sqlQuery": null,
                  "attributeQuery": null,
                  "cacheTtl": 0,
                },
                "resultDescriptor": {
                  "dataType": "MultiPoint",
                  "spatialReference": "EPSG:4326",
                  "columns": {
                    "id": {
                      "dataType": "int",
                      "measurement": {
                        "type": "unitless"
                      }
                    },
                    "name": {
                      "dataType": "text",
                      "measurement": {
                        "type": "unitless"
                      }
                    }
                  },
                  "time": null,
                  "bbox": null
                }
              }
            })
        );

        Ok(())
    }

    #[ge_context::test]
    async fn it_deletes_system_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let volume = VolumeName("test_data".to_string());

        let mut meta_data = create_ndvi_meta_data();

        // make path relative to volume
        meta_data.params.file_path = "raster/modis_ndvi/MOD13A2_M_NDVI_%_START_TIME_%.TIFF".into();

        let create = CreateDataset {
            data_path: DataPath::Volume(volume.clone()),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "ndvi".to_string(),
                    description: "ndvi".to_string(),
                    source_operator: "GdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: None,
                },
                meta_data: MetaDataDefinition::GdalMetaDataRegular(meta_data.into()),
            },
        };

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);
        let res = send_test_request(req, app_ctx.clone()).await;

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;

        let db = ctx.db();
        let dataset_id = db
            .resolve_dataset_name_to_id(&dataset_name)
            .await
            .unwrap()
            .unwrap();
        assert!(db.load_dataset(&dataset_id).await.is_ok());

        let req = actix_web::test::TestRequest::delete()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert!(db.load_dataset(&dataset_id).await.is_err());

        Ok(())
    }

    #[ge_context::test]
    async fn it_gets_loading_info(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
        };

        let meta = crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: String::new(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            },
            result_descriptor: descriptor,
            phantom: Default::default(),
        });

        let db = ctx.db();
        let DatasetIdAndName {
            id: _,
            name: dataset_name,
        } = db.add_dataset(ds.into(), meta).await?;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/{dataset_name}/loadingInfo"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        let res_body = serde_json::from_str::<Value>(&read_body_string(res).await).unwrap();
        assert_eq!(res_status, 200, "{res_body}");

        assert_eq!(
            res_body,
            json!({
                "loadingInfo":  {
                    "attributeQuery": null,
                    "cacheTtl": 0,
                    "columns": null,
                    "dataType": null,
                    "defaultGeometry": null,
                    "fileName": "",
                    "forceOgrSpatialFilter": false,
                    "forceOgrTimeFilter": false,
                    "layerName": "",
                    "onError": "ignore",
                    "sqlQuery": null,
                    "time":  {
                        "type": "none"
                    }
                },
                 "resultDescriptor":  {
                    "bbox": null,
                    "columns":  {},
                    "dataType": "Data",
                    "spatialReference": "",
                    "time": null
                },
                "type": "OgrMetaData"
            })
        );

        Ok(())
    }

    #[ge_context::test]
    async fn it_updates_loading_info(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
        };

        let meta = crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: String::new(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            },
            result_descriptor: descriptor.clone(),
            phantom: Default::default(),
        });

        let db = ctx.db();
        let DatasetIdAndName {
            id,
            name: dataset_name,
        } = db.add_dataset(ds.into(), meta).await?;

        let update: MetaDataDefinition =
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: "foo.bar".into(),
                    layer_name: "baz".to_string(),
                    data_type: None,
                    time: Default::default(),
                    default_geometry: None,
                    columns: None,
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: CacheTtlSeconds::default(),
                },
                result_descriptor: descriptor,
                phantom: Default::default(),
            })
            .into();

        let req = actix_web::test::TestRequest::put()
            .uri(&format!("/dataset/{dataset_name}/loadingInfo"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(update.clone());

        let res = send_test_request(req, app_ctx).await;
        assert_eq!(res.status(), 200);

        let loading_info: MetaDataDefinition = db.load_loading_info(&id).await.unwrap().into();

        assert_eq!(loading_info, update);

        Ok(())
    }

    #[ge_context::test]
    async fn it_gets_updates_symbology(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let DatasetIdAndName {
            id: dataset_id,
            name: dataset_name,
        } = add_file_definition_to_datasets(&ctx.db(), test_data!("dataset_defs/ndvi.json")).await;

        let symbology = Symbology::Raster(RasterSymbology {
            r#type: Default::default(),
            opacity: 1.0,
            raster_colorizer: RasterColorizer::SingleBand {
                band: 0,
                band_colorizer: geoengine_datatypes::operations::image::Colorizer::linear_gradient(
                    vec![
                        (0.0, RgbaColor::white())
                            .try_into()
                            .expect("valid breakpoint"),
                        (10_000.0, RgbaColor::black())
                            .try_into()
                            .expect("valid breakpoint"),
                    ],
                    RgbaColor::transparent(),
                    RgbaColor::white(),
                    RgbaColor::black(),
                )
                .expect("valid colorizer"),
            },
        });

        let req = actix_web::test::TestRequest::put()
            .uri(&format!("/dataset/{dataset_name}/symbology"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(symbology.clone());
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        assert_eq!(res_status, 200);

        let dataset = ctx.db().load_dataset(&dataset_id).await?;

        assert_eq!(dataset.symbology, Some(symbology));

        Ok(())
    }

    #[ge_context::test()]
    async fn it_updates_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let DatasetIdAndName {
            id: dataset_id,
            name: dataset_name,
        } = add_file_definition_to_datasets(&ctx.db(), test_data!("dataset_defs/ndvi.json")).await;

        let update: UpdateDataset = UpdateDataset {
            name: DatasetName::new(None, "new_name"),
            display_name: "new display name".to_string(),
            description: "new description".to_string(),
            tags: vec!["foo".to_string(), "bar".to_string()],
        };

        let req = actix_web::test::TestRequest::post()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(update.clone());
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        assert_eq!(res_status, 200);

        let dataset = ctx.db().load_dataset(&dataset_id).await?;

        assert_eq!(dataset.name, update.name);
        assert_eq!(dataset.display_name, update.display_name);
        assert_eq!(dataset.description, update.description);
        assert_eq!(dataset.tags, Some(update.tags));

        Ok(())
    }

    #[ge_context::test()]
    async fn it_updates_provenance(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let DatasetIdAndName {
            id: dataset_id,
            name: dataset_name,
        } = add_file_definition_to_datasets(&ctx.db(), test_data!("dataset_defs/ndvi.json")).await;

        let provenances: Provenances = Provenances {
            provenances: vec![Provenance {
                citation: "foo".to_string(),
                license: "bar".to_string(),
                uri: "http://example.com".to_string(),
            }],
        };

        let req = actix_web::test::TestRequest::put()
            .uri(&format!("/dataset/{dataset_name}/provenance"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(provenances.clone());
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        assert_eq!(res_status, 200);

        let dataset = ctx.db().load_dataset(&dataset_id).await?;

        assert_eq!(
            dataset.provenance,
            Some(
                provenances
                    .provenances
                    .into_iter()
                    .map(Into::into)
                    .collect()
            )
        );

        Ok(())
    }

    // TODO: better way to get to the root of the project
    struct TestWorkdirChanger {
        package_dir: &'static str,
        modified: bool,
    }

    impl TestWorkdirChanger {
        fn go_to_workspace(package_dir: &'static str) -> Self {
            let mut working_dir = std::env::current_dir().unwrap();

            if !working_dir.ends_with(package_dir) {
                return Self {
                    package_dir,
                    modified: false,
                };
            }

            working_dir.pop();

            std::env::set_current_dir(working_dir).unwrap();

            Self {
                package_dir,
                modified: true,
            }
        }
    }

    impl Drop for TestWorkdirChanger {
        fn drop(&mut self) {
            if !self.modified {
                return;
            }

            let mut working_dir = std::env::current_dir().unwrap();
            working_dir.push(self.package_dir);
            std::env::set_current_dir(working_dir).unwrap();
        }
    }

    #[ge_context::test(test_execution = "serial")]
    async fn it_lists_layers(app_ctx: PostgresContext<NoTls>) {
        let changed_workdir = TestWorkdirChanger::go_to_workspace("services");

        let session = admin_login(&app_ctx).await;

        let volume_name = "test_data";
        let file_name = "vector%2Fdata%2Ftwo_layers.gpkg";

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/dataset/volumes/{volume_name}/files/{file_name}/layers"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));

        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200, "{res:?}");

        let layers: VolumeFileLayersResponse = actix_web::test::read_body_json(res).await;

        assert_eq!(
            layers.layers,
            vec![
                "points_with_time".to_string(),
                "points_with_time_and_more".to_string(),
                "layer_styles".to_string() // TOOO: remove once internal/system layers are hidden
            ]
        );

        drop(changed_workdir);
    }

    /// override the pixel size since this test was designed for 600 x 600 pixel tiles
    fn create_dataset_tiling_specification() -> TilingSpecification {
        TilingSpecification {
            origin_coordinate: (0., 0.).into(),
            tile_size_in_pixels: GridShape2D::new([600, 600]),
        }
    }

    #[ge_context::test(tiling_spec = "create_dataset_tiling_specification")]
    async fn create_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let upload_id = upload_ne_10m_ports_files(app_ctx.clone(), session.id()).await?;
        test_data.uploads.push(upload_id);

        let dataset_name =
            construct_dataset_from_upload(app_ctx.clone(), upload_id, session.id()).await;
        let exe_ctx = ctx.execution_context()?;

        let source = make_ogr_source(
            &exe_ctx,
            geoengine_datatypes::dataset::NamedData::from(dataset_name).into(),
        )
        .await?;

        let query_processor = source.query_processor()?.multi_point().unwrap();
        let query_ctx = ctx.mock_query_context()?;

        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((1.85, 50.88).into(), (4.82, 52.95).into())?,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                    attributes: ColumnSelection::all(),
                },
                &query_ctx,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await?;

        let coords = result[0].coordinates();
        assert_eq!(coords.len(), 10);
        assert_eq!(
            coords,
            &[
                [2.933_686_69, 51.23].into(),
                [3.204_593_64_f64, 51.336_388_89].into(),
                [4.651_413_428, 51.805_833_33].into(),
                [4.11, 51.95].into(),
                [4.386_160_188, 50.886_111_11].into(),
                [3.767_373_38, 51.114_444_44].into(),
                [4.293_757_362, 51.297_777_78].into(),
                [1.850_176_678, 50.965_833_33].into(),
                [2.170_906_949, 51.021_666_67].into(),
                [4.292_873_969, 51.927_222_22].into(),
            ]
        );

        Ok(())
    }

    #[ge_context::test]
    async fn it_creates_volume_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let volume = VolumeName("test_data".to_string());

        let mut meta_data = create_ndvi_meta_data();

        // make path relative to volume
        meta_data.params.file_path = "raster/modis_ndvi/MOD13A2_M_NDVI_%_START_TIME_%.TIFF".into();

        let create = CreateDataset {
            data_path: DataPath::Volume(volume.clone()),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "ndvi".to_string(),
                    description: "ndvi".to_string(),
                    source_operator: "GdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data: MetaDataDefinition::GdalMetaDataRegular(meta_data.into()),
            },
        };

        // create via admin session
        let admin_session = admin_login(&app_ctx).await;
        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session.id().to_string()),
            ))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_json(create);
        let res = send_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200);

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200);

        Ok(())
    }

    #[ge_context::test]
    async fn it_deletes_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        let session = app_ctx.create_anonymous_session().await.unwrap();
        let session_id = session.id();
        let ctx = app_ctx.session_context(session);

        let upload_id = upload_ne_10m_ports_files(app_ctx.clone(), session_id).await?;
        test_data.uploads.push(upload_id);

        let dataset_name =
            construct_dataset_from_upload(app_ctx.clone(), upload_id, session_id).await;

        let db = ctx.db();
        let dataset_id = db
            .resolve_dataset_name_to_id(&dataset_name)
            .await
            .unwrap()
            .unwrap();

        assert!(db.load_dataset(&dataset_id).await.is_ok());

        let req = actix_web::test::TestRequest::delete()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "response: {res:?}");

        assert!(db.load_dataset(&dataset_id).await.is_err());

        Ok(())
    }

    #[ge_context::test]
    async fn it_deletes_volume_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let volume = VolumeName("test_data".to_string());

        let mut meta_data = create_ndvi_meta_data();

        // make path relative to volume
        meta_data.params.file_path = "raster/modis_ndvi/MOD13A2_M_NDVI_%_START_TIME_%.TIFF".into();

        let create = CreateDataset {
            data_path: DataPath::Volume(volume.clone()),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "ndvi".to_string(),
                    description: "ndvi".to_string(),
                    source_operator: "GdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data: MetaDataDefinition::GdalMetaDataRegular(meta_data.into()),
            },
        };

        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let db = ctx.db();

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);
        let res = send_test_request(req, app_ctx.clone()).await;

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;
        let dataset_id = db
            .resolve_dataset_name_to_id(&dataset_name)
            .await
            .unwrap()
            .unwrap();

        assert!(db.load_dataset(&dataset_id).await.is_ok());

        let req = actix_web::test::TestRequest::delete()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert!(db.load_dataset(&dataset_id).await.is_err());

        Ok(())
    }
}
