use crate::api::handlers::datasets::add_tag;
use crate::datasets::listing::DatasetProvider;
use crate::datasets::storage::{check_reserved_tags, ReservedTags};
use crate::datasets::upload::{UploadDb, UploadId};
use crate::datasets::DatasetName;
use crate::pro::datasets::{
    ChangeDatasetExpiration, DatasetAccessStatusResponse, ExpirationChange,
    UploadedUserDatasetStore,
};
use crate::{
    api::{
        handlers::datasets::{
            adjust_meta_data_path, auto_create_dataset_handler, delete_dataset_handler,
            get_dataset_handler, get_loading_info_handler, list_datasets_handler,
            list_volumes_handler, suggest_meta_data_handler, update_dataset_handler,
            update_dataset_provenance_handler, update_dataset_symbology_handler,
            update_loading_info_handler,
        },
        model::{
            responses::datasets::{errors::*, DatasetNameResponse},
            services::{CreateDataset, DataPath, DatasetDefinition},
        },
    },
    contexts::{ApplicationContext, SessionContext},
    datasets::{
        storage::DatasetStore,
        upload::{Volume, VolumeName},
    },
    error,
    error::Result,
    pro::{
        contexts::{ProApplicationContext, ProGeoEngineDb},
        permissions::{Permission, PermissionDb, Role},
    },
    util::config::{get_config_element, Data},
};
use actix_web::{web, FromRequest, HttpResponse, HttpResponseBuilder, Responder};
use geoengine_datatypes::error::BoxedResultExt;
use snafu::ResultExt;

pub(crate) fn init_dataset_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProApplicationContext,
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
    C::Session: FromRequest,
{
    cfg.service(
        web::scope("/dataset")
            .service(
                web::resource("/suggest").route(web::post().to(suggest_meta_data_handler::<C>)),
            )
            .service(web::resource("/auto").route(web::post().to(auto_create_dataset_handler::<C>)))
            .service(web::resource("/volumes").route(web::get().to(list_volumes_handler::<C>)))
            .service(web::resource("/gc").route(web::post().to(gc_expired_datasets::<C>)))
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
                web::resource("/{dataset}/expiration")
                    .route(web::put().to(set_dataset_expiration::<C>)),
            )
            .service(
                web::resource("/{dataset}/status").route(web::get().to(get_dataset_status::<C>)),
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

/// Creates a new dataset using files available on the volumes.
/// The created dataset will be visible to all users.
/// Requires an admin session.
#[utoipa::path(
    tag = "Datasets",
    post,
    path = "/dataset/public", 
    request_body = CreateDataset,
    responses(
        (status = 200, response = DatasetNameResponse),
    ),
    security(
        ("session_token" = [])
    )
)]
async fn create_dataset_handler<C: ProApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    create: web::Json<CreateDataset>,
) -> Result<web::Json<DatasetNameResponse>, CreateDatasetError>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let create = create.into_inner();
    match create {
        CreateDataset {
            data_path: DataPath::Volume(volume),
            definition,
        } => create_system_dataset(session, app_ctx, volume, definition).await,
        CreateDataset {
            data_path: DataPath::Upload(upload),
            definition,
        } => create_upload_dataset(session, app_ctx, upload, definition).await,
    }
}

async fn create_system_dataset<C: ProApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    volume_name: VolumeName,
    mut definition: DatasetDefinition,
) -> Result<web::Json<DatasetNameResponse>, CreateDatasetError>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let volumes = get_config_element::<Data>()
        .context(CannotAccessConfig)?
        .volumes;
    let volume_path = volumes
        .get(&volume_name)
        .ok_or(CreateDatasetError::UnknownVolume)?;
    let volume = Volume {
        name: volume_name,
        path: volume_path.clone(),
    };

    adjust_meta_data_path(&mut definition.meta_data, &volume)
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

pub async fn create_upload_dataset<C: ProApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    upload_id: UploadId,
    mut definition: DatasetDefinition,
) -> Result<web::Json<DatasetNameResponse>, CreateDatasetError>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let db = app_ctx.session_context(session).db();
    let upload = db.load_upload(upload_id).await.context(UploadNotFound)?;

    check_reserved_tags(&definition.properties.tags);
    add_tag(&mut definition.properties, ReservedTags::Upload.to_string());

    adjust_meta_data_path(&mut definition.meta_data, &upload)
        .context(CannotResolveUploadFilePath)?;

    let result = db
        .add_uploaded_dataset(
            upload_id,
            definition.properties.into(),
            definition.meta_data.into(),
        )
        .await
        .context(CannotCreateDataset)?;

    Ok(web::Json(result.name.into()))
}

/// Sets an expiration date for the dataset with the given name.
/// Will expire immediately if no timestamp is provided.
#[utoipa::path(
    tag = "Datasets",
    put,
    path = "/dataset/{dataset}/expiration",
    request_body(content = ExpirationChange, examples(
    ("Delete" = (value = json!({
            "type": "setExpire",
            "deletionType": "DeleteData"
            }))),
    ("Expire" = (value = json!({
            "type": "setExpire",
            "deletionType": "DeleteRecordAndData",
            "deletionTimestamp": "2024-06-28T14:52:39.655Z"
            }))),
    ("Undo Expire" = (value = json!({
            "type": "unsetExpire"
    })))
    )),
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
async fn set_dataset_expiration<C: ProApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    dataset: web::Path<DatasetName>,
    expiration: web::Json<ExpirationChange>,
) -> Result<impl Responder>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let db = app_ctx.session_context(session).db();

    let dataset_name = dataset.into_inner();

    let dataset_id = db.resolve_dataset_name_to_id(&dataset_name).await?;

    let dataset_id = dataset_id.ok_or(error::Error::UnknownDatasetName {
        dataset_name: dataset_name.to_string(),
    })?;

    let expire_dataset = ChangeDatasetExpiration {
        dataset_id,
        expiration_change: expiration.into_inner(),
    };

    db.expire_uploaded_dataset(expire_dataset).await?;

    Ok(HttpResponse::Ok())
}

/// Clears expired datasets.
/// Requires an admin session.
#[utoipa::path(
    tag = "Datasets",
    post,
    path = "/dataset/gc",
    responses(
        (status = 200, description = "OK"),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    security(
        ("session_token" = [])
    )
)]
async fn gc_expired_datasets<C: ProApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<HttpResponseBuilder>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    if !session.is_admin() {
        return Err(error::Error::Unauthorized {
            source: Box::new(error::Error::OperationRequiresAdminPrivilige),
        });
    }

    app_ctx
        .session_context(session)
        .db()
        .clear_expired_datasets()
        .await?;

    Ok(HttpResponse::Ok())
}

/// Returns the access status of the current user for the dataset with the given name.
#[utoipa::path(
    tag = "Datasets",
    get,
    path = "/dataset/{dataset}/status",
    responses(
        (status = 200, description = "OK", body = DatasetAccessStatusResponse),
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
async fn get_dataset_status<C: ProApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    dataset: web::Path<DatasetName>,
) -> Result<impl Responder>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let db = app_ctx.session_context(session).db();

    let dataset_name = dataset.into_inner();

    let dataset_id = db.resolve_dataset_name_to_id(&dataset_name).await?;

    let dataset_id = dataset_id.ok_or(error::Error::UnknownDatasetName {
        dataset_name: dataset_name.to_string(),
    })?;

    let status: DatasetAccessStatusResponse =
        db.get_dataset_access_status(&dataset_id).await?.into();

    Ok(web::Json(status))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::model::responses::IdResponse;
    use crate::datasets::DatasetName;
    use crate::pro::contexts::ProPostgresContext;
    use crate::pro::datasets::DatasetAccessStatusResponse;
    use crate::pro::ge_context;
    use crate::pro::util::tests::get_db_timestamp;
    use crate::{
        api::model::services::{AddDataset, DataPath, DatasetDefinition, MetaDataDefinition},
        contexts::{Session, SessionContext, SessionId},
        datasets::{
            listing::DatasetProvider,
            upload::{UploadId, UploadRootPath, VolumeName},
        },
        pro::{
            users::UserAuth,
            util::tests::{admin_login, send_pro_test_request},
        },
        util::tests::{SetMultipartBody, TestDataUploads},
    };
    use actix_http::header;
    use actix_web_httpauth::headers::authorization::Bearer;
    use futures::TryStreamExt;
    use geoengine_datatypes::dataset::NamedData;
    use geoengine_datatypes::primitives::{ColumnSelection, Duration};
    use geoengine_datatypes::{
        collections::{GeometryCollection, MultiPointCollection},
        primitives::{BoundingBox2D, SpatialResolution, VectorQueryRectangle},
        raster::{GridShape2D, TilingSpecification},
        test_data,
    };
    use geoengine_operators::{
        engine::{
            ExecutionContext, InitializedVectorOperator, QueryProcessor, VectorOperator,
            WorkflowOperatorPath,
        },
        source::{OgrSource, OgrSourceParameters},
        util::gdal::create_ndvi_meta_data,
    };
    use serde_json::json;
    use std::ops::Add;
    use tokio_postgres::NoTls;

    pub async fn upload_ne_10m_ports_files(
        app_ctx: ProPostgresContext<NoTls>,
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
        let res = send_pro_test_request(req, app_ctx).await;
        assert_eq!(res.status(), 200, "{res:?}");

        let upload: IdResponse<UploadId> = actix_web::test::read_body_json(res).await;
        let root = upload.id.root_path()?;

        for file in files {
            let file_name = file.file_name().unwrap();
            assert!(root.join(file_name).exists());
        }

        Ok(upload.id)
    }

    pub async fn construct_dataset_from_upload(
        app_ctx: ProPostgresContext<NoTls>,
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
        let res = send_pro_test_request(req, app_ctx).await;
        assert_eq!(res.status(), 200, "response: {res:?}");

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;
        dataset_name
    }

    pub async fn make_ogr_source<C: ExecutionContext>(
        exe_ctx: &C,
        named_data: NamedData,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        OgrSource {
            params: OgrSourceParameters {
                data: named_data,
                attribute_projection: None,
                attribute_filters: None,
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), exe_ctx)
        .await
        .map_err(Into::into)
    }

    /// override the pixel size since this test was designed for 600 x 600 pixel tiles
    fn create_dataset_tiling_specification() -> TilingSpecification {
        TilingSpecification {
            origin_coordinate: (0., 0.).into(),
            tile_size_in_pixels: GridShape2D::new([600, 600]),
        }
    }

    #[ge_context::test(tiling_spec = "create_dataset_tiling_specification")]
    async fn create_dataset(app_ctx: ProPostgresContext<NoTls>) -> Result<()> {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let upload_id = upload_ne_10m_ports_files(app_ctx.clone(), session_id).await?;
        test_data.uploads.push(upload_id);

        let dataset_name =
            construct_dataset_from_upload(app_ctx.clone(), upload_id, session_id).await;
        let exe_ctx = ctx.execution_context()?;

        let source = make_ogr_source(&exe_ctx, dataset_name.into()).await?;

        let query_processor = source.query_processor()?.multi_point().unwrap();
        let query_ctx = ctx.query_context()?;

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
    async fn it_creates_volume_dataset(app_ctx: ProPostgresContext<NoTls>) -> Result<()> {
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
        let res = send_pro_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200);

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));

        let res = send_pro_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200);

        Ok(())
    }

    #[ge_context::test]
    async fn it_deletes_dataset(app_ctx: ProPostgresContext<NoTls>) -> Result<()> {
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

        let res = send_pro_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "response: {res:?}");

        assert!(db.load_dataset(&dataset_id).await.is_err());

        Ok(())
    }

    #[ge_context::test]
    async fn it_deletes_volume_dataset(app_ctx: ProPostgresContext<NoTls>) -> Result<()> {
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
        let res = send_pro_test_request(req, app_ctx.clone()).await;

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

        let res = send_pro_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert!(db.load_dataset(&dataset_id).await.is_err());

        Ok(())
    }

    #[ge_context::test]
    async fn it_expires_dataset_and_checks_status(
        app_ctx: ProPostgresContext<NoTls>,
    ) -> Result<()> {
        let mut test_data = TestDataUploads::default();

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

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/{dataset_name}/status"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_pro_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200, "response: {res:?}");
        let status: DatasetAccessStatusResponse = actix_web::test::read_body_json(res).await;
        assert!(status.is_user_upload);
        assert!(status.is_available);
        assert!(status.expiration.is_none());

        let current_time = get_db_timestamp(&app_ctx).await;
        let future_time = current_time.add(Duration::seconds(2));

        let expiration =
            ChangeDatasetExpiration::expire_full(dataset_id, future_time).expiration_change;

        let req = actix_web::test::TestRequest::put()
            .uri(&format!("/dataset/{dataset_name}/expiration"))
            .set_json(expiration)
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"));

        let res = send_pro_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "response: {res:?}");

        assert!(db.load_dataset(&dataset_id).await.is_ok());

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/{dataset_name}/status"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_pro_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200, "response: {res:?}");
        let status: DatasetAccessStatusResponse = actix_web::test::read_body_json(res).await;
        assert!(status.is_user_upload);
        assert!(status.is_available);
        assert!(status.expiration.is_some());

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        assert!(db.load_dataset(&dataset_id).await.is_err());

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/{dataset_name}/status"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_pro_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 400, "response: {res:?}");

        Ok(())
    }
}
