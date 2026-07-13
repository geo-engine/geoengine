#![allow(clippy::unwrap_used)] // okay in tests

use crate::{
    api::{
        handlers::{self},
        model::{
            processing_graphs::{
                DeriveOutRasterSpecsSource, GdalSource as NewGdalSource,
                GdalSourceParameters as NewGdalSourceParameters,
                RasterOperator as NewRasterOperator, Reprojection, ReprojectionParameters,
                SingleRasterOrVectorOperator, SingleRasterOrVectorSource,
                TypedOperator as NewTypedOperator,
            },
            responses::ErrorResponse,
        },
    },
    config::{Postgres, Quota, get_config_element},
    contexts::{ApplicationContext, GeoEngineDb, PostgresContext, SessionContext, SessionId},
    datasets::{
        AddDataset, DatasetIdAndName, DatasetName,
        listing::Provenance,
        storage::{DatasetDefinition, DatasetStore, MetaDataDefinition},
        upload::{UploadId, UploadRootPath},
    },
    layers::{
        layer::AddLayer,
        listing::LayerCollectionProvider,
        storage::{INTERNAL_PROVIDER_ID, LayerDb},
    },
    permissions::{Permission, PermissionDb, Role},
    projects::{
        CreateProject, LayerUpdate, ProjectDb, ProjectId, ProjectLayer, RasterSymbology,
        STRectangle, Symbology, UpdateProject,
    },
    users::{
        OidcManager, UserAuth, UserCredentials, UserId, UserInfo, UserRegistration, UserSession,
    },
    util::{
        Identifier,
        middleware::OutputRequestId,
        postgres::DatabaseConnectionConfig,
        server::{configure_extractors, render_404, render_405},
    },
    workflows::{
        registry::WorkflowRegistry,
        workflow::{Workflow, WorkflowId},
    },
};
use actix_web::dev::ServiceResponse;
use actix_web::{
    App, HttpResponse, Responder, http, http::Method, http::header, middleware, test, web,
};
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::bb8::ManageConnection;
use flexi_logger::Logger;
use futures_util::Future;
use geoengine_datatypes::{
    dataset::{DataProviderId, DatasetId, LayerId, NamedData},
    operations::image::{Colorizer, RasterColorizer, RgbaColor},
    primitives::{CacheTtlSeconds, Coordinate2D, DateTime},
    raster::{GeoTransform, GridBoundingBox2D, RasterDataType, RenameBands, TilingSpecification},
    spatial_reference::{SpatialReference, SpatialReferenceOption},
    test_data,
    util::test::TestDefault,
};
use geoengine_operators::{
    engine::{
        ChunkByteSize, MultipleRasterSources, QueryContext, RasterBandDescriptor,
        RasterBandDescriptors, RasterOperator, RasterResultDescriptor, SpatialGridDescriptor,
        TimeDescriptor, TypedOperator, WorkflowOperatorPath,
    },
    meta::quota::QuotaTracking,
    processing::{RasterStacker, RasterStackerParams},
    source::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetaDataStatic,
        GdalSource, GdalSourceParameters,
    },
    util::gdal::{
        create_ndvi_meta_data, create_ndvi_meta_data_with_cache_ttl, create_ports_meta_data,
    },
};
use rand::Rng;
use std::fs::File;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::runtime::Handle;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::RwLock;
use tokio::sync::Semaphore;
use tokio_postgres::NoTls;
use tracing::debug;
use tracing_actix_web::TracingLogger;
use uuid::Uuid;

#[allow(clippy::missing_panics_doc)]
pub async fn create_project_helper(
    ctx: &<PostgresContext<NoTls> as ApplicationContext>::SessionContext,
) -> ProjectId {
    ctx.db()
        .create_project(CreateProject {
            name: "Test".to_string(),
            description: "Foo".to_string(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        })
        .await
        .unwrap()
}

pub fn update_project_helper(project: ProjectId) -> UpdateProject {
    UpdateProject {
        id: project,
        name: Some("TestUpdate".to_string()),
        description: None,
        layers: Some(vec![LayerUpdate::UpdateOrInsert(ProjectLayer {
            workflow: WorkflowId::new(),
            name: "L1".to_string(),
            visibility: Default::default(),
            symbology: Symbology::Raster(RasterSymbology {
                r#type: Default::default(),
                opacity: 1.0,
                raster_colorizer: RasterColorizer::SingleBand {
                    band: 0,
                    band_colorizer: Colorizer::test_default(),
                },
            }),
        })]),
        plots: None,
        bounds: None,
        time_step: None,
    }
}

#[allow(clippy::missing_panics_doc)]
pub async fn register_ndvi_workflow_helper(
    app_ctx: &PostgresContext<NoTls>,
) -> (Workflow, WorkflowId) {
    register_ndvi_workflow_helper_with_cache_ttl(app_ctx, CacheTtlSeconds::default()).await
}

#[allow(clippy::missing_panics_doc)]
pub async fn register_ndvi_workflow_helper_with_cache_ttl(
    app_ctx: &PostgresContext<NoTls>,
    cache_ttl: CacheTtlSeconds,
) -> (Workflow, WorkflowId) {
    let (_, dataset) = add_ndvi_to_datasets_with_cache_ttl(app_ctx, cache_ttl).await;

    let workflow = Workflow::Legacy {
        operator: TypedOperator::Raster(
            GdalSource {
                params: GdalSourceParameters::new(dataset),
            }
            .boxed(),
        ),
    };

    let session = admin_login(app_ctx).await;

    let id = app_ctx
        .session_context(session)
        .db()
        .register_workflow(workflow.clone())
        .await
        .unwrap();

    (workflow, id)
}

pub async fn add_ndvi_to_datasets(app_ctx: &PostgresContext<NoTls>) -> (DatasetId, NamedData) {
    add_ndvi_to_datasets_with_cache_ttl(app_ctx, CacheTtlSeconds::default()).await
}

/// .
///
/// # Panics
///
/// Panics if the default session context could not be created.
pub async fn add_ndvi_to_datasets_with_cache_ttl(
    app_ctx: &PostgresContext<NoTls>,
    cache_ttl: CacheTtlSeconds,
) -> (DatasetId, NamedData) {
    let dataset_name = DatasetName {
        namespace: None,
        name: "NDVI".to_string(),
    };

    let ndvi = DatasetDefinition {
        properties: AddDataset {
            name: Some(dataset_name.clone()),
            display_name: "NDVI".to_string(),
            description: "NDVI data from MODIS".to_string(),
            source_operator: "GdalSource".to_string(),
            symbology: None,
            provenance: Some(vec![Provenance {
                citation: "Sample Citation".to_owned(),
                license: "Sample License".to_owned(),
                uri: "http://example.org/".to_owned(),
            }]),
            tags: Some(vec!["raster".to_owned(), "test".to_owned()]),
        },
        meta_data: MetaDataDefinition::GdalMetaDataRegular(create_ndvi_meta_data_with_cache_ttl(
            cache_ttl,
        )),
    };

    let session = admin_login(app_ctx).await;
    let ctx = app_ctx.session_context(session);
    let dataset_id = ctx
        .db()
        .add_dataset(ndvi.properties, ndvi.meta_data, None)
        .await
        .expect("dataset db access")
        .id;

    ctx.db()
        .add_permission(
            Role::registered_user_role_id(),
            dataset_id,
            Permission::Read,
        )
        .await
        .unwrap();

    ctx.db()
        .add_permission(Role::anonymous_role_id(), dataset_id, Permission::Read)
        .await
        .unwrap();

    let named_data = NamedData {
        namespace: dataset_name.namespace,
        provider: None,
        name: dataset_name.name,
    };

    (dataset_id, named_data)
}

#[allow(clippy::missing_panics_doc, clippy::too_many_lines)]
pub async fn add_land_cover_to_datasets<D: GeoEngineDb>(db: &D) -> DatasetName {
    let ndvi = DatasetDefinition {
        properties: AddDataset {
            name: Some(DatasetName::new(None, "land_cover_raster_test".to_string())),
            display_name: "Land Cover".to_string(),
            description: "Land Cover derived from MODIS/Terra+Aqua Land Cover".to_string(),
            source_operator: "GdalSource".to_string(),
            tags: Some(vec!["raster".to_owned(), "test".to_owned()]),
            symbology: Some(Symbology::Raster(RasterSymbology {
                r#type: Default::default(),
                opacity: 1.0,
                raster_colorizer: RasterColorizer::SingleBand {
                    band: 0, band_colorizer: Colorizer::palette(
                    [
                        (0.0.try_into().unwrap(), RgbaColor::new(134, 201, 227, 255)),
                        (1.0.try_into().unwrap(), RgbaColor::new(30, 129, 62, 255)),
                        (2.0.try_into().unwrap(), RgbaColor::new(59, 194, 212, 255)),
                        (3.0.try_into().unwrap(), RgbaColor::new(157, 194, 63, 255)),
                        (4.0.try_into().unwrap(), RgbaColor::new(159, 225, 127, 255)),
                        (5.0.try_into().unwrap(), RgbaColor::new(125, 194, 127, 255)),
                        (6.0.try_into().unwrap(), RgbaColor::new(195, 127, 126, 255)),
                        (7.0.try_into().unwrap(), RgbaColor::new(188, 221, 190, 255)),
                        (8.0.try_into().unwrap(), RgbaColor::new(224, 223, 133, 255)),
                        (9.0.try_into().unwrap(), RgbaColor::new(226, 221, 7, 255)),
                        (10.0.try_into().unwrap(), RgbaColor::new(223, 192, 125, 255)),
                        (11.0.try_into().unwrap(), RgbaColor::new(66, 128, 189, 255)),
                        (12.0.try_into().unwrap(), RgbaColor::new(225, 222, 127, 255)),
                        (13.0.try_into().unwrap(), RgbaColor::new(253, 2, 0, 255)),
                        (14.0.try_into().unwrap(), RgbaColor::new(162, 159, 66, 255)),
                        (15.0.try_into().unwrap(), RgbaColor::new(255, 255, 255, 255)),
                        (16.0.try_into().unwrap(), RgbaColor::new(192, 192, 192, 255)),
                    ]
                    .iter()
                    .copied()
                    .collect(),
                    RgbaColor::transparent(),
                    RgbaColor::transparent(),
                ).unwrap()},
            })),
            provenance: Some(vec![Provenance {
                citation: "Friedl, M., D. Sulla-Menashe. MCD12C1 MODIS/Terra+Aqua Land Cover Type Yearly L3 Global 0.05Deg CMG V006. 2015, distributed by NASA EOSDIS Land Processes DAAC, https://doi.org/10.5067/MODIS/MCD12C1.006. Accessed 2022-03-16.".to_owned(),
                license: "All data distributed by the LP DAAC contain no restrictions on the data reuse. (https://lpdaac.usgs.gov/resources/faqs/#am-i-allowed-to-reuse-lp-daac-data)".to_owned(),
                uri: "https://doi.org/10.5067/MODIS/MCD12C1.006".to_owned(),
            }]),
        },
        meta_data: MetaDataDefinition::GdalStatic(GdalMetaDataStatic {
            time: Some(geoengine_datatypes::primitives::TimeInterval::default()),
            params: GdalDatasetParameters {
                file_path: test_data!("raster/landcover/landcover.tif").into(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: Coordinate2D { x: -180., y: 90.},
                    x_pixel_size: 0.1,
                    y_pixel_size: -0.1,
                },
                width: 3600,
                height: 1800,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: Some(255.),
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: false,
                retry: None,
            },
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReferenceOption::SpatialReference(SpatialReference::epsg_4326()),
                time: TimeDescriptor::new_irregular(Some(geoengine_datatypes::primitives::TimeInterval::default())),
                spatial_grid: SpatialGridDescriptor::source_from_parts(
                 GeoTransform::new(Coordinate2D::new(-180.,  90.), 0.1, -0.1),
                 GridBoundingBox2D::new_min_max(0,1799, 0, 1599).unwrap(),
                ),
                bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new("band".into(), geoengine_datatypes::primitives::Measurement::classification("Land Cover".to_string(), 
                [
                    (0_u8, "Water Bodies".to_string()),
                    (1, "Evergreen Needleleaf Forests".to_string()),
                    (2, "Evergreen Broadleaf Forests".to_string()),
                    (3, "Deciduous Needleleaf Forests".to_string()),
                    (4, "Deciduous Broadleleaf Forests".to_string()),
                    (5, "Mixed Forests".to_string()),
                    (6, "Closed Shrublands".to_string()),
                    (7, "Open Shrublands".to_string()),
                    (8, "Woody Savannas".to_string()),
                    (9, "Savannas".to_string()),
                    (10, "Grasslands".to_string()),
                    (11, "Permanent Wetlands".to_string()),
                    (12, "Croplands".to_string()),
                    (13, "Urban and Built-Up".to_string()),
                    (14, "Cropland-Natural Vegetation Mosaics".to_string()),
                    (15, "Snow and Ice".to_string()),
                    (16, "Barren or Sparsely Vegetated".to_string()),
                ].into()))]).unwrap(),
            },
            cache_ttl: CacheTtlSeconds::default(),
        }),
    };

    db.add_dataset(ndvi.properties, ndvi.meta_data, None)
        .await
        .expect("dataset db access")
        .name
}

#[allow(clippy::missing_panics_doc)]
pub async fn register_ne2_multiband_workflow(
    app_ctx: &PostgresContext<NoTls>,
) -> (Workflow, WorkflowId) {
    let session = admin_login(app_ctx).await;
    let ctx = app_ctx.session_context(session);

    let red = add_file_definition_to_datasets(
        &ctx.db(),
        test_data!("dataset_defs/natural_earth_2_red.json"),
    )
    .await;
    let green = add_file_definition_to_datasets(
        &ctx.db(),
        test_data!("dataset_defs/natural_earth_2_green.json"),
    )
    .await;
    let blue = add_file_definition_to_datasets(
        &ctx.db(),
        test_data!("dataset_defs/natural_earth_2_blue.json"),
    )
    .await;

    let workflow = Workflow::Legacy {
        operator: TypedOperator::Raster(
            RasterStacker {
                params: RasterStackerParams {
                    rename_bands: RenameBands::Rename(vec![
                        "blue".into(),
                        "green".into(),
                        "red".into(),
                    ]),
                },
                sources: MultipleRasterSources {
                    rasters: vec![
                        GdalSource {
                            params: GdalSourceParameters {
                                data: blue.name.into(),
                                overview_level: None,
                            },
                        }
                        .boxed(),
                        GdalSource {
                            params: GdalSourceParameters {
                                data: green.name.into(),
                                overview_level: None,
                            },
                        }
                        .boxed(),
                        GdalSource {
                            params: GdalSourceParameters {
                                data: red.name.into(),
                                overview_level: None,
                            },
                        }
                        .boxed(),
                    ],
                },
            }
            .boxed(),
        ),
    };

    let id = ctx.db().register_workflow(workflow.clone()).await.unwrap();

    for dataset_id in [red.id, green.id, blue.id] {
        ctx.db()
            .add_permission(
                Role::registered_user_role_id(),
                dataset_id,
                Permission::Read,
            )
            .await
            .unwrap();

        ctx.db()
            .add_permission(Role::anonymous_role_id(), dataset_id, Permission::Read)
            .await
            .unwrap();
    }

    (workflow, id)
}

/// Add a definition from a file to the datasets.
#[allow(clippy::missing_panics_doc)]
pub async fn add_file_definition_to_datasets<D: GeoEngineDb>(
    db: &D,
    definition: &Path,
) -> DatasetIdAndName {
    let mut def: DatasetDefinition =
        serde_json::from_reader(BufReader::new(File::open(definition).unwrap())).unwrap();

    // rewrite metadata to use the correct file path
    def.meta_data = match def.meta_data {
        MetaDataDefinition::GdalStatic(mut meta_data) => {
            meta_data.params.file_path = test_data!(
                meta_data
                    .params
                    .file_path
                    .strip_prefix("test_data/")
                    .unwrap()
            )
            .into();
            MetaDataDefinition::GdalStatic(meta_data)
        }
        MetaDataDefinition::GdalMetaDataRegular(mut meta_data) => {
            meta_data.params.file_path = test_data!(
                meta_data
                    .params
                    .file_path
                    .strip_prefix("test_data/")
                    .unwrap()
            )
            .into();
            MetaDataDefinition::GdalMetaDataRegular(meta_data)
        }
        MetaDataDefinition::OgrMetaData(mut meta_data) => {
            meta_data.loading_info.file_name = test_data!(
                meta_data
                    .loading_info
                    .file_name
                    .strip_prefix("test_data/")
                    .unwrap()
            )
            .into();
            MetaDataDefinition::OgrMetaData(meta_data)
        }
        _ => todo!("Implement for other meta data types when used"),
    };

    let dataset = db
        .add_dataset(def.properties.clone(), def.meta_data.clone(), None)
        .await
        .unwrap();

    for role in [Role::registered_user_role_id(), Role::anonymous_role_id()] {
        db.add_permission(role, dataset.id, Permission::Read)
            .await
            .unwrap();
    }

    dataset
}

/// Add a definition from a file to the datasets.
#[allow(clippy::missing_panics_doc)]
pub async fn add_file_definition_to_datasets_and_return_layer<D: GeoEngineDb>(
    db: &D,
    definition: &Path,
    symbology: Option<Symbology>,
) -> (DataProviderId, LayerId) {
    let dataset: DatasetIdAndName = add_file_definition_to_datasets(db, definition).await;
    let layer_id = LayerId(dataset.id.to_string());

    let root_collection_id = db.get_root_layer_collection_id().await.unwrap();

    let dataset_metadata = db.load_dataset(&dataset.id).await.unwrap();

    let operator = match dataset_metadata.source_operator.as_str() {
        "GdalSource" => NewTypedOperator::Raster(NewRasterOperator::GdalSource(NewGdalSource {
            r#type: Default::default(),
            params: NewGdalSourceParameters {
                data: dataset.name.to_string(),
                overview_level: None,
            },
        })),
        _ => panic!("Only GdalSource is supported in this helper function"),
    };

    db.add_layer_with_id(
        &layer_id,
        AddLayer {
            name: dataset_metadata.display_name,
            description: dataset_metadata.description,
            workflow: Workflow::Typed { operator },
            symbology,
            properties: vec![],
            metadata: Default::default(),
        },
        &root_collection_id,
    )
    .await
    .unwrap();

    (INTERNAL_PROVIDER_ID, layer_id)
}

/// Add a definition from a file to the datasets as admin.
#[allow(clippy::missing_panics_doc)]
pub async fn add_pro_file_definition_to_datasets_as_admin(
    app_ctx: &PostgresContext<NoTls>,
    definition: &Path,
) -> DatasetIdAndName {
    let session = admin_login(app_ctx).await;
    let ctx = app_ctx.session_context(session);

    add_file_definition_to_datasets(&ctx.db(), definition).await
}

pub async fn check_allowed_http_methods2<T, TRes, P, PParam>(
    test_helper: T,
    allowed_methods: &[Method],
    projector: P,
) where
    T: Fn(Method) -> TRes,
    TRes: futures::Future<Output = PParam>,
    P: Fn(PParam) -> ServiceResponse,
{
    const HTTP_METHODS: [Method; 9] = [
        Method::GET,
        Method::HEAD,
        Method::POST,
        Method::PUT,
        Method::DELETE,
        Method::CONNECT,
        Method::OPTIONS,
        Method::TRACE,
        Method::PATCH,
    ];

    for method in HTTP_METHODS {
        if !allowed_methods.contains(&method) {
            let res = test_helper(method).await;
            let res = projector(res);

            ErrorResponse::assert(res, 405, "MethodNotAllowed", "HTTP method not allowed.").await;
        }
    }
}

pub fn check_allowed_http_methods<'a, T, TRes>(
    test_helper: T,
    allowed_methods: &'a [Method],
) -> impl futures::Future + 'a
where
    T: Fn(Method) -> TRes + 'a,
    TRes: futures::Future<Output = ServiceResponse> + 'a,
{
    check_allowed_http_methods2(test_helper, allowed_methods, |res| res)
}

#[actix_web::get("/dummy")]
#[allow(clippy::unused_async)]
async fn dummy_handler() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

pub fn create_test_app(
    app_ctx: PostgresContext<NoTls>,
) -> App<
    impl actix_web::dev::ServiceFactory<
        actix_web::dev::ServiceRequest,
        Config = (),
        Response = ServiceResponse<
            tracing_actix_web::StreamSpan<actix_web::body::EitherBody<actix_web::body::BoxBody>>,
        >,
        Error = actix_web::Error,
        InitError = (),
    >,
> {
    App::new()
        .app_data(web::Data::new(app_ctx))
        .wrap(OutputRequestId)
        .wrap(
            middleware::ErrorHandlers::default()
                .handler(http::StatusCode::NOT_FOUND, render_404)
                .handler(http::StatusCode::METHOD_NOT_ALLOWED, render_405),
        )
        .wrap(middleware::NormalizePath::trim())
        .wrap(TracingLogger::default())
        .configure(configure_extractors)
        .configure(handlers::datasets::init_dataset_routes::<PostgresContext<NoTls>>)
        .configure(handlers::layers::init_layer_routes::<PostgresContext<NoTls>>)
        .configure(handlers::permissions::init_permissions_routes::<PostgresContext<NoTls>>)
        .configure(handlers::plots::init_plot_routes::<PostgresContext<NoTls>>)
        .configure(handlers::projects::init_project_routes::<PostgresContext<NoTls>>)
        .configure(handlers::users::init_user_routes::<PostgresContext<NoTls>>)
        .configure(
            handlers::spatial_references::init_spatial_reference_routes::<PostgresContext<NoTls>>,
        )
        .configure(handlers::upload::init_upload_routes::<PostgresContext<NoTls>>)
        .configure(handlers::tasks::init_task_routes::<PostgresContext<NoTls>>)
        .configure(handlers::wcs::init_wcs_routes::<PostgresContext<NoTls>>)
        .configure(handlers::wfs::init_wfs_routes::<PostgresContext<NoTls>>)
        .configure(handlers::wms::init_wms_routes::<PostgresContext<NoTls>>)
        .configure(handlers::workflows::init_workflow_routes::<PostgresContext<NoTls>>)
        .configure(handlers::machine_learning::init_ml_routes::<PostgresContext<NoTls>>)
        .configure(handlers::ogc::init_ogc_routes::<PostgresContext<NoTls>>)
        .service(dummy_handler)
}

pub async fn send_test_request(
    req: test::TestRequest,
    app_ctx: PostgresContext<NoTls>,
) -> ServiceResponse {
    #[allow(unused_mut)]
    let mut app = create_test_app(app_ctx);

    let app = test::init_service(app).await;
    test::call_service(&app, req.to_request())
        .await
        .map_into_boxed_body()
}

/// # Panics
///
/// Panics if response string is not valid utf8
///
pub async fn read_body_string(res: ServiceResponse) -> String {
    let body = test::read_body(res).await;
    String::from_utf8(body.to_vec()).expect("Body is utf 8 string")
}

/// # Panics
///
/// * Panics if response string is not valid utf8.
/// * Panics if response body is not valid json.
///
pub async fn read_body_json(res: ServiceResponse) -> serde_json::Value {
    let body = test::read_body(res).await;
    let s = String::from_utf8(body.to_vec()).expect("Body is utf 8 string");
    serde_json::from_str(&s).expect("Body is valid json")
}

/// Helper struct that removes all specified uploads on drop
#[derive(Default)]
pub struct TestDataUploads {
    pub uploads: Vec<UploadId>,
}

impl Drop for TestDataUploads {
    fn drop(&mut self) {
        for upload in &self.uploads {
            if let Ok(path) = upload.root_path() {
                let _res = std::fs::remove_dir_all(path);
            }
        }
    }
}

/// Initialize a basic logger within tests.
/// You should only use this for debugging.
///
/// # Panics
/// This function will panic if the logger cannot be initialized.
///
pub fn initialize_debugging_in_test() {
    Logger::try_with_str("debug").unwrap().start().unwrap();
}

pub trait SetMultipartBody {
    #[must_use]
    fn set_multipart<B: Into<Vec<u8>>>(self, parts: Vec<(&str, B)>) -> Self;

    #[must_use]
    fn set_multipart_files(self, file_paths: &[PathBuf]) -> Self
    where
        Self: Sized,
    {
        self.set_multipart(
            file_paths
                .iter()
                .map(|o| {
                    (
                        o.file_name().unwrap().to_str().unwrap(),
                        std::fs::read(o).unwrap(),
                    )
                })
                .collect(),
        )
    }
}

impl SetMultipartBody for test::TestRequest {
    fn set_multipart<B: Into<Vec<u8>>>(self, parts: Vec<(&str, B)>) -> Self {
        let mut body: Vec<u8> = Vec::new();

        for (file_name, content) in parts {
            write!(body, "--10196671711503402186283068890\r\n").unwrap();
            write!(
                body,
                "Content-Disposition: form-data; name=\"files[]\"; filename=\"{file_name}\"\r\n\r\n"
            )
            .unwrap();
            body.append(&mut content.into());
            write!(body, "\r\n").unwrap();
        }
        write!(body, "--10196671711503402186283068890--\r\n").unwrap();

        self.append_header((header::CONTENT_LENGTH, body.len()))
            .append_header((
                header::CONTENT_TYPE,
                "multipart/form-data; boundary=10196671711503402186283068890",
            ))
            .set_payload(body)
    }
}

/// configure the number of concurrently running tests that use the database
const CONCURRENT_DB_TESTS: usize = 10;
static DB: OnceLock<RwLock<Arc<Semaphore>>> = OnceLock::new();

/// Setup database schema and return its name.
pub(crate) async fn setup_db() -> (OwnedSemaphorePermit, DatabaseConnectionConfig) {
    // acquire a permit from the semaphore that limits the number of concurrently running tests that use the database
    let permit = DB
        .get_or_init(|| RwLock::new(Arc::new(Semaphore::new(CONCURRENT_DB_TESTS))))
        .read()
        .await
        .clone()
        .acquire_owned()
        .await
        .unwrap();

    let mut db_config = get_config_element::<Postgres>().unwrap();
    db_config.schema = format!("geoengine_test_{}", rand::rng().next_u64()); // generate random temp schema
    debug!("Creating schema {}", db_config.schema);

    let db_config = DatabaseConnectionConfig {
        host: db_config.host,
        port: db_config.port,
        user: db_config.user,
        password: db_config.password,
        database: db_config.database,
        schema: db_config.schema,
    };

    // generate schema with prior connection
    PostgresConnectionManager::new(db_config.pg_config(), NoTls)
        .connect()
        .await
        .unwrap()
        .batch_execute(&format!("CREATE SCHEMA {};", db_config.schema))
        .await
        .unwrap();

    (permit, db_config)
}

/// Tear down database schema.
pub(crate) async fn tear_down_db(pg_config: tokio_postgres::Config, schema: &str) {
    // generate schema with prior connection
    // TODO: backoff and retry if no connections slot are available
    PostgresConnectionManager::new(pg_config, NoTls)
        .connect()
        .await
        .unwrap()
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE;"))
        .await
        .unwrap();
}

#[cfg(test)]
/// A matcher that inspects the request and prints it to the console.
pub struct HttpTestInspectMatcher {}

#[cfg(test)]
#[allow(clippy::dbg_macro)]
impl<IN> httptest::matchers::Matcher<IN> for HttpTestInspectMatcher
where
    IN: std::fmt::Debug,
{
    fn matches(&mut self, req: &IN, _ctx: &mut httptest::matchers::ExecutionContext) -> bool {
        dbg!(req);
        true
    }

    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "InspectMatcher")
    }
}

pub trait MockQueryContext {
    type Q: QueryContext;
    fn mock_query_context(&self) -> Result<Self::Q, crate::error::Error>;
}

impl<C> MockQueryContext for C
where
    C: SessionContext,
{
    type Q = C::QueryContext;
    fn mock_query_context(&self) -> Result<C::QueryContext, crate::error::Error> {
        self.query_context(Uuid::new_v4(), Uuid::new_v4())
    }
}

#[allow(clippy::missing_panics_doc)]
pub async fn create_session_helper<C: UserAuth>(app_ctx: &C) -> UserSession {
    app_ctx
        .register_user(UserRegistration {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
            real_name: "Foo Bar".to_string(),
        })
        .await
        .unwrap();

    app_ctx
        .login(UserCredentials {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
        })
        .await
        .unwrap()
}

pub fn create_random_user_session_helper() -> UserSession {
    let user_id = UserId::new();

    UserSession {
        id: SessionId::new(),
        user: UserInfo {
            id: user_id,
            email: Some(user_id.to_string()),
            real_name: Some(user_id.to_string()),
        },
        created: DateTime::MIN,
        valid_until: DateTime::MAX,
        project: None,
        view: None,
        roles: vec![user_id.into(), Role::registered_user_role_id()],
    }
}

#[allow(clippy::missing_panics_doc)]
pub async fn create_project_helper2<C: ApplicationContext<Session = UserSession> + UserAuth>(
    app_ctx: &C,
) -> (UserSession, ProjectId) {
    let session = create_session_helper(app_ctx).await;

    let project = app_ctx
        .session_context(session.clone())
        .db()
        .create_project(CreateProject {
            name: "Test".to_string(),
            description: "Foo".to_string(),
            bounds: STRectangle::new(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1)
                .unwrap(),
            time_step: None,
        })
        .await
        .unwrap();

    (session, project)
}

#[allow(clippy::missing_panics_doc)]
pub async fn add_ndvi_to_datasets2<C: ApplicationContext<Session = UserSession>>(
    app_ctx: &C,
    share_with_users: bool,
    share_with_anonymous: bool,
) -> (DatasetId, NamedData) {
    let dataset_name = DatasetName {
        namespace: None,
        name: "NDVI".to_string(),
    };

    let ndvi = DatasetDefinition {
        properties: AddDataset {
            name: Some(dataset_name.clone()),
            display_name: "NDVI".to_string(),
            description: "NDVI data from MODIS".to_string(),
            source_operator: "GdalSource".to_string(),
            symbology: None,
            provenance: Some(vec![Provenance {
                citation: "Sample Citation".to_owned(),
                license: "Sample License".to_owned(),
                uri: "http://example.org/".to_owned(),
            }]),
            tags: Some(vec!["raster".to_owned(), "test".to_owned()]),
        },
        meta_data: MetaDataDefinition::GdalMetaDataRegular(create_ndvi_meta_data()),
    };

    let system_session = UserSession::admin_session();

    let db = app_ctx.session_context(system_session).db();

    let dataset_id = db
        .add_dataset(ndvi.properties, ndvi.meta_data, None)
        .await
        .expect("dataset db access")
        .id;

    if share_with_users {
        db.add_permission(
            Role::registered_user_role_id(),
            dataset_id,
            Permission::Read,
        )
        .await
        .unwrap();
    }

    if share_with_anonymous {
        db.add_permission(Role::anonymous_role_id(), dataset_id, Permission::Read)
            .await
            .unwrap();
    }

    (dataset_id, dataset_name.into())
}

#[allow(clippy::missing_panics_doc)]
pub async fn add_ndvi_to_layers<C: ApplicationContext<Session = UserSession>>(
    app_ctx: &C,
) -> (DataProviderId, LayerId) {
    let (dataset_id, named_data) = add_ndvi_to_datasets2(app_ctx, true, true).await;
    let layer_id = LayerId(dataset_id.to_string());

    let system_session = UserSession::admin_session();
    let db = app_ctx.session_context(system_session).db();

    let root_collection_id = db.get_root_layer_collection_id().await.unwrap();

    db.add_layer_with_id(
        &layer_id,
        AddLayer {
            name: "NDVI".to_string(),
            description: "NDVI Layer".to_string(),
            workflow: Workflow::Typed {
                operator: NewTypedOperator::Raster(NewRasterOperator::GdalSource(NewGdalSource {
                    r#type: Default::default(),
                    params: NewGdalSourceParameters {
                        data: named_data.to_string(),
                        overview_level: None,
                    },
                })),
            },
            symbology: Some(Symbology::Raster(RasterSymbology {
                r#type: Default::default(),
                opacity: 1.0,
                raster_colorizer: RasterColorizer::SingleBand {
                    band: 0,
                    band_colorizer: ndvi_255_colorizer(),
                },
            })),
            properties: vec![],
            metadata: Default::default(),
        },
        &root_collection_id,
    )
    .await
    .unwrap();

    (INTERNAL_PROVIDER_ID, layer_id)
}

pub fn ndvi_255_symbology() -> Symbology {
    Symbology::Raster(RasterSymbology {
        r#type: Default::default(),
        opacity: 1.0,
        raster_colorizer: RasterColorizer::SingleBand {
            band: 0,
            band_colorizer: ndvi_255_colorizer(),
        },
    })
}

#[allow(
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    reason = "Test function"
)]
pub fn ndvi_255_colorizer() -> Colorizer {
    let breakpoints = [
        (0, [236, 224, 215, 0]),
        (1, [235, 223, 214, 255]),
        (2, [234, 222, 212, 255]),
        (3, [234, 221, 211, 255]),
        (4, [233, 221, 209, 255]),
        (5, [232, 220, 208, 255]),
        (6, [231, 219, 206, 255]),
        (7, [231, 218, 205, 255]),
        (8, [230, 217, 204, 255]),
        (9, [229, 216, 202, 255]),
        (10, [228, 215, 201, 255]),
        (11, [227, 214, 199, 255]),
        (12, [227, 214, 198, 255]),
        (13, [226, 213, 197, 255]),
        (14, [225, 212, 195, 255]),
        (15, [224, 211, 194, 255]),
        (16, [224, 210, 192, 255]),
        (17, [223, 209, 191, 255]),
        (18, [222, 208, 189, 255]),
        (19, [221, 207, 188, 255]),
        (20, [221, 207, 187, 255]),
        (21, [220, 206, 185, 255]),
        (22, [219, 205, 184, 255]),
        (23, [218, 204, 182, 255]),
        (24, [217, 203, 181, 255]),
        (25, [217, 202, 180, 255]),
        (26, [216, 201, 178, 255]),
        (27, [215, 200, 177, 255]),
        (28, [214, 200, 175, 255]),
        (29, [214, 199, 174, 255]),
        (30, [213, 198, 172, 255]),
        (31, [212, 197, 171, 255]),
        (32, [211, 196, 170, 255]),
        (33, [210, 195, 168, 255]),
        (34, [209, 195, 167, 255]),
        (35, [209, 194, 165, 255]),
        (36, [208, 193, 164, 255]),
        (37, [207, 192, 162, 255]),
        (38, [206, 192, 161, 255]),
        (39, [205, 191, 159, 255]),
        (40, [204, 190, 158, 255]),
        (41, [204, 189, 156, 255]),
        (42, [203, 188, 155, 255]),
        (43, [202, 188, 153, 255]),
        (44, [201, 187, 152, 255]),
        (45, [200, 186, 150, 255]),
        (46, [199, 185, 149, 255]),
        (47, [199, 185, 148, 255]),
        (48, [198, 184, 146, 255]),
        (49, [197, 183, 145, 255]),
        (50, [196, 182, 143, 255]),
        (51, [195, 181, 142, 255]),
        (52, [194, 181, 140, 255]),
        (53, [193, 180, 139, 255]),
        (54, [193, 179, 137, 255]),
        (55, [192, 178, 136, 255]),
        (56, [191, 177, 134, 255]),
        (57, [190, 177, 133, 255]),
        (58, [189, 176, 131, 255]),
        (59, [188, 175, 130, 255]),
        (60, [188, 174, 128, 255]),
        (61, [187, 174, 127, 255]),
        (62, [186, 173, 125, 255]),
        (63, [185, 172, 124, 255]),
        (64, [184, 171, 122, 255]),
        (65, [183, 171, 121, 255]),
        (66, [182, 170, 119, 255]),
        (67, [181, 169, 117, 255]),
        (68, [180, 169, 116, 255]),
        (69, [179, 168, 114, 255]),
        (70, [178, 167, 112, 255]),
        (71, [177, 167, 111, 255]),
        (72, [176, 166, 109, 255]),
        (73, [175, 165, 107, 255]),
        (74, [174, 164, 105, 255]),
        (75, [173, 164, 104, 255]),
        (76, [172, 163, 102, 255]),
        (77, [171, 162, 100, 255]),
        (78, [170, 162, 99, 255]),
        (79, [170, 161, 97, 255]),
        (80, [169, 160, 95, 255]),
        (81, [168, 160, 94, 255]),
        (82, [167, 159, 92, 255]),
        (83, [166, 158, 90, 255]),
        (84, [165, 158, 89, 255]),
        (85, [164, 157, 87, 255]),
        (86, [163, 156, 85, 255]),
        (87, [162, 156, 84, 255]),
        (88, [161, 155, 82, 255]),
        (89, [160, 154, 80, 255]),
        (90, [159, 153, 78, 255]),
        (91, [158, 153, 77, 255]),
        (92, [157, 152, 75, 255]),
        (93, [156, 151, 73, 255]),
        (94, [155, 151, 72, 255]),
        (95, [154, 150, 70, 255]),
        (96, [153, 149, 69, 255]),
        (97, [152, 149, 68, 255]),
        (98, [151, 148, 66, 255]),
        (99, [150, 147, 65, 255]),
        (100, [149, 147, 64, 255]),
        (101, [147, 146, 63, 255]),
        (102, [146, 145, 61, 255]),
        (103, [145, 145, 60, 255]),
        (104, [144, 144, 59, 255]),
        (105, [143, 143, 58, 255]),
        (106, [142, 142, 56, 255]),
        (107, [141, 142, 55, 255]),
        (108, [140, 141, 54, 255]),
        (109, [139, 140, 53, 255]),
        (110, [138, 140, 51, 255]),
        (111, [137, 139, 50, 255]),
        (112, [135, 138, 49, 255]),
        (113, [134, 138, 48, 255]),
        (114, [133, 137, 46, 255]),
        (115, [132, 136, 45, 255]),
        (116, [131, 136, 44, 255]),
        (117, [130, 135, 43, 255]),
        (118, [129, 134, 41, 255]),
        (119, [128, 134, 40, 255]),
        (120, [127, 133, 39, 255]),
        (121, [126, 132, 38, 255]),
        (122, [124, 131, 36, 255]),
        (123, [123, 131, 35, 255]),
        (124, [122, 130, 34, 255]),
        (125, [121, 129, 33, 255]),
        (126, [120, 129, 31, 255]),
        (127, [119, 128, 30, 255]),
        (128, [118, 127, 30, 255]),
        (129, [117, 127, 30, 255]),
        (130, [116, 126, 30, 255]),
        (131, [115, 125, 30, 255]),
        (132, [114, 124, 30, 255]),
        (133, [113, 124, 31, 255]),
        (134, [112, 123, 31, 255]),
        (135, [111, 122, 31, 255]),
        (136, [110, 121, 31, 255]),
        (137, [109, 121, 31, 255]),
        (138, [108, 120, 31, 255]),
        (139, [107, 119, 31, 255]),
        (140, [106, 118, 31, 255]),
        (141, [105, 118, 31, 255]),
        (142, [104, 117, 31, 255]),
        (143, [103, 116, 32, 255]),
        (144, [102, 115, 32, 255]),
        (145, [101, 115, 32, 255]),
        (146, [100, 114, 32, 255]),
        (147, [99, 113, 32, 255]),
        (148, [98, 112, 32, 255]),
        (149, [97, 112, 32, 255]),
        (150, [96, 111, 32, 255]),
        (151, [95, 110, 32, 255]),
        (152, [94, 109, 32, 255]),
        (153, [93, 109, 32, 255]),
        (154, [92, 108, 33, 255]),
        (155, [91, 107, 33, 255]),
        (156, [90, 106, 33, 255]),
        (157, [89, 106, 33, 255]),
        (158, [88, 105, 33, 255]),
        (159, [87, 104, 33, 255]),
        (160, [86, 103, 33, 255]),
        (161, [85, 102, 33, 255]),
        (162, [85, 102, 33, 255]),
        (163, [84, 101, 33, 255]),
        (164, [83, 100, 33, 255]),
        (165, [82, 99, 33, 255]),
        (166, [81, 99, 33, 255]),
        (167, [81, 98, 33, 255]),
        (168, [80, 97, 33, 255]),
        (169, [79, 96, 33, 255]),
        (170, [78, 95, 33, 255]),
        (171, [77, 95, 33, 255]),
        (172, [76, 94, 33, 255]),
        (173, [76, 93, 33, 255]),
        (174, [75, 92, 33, 255]),
        (175, [74, 92, 33, 255]),
        (176, [73, 91, 33, 255]),
        (177, [72, 90, 33, 255]),
        (178, [72, 89, 33, 255]),
        (179, [71, 88, 33, 255]),
        (180, [70, 88, 33, 255]),
        (181, [69, 87, 33, 255]),
        (182, [68, 86, 33, 255]),
        (183, [68, 85, 33, 255]),
        (184, [67, 84, 33, 255]),
        (185, [66, 84, 33, 255]),
        (186, [65, 83, 33, 255]),
        (187, [64, 82, 33, 255]),
        (188, [63, 81, 33, 255]),
        (189, [63, 81, 33, 255]),
        (190, [62, 80, 33, 255]),
        (191, [61, 79, 33, 255]),
        (192, [60, 78, 32, 255]),
        (193, [59, 78, 31, 255]),
        (194, [58, 77, 30, 255]),
        (195, [57, 76, 29, 255]),
        (196, [56, 76, 28, 255]),
        (197, [55, 75, 27, 255]),
        (198, [54, 74, 26, 255]),
        (199, [53, 74, 25, 255]),
        (200, [52, 73, 24, 255]),
        (201, [51, 72, 23, 255]),
        (202, [50, 72, 22, 255]),
        (203, [49, 71, 21, 255]),
        (204, [48, 70, 20, 255]),
        (205, [47, 70, 19, 255]),
        (206, [46, 69, 18, 255]),
        (207, [46, 69, 17, 255]),
        (208, [45, 68, 15, 255]),
        (209, [44, 67, 14, 255]),
        (210, [43, 67, 13, 255]),
        (211, [42, 66, 12, 255]),
        (212, [41, 65, 11, 255]),
        (213, [40, 65, 10, 255]),
        (214, [39, 64, 9, 255]),
        (215, [38, 63, 8, 255]),
        (216, [37, 63, 7, 255]),
        (217, [36, 62, 6, 255]),
        (218, [35, 61, 5, 255]),
        (219, [34, 61, 4, 255]),
        (220, [33, 60, 3, 255]),
        (221, [32, 59, 2, 255]),
        (222, [31, 59, 1, 255]),
        (223, [30, 58, 0, 255]),
        (224, [29, 57, 0, 255]),
        (225, [29, 57, 0, 255]),
        (226, [28, 56, 0, 255]),
        (227, [28, 55, 0, 255]),
        (228, [27, 54, 0, 255]),
        (229, [26, 54, 1, 255]),
        (230, [26, 53, 1, 255]),
        (231, [25, 52, 1, 255]),
        (232, [24, 52, 1, 255]),
        (233, [24, 51, 1, 255]),
        (234, [23, 50, 1, 255]),
        (235, [23, 49, 1, 255]),
        (236, [22, 49, 1, 255]),
        (237, [21, 48, 1, 255]),
        (238, [21, 47, 1, 255]),
        (239, [20, 47, 2, 255]),
        (240, [19, 46, 2, 255]),
        (241, [19, 45, 2, 255]),
        (242, [18, 44, 2, 255]),
        (243, [18, 44, 2, 255]),
        (244, [17, 43, 2, 255]),
        (245, [16, 42, 2, 255]),
        (246, [16, 41, 2, 255]),
        (247, [15, 41, 2, 255]),
        (248, [14, 40, 2, 255]),
        (249, [14, 39, 2, 255]),
        (250, [13, 39, 3, 255]),
        (251, [13, 38, 3, 255]),
        (252, [12, 37, 3, 255]),
        (253, [11, 36, 3, 255]),
        (254, [11, 36, 3, 255]),
        (255, [0, 0, 0, 255]),
    ];

    Colorizer::palette(
        breakpoints
            .into_iter()
            .map(|(value, [r, g, b, a])| (value.into(), RgbaColor::new(r, g, b, a)))
            .collect(),
        RgbaColor::transparent(),
        RgbaColor::transparent(),
    )
    .unwrap()
}

#[allow(clippy::missing_panics_doc)]
pub async fn add_ndvi_3857_to_layers<C: ApplicationContext<Session = UserSession>>(
    app_ctx: &C,
) -> (DataProviderId, LayerId) {
    let (dataset_id, named_data) = add_ndvi_to_datasets2(app_ctx, true, true).await;
    let layer_id = LayerId(dataset_id.to_string());

    let system_session = UserSession::admin_session();
    let db = app_ctx.session_context(system_session).db();

    let root_collection_id = db.get_root_layer_collection_id().await.unwrap();

    db.add_layer_with_id(
        &layer_id,
        AddLayer {
            name: "NDVI".to_string(),
            description: "NDVI Layer".to_string(),
            workflow: Workflow::Typed {
                operator: NewTypedOperator::Raster(NewRasterOperator::Reprojection(Reprojection {
                    r#type: Default::default(),
                    params: ReprojectionParameters {
                        target_spatial_reference: SpatialReference::web_mercator().into(),
                        derive_out_spec: DeriveOutRasterSpecsSource::DataBounds,
                    },
                    sources: SingleRasterOrVectorSource {
                        source: SingleRasterOrVectorOperator::Raster(
                            NewRasterOperator::GdalSource(NewGdalSource {
                                r#type: Default::default(),
                                params: NewGdalSourceParameters {
                                    data: named_data.to_string(),
                                    overview_level: None,
                                },
                            }),
                        ),
                    }
                    .into(),
                })),
            },
            symbology: Some(Symbology::Raster(RasterSymbology {
                r#type: Default::default(),
                opacity: 1.0,
                raster_colorizer: RasterColorizer::SingleBand {
                    band: 0,
                    band_colorizer: ndvi_255_colorizer(),
                },
            })),
            properties: vec![],
            metadata: Default::default(),
        },
        &root_collection_id,
    )
    .await
    .unwrap();

    (INTERNAL_PROVIDER_ID, layer_id)
}

#[allow(clippy::missing_panics_doc)]
pub async fn add_ports_to_datasets<C: ApplicationContext<Session = UserSession>>(
    app_ctx: &C,
    share_with_users: bool,
    share_with_anonymous: bool,
) -> (DatasetId, NamedData) {
    let dataset_name = DatasetName {
        namespace: None,
        name: "ne_10m_ports".to_string(),
    };

    let ndvi = DatasetDefinition {
        properties: AddDataset {
            name: Some(dataset_name.clone()),
            display_name: "Natural Earth 10m Ports".to_string(),
            description: "Ports from Natural Earth".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: Some(vec![Provenance {
                citation: "Sample Citation".to_owned(),
                license: "Sample License".to_owned(),
                uri: "http://example.org/".to_owned(),
            }]),
            tags: Some(vec!["vector".to_owned(), "test".to_owned()]),
        },
        meta_data: MetaDataDefinition::OgrMetaData(create_ports_meta_data()),
    };

    let system_session = UserSession::admin_session();

    let db = app_ctx.session_context(system_session).db();

    let dataset_id = db
        .add_dataset(ndvi.properties, ndvi.meta_data, None)
        .await
        .expect("dataset db access")
        .id;

    if share_with_users {
        db.add_permission(
            Role::registered_user_role_id(),
            dataset_id,
            Permission::Read,
        )
        .await
        .unwrap();
    }

    if share_with_anonymous {
        db.add_permission(Role::anonymous_role_id(), dataset_id, Permission::Read)
            .await
            .unwrap();
    }

    (dataset_id, dataset_name.into())
}

#[allow(clippy::missing_panics_doc)]
pub async fn admin_login<C: ApplicationContext<Session = UserSession> + UserAuth>(
    ctx: &C,
) -> UserSession {
    let user_config = get_config_element::<crate::config::User>().unwrap();

    ctx.login(UserCredentials {
        email: user_config.admin_email.clone(),
        password: user_config.admin_password.clone(),
    })
    .await
    .unwrap()
}

/// Loads a pretrained mock model from disk
pub async fn load_mock_model_from_disk() -> Result<String, std::io::Error> {
    let path = test_data!("ml/")
        .join("b764bf81-e21d-4eb8-bf01-fac9af13faee")
        .join("mock_model.json");

    tokio::fs::read_to_string(path).await
}

/// Execute a test function with a temporary database schema. It will be cleaned up afterwards.
///
/// # Panics
///
/// Panics if the `PostgresContext` could not be created.
///
pub async fn with_temp_context<F, Fut, R>(f: F) -> R
where
    F: FnOnce(PostgresContext<NoTls>, DatabaseConnectionConfig) -> Fut
        + std::panic::UnwindSafe
        + Send
        + 'static,
    Fut: Future<Output = R>,
{
    with_temp_context_from_spec(
        TestDefault::test_default(),
        TestDefault::test_default(),
        get_config_element::<Quota>().unwrap(),
        OidcManager::default,
        f,
    )
    .await
}

/// Execute a test function with a temporary database schema. It will be cleaned up afterwards.
///
/// # Panics
///
/// Panics if the `PostgresContext` could not be created.
///
pub async fn with_temp_context_from_spec<F, Fut, R>(
    exe_ctx_tiling_spec: TilingSpecification,
    query_ctx_chunk_size: ChunkByteSize,
    quota_config: Quota,
    oidc_db: impl FnOnce() -> OidcManager + std::panic::UnwindSafe + Send + 'static,
    f: F,
) -> R
where
    F: FnOnce(PostgresContext<NoTls>, DatabaseConnectionConfig) -> Fut
        + std::panic::UnwindSafe
        + Send
        + 'static,
    Fut: Future<Output = R>,
{
    let (_permit, db_config) = setup_db().await;

    // catch all panics and clean up first…
    let executed_fn = {
        let db_config = db_config.clone();
        std::panic::catch_unwind(move || {
            tokio::task::block_in_place(move || {
                Handle::current().block_on(async move {
                    let ctx = PostgresContext::new_with_context_spec(
                        db_config.pg_config(),
                        tokio_postgres::NoTls,
                        exe_ctx_tiling_spec,
                        query_ctx_chunk_size,
                        quota_config,
                        oidc_db(),
                    )
                    .await
                    .unwrap();
                    f(ctx, db_config.clone()).await
                })
            })
        })
    };

    tear_down_db(db_config.pg_config(), &db_config.schema).await;

    match executed_fn {
        Ok(res) => res,
        Err(err) => std::panic::resume_unwind(err),
    }
}

pub trait MockQuotaTracking {
    fn mock_work_unit_done(&self);
}

impl MockQuotaTracking for QuotaTracking {
    fn mock_work_unit_done(&self) {
        self.work_unit_done("test", WorkflowOperatorPath::initialize_root(), None);
    }
}

/// A responder that serves a json file with content type application/json.
///
/// # Panics
/// Panics if the file cannot be read.
///
#[cfg(test)]
pub fn json_file_responder(path: &Path) -> impl httptest::responders::Responder + use<> {
    let json = std::fs::read_to_string(path).unwrap();
    httptest::responders::status_code(200)
        .append_header("Content-Type", "application/json")
        .body(json)
}

#[cfg(test)]
pub(crate) mod mock_oidc {
    use crate::config::Oidc;
    use crate::users::{DefaultJsonWebKeySet, DefaultProviderMetadata};
    use crate::util::join_base_url_and_path;
    use chrono::{Duration, Utc};
    use httptest::matchers::{matches, request};
    use httptest::responders::status_code;
    use httptest::{Expectation, Server, all_of};
    use oauth2::basic::BasicTokenType;
    use oauth2::{
        AccessToken, AuthUrl, EmptyExtraTokenFields, RefreshToken, Scope, StandardTokenResponse,
        TokenUrl,
    };
    use openidconnect::core::{
        CoreClaimName, CoreIdToken, CoreIdTokenClaims, CoreIdTokenFields, CoreJsonWebKey,
        CoreJwsSigningAlgorithm, CoreProviderMetadata, CoreResponseType, CoreRsaPrivateSigningKey,
        CoreTokenResponse, CoreTokenType,
    };
    use openidconnect::{
        Audience, EmptyAdditionalClaims, EmptyAdditionalProviderMetadata, EndUserEmail,
        EndUserName, EndUserUsername, IssuerUrl, JsonWebKeySet, JsonWebKeySetUrl, LocalizedClaim,
        Nonce, ResponseTypes, StandardClaims, SubjectIdentifier,
    };
    use url::Url;

    const TEST_PRIVATE_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----\n\
	    MIIEogIBAAKCAQEAxIm5pngAgY4V+6XJPtlATkU6Gbcen22M3Tf16Gwl4uuFagEp\n\
	    SQ4u/HXvcyAYvdNfAwR34nsAyS1qFQasWYtcU4HwmFvo5ADfdJpfo6myRiGN3ocA\n\
	    4+/S1tH8HqLH+w7U/9SopwUP0n0+N0UaaFA1htkRY4zNWEDnJ2AVN2Vi0dUtS62D\n\
	    jOfvz+QMd04mAZaLkLdSxlHCYKjx6jmTQEbVFwSt/Pm1MryF7gkXg6YeiNG6Ehgm\n\
	    LUHv50Jwt1salVH9/FQVNkqiVivHNAW4cEVbuTZJl8TjtQn6MnOZSP7n8TkonrUd\n\
	    ULoIxIl3L+kneJABBaQ6zg52w00W1MXwlu+C8wIDAQABAoIBACW+dWLc5Ov8h4g+\n\
	    fHmPa2Qcs13A5yai+Ux6tMUgD96WcJa9Blq7WJavZ37qiRXbhAGmWAesq6f3Cspi\n\
	    77J6qw52g+gerokrCb7w7rEVo+EIDKDRuIANzKXoycxwYot6e7lt872voSxBVTN0\n\
	    F/A0hzMQeOBvZ/gs7reHIkvzMpktSyKVJOt9ie1cZ1jp7r1bazbFs2qIyDc5Z521\n\
	    BQ6GgRyNJ5toTttmF5ZxpSQXWyvumldWL5Ue9wNEIPjRgsL9UatqagxgmouGxEOL\n\
	    0F9bFWUFlrsqTArTWNxg5R0zFwfzFqidx0HwyF9SyidVq9Bz8/FtgVe2ed4u7snm\n\
	    vYOUbsECgYEA7yg6gyhlQvA0j5MAe6rhoMD0sYRG07ZR0vNzzZRoud9DSdE749f+\n\
	    ZvqUqv3Wuv5p97dd4sGuMkzihXdGqcpWO4CAbalvB2CB5HKVMIKR5cjMIzeVE17v\n\
	    0Hcdd2Spx6yMahFX3eePLl3wDDLSP2ITYi6m4SGckGwd5BeFkn4gNyMCgYEA0mEd\n\
	    Vt2bGF9+5sFfsZgd+3yNAnqLGZ+bxZuYcF/YayH8dKKHdrmhTJ+1w78JdFC5uV2G\n\
	    F75ubyrEEY09ftE/HNG90fanUAYxmVJXMFxxgMIE8VqsjiB/i1Q3ofN2HOlOB1W+\n\
	    4e8BEXrAxCgsXMGCwU73b52474/BDq4Bh1cNKfECgYB4cfw1/ewxsCPogxJlNgR4\n\
	    H3WcyY+aJGJFKZMS4EF2CvkqfhP5hdh8KIsjKsAwYN0hgtnnz79ZWdtjeFTAQkT3\n\
	    ppoHoKNoRbRlR0fXrIqp/VzCB8YugUup47OVY78V7tKwwJdODMbRhUHWAupcPZqh\n\
	    gflNvM3K9oh/TVFaG+dBnQKBgHE2mddZQlGHcn8zqQ+lUN05VZjz4U9UuTtKVGqE\n\
	    6a4diAIsRMH7e3YErIg+khPqLUg3sCWu8TcZyJG5dFJ+wHv90yzek4NZEe/0g78e\n\
	    wGYOAyLvLNT/YCPWmmmo3vMIClmgJyzmtah2aq4lAFqaOIdWu4lxU0h4D+iac3Al\n\
	    xIvBAoGAZtOeVlJCzmdfP8/J1IMHqFX3/unZEundqL1tiy5UCTK/RJTftr6aLkGL\n\
	    xN3QxN+Kuc5zMyHeQWY9jKO8SUwyuzrCuwduzzqC1OXEWinfcvCPg1yotRxgPGsV\n\
	    Wj4iz6nkuRK0fTLfTu6Nglx6mjX8Q3rz0UUFVjOL/gpgEWxzoHk=\n\
	    -----END RSA PRIVATE KEY-----";

    const TEST_JWK: &str = "{\
        \"kty\":\"RSA\",
        \"use\":\"sig\",
        \"n\":\"xIm5pngAgY4V-6XJPtlATkU6Gbcen22M3Tf16Gwl4uuFagEpSQ4u_HXvcyAYv\
            dNfAwR34nsAyS1qFQasWYtcU4HwmFvo5ADfdJpfo6myRiGN3ocA4-_S1tH8HqLH-w\
            7U_9SopwUP0n0-N0UaaFA1htkRY4zNWEDnJ2AVN2Vi0dUtS62DjOfvz-QMd04mAZa\
            LkLdSxlHCYKjx6jmTQEbVFwSt_Pm1MryF7gkXg6YeiNG6EhgmLUHv50Jwt1salVH9\
            _FQVNkqiVivHNAW4cEVbuTZJl8TjtQn6MnOZSP7n8TkonrUdULoIxIl3L-kneJABB\
            aQ6zg52w00W1MXwlu-C8w\",
        \"e\":\"AQAB\",
        \"d\":\"Jb51Ytzk6_yHiD58eY9rZByzXcDnJqL5THq0xSAP3pZwlr0GWrtYlq9nfuqJF\
            duEAaZYB6yrp_cKymLvsnqrDnaD6B6uiSsJvvDusRWj4QgMoNG4gA3MpejJzHBii3\
            p7uW3zva-hLEFVM3QX8DSHMxB44G9n-Czut4ciS_MymS1LIpUk632J7VxnWOnuvVt\
            rNsWzaojINzlnnbUFDoaBHI0nm2hO22YXlnGlJBdbK-6aV1YvlR73A0Qg-NGCwv1R\
            q2pqDGCai4bEQ4vQX1sVZQWWuypMCtNY3GDlHTMXB_MWqJ3HQfDIX1LKJ1Wr0HPz8\
            W2BV7Z53i7uyea9g5RuwQ\"
        }";

    const ACCESS_TOKEN: &str = "DUMMY_ACCESS_TOKEN_1";

    pub const SINGLE_STATE: &str = "State_1";
    pub const SINGLE_NONCE: &str = "Nonce_1";

    pub struct MockTokenConfig {
        issuer: Url,
        client_id: String,
        pub email: Option<EndUserEmail>,
        pub name: Option<LocalizedClaim<EndUserName>>,
        pub nonce: Option<Nonce>,
        pub duration: Option<core::time::Duration>,
        pub access: String,
        pub access_for_id: String,
        pub refresh: Option<String>,
        pub signing_alg: Option<CoreJwsSigningAlgorithm>,
        pub preferred_username: Option<String>,
    }

    impl MockTokenConfig {
        pub fn create_from_issuer_and_client(issuer: Url, client_id: String) -> Self {
            let mut name = LocalizedClaim::new();
            name.insert(None, EndUserName::new("Robin".to_string()));
            let name = Some(name);

            MockTokenConfig {
                issuer,
                client_id,
                email: Some(EndUserEmail::new("robin@dummy_db.com".to_string())),
                name,
                nonce: Some(Nonce::new(SINGLE_NONCE.to_string())),
                duration: Some(core::time::Duration::from_mins(30)),
                access: ACCESS_TOKEN.to_string(),
                access_for_id: ACCESS_TOKEN.to_string(),
                refresh: None,
                signing_alg: Some(CoreJwsSigningAlgorithm::RsaSsaPssSha256),
                preferred_username: None,
            }
        }

        pub fn create_from_tokens(
            issuer: Url,
            client_id: String,
            duration: core::time::Duration,
            access_token: String,
            refresh_token: String,
        ) -> Self {
            let mut name = LocalizedClaim::new();
            name.insert(None, EndUserName::new("Robin".to_string()));
            let name = Some(name);

            MockTokenConfig {
                issuer,
                client_id,
                email: Some(EndUserEmail::new("robin@dummy_db.com".to_string())),
                name,
                nonce: Some(Nonce::new(SINGLE_NONCE.to_string())),
                duration: Some(duration),
                access: access_token.clone(),
                access_for_id: access_token,
                refresh: Some(refresh_token),
                signing_alg: Some(CoreJwsSigningAlgorithm::RsaSsaPssSha256),
                preferred_username: None,
            }
        }
    }

    pub fn mock_provider_metadata(provider_base_url: &Url) -> DefaultProviderMetadata {
        CoreProviderMetadata::new(
            IssuerUrl::from_url(provider_base_url.clone()),
            AuthUrl::from_url(
                join_base_url_and_path(provider_base_url, "authorize")
                    .expect("Parsing mock auth url should not fail"),
            ),
            JsonWebKeySetUrl::from_url(
                join_base_url_and_path(provider_base_url, "jwk")
                    .expect("Parsing mock jwk url should not fail"),
            ),
            vec![ResponseTypes::new(vec![CoreResponseType::Code])],
            vec![],
            vec![CoreJwsSigningAlgorithm::RsaSsaPssSha256],
            EmptyAdditionalProviderMetadata {},
        )
        .set_token_endpoint(Some(TokenUrl::from_url(
            join_base_url_and_path(provider_base_url, "token")
                .expect("Parsing mock token url should not fail"),
        )))
        .set_scopes_supported(Some(vec![
            Scope::new("openid".to_string()),
            Scope::new("email".to_string()),
            Scope::new("profile".to_string()),
        ]))
        .set_claims_supported(Some(vec![
            CoreClaimName::new("sub".to_string()),
            CoreClaimName::new("email".to_string()),
            CoreClaimName::new("name".to_string()),
        ]))
    }

    pub fn mock_jwks() -> DefaultJsonWebKeySet {
        let jwk: CoreJsonWebKey =
            serde_json::from_str(TEST_JWK).expect("Parsing mock jwk should not fail");
        JsonWebKeySet::new(vec![jwk])
    }

    pub fn mock_token_response(
        mock_token_config: MockTokenConfig,
    ) -> StandardTokenResponse<CoreIdTokenFields, BasicTokenType> {
        let id_token = CoreIdToken::new(
            CoreIdTokenClaims::new(
                IssuerUrl::new(mock_token_config.issuer.to_string())
                    .expect("Parsing mock issuer should not fail"),
                vec![Audience::new(mock_token_config.client_id)],
                Utc::now() + Duration::seconds(300),
                Utc::now(),
                StandardClaims::new(SubjectIdentifier::new("DUMMY_SUBJECT_ID".to_string()))
                    .set_email(mock_token_config.email)
                    .set_name(mock_token_config.name)
                    .set_preferred_username(
                        mock_token_config
                            .preferred_username
                            .map(EndUserUsername::new),
                    ),
                EmptyAdditionalClaims {},
            )
            .set_nonce(mock_token_config.nonce),
            &CoreRsaPrivateSigningKey::from_pem(TEST_PRIVATE_KEY, None)
                .expect("Cannot create mock of RSA private key"),
            mock_token_config
                .signing_alg
                .unwrap_or(CoreJwsSigningAlgorithm::RsaSsaPssSha256),
            Some(&AccessToken::new(mock_token_config.access_for_id.clone())),
            None,
        )
        .expect("Cannot create mock of ID Token");

        let mut result = CoreTokenResponse::new(
            AccessToken::new(mock_token_config.access.clone()),
            CoreTokenType::Bearer,
            CoreIdTokenFields::new(Some(id_token), EmptyExtraTokenFields {}),
        );

        result.set_expires_in(mock_token_config.duration.as_ref());

        if let Some(refresh) = mock_token_config.refresh {
            result.set_refresh_token(Some(RefreshToken::new(refresh)));
        }

        result
    }

    pub fn mock_valid_provider_discovery(expected_discoveries: usize) -> Server {
        let server = Server::run();
        let server_url = Url::parse(&server.url_str("/")).unwrap();

        let provider_metadata = mock_provider_metadata(&server_url);
        let jwks = mock_jwks();

        server.expect(
            Expectation::matching(request::method_path(
                "GET",
                "/.well-known/openid-configuration",
            ))
            .times(expected_discoveries)
            .respond_with(
                status_code(200)
                    .insert_header("content-type", "application/json")
                    .body(serde_json::to_string(&provider_metadata).unwrap()),
            ),
        );
        server.expect(
            Expectation::matching(request::method_path("GET", "/jwk"))
                .times(expected_discoveries)
                .respond_with(
                    status_code(200)
                        .insert_header("content-type", "application/json")
                        .body(serde_json::to_string(&jwks).unwrap()),
                ),
        );
        server
    }

    pub struct MockRefreshServerConfig {
        pub expected_discoveries: usize,
        pub token_duration: core::time::Duration,
        pub creates_first_token: bool,
        pub first_access_token: String,
        pub first_refresh_token: String,
        pub second_access_token: String,
        pub second_refresh_token: String,
        pub client_side_password: Option<String>,
    }

    pub fn mock_refresh_server(config: MockRefreshServerConfig) -> (Server, Oidc) {
        let client_id = "";

        let server = mock_valid_provider_discovery(config.expected_discoveries);
        let server_url = Url::parse(&server.url_str("/")).unwrap();

        if config.creates_first_token {
            let mock_token_config = MockTokenConfig::create_from_tokens(
                server_url.clone(),
                client_id.into(),
                config.token_duration,
                config.first_access_token,
                config.first_refresh_token,
            );
            let token_response = mock_token_response(mock_token_config);
            server.expect(
                Expectation::matching(all_of![
                    request::method_path("POST", "/token"),
                    request::body(matches("^grant_type=authorization_code.*$")),
                ])
                .respond_with(
                    status_code(200)
                        .insert_header("content-type", "application/json")
                        .body(serde_json::to_string(&token_response).unwrap()),
                ),
            );
        }

        let mock_refresh_response = MockTokenConfig::create_from_tokens(
            server_url.clone(),
            client_id.into(),
            config.token_duration,
            config.second_access_token,
            config.second_refresh_token,
        );
        let refresh_response = mock_token_response(mock_refresh_response);
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/token"),
                request::body(matches("^grant_type=refresh_token.*$"))
            ])
            .respond_with(
                status_code(200)
                    .insert_header("content-type", "application/json")
                    .body(serde_json::to_string(&refresh_response).unwrap()),
            ),
        );

        (
            server,
            Oidc {
                enabled: true,
                issuer: server_url.clone(),
                client_id: client_id.into(),
                client_secret: None,
                scopes: vec!["profile".to_string(), "email".to_string()],
                token_encryption_password: config.client_side_password,
            },
        )
    }
}
