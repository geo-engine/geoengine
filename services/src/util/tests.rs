#![allow(clippy::unwrap_used)] // okay in tests

use super::postgres::DatabaseConnectionConfig;
use crate::api::model::responses::ErrorResponse;
use crate::config::{get_config_element, Postgres};
use crate::contexts::ApplicationContext;
use crate::contexts::GeoEngineDb;
use crate::contexts::PostgresContext;
use crate::datasets::listing::Provenance;
use crate::datasets::storage::DatasetStore;
use crate::datasets::upload::UploadId;
use crate::datasets::upload::UploadRootPath;
use crate::datasets::AddDataset;
use crate::datasets::DatasetIdAndName;
use crate::datasets::DatasetName;
use crate::permissions::Permission;
use crate::permissions::PermissionDb;
use crate::permissions::Role;
use crate::projects::{
    CreateProject, LayerUpdate, ProjectDb, ProjectId, ProjectLayer, RasterSymbology, STRectangle,
    Symbology, UpdateProject,
};
use crate::users::OidcManager;
use crate::util::middleware::OutputRequestId;
use crate::util::server::{configure_extractors, render_404, render_405};
use crate::util::Identifier;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};
use crate::{
    api::handlers,
    contexts::SessionContext,
    datasets::storage::{DatasetDefinition, MetaDataDefinition},
};
use crate::{
    config::Quota,
    contexts::SessionId,
    users::{UserAuth, UserCredentials, UserId, UserInfo, UserRegistration, UserSession},
};
use actix_web::dev::ServiceResponse;
use actix_web::{
    http, http::header, http::Method, middleware, test, web, App, HttpResponse, Responder,
};
use bb8_postgres::bb8::ManageConnection;
use bb8_postgres::PostgresConnectionManager;
use flexi_logger::Logger;
use futures_util::Future;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::dataset::NamedData;
use geoengine_datatypes::operations::image::Colorizer;
use geoengine_datatypes::operations::image::RasterColorizer;
use geoengine_datatypes::operations::image::RgbaColor;
use geoengine_datatypes::primitives::CacheTtlSeconds;
use geoengine_datatypes::primitives::Coordinate2D;
use geoengine_datatypes::primitives::SpatialResolution;
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::raster::RenameBands;
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use geoengine_datatypes::test_data;
use geoengine_datatypes::util::test::TestDefault;
use geoengine_datatypes::{primitives::DateTime, raster::TilingSpecification};
use geoengine_operators::engine::QueryContext;
use geoengine_operators::engine::RasterBandDescriptor;
use geoengine_operators::engine::RasterBandDescriptors;
use geoengine_operators::engine::RasterResultDescriptor;
use geoengine_operators::engine::WorkflowOperatorPath;
use geoengine_operators::engine::{ChunkByteSize, MultipleRasterSources};
use geoengine_operators::engine::{RasterOperator, TypedOperator};
use geoengine_operators::meta::quota::QuotaTracking;
use geoengine_operators::processing::RasterStacker;
use geoengine_operators::processing::RasterStackerParams;
use geoengine_operators::source::GdalDatasetGeoTransform;
use geoengine_operators::source::GdalMetaDataStatic;
use geoengine_operators::source::{FileNotFoundHandling, GdalDatasetParameters};
use geoengine_operators::source::{GdalSource, GdalSourceParameters};
use geoengine_operators::util::gdal::create_ndvi_meta_data_with_cache_ttl;
use geoengine_operators::util::gdal::{create_ndvi_meta_data, create_ports_meta_data};
use rand::RngCore;
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

    let workflow = Workflow {
        operator: TypedOperator::Raster(
            GdalSource {
                params: GdalSourceParameters { data: dataset },
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
        .add_dataset(ndvi.properties, ndvi.meta_data)
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
            name: None,
            display_name: "Land Cover".to_string(),
            description: "Land Cover derived from MODIS/Terra+Aqua Land Cover".to_string(),
            source_operator: "GdalSource".to_string(),
            tags: Some(vec!["raster".to_owned(), "test".to_owned()]),
            symbology: Some(Symbology::Raster(RasterSymbology {
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
                time: Some(geoengine_datatypes::primitives::TimeInterval::default()),
                bbox: Some(geoengine_datatypes::primitives::SpatialPartition2D::new((-180., 90.).into(),
                     (180., -90.).into()).unwrap()),
                resolution: Some(SpatialResolution {
                    x: 0.1, y: 0.1,
                }),
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

    db.add_dataset(ndvi.properties, ndvi.meta_data)
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

    let workflow = Workflow {
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
                            },
                        }
                        .boxed(),
                        GdalSource {
                            params: GdalSourceParameters {
                                data: green.name.into(),
                            },
                        }
                        .boxed(),
                        GdalSource {
                            params: GdalSourceParameters {
                                data: red.name.into(),
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
            meta_data.params.file_path = test_data!(meta_data
                .params
                .file_path
                .strip_prefix("test_data/")
                .unwrap())
            .into();
            MetaDataDefinition::GdalStatic(meta_data)
        }
        MetaDataDefinition::GdalMetaDataRegular(mut meta_data) => {
            meta_data.params.file_path = test_data!(meta_data
                .params
                .file_path
                .strip_prefix("test_data/")
                .unwrap())
            .into();
            MetaDataDefinition::GdalMetaDataRegular(meta_data)
        }
        MetaDataDefinition::OgrMetaData(mut meta_data) => {
            meta_data.loading_info.file_name = test_data!(meta_data
                .loading_info
                .file_name
                .strip_prefix("test_data/")
                .unwrap())
            .into();
            MetaDataDefinition::OgrMetaData(meta_data)
        }
        _ => todo!("Implement for other meta data types when used"),
    };

    let dataset = db
        .add_dataset(def.properties.clone(), def.meta_data.clone())
        .await
        .unwrap();

    for role in [Role::registered_user_role_id(), Role::anonymous_role_id()] {
        db.add_permission(role, dataset.id, Permission::Read)
            .await
            .unwrap();
    }

    dataset
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

pub async fn send_test_request(
    req: test::TestRequest,
    app_ctx: PostgresContext<NoTls>,
) -> ServiceResponse {
    #[allow(unused_mut)]
    let mut app = App::new()
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
        .service(dummy_handler);

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
    db_config.schema = format!("geoengine_test_{}", rand::thread_rng().next_u64()); // generate random temp schema

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
        .batch_execute(&format!("CREATE SCHEMA {};", &db_config.schema))
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
) -> (UserSession, ProjectId)
where
{
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
) -> (DatasetId, NamedData)
where
{
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
        .add_dataset(ndvi.properties, ndvi.meta_data)
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
        .add_dataset(ndvi.properties, ndvi.meta_data)
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

#[cfg(test)]
pub(crate) mod mock_oidc {
    use crate::config::Oidc;
    use crate::users::{DefaultJsonWebKeySet, DefaultProviderMetadata};
    use chrono::{Duration, Utc};
    use httptest::matchers::{matches, request};
    use httptest::responders::status_code;
    use httptest::{all_of, Expectation, Server};
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
        EndUserName, IssuerUrl, JsonWebKeySet, JsonWebKeySetUrl, LocalizedClaim, Nonce,
        ResponseTypes, StandardClaims, SubjectIdentifier,
    };

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
        issuer: String,
        client_id: String,
        pub email: Option<EndUserEmail>,
        pub name: Option<LocalizedClaim<EndUserName>>,
        pub nonce: Option<Nonce>,
        pub duration: Option<core::time::Duration>,
        pub access: String,
        pub access_for_id: String,
        pub refresh: Option<String>,
    }

    impl MockTokenConfig {
        pub fn create_from_issuer_and_client(issuer: String, client_id: String) -> Self {
            let mut name = LocalizedClaim::new();
            name.insert(None, EndUserName::new("Robin".to_string()));
            let name = Some(name);

            MockTokenConfig {
                issuer,
                client_id,
                email: Some(EndUserEmail::new("robin@dummy_db.com".to_string())),
                name,
                nonce: Some(Nonce::new(SINGLE_NONCE.to_string())),
                duration: Some(core::time::Duration::from_secs(1800)),
                access: ACCESS_TOKEN.to_string(),
                access_for_id: ACCESS_TOKEN.to_string(),
                refresh: None,
            }
        }

        pub fn create_from_tokens(
            issuer: String,
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
            }
        }
    }

    pub fn mock_provider_metadata(provider_base_url: &str) -> DefaultProviderMetadata {
        CoreProviderMetadata::new(
            IssuerUrl::new(provider_base_url.to_string())
                .expect("Parsing mock issuer should not fail"),
            AuthUrl::new(provider_base_url.to_owned() + "/authorize")
                .expect("Parsing mock auth url should not fail"),
            JsonWebKeySetUrl::new(provider_base_url.to_owned() + "/jwk")
                .expect("Parsing mock jwk url should not fail"),
            vec![ResponseTypes::new(vec![CoreResponseType::Code])],
            vec![],
            vec![CoreJwsSigningAlgorithm::RsaSsaPssSha256],
            EmptyAdditionalProviderMetadata {},
        )
        .set_token_endpoint(Some(
            TokenUrl::new(provider_base_url.to_owned() + "/token")
                .expect("Parsing mock token url should not fail"),
        ))
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
                IssuerUrl::new(mock_token_config.issuer)
                    .expect("Parsing mock issuer should not fail"),
                vec![Audience::new(mock_token_config.client_id)],
                Utc::now() + Duration::seconds(300),
                Utc::now(),
                StandardClaims::new(SubjectIdentifier::new("DUMMY_SUBJECT_ID".to_string()))
                    .set_email(mock_token_config.email)
                    .set_name(mock_token_config.name),
                EmptyAdditionalClaims {},
            )
            .set_nonce(mock_token_config.nonce),
            &CoreRsaPrivateSigningKey::from_pem(TEST_PRIVATE_KEY, None)
                .expect("Cannot create mock of RSA private key"),
            CoreJwsSigningAlgorithm::RsaSsaPssSha256,
            Some(&AccessToken::new(
                mock_token_config.access_for_id.to_string(),
            )),
            None,
        )
        .expect("Cannot create mock of ID Token");

        let mut result = CoreTokenResponse::new(
            AccessToken::new(mock_token_config.access.to_string()),
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
        let server_url = format!("http://{}", server.addr());

        let provider_metadata = mock_provider_metadata(server_url.as_str());
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
        let server_url = format!("http://{}", server.addr());

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
                redirect_uri: "https://dummy-redirect.com/".into(),
                scopes: vec!["profile".to_string(), "email".to_string()],
                token_encryption_password: config.client_side_password,
            },
        )
    }
}
