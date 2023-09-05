use crate::api::model::datatypes::Coordinate2D;
use crate::api::model::datatypes::DatasetName;
use crate::api::model::datatypes::NamedData;
use crate::api::model::datatypes::RasterDataType;
use crate::api::model::datatypes::SpatialResolution;
use crate::api::model::operators::FileNotFoundHandling;
use crate::api::model::operators::GdalDatasetGeoTransform;
use crate::api::model::operators::GdalDatasetParameters;
use crate::api::model::operators::GdalMetaDataStatic;
use crate::api::model::operators::RasterResultDescriptor;
use crate::api::model::services::AddDataset;
use crate::contexts::GeoEngineDb;
use crate::contexts::PostgresContext;
use crate::contexts::SimpleApplicationContext;
use crate::datasets::listing::Provenance;
use crate::datasets::storage::DatasetStore;
use crate::datasets::upload::UploadId;
use crate::datasets::upload::UploadRootPath;
use crate::handlers::ErrorResponse;
use crate::projects::{
    CreateProject, LayerUpdate, ProjectDb, ProjectId, ProjectLayer, RasterSymbology, STRectangle,
    Symbology, UpdateProject,
};
use crate::util::server::{configure_extractors, render_404, render_405};
use crate::util::Identifier;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};
use crate::{
    contexts::SessionContext,
    datasets::storage::{DatasetDefinition, MetaDataDefinition},
    handlers,
};
use actix_web::dev::ServiceResponse;
use actix_web::{http, http::header, http::Method, middleware, test, web, App};
use bb8_postgres::bb8::ManageConnection;
use bb8_postgres::PostgresConnectionManager;
use flexi_logger::Logger;
use futures_util::Future;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::operations::image::Colorizer;
use geoengine_datatypes::operations::image::RgbaColor;
use geoengine_datatypes::primitives::CacheTtlSeconds;
use geoengine_datatypes::raster::TilingSpecification;
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use geoengine_datatypes::test_data;
use geoengine_datatypes::util::test::TestDefault;
use geoengine_operators::engine::ChunkByteSize;
use geoengine_operators::engine::{RasterOperator, TypedOperator};
use geoengine_operators::source::{GdalSource, GdalSourceParameters};
use geoengine_operators::util::gdal::create_ndvi_meta_data_with_cache_ttl;
use rand::RngCore;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::runtime::Handle;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::RwLock;
use tokio::sync::Semaphore;
use tokio_postgres::NoTls;

use super::config::get_config_element;
use super::config::Postgres;

#[allow(clippy::missing_panics_doc)]
pub async fn create_project_helper<C: SimpleApplicationContext>(app_ctx: &C) -> ProjectId {
    app_ctx
        .default_session_context()
        .await
        .unwrap()
        .db()
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
                colorizer: Colorizer::Rgba.into(),
            }),
        })]),
        plots: None,
        bounds: None,
        time_step: None,
    }
}

#[allow(clippy::missing_panics_doc)]
pub async fn register_ndvi_workflow_helper<A: SimpleApplicationContext>(
    app_ctx: &A,
) -> (Workflow, WorkflowId) {
    register_ndvi_workflow_helper_with_cache_ttl(app_ctx, CacheTtlSeconds::default()).await
}

#[allow(clippy::missing_panics_doc)]
pub async fn register_ndvi_workflow_helper_with_cache_ttl<A: SimpleApplicationContext>(
    app_ctx: &A,
    cache_ttl: CacheTtlSeconds,
) -> (Workflow, WorkflowId) {
    let (_, dataset) = add_ndvi_to_datasets_with_cache_ttl(app_ctx, cache_ttl).await;

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

    let session = app_ctx.default_session().await.unwrap();

    let id = app_ctx
        .session_context(session)
        .db()
        .register_workflow(workflow.clone())
        .await
        .unwrap();

    (workflow, id)
}

pub async fn add_ndvi_to_datasets<A: SimpleApplicationContext>(
    app_ctx: &A,
) -> (DatasetId, NamedData) {
    add_ndvi_to_datasets_with_cache_ttl(app_ctx, CacheTtlSeconds::default()).await
}

/// .
///
/// # Panics
///
/// Panics if the default session context could not be created.
pub async fn add_ndvi_to_datasets_with_cache_ttl<A: SimpleApplicationContext>(
    app_ctx: &A,
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
        },
        meta_data: MetaDataDefinition::GdalMetaDataRegular(create_ndvi_meta_data_with_cache_ttl(
            cache_ttl,
        )),
    };

    let db = &app_ctx.default_session_context().await.unwrap().db();
    let dataset_id = db
        .add_dataset(ndvi.properties, db.wrap_meta_data(ndvi.meta_data))
        .await
        .expect("dataset db access")
        .id;

    let named_data = NamedData {
        namespace: dataset_name.namespace,
        provider: None,
        name: dataset_name.name,
    };

    (dataset_id.into(), named_data)
}

#[allow(clippy::missing_panics_doc, clippy::too_many_lines)]
pub async fn add_land_cover_to_datasets<D: GeoEngineDb>(db: &D) -> DatasetId {
    let ndvi = DatasetDefinition {
        properties: AddDataset {
            name: None,
            display_name: "Land Cover".to_string(),
            description: "Land Cover derived from MODIS/Terra+Aqua Land Cover".to_string(),
            source_operator: "GdalSource".to_string(),
            symbology: Some(Symbology::Raster(RasterSymbology {
                opacity: 1.0,
                colorizer: Colorizer::palette(
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
                ).unwrap().into(),
            })),
            provenance: Some(vec![Provenance {
                citation: "Friedl, M., D. Sulla-Menashe. MCD12C1 MODIS/Terra+Aqua Land Cover Type Yearly L3 Global 0.05Deg CMG V006. 2015, distributed by NASA EOSDIS Land Processes DAAC, https://doi.org/10.5067/MODIS/MCD12C1.006. Accessed 2022-03-16.".to_owned(),
                license: "All data distributed by the LP DAAC contain no restrictions on the data reuse. (https://lpdaac.usgs.gov/resources/faqs/#am-i-allowed-to-reuse-lp-daac-data)".to_owned(),
                uri: "https://doi.org/10.5067/MODIS/MCD12C1.006".to_owned(),
            }]),
        },
        meta_data: MetaDataDefinition::GdalStatic(GdalMetaDataStatic {
            time: Some(geoengine_datatypes::primitives::TimeInterval::default().into()),
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
            },
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReferenceOption::SpatialReference(SpatialReference::epsg_4326()).into(),
                measurement: geoengine_datatypes::primitives::Measurement::classification("Land Cover".to_string(), 
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
                ].into()).into(),
                time: Some(geoengine_datatypes::primitives::TimeInterval::default().into()),
                bbox: Some(geoengine_datatypes::primitives::SpatialPartition2D::new((-180., 90.).into(),
                     (180., -90.).into()).unwrap()
                .into()),
                resolution: Some(SpatialResolution {
                    x: 0.1, y: 0.1,
                }),
            },
            cache_ttl: CacheTtlSeconds::default(),
        }.into()),
    };

    db.add_dataset(ndvi.properties, db.wrap_meta_data(ndvi.meta_data))
        .await
        .expect("dataset db access")
        .id
        .into()
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

pub async fn send_test_request<C: SimpleApplicationContext>(
    req: test::TestRequest,
    app_ctx: C,
) -> ServiceResponse {
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(app_ctx))
            .wrap(
                middleware::ErrorHandlers::default()
                    .handler(http::StatusCode::NOT_FOUND, render_404)
                    .handler(http::StatusCode::METHOD_NOT_ALLOWED, render_405),
            )
            .configure(configure_extractors)
            .configure(handlers::datasets::init_dataset_routes::<C>)
            .configure(handlers::layers::init_layer_routes::<C>)
            .configure(handlers::plots::init_plot_routes::<C>)
            .configure(handlers::projects::init_project_routes::<C>)
            .configure(handlers::session::init_session_routes::<C>)
            .configure(handlers::spatial_references::init_spatial_reference_routes::<C>)
            .configure(handlers::upload::init_upload_routes::<C>)
            .configure(handlers::tasks::init_task_routes::<C>)
            .configure(handlers::wcs::init_wcs_routes::<C>)
            .configure(handlers::wfs::init_wfs_routes::<C>)
            .configure(handlers::wms::init_wms_routes::<C>)
            .configure(handlers::workflows::init_workflow_routes::<C>),
    )
    .await;
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
pub(crate) async fn setup_db() -> (OwnedSemaphorePermit, tokio_postgres::Config, String) {
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

    let mut pg_config = tokio_postgres::Config::new();
    pg_config
        .user(&db_config.user)
        .password(&db_config.password)
        .host(&db_config.host)
        .dbname(&db_config.database);

    // generate schema with prior connection
    PostgresConnectionManager::new(pg_config.clone(), NoTls)
        .connect()
        .await
        .unwrap()
        .batch_execute(&format!("CREATE SCHEMA {};", &db_config.schema))
        .await
        .unwrap();

    // fix schema by providing `search_path` option
    pg_config.options(&format!("-c search_path={}", db_config.schema));

    (permit, pg_config, db_config.schema)
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

/// Execute a test function with a temporary database schema. It will be cleaned up afterwards.
///
/// # Panics
///
/// Panics if the `PostgresContext` could not be created.
///
pub async fn with_temp_context<F, Fut, R>(f: F) -> R
where
    F: FnOnce(PostgresContext<NoTls>, tokio_postgres::Config) -> Fut
        + std::panic::UnwindSafe
        + Send
        + 'static,
    Fut: Future<Output = R>,
{
    with_temp_context_from_spec(TestDefault::test_default(), TestDefault::test_default(), f).await
}

/// Execute a test function with a temporary database schema. It will be cleaned up afterwards.
///
/// # Panics
///
/// Panics if the `PostgresContext` could not be created.
///
pub async fn with_temp_context_from_spec<F, Fut, R>(
    tiling_spec: TilingSpecification,
    query_ctx_chunk_size: ChunkByteSize,
    f: F,
) -> R
where
    F: FnOnce(PostgresContext<NoTls>, tokio_postgres::Config) -> Fut
        + std::panic::UnwindSafe
        + Send
        + 'static,
    Fut: Future<Output = R>,
{
    let (_permit, pg_config, schema) = setup_db().await;

    // catch all panics and clean up first…
    let executed_fn = {
        let pg_config = pg_config.clone();
        std::panic::catch_unwind(move || {
            tokio::task::block_in_place(move || {
                Handle::current().block_on(async move {
                    let ctx = PostgresContext::new_with_context_spec(
                        pg_config.clone(),
                        tokio_postgres::NoTls,
                        tiling_spec,
                        query_ctx_chunk_size,
                    )
                    .await
                    .unwrap();
                    f(ctx, pg_config.clone()).await
                })
            })
        })
    };

    tear_down_db(pg_config, &schema).await;

    // then throw errors afterwards
    match executed_fn {
        Ok(res) => res,
        Err(err) => std::panic::resume_unwind(err),
    }
}
