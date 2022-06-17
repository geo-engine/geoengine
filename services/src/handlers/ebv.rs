//! GEO BON EBV Portal catalog lookup service
//!
//! Connects to <https://portal.geobon.org/api/v1/>.

use super::tasks::TaskResponse;
use crate::contexts::AdminSession;
use crate::datasets::external::netcdfcf::{
    NetCdfOverview, OverviewGeneration, NETCDF_CF_PROVIDER_ID,
};
use crate::datasets::listing::ExternalDatasetProvider;
use crate::datasets::storage::DatasetProviderDb;
use crate::error::Result;
use crate::tasks::{Task, TaskContext, TaskManager, TaskStatusInfo};
use crate::{contexts::Context, datasets::external::netcdfcf::NetCdfCfDataProvider};
use actix_web::{
    web::{self, ServiceConfig},
    FromRequest, Responder,
};
use geoengine_datatypes::dataset::DatasetProviderId;
use geoengine_datatypes::error::{BoxedResultExt, ErrorSource};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::path::PathBuf;
use std::sync::Arc;
use url::Url;

/// Initialize ebv routes
///
/// # Panics
/// This route initializer panics if `base_url` is `None` and the `ebv` config is not defined.
///
pub(crate) fn init_ebv_routes<C>(base_url: Option<Url>) -> Box<dyn FnOnce(&mut ServiceConfig)>
where
    C: Context,
    C::Session: FromRequest,
{
    Box::new(move |cfg: &mut web::ServiceConfig| {
        cfg.app_data(web::Data::new(BaseUrl(base_url.unwrap_or_else(|| {
            let ebv_config = crate::util::config::get_config_element::<crate::util::config::Ebv>()
                .expect("ebv config must exist for this route");
            ebv_config.api_base_url
        }))))
        .service(web::resource("/classes").route(web::get().to(get_classes::<C>)))
        .service(web::resource("/datasets/{ebv_name}").route(web::get().to(get_ebv_datasets::<C>)))
        .service(web::resource("/dataset/{id}").route(web::get().to(get_ebv_dataset::<C>)))
        .service(
            web::resource("/dataset/{id}/subdatasets")
                .route(web::get().to(get_ebv_subdatasets::<C>)),
        )
        .service(web::resource("/create_overviews").route(web::post().to(create_overviews::<C>)))
        .service(web::resource("/create_overview").route(web::post().to(create_overview::<C>)));
    })
}

struct BaseUrl(Url);

impl AsRef<Url> for BaseUrl {
    fn as_ref(&self) -> &Url {
        &self.0
    }
}

mod portal_responses {
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    pub struct EbvClassesResponse {
        pub data: Vec<EbvClassesResponseClass>,
    }

    #[derive(Debug, Deserialize)]
    pub struct EbvClassesResponseClass {
        pub ebv_class: String,
        pub ebv_name: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    pub struct EbvDatasetsResponse {
        pub data: Vec<EbvDatasetsResponseData>,
    }

    #[derive(Debug, Deserialize)]
    pub struct EbvDatasetsResponseData {
        pub id: String,
        pub title: String,
        pub summary: String,
        pub creator: EbvDatasetsResponseCreator,
        pub license: String,
        pub dataset: EbvDatasetsResponseDataset,
        pub ebv: EbvDatasetsResponseEbv,
    }

    #[derive(Debug, Deserialize)]
    pub struct EbvDatasetsResponseCreator {
        pub creator_name: String,
        pub creator_institution: String,
    }

    #[derive(Debug, Deserialize)]
    pub struct EbvDatasetsResponseDataset {
        pub pathname: String,
    }

    #[derive(Debug, Deserialize)]
    pub struct EbvDatasetsResponseEbv {
        pub ebv_class: String,
        pub ebv_name: String,
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(module(error), context(suffix(false)))] // disables default `Snafu` suffix
pub enum EbvError {
    #[snafu(display("Cannot parse NetCDF file metadata: {source}"))]
    CannotParseNetCdfFile { source: Box<dyn ErrorSource> },
    #[snafu(display("Cannot lookup dataset with id {id}"))]
    CannotLookupDataset { id: usize },
    #[snafu(display("Cannot find NetCdfCf provider with id {id}"))]
    NoNetCdfCfProviderForId { id: DatasetProviderId },
    #[snafu(display("NetCdfCf provider with id {id} cannot list files"))]
    CdfCfProviderCannotListFiles { id: DatasetProviderId },
    #[snafu(display("Internal server error"))]
    Internal { source: Box<dyn ErrorSource> },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EbvClass {
    name: String,
    ebv_names: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EbvClasses {
    classes: Vec<EbvClass>,
}

async fn get_classes<C: Context>(
    _params: web::Query<()>,
    base_url: web::Data<BaseUrl>,
    _session: C::Session,
    _ctx: web::Data<C>,
) -> Result<impl Responder> {
    let base_url = base_url.get_ref().as_ref();
    let url = format!("{base_url}/ebv");

    debug!("Calling {url}");

    let response = reqwest::get(url)
        .await?
        .json::<portal_responses::EbvClassesResponse>()
        .await?;

    let classes: Vec<EbvClass> = response
        .data
        .into_iter()
        .map(|c| EbvClass {
            name: c.ebv_class,
            ebv_names: c.ebv_name,
        })
        .collect();

    Ok(web::Json(classes))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EbvDataset {
    id: String,
    name: String,
    author_name: String,
    author_institution: String,
    description: String,
    license: String,
    dataset_path: String,
    ebv_class: String,
    ebv_name: String,
}

async fn get_ebv_datasets<C: Context>(
    ebv_name: web::Path<String>,
    _params: web::Query<()>,
    base_url: web::Data<BaseUrl>,
    _session: C::Session,
    _ctx: web::Data<C>,
) -> Result<impl Responder> {
    let base_url = base_url.get_ref().as_ref();
    let url = format!("{base_url}/datasets/filter");

    debug!("Calling {url}");

    let response = reqwest::Client::new()
        .get(url)
        .query(&[("ebvName", &ebv_name.into_inner())])
        .send()
        .await?
        .json::<portal_responses::EbvDatasetsResponse>()
        .await?;

    let datasets: Vec<EbvDataset> = response
        .data
        .into_iter()
        .map(|data| EbvDataset {
            id: data.id,
            name: data.title,
            author_name: data.creator.creator_name,
            author_institution: data.creator.creator_institution,
            description: data.summary,
            license: data.license,
            dataset_path: data.dataset.pathname,
            ebv_class: data.ebv.ebv_class,
            ebv_name: data.ebv.ebv_name,
        })
        .collect();

    Ok(web::Json(datasets))
}

async fn get_ebv_dataset<C: Context>(
    id: web::Path<usize>,
    _params: web::Query<()>,
    base_url: web::Data<BaseUrl>,
    _session: C::Session,
    _ctx: web::Data<C>,
) -> Result<impl Responder> {
    let dataset = get_dataset_metadata(base_url.get_ref(), id.into_inner()).await?;

    Ok(web::Json(dataset))
}

async fn get_dataset_metadata(base_url: &BaseUrl, id: usize) -> Result<EbvDataset> {
    let base_url = base_url.as_ref();
    let url = format!("{base_url}/datasets/{id}");

    debug!("Calling {url}");

    let response = reqwest::get(url)
        .await?
        .json::<portal_responses::EbvDatasetsResponse>()
        .await?;

    let dataset: Option<EbvDataset> = response
        .data
        .into_iter()
        .map(|data| EbvDataset {
            id: data.id,
            name: data.title,
            author_name: data.creator.creator_name,
            author_institution: data.creator.creator_institution,
            description: data.summary,
            license: data.license,
            dataset_path: data.dataset.pathname,
            ebv_class: data.ebv.ebv_class,
            ebv_name: data.ebv.ebv_name,
        })
        .next();

    match dataset {
        Some(dataset) => Ok(dataset),
        None => Err(EbvError::CannotLookupDataset { id }.into()),
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EbvHierarchy {
    provider_id: DatasetProviderId,
    tree: NetCdfOverview,
}

async fn get_ebv_subdatasets<C: Context>(
    id: web::Path<usize>,
    _params: web::Query<()>,
    base_url: web::Data<BaseUrl>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let dataset = get_dataset_metadata(base_url.get_ref(), id.into_inner()).await?;

    let listing = {
        let dataset_path = PathBuf::from(dataset.dataset_path.trim_start_matches('/'));

        debug!("Accessing dataset {}", dataset_path.display());

        let provider_paths = netcdfcf_provider_path(ctx.as_ref(), &session).await?;

        crate::util::spawn_blocking(move || {
            NetCdfCfDataProvider::build_netcdf_tree(
                &provider_paths.provider_path,
                Some(&provider_paths.overview_path),
                &dataset_path,
            )
        })
        .await?
        .map_err(|e| Box::new(e) as _)
        .context(error::CannotParseNetCdfFile)?
    };

    Ok(web::Json(EbvHierarchy {
        provider_id: NETCDF_CF_PROVIDER_ID,
        tree: listing,
    }))
}

struct NetCdfCfDataProviderPaths {
    pub provider_path: PathBuf,
    pub overview_path: PathBuf,
}

async fn netcdfcf_provider_path<C: Context>(
    ctx: &C,
    session: &C::Session,
) -> Result<NetCdfCfDataProviderPaths, EbvError> {
    with_netcdfcf_provider(ctx, session, |concrete_provider| {
        Ok(NetCdfCfDataProviderPaths {
            provider_path: concrete_provider.path.clone(),
            overview_path: concrete_provider.overviews.clone(),
        })
    })
    .await
}

async fn with_netcdfcf_provider<C: Context, T, F>(
    ctx: &C,
    session: &C::Session,
    f: F,
) -> Result<T, EbvError>
where
    T: Send + 'static,
    F: FnOnce(&NetCdfCfDataProvider) -> Result<T, EbvError> + Send + 'static,
{
    let provider: Box<dyn ExternalDatasetProvider> = ctx
        .dataset_db_ref()
        .dataset_provider(session, NETCDF_CF_PROVIDER_ID)
        .await
        .map_err(|_| EbvError::NoNetCdfCfProviderForId {
            id: NETCDF_CF_PROVIDER_ID,
        })?;

    crate::util::spawn_blocking(move || {
        if let Some(concrete_provider) = provider.as_any().downcast_ref::<NetCdfCfDataProvider>() {
            f(concrete_provider)
        } else {
            Err(EbvError::NoNetCdfCfProviderForId {
                id: NETCDF_CF_PROVIDER_ID,
            })
        }
    })
    .await
    .boxed_context(error::Internal)?
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NetCdfCfOverviewResponse {
    success: Vec<PathBuf>,
    skip: Vec<PathBuf>,
    error: Vec<PathBuf>,
}

/// Create overviews for a all `NetCDF` files of the provider
async fn create_overviews<C: Context>(
    session: AdminSession,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let ctx = ctx.into_inner();

    let task: Box<dyn Task<C::TaskContext>> = EvbMultiOverviewTask::<C> {
        session,
        ctx: ctx.clone(),
    }
    .boxed();

    let task_id = ctx.tasks_ref().register(task).await?;

    Ok(web::Json(TaskResponse::new(task_id)))
}

struct EvbMultiOverviewTask<C: Context> {
    session: AdminSession,
    ctx: Arc<C>,
}

impl<C: Context> EvbMultiOverviewTask<C> {
    fn update_pct(task_ctx: Arc<C::TaskContext>, pct: u8, status: NetCdfCfOverviewResponse) {
        crate::util::spawn(async move {
            task_ctx.set_completion(pct, status.boxed()).await;
        });
    }
}

#[async_trait::async_trait]
impl<C: Context> Task<C::TaskContext> for EvbMultiOverviewTask<C> {
    async fn run(
        self: Box<Self>,
        task_ctx: C::TaskContext,
    ) -> Result<Box<dyn crate::tasks::TaskStatusInfo>, Box<dyn ErrorSource>> {
        let task_ctx = Arc::new(task_ctx);

        let response =
            with_netcdfcf_provider(self.ctx.as_ref(), &self.session.into(), move |provider| {
                let mut status = NetCdfCfOverviewResponse {
                    success: vec![],
                    skip: vec![],
                    error: vec![],
                };

                let files =
                    provider
                        .list_files()
                        .map_err(|_| EbvError::CdfCfProviderCannotListFiles {
                            id: NETCDF_CF_PROVIDER_ID,
                        })?;

                let num_files = files.len();

                for (i, file) in files.into_iter().enumerate() {
                    // TODO: provide some more detailed pct status

                    match provider.create_overviews(&file) {
                        Ok(OverviewGeneration::Created) => status.success.push(file),
                        Ok(OverviewGeneration::Skipped) => status.skip.push(file),
                        Err(e) => {
                            warn!("Failed to create overviews for {}: {e}", file.display());
                            status.error.push(file);
                        }
                    }

                    Self::update_pct(
                        task_ctx.clone(),
                        ((i + 1) / num_files) as u8,
                        status.clone(),
                    );
                }

                Result::<_, EbvError>::Ok(status.boxed())
            })
            .await;

        response.map_err(ErrorSource::boxed)
    }
}

#[derive(Debug, Deserialize)]
struct CreateOverviewParams {
    file: PathBuf,
}

/// Create overviews for a single `NetCDF` file
async fn create_overview<C: Context>(
    session: AdminSession,
    ctx: web::Data<C>,
    params: web::Json<CreateOverviewParams>,
) -> Result<impl Responder> {
    let ctx = ctx.into_inner();

    let task: Box<dyn Task<C::TaskContext>> = EvbOverviewTask::<C> {
        session,
        ctx: ctx.clone(),
        params: params.into_inner(),
    }
    .boxed();

    let task_id = ctx.tasks_ref().register(task).await?;

    Ok(web::Json(TaskResponse::new(task_id)))
}

struct EvbOverviewTask<C: Context> {
    session: AdminSession,
    ctx: Arc<C>,
    params: CreateOverviewParams,
}

#[async_trait::async_trait]
impl<C: Context> Task<C::TaskContext> for EvbOverviewTask<C> {
    async fn run(
        self: Box<Self>,
        _ctx: C::TaskContext,
    ) -> Result<Box<dyn crate::tasks::TaskStatusInfo>, Box<dyn ErrorSource>> {
        let file = self.params.file;

        let response =
            with_netcdfcf_provider(self.ctx.as_ref(), &self.session.into(), move |provider| {
                // TODO: provide some detailed pct status

                Ok(match provider.create_overviews(&file) {
                    Ok(OverviewGeneration::Created) => NetCdfCfOverviewResponse {
                        success: vec![file],
                        skip: vec![],
                        error: vec![],
                    },
                    Ok(OverviewGeneration::Skipped) => NetCdfCfOverviewResponse {
                        success: vec![],
                        skip: vec![file],
                        error: vec![],
                    },
                    Err(e) => {
                        warn!("Failed to create overviews for {}: {e}", file.display());
                        NetCdfCfOverviewResponse {
                            success: vec![],
                            skip: vec![],
                            error: vec![file],
                        }
                    }
                })
            })
            .await;

        response
            .map(TaskStatusInfo::boxed)
            .map_err(ErrorSource::boxed)
    }
}

impl TaskStatusInfo for NetCdfCfOverviewResponse {}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        contexts::{InMemoryContext, Session, SimpleContext},
        datasets::external::netcdfcf::NetCdfCfDataProviderDefinition,
        server::{configure_extractors, render_404, render_405},
        util::tests::read_body_string,
    };
    use actix_web::{dev::ServiceResponse, http, http::header, middleware, test, web, App};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::{test_data, util::test::TestDefault};
    use httptest::{matchers::request, responders::status_code, Expectation};
    use serde_json::json;

    async fn send_test_request<C: SimpleContext>(
        req: test::TestRequest,
        ctx: C,
        mock_address: String,
    ) -> ServiceResponse {
        let mock_address = mock_address.parse().unwrap();

        let app = test::init_service({
            let app = App::new()
                .app_data(web::Data::new(ctx))
                .wrap(
                    middleware::ErrorHandlers::default()
                        .handler(http::StatusCode::NOT_FOUND, render_404)
                        .handler(http::StatusCode::METHOD_NOT_ALLOWED, render_405),
                )
                .wrap(middleware::NormalizePath::trim())
                .configure(configure_extractors)
                .service(web::scope("/ebv").configure(init_ebv_routes::<C>(Some(mock_address))));

            app
        })
        .await;
        test::call_service(&app, req.to_request())
            .await
            .map_into_boxed_body()
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_get_subdatasets() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        ctx.dataset_db_ref()
            .add_dataset_provider(
                &*ctx.default_session_ref().await,
                Box::new(NetCdfCfDataProviderDefinition {
                    name: "test".to_string(),
                    path: test_data!("netcdf4d").to_path_buf(),
                    overviews: test_data!("netcdf4d/overviews").to_path_buf(),
                }),
            )
            .await
            .unwrap();

        let mock_server = httptest::Server::run();
        mock_server.expect(
            Expectation::matching(request::method_path("GET", "/api/v1/datasets/5"))
                .respond_with(
                    status_code(200)
                        .append_header("Content-Type", "application/json")
                        .body(r#"{
                            "code": 200,
                            "message": "List of dataset(s).",
                            "data": [
                                {
                                    "id": "5",
                                    "naming_authority": "The German Centre for Integrative Biodiversity Research (iDiv) Halle-Jena-Leipzig",
                                    "title": "Global habitat availability for mammals from 2015-2055",
                                    "date_created": "2020-01-01",
                                    "summary": "Global habitat availability for 5,090 mammals in 5 year intervals (subset from 2015 to 2055).",
                                    "references": [
                                        "10.2139\/ssrn.3451453",
                                        "10.1016\/j.oneear.2020.05.015"
                                    ],
                                    "source": "More info here: https:\/\/doi.org\/10.1016\/j.oneear.2020.05.015",
                                    "coverage_content_type": "modelResult",
                                    "processing_level": "N\/A",
                                    "project": "Global Mammal Assessment (GMA)",
                                    "project_url": [
                                        "https:\/\/globalmammal.org"
                                    ],
                                    "creator": {
                                        "creator_name": "Daniele Baisero",
                                        "creator_email": "daniele.baisero@gmail.com",
                                        "creator_institution": "Department of Biology and Biotechnology, Sapienza University of Rome",
                                        "creator_country": "Italy"
                                    },
                                    "contributor_name": "N\/A",
                                    "license": "https:\/\/creativecommons.org\/licenses\/by\/4.0",
                                    "publisher": {
                                        "publisher_name": "Daniele Baisero",
                                        "publisher_email": "daniele.baisero@gmail.com",
                                        "publisher_institution": "Department of Biology and Biotechnology, Sapienza University of Rome",
                                        "publisher_country": "Italy"
                                    },
                                    "ebv": {
                                        "ebv_class": "Species populations",
                                        "ebv_name": "Species distributions"
                                    },
                                    "ebv_entity": {
                                        "ebv_entity_type": "Species",
                                        "ebv_entity_scope": "Mammals",
                                        "ebv_entity_classification_name": "N\/A",
                                        "ebv_entity_classification_url": "N\/A"
                                    },
                                    "ebv_metric": {
                                        "ebv_metric_1": {
                                            ":standard_name": "Habitat availability",
                                            ":long_name": "Land-use of 5,090 mammals calculated in km2",
                                            ":units": "km2"
                                        }
                                    },
                                    "ebv_scenario": {
                                        "ebv_scenario_classification_name": "Shared Socioeconomic Pathways (SSPs) \/ Representative Concentration Pathway (RCPs)",
                                        "ebv_scenario_classification_version": "N\/A",
                                        "ebv_scenario_classification_url": "N\/A",
                                        "ebv_scenario_1": {
                                            ":standard_name": "Sustainability",
                                            ":long_name": "SSP1-RCP2.6"
                                        },
                                        "ebv_scenario_2": {
                                            ":standard_name": "Middle of the Road ",
                                            ":long_name": "SSP2-RCP4.5"
                                        },
                                        "ebv_scenario_3": {
                                            ":standard_name": "Regional Rivalry",
                                            ":long_name": "SSP3-RCP6.0"
                                        },
                                        "ebv_scenario_4": {
                                            ":standard_name": "Inequality",
                                            ":long_name": "SSP4-RCP6.0"
                                        },
                                        "ebv_scenario_5": {
                                            ":standard_name": "Fossil-fueled Development",
                                            ":long_name": "SSP5-RCP8.5"
                                        }
                                    },
                                    "ebv_spatial": {
                                        "ebv_spatial_scope": "Global",
                                        "ebv_spatial_description": "N\/A",
                                        "ebv_spatial_resolution": null
                                    },
                                    "geospatial_lat_units": "degrees_north",
                                    "geospatial_lon_units": "degrees_east",
                                    "time_coverage": {
                                        "time_coverage_resolution": "Every 5 years",
                                        "time_coverage_start": "2015-01-01",
                                        "time_coverage_end": "2055-01-01"
                                    },
                                    "ebv_domain": "Terrestrial",
                                    "comment": "N\/A",
                                    "dataset": {
                                        "pathname": "dataset_sm.nc",
                                        "download": "portal.geobon.org\/data\/upload\/5\/public\/v1_rodinini_001.nc",
                                        "metadata_json": "portal.geobon.org\/data\/upload\/5\/public\/v1_metadata.js",
                                        "metadata_xml": "portal.geobon.org\/data\/upload\/5\/public\/v1_metadata.xml"
                                    },
                                    "file": {
                                        "download": "portal.geobon.org\/img\/5\/49630_insights.png"
                                    }
                                }
                            ]
                        }"#),
                ),
        );

        let req = actix_web::test::TestRequest::get()
            .uri("/ebv/dataset/5/subdatasets")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx, mock_server.url_str("/api/v1")).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        assert_eq!(
            read_body_string(res).await,
            json!({
                "providerId": "1690c483-b17f-4d98-95c8-00a64849cd0b",
                "tree": {
                    "fileName": "dataset_sm.nc",
                    "title": "Test dataset metric and scenario",
                    "summary": "Fake description of test dataset with metric and scenario.",
                    "spatialReference": "EPSG:3035",
                    "groups": [{
                            "name": "scenario_1",
                            "title": "Sustainability",
                            "description": "SSP1-RCP2.6",
                            "dataType": null,
                            "unit": "",
                            "groups": [{
                                    "name": "metric_1",
                                    "title": "Random metric 1",
                                    "description": "Randomly created data",
                                    "dataType": "I16",
                                    "unit": "",
                                    "groups": []
                                },
                                {
                                    "name": "metric_2",
                                    "title": "Random metric 2",
                                    "description": "Randomly created data",
                                    "dataType": "I16",
                                    "unit": "",
                                    "groups": []
                                }
                            ]
                        },
                        {
                            "name": "scenario_2",
                            "title": "Middle of the Road ",
                            "description": "SSP2-RCP4.5",
                            "dataType": null,
                            "unit": "",
                            "groups": [{
                                    "name": "metric_1",
                                    "title": "Random metric 1",
                                    "description": "Randomly created data",
                                    "dataType": "I16",
                                    "unit": "",
                                    "groups": []
                                },
                                {
                                    "name": "metric_2",
                                    "title": "Random metric 2",
                                    "description": "Randomly created data",
                                    "dataType": "I16",
                                    "unit": "",
                                    "groups": []
                                }
                            ]
                        },
                        {
                            "name": "scenario_3",
                            "title": "Regional Rivalry",
                            "description": "SSP3-RCP6.0",
                            "dataType": null,
                            "unit": "",
                            "groups": [{
                                    "name": "metric_1",
                                    "title": "Random metric 1",
                                    "description": "Randomly created data",
                                    "dataType": "I16",
                                    "unit": "",
                                    "groups": []
                                },
                                {
                                    "name": "metric_2",
                                    "title": "Random metric 2",
                                    "description": "Randomly created data",
                                    "dataType": "I16",
                                    "unit": "",
                                    "groups": []
                                }
                            ]
                        },
                        {
                            "name": "scenario_4",
                            "title": "Inequality",
                            "description": "SSP4-RCP6.0",
                            "dataType": null,
                            "unit": "",
                            "groups": [{
                                    "name": "metric_1",
                                    "title": "Random metric 1",
                                    "description": "Randomly created data",
                                    "dataType": "I16",
                                    "unit": "",
                                    "groups": []
                                },
                                {
                                    "name": "metric_2",
                                    "title": "Random metric 2",
                                    "description": "Randomly created data",
                                    "dataType": "I16",
                                    "unit": "",
                                    "groups": []
                                }
                            ]
                        },
                        {
                            "name": "scenario_5",
                            "title": "Fossil-fueled Development",
                            "description": "SSP5-RCP8.5",
                            "dataType": null,
                            "unit": "",
                            "groups": [{
                                    "name": "metric_1",
                                    "title": "Random metric 1",
                                    "description": "Randomly created data",
                                    "dataType": "I16",
                                    "unit": "",
                                    "groups": []
                                },
                                {
                                    "name": "metric_2",
                                    "title": "Random metric 2",
                                    "description": "Randomly created data",
                                    "dataType": "I16",
                                    "unit": "",
                                    "groups": []
                                }
                            ]
                        }
                    ],
                    "entities": [{
                            "id": 0,
                            "name": "entity01"
                        },
                        {
                            "id": 1,
                            "name": "entity02"
                        }
                    ],
                    "time": {
                        "start": 946_684_800_000_i64,
                        "end": 1_893_456_000_000_i64
                    },
                    "timeStep": {
                        "granularity": "years",
                        "step": 10
                    },
                    "colorizer": {
                        "type": "linearGradient",
                        "breakpoints": [
                            { "value": 0.0, "color": [68, 1, 84, 255] },
                            { "value": 50.0, "color": [33, 145, 140, 255] },
                            { "value": 100.0, "color": [253, 231, 37, 255] }
                        ],
                        "noDataColor": [0, 0, 0, 0],
                        "defaultColor": [0, 0, 0, 0]
                    }
                }
            })
            .to_string()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_get_classes() {
        let mock_server = httptest::Server::run();
        mock_server.expect(
            Expectation::matching(request::method_path("GET", "/api/v1/ebv")).respond_with(
                status_code(200)
                    .append_header("Content-Type", "application/json")
                    .body(
                        r#"{
                    "code": 200,
                    "message": "List of all EBV classes and names.",
                    "data": [
                        {
                            "ebv_class": "Genetic composition",
                            "ebv_name": [
                                "Intraspecific genetic diversity",
                                "Genetic differentiation",
                                "Effective population size",
                                "Inbreeding"
                            ]
                        },
                        {
                            "ebv_class": "Species populations",
                            "ebv_name": [
                                "Species distributions",
                                "Species abundances"
                            ]
                        },
                        {
                            "ebv_class": "Species traits",
                            "ebv_name": [
                                "Morphology",
                                "Physiology",
                                "Phenology",
                                "Movement"
                            ]
                        },
                        {
                            "ebv_class": "Community composition",
                            "ebv_name": [
                                "Community abundance",
                                "Taxonomic and phylogenetic diversity",
                                "Trait diversity",
                                "Interaction diversity"
                            ]
                        },
                        {
                            "ebv_class": "Ecosystem functioning",
                            "ebv_name": [
                                "Primary productivity",
                                "Ecosystem phenology",
                                "Ecosystem disturbances"
                            ]
                        },
                        {
                            "ebv_class": "Ecosystem structure",
                            "ebv_name": [
                                "Live cover fraction",
                                "Ecosystem distribution",
                                "Ecosystem Vertical Profile"
                            ]
                        },
                        {
                            "ebv_class": "Ecosystem services",
                            "ebv_name": [
                                "Pollination"
                            ]
                        }
                    ]
                }"#,
                    ),
            ),
        );

        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let req = actix_web::test::TestRequest::get()
            .uri("/ebv/classes")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx, mock_server.url_str("/api/v1")).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        assert_eq!(
            read_body_string(res).await,
            json!([{
                    "name": "Genetic composition",
                    "ebvNames": [
                        "Intraspecific genetic diversity",
                        "Genetic differentiation",
                        "Effective population size",
                        "Inbreeding"
                    ]
                },
                {
                    "name": "Species populations",
                    "ebvNames": [
                        "Species distributions",
                        "Species abundances"
                    ]
                },
                {
                    "name": "Species traits",
                    "ebvNames": [
                        "Morphology",
                        "Physiology",
                        "Phenology",
                        "Movement"
                    ]
                },
                {
                    "name": "Community composition",
                    "ebvNames": [
                        "Community abundance",
                        "Taxonomic and phylogenetic diversity",
                        "Trait diversity",
                        "Interaction diversity"
                    ]
                },
                {
                    "name": "Ecosystem functioning",
                    "ebvNames": [
                        "Primary productivity",
                        "Ecosystem phenology",
                        "Ecosystem disturbances"
                    ]
                },
                {
                    "name": "Ecosystem structure",
                    "ebvNames": [
                        "Live cover fraction",
                        "Ecosystem distribution",
                        "Ecosystem Vertical Profile"
                    ]
                },
                {
                    "name": "Ecosystem services",
                    "ebvNames": [
                        "Pollination"
                    ]
                }
            ])
            .to_string()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_get_ebv_datasets() {
        let mock_server = httptest::Server::run();
        mock_server.expect(
            Expectation::matching(request::method_path("GET", "/api/v1/datasets/filter"))
                .respond_with(
                    status_code(200)
                        .append_header("Content-Type", "application/json")
                        .body(r#"{
                            "code": 200,
                            "message": "List of dataset(s).",
                            "data": [
                                {
                                    "id": "5",
                                    "naming_authority": "The German Centre for Integrative Biodiversity Research (iDiv) Halle-Jena-Leipzig",
                                    "title": "Global habitat availability for mammals from 2015-2055",
                                    "date_created": "2020-01-01",
                                    "summary": "Global habitat availability for 5,090 mammals in 5 year intervals (subset from 2015 to 2055).",
                                    "references": [
                                        "10.2139\/ssrn.3451453",
                                        "10.1016\/j.oneear.2020.05.015"
                                    ],
                                    "source": "More info here: https:\/\/doi.org\/10.1016\/j.oneear.2020.05.015",
                                    "coverage_content_type": "modelResult",
                                    "processing_level": "N\/A",
                                    "project": "Global Mammal Assessment (GMA)",
                                    "project_url": [
                                        "https:\/\/globalmammal.org"
                                    ],
                                    "creator": {
                                        "creator_name": "Daniele Baisero",
                                        "creator_email": "daniele.baisero@gmail.com",
                                        "creator_institution": "Department of Biology and Biotechnology, Sapienza University of Rome",
                                        "creator_country": "Italy"
                                    },
                                    "contributor_name": "N\/A",
                                    "license": "https:\/\/creativecommons.org\/licenses\/by\/4.0",
                                    "publisher": {
                                        "publisher_name": "Daniele Baisero",
                                        "publisher_email": "daniele.baisero@gmail.com",
                                        "publisher_institution": "Department of Biology and Biotechnology, Sapienza University of Rome",
                                        "publisher_country": "Italy"
                                    },
                                    "ebv": {
                                        "ebv_class": "Species populations",
                                        "ebv_name": "Species distributions"
                                    },
                                    "ebv_entity": {
                                        "ebv_entity_type": "Species",
                                        "ebv_entity_scope": "Mammals",
                                        "ebv_entity_classification_name": "N\/A",
                                        "ebv_entity_classification_url": "N\/A"
                                    },
                                    "ebv_metric": {
                                        "ebv_metric_1": {
                                            ":standard_name": "Habitat availability",
                                            ":long_name": "Land-use of 5,090 mammals calculated in km2",
                                            ":units": "km2"
                                        }
                                    },
                                    "ebv_scenario": {
                                        "ebv_scenario_classification_name": "Shared Socioeconomic Pathways (SSPs) \/ Representative Concentration Pathway (RCPs)",
                                        "ebv_scenario_classification_version": "N\/A",
                                        "ebv_scenario_classification_url": "N\/A",
                                        "ebv_scenario_1": {
                                            ":standard_name": "Sustainability",
                                            ":long_name": "SSP1-RCP2.6"
                                        },
                                        "ebv_scenario_2": {
                                            ":standard_name": "Middle of the Road ",
                                            ":long_name": "SSP2-RCP4.5"
                                        },
                                        "ebv_scenario_3": {
                                            ":standard_name": "Regional Rivalry",
                                            ":long_name": "SSP3-RCP6.0"
                                        },
                                        "ebv_scenario_4": {
                                            ":standard_name": "Inequality",
                                            ":long_name": "SSP4-RCP6.0"
                                        },
                                        "ebv_scenario_5": {
                                            ":standard_name": "Fossil-fueled Development",
                                            ":long_name": "SSP5-RCP8.5"
                                        }
                                    },
                                    "ebv_spatial": {
                                        "ebv_spatial_scope": "Global",
                                        "ebv_spatial_description": "N\/A",
                                        "ebv_spatial_resolution": null
                                    },
                                    "geospatial_lat_units": "degrees_north",
                                    "geospatial_lon_units": "degrees_east",
                                    "time_coverage": {
                                        "time_coverage_resolution": "Every 5 years",
                                        "time_coverage_start": "2015-01-01",
                                        "time_coverage_end": "2055-01-01"
                                    },
                                    "ebv_domain": "Terrestrial",
                                    "comment": "N\/A",
                                    "dataset": {
                                        "pathname": "\/5\/public\/v1_rodinini_001.nc",
                                        "download": "portal.geobon.org\/data\/upload\/5\/public\/v1_rodinini_001.nc",
                                        "metadata_json": "portal.geobon.org\/data\/upload\/5\/public\/v1_metadata.js",
                                        "metadata_xml": "portal.geobon.org\/data\/upload\/5\/public\/v1_metadata.xml"
                                    },
                                    "file": {
                                        "download": "portal.geobon.org\/img\/5\/49630_insights.png"
                                    }
                                }
                            ]
                        }"#),
                ),
        );

        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let req = actix_web::test::TestRequest::get()
            .uri("/ebv/datasets/Species%20distributions")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx, mock_server.url_str("/api/v1")).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        assert_eq!(
            read_body_string(res).await,
            json!([{
                "id": "5",
                "name": "Global habitat availability for mammals from 2015-2055",
                "authorName": "Daniele Baisero",
                "authorInstitution": "Department of Biology and Biotechnology, Sapienza University of Rome",
                "description": "Global habitat availability for 5,090 mammals in 5 year intervals (subset from 2015 to 2055).",
                "license": "https://creativecommons.org/licenses/by/4.0",
                "datasetPath": "/5/public/v1_rodinini_001.nc",
                "ebvClass": "Species populations",
                "ebvName": "Species distributions"
            }])
            .to_string()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_get_ebv_dataset() {
        let mock_server = httptest::Server::run();
        mock_server.expect(
            Expectation::matching(request::method_path("GET", "/api/v1/datasets/5"))
                .respond_with(
                    status_code(200)
                        .append_header("Content-Type", "application/json")
                        .body(r#"{
                            "code": 200,
                            "message": "List of dataset(s).",
                            "data": [
                                {
                                    "id": "5",
                                    "naming_authority": "The German Centre for Integrative Biodiversity Research (iDiv) Halle-Jena-Leipzig",
                                    "title": "Global habitat availability for mammals from 2015-2055",
                                    "date_created": "2020-01-01",
                                    "summary": "Global habitat availability for 5,090 mammals in 5 year intervals (subset from 2015 to 2055).",
                                    "references": [
                                        "10.2139\/ssrn.3451453",
                                        "10.1016\/j.oneear.2020.05.015"
                                    ],
                                    "source": "More info here: https:\/\/doi.org\/10.1016\/j.oneear.2020.05.015",
                                    "coverage_content_type": "modelResult",
                                    "processing_level": "N\/A",
                                    "project": "Global Mammal Assessment (GMA)",
                                    "project_url": [
                                        "https:\/\/globalmammal.org"
                                    ],
                                    "creator": {
                                        "creator_name": "Daniele Baisero",
                                        "creator_email": "daniele.baisero@gmail.com",
                                        "creator_institution": "Department of Biology and Biotechnology, Sapienza University of Rome",
                                        "creator_country": "Italy"
                                    },
                                    "contributor_name": "N\/A",
                                    "license": "https:\/\/creativecommons.org\/licenses\/by\/4.0",
                                    "publisher": {
                                        "publisher_name": "Daniele Baisero",
                                        "publisher_email": "daniele.baisero@gmail.com",
                                        "publisher_institution": "Department of Biology and Biotechnology, Sapienza University of Rome",
                                        "publisher_country": "Italy"
                                    },
                                    "ebv": {
                                        "ebv_class": "Species populations",
                                        "ebv_name": "Species distributions"
                                    },
                                    "ebv_entity": {
                                        "ebv_entity_type": "Species",
                                        "ebv_entity_scope": "Mammals",
                                        "ebv_entity_classification_name": "N\/A",
                                        "ebv_entity_classification_url": "N\/A"
                                    },
                                    "ebv_metric": {
                                        "ebv_metric_1": {
                                            ":standard_name": "Habitat availability",
                                            ":long_name": "Land-use of 5,090 mammals calculated in km2",
                                            ":units": "km2"
                                        }
                                    },
                                    "ebv_scenario": {
                                        "ebv_scenario_classification_name": "Shared Socioeconomic Pathways (SSPs) \/ Representative Concentration Pathway (RCPs)",
                                        "ebv_scenario_classification_version": "N\/A",
                                        "ebv_scenario_classification_url": "N\/A",
                                        "ebv_scenario_1": {
                                            ":standard_name": "Sustainability",
                                            ":long_name": "SSP1-RCP2.6"
                                        },
                                        "ebv_scenario_2": {
                                            ":standard_name": "Middle of the Road ",
                                            ":long_name": "SSP2-RCP4.5"
                                        },
                                        "ebv_scenario_3": {
                                            ":standard_name": "Regional Rivalry",
                                            ":long_name": "SSP3-RCP6.0"
                                        },
                                        "ebv_scenario_4": {
                                            ":standard_name": "Inequality",
                                            ":long_name": "SSP4-RCP6.0"
                                        },
                                        "ebv_scenario_5": {
                                            ":standard_name": "Fossil-fueled Development",
                                            ":long_name": "SSP5-RCP8.5"
                                        }
                                    },
                                    "ebv_spatial": {
                                        "ebv_spatial_scope": "Global",
                                        "ebv_spatial_description": "N\/A",
                                        "ebv_spatial_resolution": null
                                    },
                                    "geospatial_lat_units": "degrees_north",
                                    "geospatial_lon_units": "degrees_east",
                                    "time_coverage": {
                                        "time_coverage_resolution": "Every 5 years",
                                        "time_coverage_start": "2015-01-01",
                                        "time_coverage_end": "2055-01-01"
                                    },
                                    "ebv_domain": "Terrestrial",
                                    "comment": "N\/A",
                                    "dataset": {
                                        "pathname": "\/5\/public\/v1_rodinini_001.nc",
                                        "download": "portal.geobon.org\/data\/upload\/5\/public\/v1_rodinini_001.nc",
                                        "metadata_json": "portal.geobon.org\/data\/upload\/5\/public\/v1_metadata.js",
                                        "metadata_xml": "portal.geobon.org\/data\/upload\/5\/public\/v1_metadata.xml"
                                    },
                                    "file": {
                                        "download": "portal.geobon.org\/img\/5\/49630_insights.png"
                                    }
                                }
                            ]
                        }"#),
                ),
        );

        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let req = actix_web::test::TestRequest::get()
            .uri("/ebv/dataset/5")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx, mock_server.url_str("/api/v1")).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        assert_eq!(
            read_body_string(res).await,
            json!({
                "id": "5",
                "name": "Global habitat availability for mammals from 2015-2055",
                "authorName": "Daniele Baisero",
                "authorInstitution": "Department of Biology and Biotechnology, Sapienza University of Rome",
                "description": "Global habitat availability for 5,090 mammals in 5 year intervals (subset from 2015 to 2055).",
                "license": "https://creativecommons.org/licenses/by/4.0",
                "datasetPath": "/5/public/v1_rodinini_001.nc",
                "ebvClass": "Species populations",
                "ebvName": "Species distributions"
            })
            .to_string()
        );
    }
}
