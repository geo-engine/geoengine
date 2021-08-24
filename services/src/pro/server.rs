use crate::error;
use crate::error::{Error, Result};
use crate::handlers;
use crate::pro;
#[cfg(feature = "postgres")]
use crate::pro::contexts::PostgresContext;
use crate::pro::contexts::{ProContext, ProInMemoryContext};
use crate::util::config::{self, get_config_element, Backend};

use actix_files::Files;
use actix_web::{web, App, HttpServer};
use actix_web_httpauth::middleware::HttpAuthentication;
#[cfg(feature = "postgres")]
use bb8_postgres::tokio_postgres::NoTls;
use log::info;
use snafu::ResultExt;
use std::net::SocketAddr;
use std::path::PathBuf;

use super::projects::ProProjectDb;
use crate::handlers::validate_token;
use crate::server::{configure_extractors, show_version_handler};

async fn start<C>(
    static_files_dir: Option<PathBuf>,
    bind_address: SocketAddr,
    ctx: C,
) -> Result<(), Error>
where
    C: ProContext,
    C::ProjectDB: ProProjectDb,
{
    let wrapped_ctx = web::Data::new(ctx);

    HttpServer::new(move || {
        let app = App::new()
            .app_data(wrapped_ctx.clone())
            .wrap(actix_web::middleware::Logger::default())
            .configure(configure_extractors)
            .configure(init_pro_routes::<C>);

        if let Some(static_files_dir) = static_files_dir.clone() {
            app.service(Files::new("/static", static_files_dir))
        } else {
            app
        }
    })
    .bind(bind_address)?
    .run()
    .await
    .map_err(Into::into)
}

/// Starts the webserver for the Geo Engine API.
///
/// # Panics
///  * may panic if the `Postgres` backend is chosen without compiling the `postgres` feature
///
///
pub async fn start_pro_server(static_files_dir: Option<PathBuf>) -> Result<()> {
    println!("|===========================================================================|");
    println!("| Welcome to Geo Engine Pro Version: Please refer to our license agreement. |");
    println!("| If you have any question: Visit https://www.geoengine.io.                 |");
    println!("|===========================================================================|");

    let web_config: config::Web = get_config_element()?;
    let bind_address = web_config
        .bind_address
        .parse::<SocketAddr>()
        .context(error::AddrParse)?;

    info!(
        "Starting server… {}",
        format!(
            "http://{}/",
            web_config
                .external_address
                .unwrap_or(web_config.bind_address)
        )
    );

    match web_config.backend {
        Backend::InMemory => {
            info!("Using in memory backend"); // TODO: log
            start(
                static_files_dir,
                bind_address,
                ProInMemoryContext::new_with_data().await,
            )
            .await
        }
        Backend::Postgres => {
            #[cfg(feature = "postgres")]
            {
                eprintln!("Using Postgres backend"); // TODO: log

                let db_config = config::get_config_element::<config::Postgres>()?;
                let mut pg_config = bb8_postgres::tokio_postgres::Config::new();
                pg_config
                    .user(&db_config.user)
                    .password(&db_config.password)
                    .host(&db_config.host)
                    .dbname(&db_config.database);

                let ctx = PostgresContext::new(pg_config, NoTls).await?;

                start(static_files_dir, bind_address, ctx).await
            }
            #[cfg(not(feature = "postgres"))]
            panic!("Postgres backend was selected but the postgres feature wasn't activated during compilation")
        }
    }
}

pub(crate) fn init_pro_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProContext,
    C::ProjectDB: ProProjectDb,
{
    //pro::handlers::drone_mapping::start_task_handler(ctx.clone()),
    //pro::handlers::drone_mapping::dataset_from_drone_mapping_handler(ctx.clone())
    cfg.route("/version", web::get().to(show_version_handler)) // TODO: allow disabling this function via config or feature flag
        .route("/wms", web::get().to(handlers::wms::wms_handler::<C>))
        .route("/wfs", web::get().to(handlers::wfs::wfs_handler::<C>))
        .route(
            "/wcs/{workflow}",
            web::get().to(handlers::wcs::wcs_handler::<C>),
        )
        .route(
            "/user",
            web::post().to(pro::handlers::users::register_user_handler::<C>),
        )
        .route(
            "/anonymous",
            web::post().to(pro::handlers::users::anonymous_handler::<C>),
        )
        .route(
            "/login",
            web::post().to(pro::handlers::users::login_handler::<C>),
        )
        .service(
            web::scope("")
                .wrap(HttpAuthentication::bearer(validate_token::<C>))
                .route(
                    "/logout",
                    web::post().to(pro::handlers::users::logout_handler::<C>),
                )
                .route(
                    "/workflow",
                    web::post().to(handlers::workflows::register_workflow_handler::<C>),
                )
                .route(
                    "/workflow/{id}",
                    web::get().to(handlers::workflows::load_workflow_handler::<C>),
                )
                .route(
                    "/workflow/{id}/metadata",
                    web::get().to(handlers::workflows::get_workflow_metadata_handler::<C>),
                )
                .route(
                    "/workflow/{id}/provenance",
                    web::get().to(handlers::workflows::get_workflow_provenance_handler::<C>),
                )
                .route(
                    "/session",
                    web::get().to(handlers::session::session_handler::<C>),
                )
                .route(
                    "/session/project/{project}",
                    web::post().to(pro::handlers::users::session_project_handler::<C>),
                )
                .route(
                    "/session/view",
                    web::post().to(pro::handlers::users::session_view_handler::<C>),
                )
                .route(
                    "/project",
                    web::post().to(handlers::projects::create_project_handler::<C>),
                )
                .route(
                    "/projects",
                    web::get().to(handlers::projects::list_projects_handler::<C>),
                )
                .route(
                    "/project/{project}",
                    web::patch().to(handlers::projects::update_project_handler::<C>),
                )
                .route(
                    "/project/{project}",
                    web::delete().to(handlers::projects::delete_project_handler::<C>),
                )
                .route(
                    "/project/{project}",
                    web::get().to(pro::handlers::projects::load_project_latest_handler::<C>),
                )
                .route(
                    "/project/{project}/{version}",
                    web::get().to(pro::handlers::projects::load_project_version_handler::<C>),
                )
                .route(
                    "/project/versions",
                    web::get().to(pro::handlers::projects::project_versions_handler::<C>),
                )
                .route(
                    "/project/permission/add",
                    web::post().to(pro::handlers::projects::add_permission_handler::<C>),
                )
                .route(
                    "/project/permission",
                    web::delete().to(pro::handlers::projects::remove_permission_handler::<C>),
                )
                .route(
                    "/project/{project}/permissions",
                    web::get().to(pro::handlers::projects::list_permissions_handler::<C>),
                )
                .route(
                    "/dataset/internal/{dataset}",
                    web::get().to(handlers::datasets::get_dataset_handler::<C>),
                )
                .route(
                    "/dataset/auto",
                    web::post().to(handlers::datasets::auto_create_dataset_handler::<C>),
                )
                .route(
                    "/dataset",
                    web::post().to(handlers::datasets::create_dataset_handler::<C>),
                )
                .route(
                    "/dataset/suggest",
                    web::get().to(handlers::datasets::suggest_meta_data_handler::<C>),
                )
                .route(
                    "/providers",
                    web::get().to(handlers::datasets::list_providers_handler::<C>),
                )
                .route(
                    "/datasets/external/{provider}",
                    web::get().to(handlers::datasets::list_external_datasets_handler::<C>),
                )
                .route(
                    "/datasets",
                    web::get().to(handlers::datasets::list_datasets_handler::<C>),
                )
                .route(
                    "/plot/{id}",
                    web::get().to(handlers::plots::get_plot_handler::<C>),
                )
                .route(
                    "/upload",
                    web::post().to(handlers::upload::upload_handler::<C>),
                )
                .route(
                    "/spatialReferenceSpecification/{srs_string}",
                    web::get().to(
                        handlers::spatial_references::get_spatial_reference_specification_handler::<
                            C,
                        >,
                    ),
                ),
        );
}
