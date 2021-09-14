use crate::error::{Error, Result};
use crate::handlers;
use crate::pro;
#[cfg(feature = "postgres")]
use crate::pro::contexts::PostgresContext;
use crate::pro::contexts::{ProContext, ProInMemoryContext};
use crate::util::config::{self, get_config_element, Backend};

use super::projects::ProProjectDb;
use crate::server::{configure_extractors, render_404, show_version_handler};
use actix_files::Files;
use actix_web::{middleware, web, App, HttpServer};
#[cfg(feature = "postgres")]
use bb8_postgres::tokio_postgres::NoTls;
use log::info;
use std::net::SocketAddr;
use std::path::PathBuf;
use url::Url;

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
        let mut app = App::new()
            .app_data(wrapped_ctx.clone())
            .wrap(middleware::Logger::default())
            .wrap(middleware::NormalizePath::default())
            .configure(configure_extractors)
            .configure(handlers::datasets::init_dataset_routes::<C>)
            .configure(handlers::plots::init_plot_routes::<C>)
            .configure(pro::handlers::projects::init_project_routes::<C>)
            .configure(pro::handlers::users::init_user_routes::<C>)
            .configure(handlers::spatial_references::init_spatial_reference_routes::<C>)
            .configure(handlers::upload::init_upload_routes::<C>)
            .configure(handlers::wcs::init_wcs_routes::<C>)
            .configure(handlers::wfs::init_wfs_routes::<C>)
            .configure(handlers::wms::init_wms_routes::<C>)
            .configure(handlers::workflows::init_workflow_routes::<C>)
            .route("/version", web::get().to(show_version_handler)) // TODO: allow disabling this function via config or feature flag
            .default_service(web::route().to(render_404));
        #[cfg(feature = "odm")]
        {
            app = app.configure(pro::handlers::drone_mapping::init_drone_mapping_routes::<C>);
        }
        if let Some(static_files_dir) = static_files_dir.clone() {
            app = app.service(Files::new("/static", static_files_dir));
        }
        app
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

    info!(
        "Starting server… {}",
        web_config
            .external_address
            .unwrap_or(Url::parse(&format!("http://{}/", web_config.bind_address))?)
    );

    let data_path_config: config::DataProvider = get_config_element()?;

    match web_config.backend {
        Backend::InMemory => {
            info!("Using in memory backend");
            start(
                static_files_dir,
                web_config.bind_address,
                ProInMemoryContext::new_with_data(
                    data_path_config.dataset_defs_path,
                    data_path_config.provider_defs_path,
                )
                .await,
            )
            .await
        }
        Backend::Postgres => {
            #[cfg(feature = "postgres")]
            {
                info!("Using Postgres backend");

                let db_config = config::get_config_element::<config::Postgres>()?;
                let mut pg_config = bb8_postgres::tokio_postgres::Config::new();
                pg_config
                    .user(&db_config.user)
                    .password(&db_config.password)
                    .host(&db_config.host)
                    .dbname(&db_config.database)
                    // fix schema by providing `search_path` option
                    .options(&format!("-c search_path={}", db_config.schema));

                let ctx = PostgresContext::new(pg_config, NoTls).await?;

                start(static_files_dir, web_config.bind_address, ctx).await
            }
            #[cfg(not(feature = "postgres"))]
            panic!("Postgres backend was selected but the postgres feature wasn't activated during compilation")
        }
    }
}
