use crate::error::{Error, Result};
use crate::handlers;
use crate::handlers::handle_rejection;
use crate::pro;
#[cfg(feature = "postgres")]
use crate::pro::contexts::PostgresContext;
use crate::pro::contexts::{ProContext, ProInMemoryContext};
use crate::server::{serve_static_directory, show_version_handler};
use crate::util::config::{self, get_config_element, Backend};
use crate::{combine, error};

#[cfg(feature = "postgres")]
use bb8_postgres::tokio_postgres::NoTls;
use log::info;
use snafu::ResultExt;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::sync::oneshot::Receiver;
use url::Url;
use warp::Filter;

use super::projects::ProProjectDb;

async fn start<C>(
    shutdown_rx: Option<Receiver<()>>,
    static_files_dir: Option<PathBuf>,
    bind_address: SocketAddr,
    ctx: C,
) -> Result<(), Error>
where
    C: ProContext,
    C::ProjectDB: ProProjectDb,
{
    let filter = combine!(
        handlers::workflows::register_workflow_handler(ctx.clone()),
        handlers::workflows::load_workflow_handler(ctx.clone()),
        handlers::workflows::get_workflow_metadata_handler(ctx.clone()),
        handlers::workflows::get_workflow_provenance_handler(ctx.clone()),
        handlers::workflows::dataset_from_workflow_handler(ctx.clone()),
        pro::handlers::users::register_user_handler(ctx.clone()),
        pro::handlers::users::anonymous_handler(ctx.clone()),
        pro::handlers::users::login_handler(ctx.clone()),
        pro::handlers::users::logout_handler(ctx.clone()),
        handlers::session::session_handler(ctx.clone()),
        pro::handlers::users::session_project_handler(ctx.clone()),
        pro::handlers::users::session_view_handler(ctx.clone()),
        pro::handlers::projects::add_permission_handler(ctx.clone()),
        pro::handlers::projects::remove_permission_handler(ctx.clone()),
        pro::handlers::projects::list_permissions_handler(ctx.clone()),
        handlers::projects::create_project_handler(ctx.clone()),
        handlers::projects::list_projects_handler(ctx.clone()),
        handlers::projects::update_project_handler(ctx.clone()),
        handlers::projects::delete_project_handler(ctx.clone()),
        pro::handlers::projects::load_project_handler(ctx.clone()),
        pro::handlers::projects::project_versions_handler(ctx.clone()),
        handlers::datasets::list_external_datasets_handler(ctx.clone()),
        handlers::datasets::list_datasets_handler(ctx.clone()),
        handlers::datasets::list_providers_handler(ctx.clone()),
        handlers::datasets::get_dataset_handler(ctx.clone()),
        handlers::datasets::auto_create_dataset_handler(ctx.clone()),
        handlers::datasets::create_dataset_handler(ctx.clone()),
        handlers::datasets::suggest_meta_data_handler(ctx.clone()),
        handlers::wcs::wcs_handler(ctx.clone()),
        handlers::wms::wms_handler(ctx.clone()),
        handlers::wfs::wfs_handler(ctx.clone()),
        handlers::plots::get_plot_handler(ctx.clone()),
        handlers::upload::upload_handler(ctx.clone()),
        handlers::spatial_references::get_spatial_reference_specification_handler(ctx.clone()),
        show_version_handler() // TODO: allow disabling this function via config or feature flag
    );

    #[cfg(feature = "odm")]
    let filter = combine!(
        filter,
        pro::handlers::drone_mapping::start_task_handler(ctx.clone()),
        pro::handlers::drone_mapping::dataset_from_drone_mapping_handler(ctx.clone())
    );

    let handler =
        combine!(filter, serve_static_directory(static_files_dir)).recover(handle_rejection);

    let task = if let Some(receiver) = shutdown_rx {
        let (_, server) = warp::serve(handler).bind_with_graceful_shutdown(bind_address, async {
            receiver.await.ok();
        });
        tokio::task::spawn(server)
    } else {
        let server = warp::serve(handler).bind(bind_address);
        tokio::task::spawn(server)
    };

    task.await.context(error::TokioJoin)
}

/// Starts the webserver for the Geo Engine API.
///
/// # Panics
///  * may panic if the `Postgres` backend is chosen without compiling the `postgres` feature
///
///
pub async fn start_pro_server(
    shutdown_rx: Option<Receiver<()>>,
    static_files_dir: Option<PathBuf>,
) -> Result<()> {
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
        web_config
            .external_address
            .unwrap_or(Url::parse(&format!("http://{}/", web_config.bind_address))?)
    );

    match web_config.backend {
        Backend::InMemory => {
            info!("Using in memory backend"); // TODO: log
            start(
                shutdown_rx,
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

                start(shutdown_rx, static_files_dir, bind_address, ctx).await
            }
            #[cfg(not(feature = "postgres"))]
            panic!("Postgres backend was selected but the postgres feature wasn't activated during compilation")
        }
    }
}
