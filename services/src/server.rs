#[cfg(feature = "postgres")]
use crate::contexts::PostgresContext;
use crate::contexts::{Context, InMemoryContext};
use crate::error;
use crate::error::{Error, Result};
use crate::handlers;
use crate::handlers::handle_rejection;
use crate::util::config;
use crate::util::config::{get_config_element, Backend};
use actix_web::web::Json;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use actix_files::Files;
#[cfg(feature = "postgres")]
use bb8_postgres::tokio_postgres;
#[cfg(feature = "postgres")]
use bb8_postgres::tokio_postgres::NoTls;
use log::info;
use snafu::ResultExt;
use std::net::SocketAddr;
use std::path::PathBuf;
#[cfg(feature = "postgres")]
use std::str::FromStr;
use tokio::signal;
use tokio::sync::oneshot::{Receiver, Sender};
use warp::fs::File;
use warp::{Filter, Rejection};

/// Starts the webserver for the Geo Engine API.
///
/// # Panics
///  * may panic if the `Postgres` backend is chosen without compiling the `postgres` feature
///
///
pub async fn start_server(static_files_dir: Option<PathBuf>) -> Result<(), Error> {
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
            info!("Using in memory backend");
            start(
                static_files_dir,
                bind_address,
                InMemoryContext::new_with_data().await,
            )
            .await
        }
        Backend::Postgres => {
            #[cfg(feature = "postgres")]
            {
                info!("Using Postgres backend");
                let ctx = PostgresContext::new(
                    tokio_postgres::config::Config::from_str(
                        &get_config_element::<config::Postgres>()?.config_string,
                    )?,
                    NoTls,
                )
                .await?;

                start(static_files_dir, bind_address, ctx).await
            }
            #[cfg(not(feature = "postgres"))]
            panic!("Postgres backend was selected but the postgres feature wasn't activated during compilation")
        }
    }
}

async fn start<C>(
    static_files_dir: Option<PathBuf>,
    bind_address: SocketAddr,
    ctx: C,
) -> Result<(), Error>
where
    C: Context,
{
    /*let handler = combine!(
        handlers::workflows::register_workflow_handler(ctx.clone()),
        handlers::workflows::load_workflow_handler(ctx.clone()),
        handlers::workflows::get_workflow_metadata_handler(ctx.clone()),
        handlers::users::register_user_handler(ctx.clone()),
        handlers::users::anonymous_handler(ctx.clone()),
        handlers::users::login_handler(ctx.clone()),
        handlers::users::logout_handler(ctx.clone()),
        handlers::users::session_handler(ctx.clone()),
        handlers::users::session_project_handler(ctx.clone()),
        handlers::users::session_view_handler(ctx.clone()),
        handlers::projects::add_permission_handler(ctx.clone()),
        handlers::projects::remove_permission_handler(ctx.clone()),
        handlers::projects::list_permissions_handler(ctx.clone()),
        handlers::projects::create_project_handler(ctx.clone()),
        handlers::projects::list_projects_handler(ctx.clone()),
        handlers::projects::update_project_handler(ctx.clone()),
        handlers::projects::delete_project_handler(ctx.clone()),
        handlers::projects::load_project_handler(ctx.clone()),
        handlers::projects::project_versions_handler(ctx.clone()),
        handlers::datasets::list_datasets_handler(ctx.clone()),
        handlers::datasets::get_dataset_handler(ctx.clone()),
        handlers::datasets::auto_create_dataset_handler(ctx.clone()),
        handlers::datasets::create_dataset_handler(ctx.clone()),
        handlers::datasets::suggest_meta_data_handler(ctx.clone()),
        handlers::wms::wms_handler(ctx.clone()),
        handlers::wfs::wfs_handler(ctx.clone()),
        handlers::plots::get_plot_handler(ctx.clone()),
        handlers::upload::upload_handler(ctx.clone()),
        handlers::spatial_references::get_spatial_reference_specification_handler(ctx.clone()),
        show_version_handler(), // TODO: allow disabling this function via config or feature flag
        serve_static_directory(static_files_dir)
    )
    .recover(handle_rejection);

    let task = if let Some(receiver) = shutdown_rx {
        let (_, server) = warp::serve(handler).bind_with_graceful_shutdown(bind_address, async {
            receiver.await.ok();
        });
        tokio::task::spawn(server)
    } else {
        let server = warp::serve(handler).bind(bind_address);
        tokio::task::spawn(server)
    };

    task.await.context(error::TokioJoin)*/
    let wrapped_ctx = web::Data::new(ctx);

    HttpServer::new(move || {
        let app = App::new()
            .app_data(wrapped_ctx.clone())
            .service(show_version); // TODO: allow disabling this function via config or feature flag

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

/// Shows information about the server software version.
///
/// # Example
///
/// ```text
/// GET /version
/// ```
/// Response:
/// ```text
/// {
///   "buildDate": "2021-05-17",
///   "commitHash": "16cd0881a79b6f03bb5f1f6ef2b2711e570b9865"
/// }
/// ```
#[get("/version")]
async fn show_version() -> impl Responder {
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct VersionInfo<'a> {
        build_date: Option<&'a str>,
        commit_hash: Option<&'a str>,
    }

    Json(&VersionInfo {
        build_date: option_env!("VERGEN_BUILD_DATE"),
        commit_hash: option_env!("VERGEN_GIT_SHA"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::ErrorResponse;
    use tokio::sync::oneshot;

    /// Test the webserver startup to ensure that `tokio` and `warp` are working properly
    #[tokio::test]
    async fn webserver_start() {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let (server, _) =
            tokio::join!(start_server(Some(shutdown_rx), None), queries(shutdown_tx),);
        server.expect("server run");
    }

    async fn queries(shutdown_tx: Sender<()>) {
        let web_config: config::Web = get_config_element().unwrap();
        let base_url = format!(
            "http://{}/",
            web_config
                .external_address
                .unwrap_or(web_config.bind_address)
        );

        assert!(wait_for_server(&base_url).await);
        issue_queries(&base_url).await;

        shutdown_tx.send(()).unwrap();
    }

    async fn issue_queries(base_url: &str) {
        let client = reqwest::Client::new();
        let body = client
            .post(&format!("{}{}", base_url, "user"))
            .body("no json")
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        assert_eq!(
            serde_json::from_str::<ErrorResponse>(&body).unwrap(),
            ErrorResponse {
                error: "BodyDeserializeError".to_string(),
                message: "expected ident at line 1 column 2".to_string()
            }
        );
    }

    const WAIT_SERVER_RETRIES: i32 = 5;
    const WAIT_SERVER_RETRY_INTERVAL: u64 = 1;

    async fn wait_for_server(base_url: &str) -> bool {
        for _ in 0..WAIT_SERVER_RETRIES {
            if reqwest::get(base_url).await.is_ok() {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_secs(WAIT_SERVER_RETRY_INTERVAL));
        }
        false
    }
}
