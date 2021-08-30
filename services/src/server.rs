use crate::contexts::{InMemoryContext, SimpleContext};
use crate::error;
use crate::error::{Error, Result};
use crate::handlers;
use crate::handlers::handle_rejection;
use crate::util::config;
use crate::util::config::get_config_element;

use log::info;
use snafu::ResultExt;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::signal;
use tokio::sync::oneshot::{Receiver, Sender};
use warp::fs::File;
use warp::{Filter, Rejection};

/// Combine filters by boxing them
/// TODO: avoid boxing while still achieving acceptable compile time
#[macro_export]
macro_rules! combine {
  ($x:expr, $($y:expr),+) => {{
      let filter = $x.boxed();
      $( let filter = filter.or($y).boxed(); )+
      filter
  }}
}

/// Starts the webserver for the Geo Engine API.
///
/// # Panics
///  * may panic if the `Postgres` backend is chosen without compiling the `postgres` feature
///
///
pub async fn start_server(
    shutdown_rx: Option<Receiver<()>>,
    static_files_dir: Option<PathBuf>,
) -> Result<()> {
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

    info!("Using in memory backend");

    start(
        shutdown_rx,
        static_files_dir,
        bind_address,
        InMemoryContext::new_with_data().await,
    )
    .await
}

async fn start<C>(
    shutdown_rx: Option<Receiver<()>>,
    static_files_dir: Option<PathBuf>,
    bind_address: SocketAddr,
    ctx: C,
) -> Result<(), Error>
where
    C: SimpleContext,
{
    let handler = combine!(
        handlers::workflows::register_workflow_handler(ctx.clone()),
        handlers::workflows::load_workflow_handler(ctx.clone()),
        handlers::workflows::get_workflow_metadata_handler(ctx.clone()),
        handlers::workflows::get_workflow_provenance_handler(ctx.clone()),
        handlers::session::anonymous_handler(ctx.clone()),
        handlers::session::session_handler(ctx.clone()),
        handlers::session::session_project_handler(ctx.clone()),
        handlers::session::session_view_handler(ctx.clone()),
        handlers::projects::create_project_handler(ctx.clone()),
        handlers::projects::list_projects_handler(ctx.clone()),
        handlers::projects::update_project_handler(ctx.clone()),
        handlers::projects::delete_project_handler(ctx.clone()),
        handlers::projects::load_project_handler(ctx.clone()),
        handlers::datasets::get_dataset_handler(ctx.clone()),
        handlers::datasets::auto_create_dataset_handler(ctx.clone()),
        handlers::datasets::create_dataset_handler(ctx.clone()),
        handlers::datasets::suggest_meta_data_handler(ctx.clone()),
        handlers::datasets::list_providers_handler(ctx.clone()),
        handlers::datasets::list_external_datasets_handler(ctx.clone()),
        handlers::datasets::list_datasets_handler(ctx.clone()), // must come after `list_external_datasets_handler`
        handlers::wcs::wcs_handler(ctx.clone()),
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

    task.await.context(error::TokioJoin)
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
pub fn show_version_handler(
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("version")
        .and(warp::get())
        .and_then(show_version)
}

// TODO: move into handler once async closures are available?
#[allow(clippy::unused_async)] // the function signature of `Filter`'s `and_then` requires it
async fn show_version() -> Result<impl warp::Reply, warp::Rejection> {
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct VersionInfo<'a> {
        build_date: Option<&'a str>,
        commit_hash: Option<&'a str>,
    }

    Ok(warp::reply::json(&VersionInfo {
        build_date: option_env!("VERGEN_BUILD_DATE"),
        commit_hash: option_env!("VERGEN_GIT_SHA"),
    }))
}

pub fn serve_static_directory(
    path: Option<PathBuf>,
) -> impl Filter<Extract = (File,), Error = Rejection> + Clone {
    let has_path = path.is_some();

    warp::path("static")
        .and(warp::get())
        .and_then(move || async move {
            if has_path {
                Ok(())
            } else {
                Err(warp::reject::not_found())
            }
        })
        .and(warp::fs::dir(path.unwrap_or_default()))
        .map(|_, dir| dir)
}

pub async fn interrupt_handler(shutdown_tx: Sender<()>, callback: Option<fn()>) -> Result<()> {
    signal::ctrl_c().await.context(error::TokioSignal)?;

    if let Some(callback) = callback {
        callback();
    }

    shutdown_tx
        .send(())
        .map_err(|_error| Error::TokioChannelSend)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{Session, SimpleSession};
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
        let base_url = format!("http://{}", web_config.bind_address);

        assert!(wait_for_server(&base_url).await);
        issue_queries(&base_url).await;

        shutdown_tx.send(()).unwrap();
    }

    async fn issue_queries(base_url: &str) {
        let client = reqwest::Client::new();

        let body = client
            .post(&format!("{}/{}", base_url, "anonymous"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        let session: SimpleSession = serde_json::from_str(&body).unwrap();

        let body = client
            .post(&format!("{}/{}", base_url, "project"))
            .header("Authorization", format!("Bearer {}", session.id()))
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
