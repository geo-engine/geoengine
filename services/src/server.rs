#[cfg(feature = "postgres")]
use crate::contexts::PostgresContext;
use crate::contexts::{Context, InMemoryContext};
use crate::error;
use crate::error::{Error, Result};
use crate::handlers;
use crate::handlers::handle_rejection;
use crate::util::config;
use crate::util::config::{get_config_element, Backend};
#[cfg(feature = "postgres")]
use bb8_postgres::tokio_postgres;
#[cfg(feature = "postgres")]
use bb8_postgres::tokio_postgres::NoTls;
use snafu::ResultExt;
use std::net::SocketAddr;
use std::path::PathBuf;
#[cfg(feature = "postgres")]
use std::str::FromStr;
use tokio::signal;
use tokio::sync::oneshot::{Receiver, Sender};
use warp::fs::File;
use warp::{Filter, Rejection};

/// Helper to combine the multiple filters together with Filter::or, possibly boxing the types in
/// the process. This greatly helps the build times for `ipfs-http`.
/// https://github.com/seanmonstar/warp/issues/507#issuecomment-615974062
macro_rules! combine {
  ($x:expr, $($y:expr),+) => {{
      let filter = boxed_on_debug!($x);
      $( let filter = boxed_on_debug!(filter.or($y)); )+
      filter
  }}
}

#[cfg(debug_assertions)]
macro_rules! boxed_on_debug {
    ($x:expr) => {
        $x.boxed()
    };
}

#[cfg(not(debug_assertions))]
macro_rules! boxed_on_debug {
    ($x:expr) => {
        $x
    };
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

    eprintln!(
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
            eprintln!("Using in memory backend"); // TODO: log
            start(
                shutdown_rx,
                static_files_dir,
                bind_address,
                InMemoryContext::new_with_data().await,
            )
            .await
        }
        Backend::Postgres => {
            #[cfg(feature = "postgres")]
            {
                eprintln!("Using Postgres backend"); // TODO: log
                let ctx = PostgresContext::new(
                    tokio_postgres::config::Config::from_str(
                        &get_config_element::<config::Postgres>()?.config_string,
                    )?,
                    NoTls,
                )
                .await?;

                start(shutdown_rx, static_files_dir, bind_address, ctx).await
            }
            #[cfg(not(feature = "postgres"))]
            panic!("Postgres backend was selected but the postgres feature wasn't activated during compilation")
        }
    }
}

async fn start<C>(
    shutdown_rx: Option<Receiver<()>>,
    static_files_dir: Option<PathBuf>,
    bind_address: SocketAddr,
    ctx: C,
) -> Result<(), Error>
where
    C: Context,
{
    // TODO: hierarchical filters workflow -> (register, load), user -> (register, login, ...)
    let handler = combine!(
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
        handlers::projects::create_project_handler(ctx.clone()),
        handlers::projects::list_projects_handler(ctx.clone()),
        handlers::projects::update_project_handler(ctx.clone()),
        handlers::projects::delete_project_handler(ctx.clone()),
        handlers::projects::load_project_handler(ctx.clone()),
        handlers::projects::project_versions_handler(ctx.clone()),
        handlers::projects::add_permission_handler(ctx.clone()),
        handlers::projects::remove_permission_handler(ctx.clone()),
        handlers::projects::list_permissions_handler(ctx.clone()),
        handlers::datasets::list_datasets_handler(ctx.clone()),
        handlers::wms::wms_handler(ctx.clone()),
        handlers::wfs::wfs_handler(ctx.clone()),
        handlers::plots::get_plot_handler(ctx.clone()),
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

fn serve_static_directory(
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

        assert_eq!(wait_for_server(&base_url).await, true);
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
        return false;
    }
}
