use warp::{Filter, Rejection};

use crate::contexts::InMemoryContext;
use crate::error;
use crate::error::{Error, Result};
use crate::handlers;
use crate::handlers::{handle_rejection, InMemoryContext};
use crate::util::config;
use crate::util::config::get_config_element;
use snafu::ResultExt;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::signal;
use tokio::sync::oneshot::{Receiver, Sender};
use warp::fs::File;

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

    // TODO: make configurable
    let ctx = InMemoryContext::default();

    // TODO: hierarchical filters workflow -> (register, load), user -> (register, login, ...)
    let handler = handlers::workflows::register_workflow_handler(ctx.clone())
        .or(handlers::workflows::load_workflow_handler(ctx.clone()))
        .or(handlers::users::register_user_handler(ctx.clone()))
        .or(handlers::users::login_handler(ctx.clone()))
        .or(handlers::users::logout_handler(ctx.clone()))
        .or(handlers::projects::create_project_handler(ctx.clone()))
        .or(handlers::projects::list_projects_handler(ctx.clone()))
        .or(handlers::projects::update_project_handler(ctx.clone()))
        .or(handlers::projects::delete_project_handler(ctx.clone()))
        .or(handlers::projects::project_versions_handler(ctx.clone()))
        .or(handlers::projects::add_permission_handler(ctx.clone()))
        .or(handlers::projects::remove_permission_handler(ctx.clone()))
        .or(handlers::projects::list_permissions_handler(ctx.clone()))
        .or(handlers::wms::wms_handler(ctx.clone()))
        .or(handlers::wfs::wfs_handler(ctx.clone()))
        .or(serve_static_directory(static_files_dir))
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

    shutdown_tx.send(()).map_err(|_| Error::TokioChannelSend)
}
