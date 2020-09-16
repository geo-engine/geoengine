use std::sync::Arc;

use tokio::sync::RwLock;
use warp::{Filter, Rejection};

use crate::error;
use crate::error::{Error, Result};
use crate::handlers;
use crate::handlers::handle_rejection;
use crate::projects::hashmap_projectdb::HashMapProjectDB;
use crate::users::hashmap_userdb::HashMapUserDB;
use crate::workflows::registry::HashMapRegistry;
use snafu::ResultExt;
use std::path::PathBuf;
use tokio::signal;
use tokio::sync::oneshot::{Receiver, Sender};
use warp::fs::File;

pub async fn start_server(
    shutdown_rx: Option<Receiver<()>>,
    static_files_dir: Option<PathBuf>,
) -> Result<()> {
    let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));
    let workflow_registry = Arc::new(RwLock::new(HashMapRegistry::default()));
    let project_db = Arc::new(RwLock::new(HashMapProjectDB::default()));

    // TODO: hierarchical filters workflow -> (register, load), user -> (register, login, ...)
    let handler = handlers::workflows::register_workflow_handler(workflow_registry.clone())
        .or(handlers::workflows::load_workflow_handler(
            workflow_registry.clone(),
        ))
        .or(handlers::users::register_user_handler(user_db.clone()))
        .or(handlers::users::login_handler(user_db.clone()))
        .or(handlers::users::logout_handler(user_db.clone()))
        .or(handlers::projects::create_project_handler(
            user_db.clone(),
            project_db.clone(),
        ))
        .or(handlers::projects::list_projects_handler(
            user_db.clone(),
            project_db.clone(),
        ))
        .or(handlers::projects::update_project_handler(
            user_db.clone(),
            project_db.clone(),
        ))
        .or(handlers::projects::delete_project_handler(
            user_db.clone(),
            project_db.clone(),
        ))
        .or(handlers::projects::project_versions_handler(
            user_db.clone(),
            project_db.clone(),
        ))
        .or(handlers::projects::add_permission_handler(
            user_db.clone(),
            project_db.clone(),
        ))
        .or(handlers::projects::remove_permission_handler(
            user_db.clone(),
            project_db.clone(),
        ))
        .or(handlers::projects::list_permissions_handler(
            user_db.clone(),
            project_db.clone(),
        ))
        .or(handlers::wms::wms_handler(workflow_registry.clone()))
        .or(handlers::wfs::wfs_handler(workflow_registry.clone()))
        .or(serve_static_directory(static_files_dir))
        .recover(handle_rejection);

    let task = if let Some(receiver) = shutdown_rx {
        let (_, server) =
            warp::serve(handler).bind_with_graceful_shutdown(([127, 0, 0, 1], 3030), async {
                receiver.await.ok();
            });
        tokio::task::spawn(server)
    } else {
        let server = warp::serve(handler).bind(([127, 0, 0, 1], 3030));
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
