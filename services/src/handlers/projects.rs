use crate::users::userdb::UserDB;
use crate::projects::projectdb::ProjectDB;
use crate::handlers::{DB, authenticate};
use warp::Filter;
use std::sync::Arc;
use crate::users::session::Session;
use crate::projects::project::{CreateProject, ProjectListOptions};
use crate::users::user::UserInput;

pub fn create_project_handler<T: UserDB, R: ProjectDB>(user_db: DB<T>, project_db: DB<R>) -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "create"))
        .and(authenticate(user_db.clone()))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&project_db)))
        .and_then(create_project)
}

// TODO: move into handler once async closures are available?
async fn create_project<T: ProjectDB>(session: Session, create: CreateProject, project_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let create = create.validated().map_err(warp::reject::custom)?;
    let id = project_db.write().await.create(session.user, create);
    Ok(warp::reply::json(&id))
}

pub fn list_projects_handler<T: UserDB, R: ProjectDB>(user_db: DB<T>, project_db: DB<R>) -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "list" ))
        .and(authenticate(user_db.clone()))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&project_db)))
        .and_then(list_projects)
}

// TODO: move into handler once async closures are available?
async fn list_projects<T: ProjectDB>(session: Session, options: ProjectListOptions, project_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let options = options.validated().map_err(warp::reject::custom)?;
    let listing = project_db.read().await.list(session.user, options);
    Ok(warp::reply::json(&listing))
}
