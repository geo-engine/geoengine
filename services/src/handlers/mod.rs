use crate::error::Result;
use crate::users::session::SessionId;
use crate::users::userdb::UserDB;
use crate::util::identifiers::Identifier;
use crate::{contexts::Context, error::Error};
use warp::Filter;
use warp::{Rejection, Reply};

pub mod projects;
pub mod users;
pub mod wfs;
pub mod wms;
pub mod workflows;

/// A handler for custom rejections
///
/// # Errors
///
/// Fails if the rejection is not custom
///
pub async fn handle_rejection(error: Rejection) -> Result<impl Reply, Rejection> {
    // TODO: handle/report serde deserialization error when e.g. a json attribute is missing/malformed
    error.find::<Error>().map_or(Err(warp::reject()), |err| {
        let json = warp::reply::json(&err.to_string());
        Ok(warp::reply::with_status(
            json,
            warp::http::StatusCode::BAD_REQUEST,
        ))
    })
}

fn authenticate<C: Context>(
    ctx: C,
) -> impl warp::Filter<Extract = (C,), Error = warp::Rejection> + Clone {
    async fn do_authenticate<C: Context>(mut ctx: C, token: String) -> Result<C, warp::Rejection> {
        let token = SessionId::from_uuid_str(&token).map_err(|_error| warp::reject())?;
        let session = ctx
            .user_db_ref()
            .await
            .session(token)
            .await
            .map_err(|_error| warp::reject())?;
        ctx.set_session(session);
        Ok(ctx)
    }

    warp::any()
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::header::<String>("authorization"))
        .and_then(do_authenticate)
}
