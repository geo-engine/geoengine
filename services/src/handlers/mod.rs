use crate::error::Result;
use crate::users::session::SessionId;
use crate::users::userdb::UserDB;
use crate::{contexts::Context, error::Error};
use serde_json::json;
use std::error::Error as StdError;
use std::str::FromStr;
use warp::http::StatusCode;
use warp::Filter;
use warp::{Rejection, Reply};

pub mod projects;
pub mod users;
pub mod wfs;
pub mod wms;
pub mod workflows;

/// A handler for custom rejections
pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Rejection> {
    let (code, message) = if err.is_not_found() {
        (StatusCode::NOT_FOUND, "Not Found".to_string())
    } else if let Some(e) = err.find::<Error>() {
        // TODO: distinguish between client/server/temporary/permanent errors
        (StatusCode::BAD_REQUEST, e.to_string())
    } else if let Some(e) = err.find::<warp::filters::body::BodyDeserializeError>() {
        (
            StatusCode::BAD_REQUEST,
            e.source()
                .map_or("Bad Request".to_string(), ToString::to_string),
        )
    } else {
        return Err(warp::reject());
    };

    let json = warp::reply::json(&json!( {
        "message": message,
    }));
    Ok(warp::reply::with_status(json, code))
}

fn authenticate<C: Context>(
    ctx: C,
) -> impl warp::Filter<Extract = (C,), Error = warp::Rejection> + Clone {
    async fn do_authenticate<C: Context>(mut ctx: C, token: String) -> Result<C, warp::Rejection> {
        let token = SessionId::from_str(&token).map_err(|_error| warp::reject())?;
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
