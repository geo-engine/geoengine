use crate::error;
use crate::error::Result;
use crate::users::session::SessionId;
use crate::users::userdb::UserDB;
use crate::{contexts::Context, error::Error};
use serde_json::json;
use snafu::ResultExt;
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
    let (code, error, message) = if let Some(e) = err.find::<Error>() {
        // custom errors

        // TODO: distinguish between client/server/temporary/permanent errors
        if let error::Error::Authorization { source: e } = e {
            let error_name: &'static str = e.as_ref().into();
            (
                StatusCode::UNAUTHORIZED,
                error_name.to_string(),
                e.to_string(),
            )
        } else {
            let error_name: &'static str = e.into();
            (
                StatusCode::BAD_REQUEST,
                error_name.to_string(),
                e.to_string(),
            )
        }
    } else if let Some(e) = err.find::<warp::filters::body::BodyDeserializeError>() {
        // serde_json deserialization errors

        (
            StatusCode::BAD_REQUEST,
            "BodyDeserializeError".to_string(),
            e.source()
                .map_or("Bad Request".to_string(), ToString::to_string),
        )
    } else {
        // no matching filter

        (
            StatusCode::NOT_FOUND,
            "NotFound".to_string(),
            "Not Found".to_string(),
        )
    };

    let json = warp::reply::json(&json!( {
        "error": error,
        "message": message,
    }));
    Ok(warp::reply::with_status(json, code))
}

fn authenticate<C: Context>(
    ctx: C,
) -> impl warp::Filter<Extract = (C,), Error = warp::Rejection> + Clone {
    async fn do_authenticate<C: Context>(
        mut ctx: C,
        token: Option<String>,
    ) -> Result<C, warp::Rejection> {
        if let Some(token) = token {
            if !token.starts_with("Bearer ") {
                return Err(Error::Authorization {
                    source: Box::new(Error::InvalidAuthorizationScheme),
                }
                .into());
            }

            let token = SessionId::from_str(&token["Bearer ".len()..])
                .map_err(Box::new)
                .context(error::Authorization)?;
            let session = ctx
                .user_db_ref()
                .await
                .session(token)
                .await
                .map_err(Box::new)
                .context(error::Authorization)?;
            ctx.set_session(session);
            Ok(ctx)
        } else {
            Err(Error::Authorization {
                source: Box::new(Error::MissingAuthorizationHeader),
            }
            .into())
        }
    }

    warp::any()
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::header::optional::<String>("authorization"))
        .and_then(do_authenticate)
}
