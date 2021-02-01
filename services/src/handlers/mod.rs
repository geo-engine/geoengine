use crate::error;
use crate::error::Result;
use crate::users::session::SessionId;
use crate::users::userdb::UserDB;
use crate::{contexts::Context, error::Error};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::error::Error as StdError;
use std::str::FromStr;
use warp::http::{Response, StatusCode};
use warp::hyper::body::Bytes;
use warp::reject::{InvalidQuery, MethodNotAllowed};
use warp::{Filter, Rejection, Reply};

pub mod projects;
pub mod users;
pub mod wfs;
pub mod wms;
pub mod workflows;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}

impl ErrorResponse {
    pub fn assert(res: &Response<Bytes>, status: u16, error: &str, message: &str) {
        assert_eq!(res.status(), status);

        let body = std::str::from_utf8(&res.body()).unwrap();
        assert_eq!(
            serde_json::from_str::<ErrorResponse>(body).unwrap(),
            ErrorResponse {
                error: error.to_string(),
                message: message.to_string(),
            }
        );
    }
}

/// A handler for custom rejections
pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Rejection> {
    let (code, error, message) = if let Some(e) = err.find::<Error>() {
        // custom errors

        // TODO: distinguish between client/server/temporary/permanent errors
        match e {
            error::Error::Authorization { source } => (
                StatusCode::UNAUTHORIZED,
                Into::<&str>::into(source.as_ref()).to_string(),
                source.to_string(),
            ),
            error::Error::RegistrationFailed { reason } if reason == "E-mail already exists" => (
                StatusCode::CONFLICT,
                Into::<&str>::into(e).to_string(),
                e.to_string(),
            ),
            _ => (
                StatusCode::BAD_REQUEST,
                Into::<&str>::into(e).to_string(),
                e.to_string(),
            ),
        }
    } else if let Some(e) = err.find::<warp::filters::body::BodyDeserializeError>() {
        // serde_json deserialization errors

        (
            StatusCode::BAD_REQUEST,
            "BodyDeserializeError".to_string(),
            e.source()
                .map_or("Bad Request".to_string(), ToString::to_string),
        )
    } else if err.find::<MethodNotAllowed>().is_some() {
        (
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed".to_string(),
            "HTTP method not allowed.".to_string(),
        )
    } else if err.find::<InvalidQuery>().is_some() {
        (
            StatusCode::BAD_REQUEST,
            "InvalidQuery".to_string(),
            "Invalid query string.".to_string(),
        )
    } else {
        // no matching filter

        (
            StatusCode::NOT_FOUND,
            "NotFound".to_string(),
            "Not Found".to_string(),
        )
    };

    let json = warp::reply::json(&ErrorResponse { error, message });
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
