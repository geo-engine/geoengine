use crate::contexts::Context;
use crate::contexts::SessionId;
use crate::error;
use crate::error::Result;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::{test, web, HttpMessage};
use actix_web_httpauth::extractors::bearer::BearerAuth;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::str::FromStr;

pub mod datasets;
pub mod plots;
pub mod projects;
pub mod session;
pub mod spatial_references;
pub mod upload;
pub mod wcs;
pub mod wfs;
pub mod wms;
pub mod workflows;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}

impl ErrorResponse {
    /// Assert that a `Response` has a certain `status` and `error` message.
    ///
    /// # Panics
    /// Panics if `status` or `error` do not match.
    ///
    pub async fn assert(res: ServiceResponse, status: u16, error: &str, message: &str) {
        assert_eq!(res.status(), status);

        let body: Self = test::read_body_json(res).await;
        assert_eq!(
            body,
            Self {
                error: error.to_string(),
                message: message.to_string(),
            }
        );
    }
}

/*// A handler for custom rejections
#[allow(clippy::unused_async)]
pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Rejection> {
    error!("Warp rejection: {:?}", err);

    let (code, error, message) = if let Some(e) = err.find::<Error>() {
        // custom errors

        // TODO: distinguish between client/server/temporary/permanent errors
        match e {
            error::Error::Authorization { source } => (
                StatusCode::UNAUTHORIZED,
                Into::<&str>::into(source.as_ref()).to_string(),
                source.to_string(),
            ),
            error::Error::Duplicate { reason: _ } => (
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
    } else if err.find::<UnsupportedMediaType>().is_some() {
        (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "UnsupportedMediaType".to_string(),
            "Unsupported content type header.".to_string(),
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
}*/

pub async fn validate_token<C: Context>(
    req: ServiceRequest,
    credentials: BearerAuth,
) -> Result<ServiceRequest, actix_web::Error> {
    let ctx: &C = req.app_data::<web::Data<C>>().unwrap();

    let token = SessionId::from_str(credentials.token())
        .map_err(Box::new)
        .context(error::Authorization)?;

    let session = ctx.session_by_id(token).await?;

    req.extensions_mut().insert(session);

    Ok(req)
}
