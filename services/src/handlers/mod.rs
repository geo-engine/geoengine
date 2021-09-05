use crate::contexts::Context;
use crate::contexts::SessionId;
use crate::error;
use crate::error::Result;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::http::StatusCode;
use actix_web::{test, web, HttpMessage, HttpResponse};
use actix_web_httpauth::extractors::bearer::BearerAuth;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::fmt;
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

impl actix_web::ResponseError for ErrorResponse {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(self)
    }

    fn status_code(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}

impl fmt::Display for ErrorResponse {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        unimplemented!("required by ResponseError")
    }
}

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
