use crate::contexts::Context;
use crate::contexts::SessionId;
use crate::error::{Error, Result};
use actix_web::dev::ServiceResponse;
use actix_web::http::{header, StatusCode};
use actix_web::{test, HttpRequest, HttpResponse};
use actix_web_httpauth::headers::authorization::{Bearer, Scheme};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

pub mod datasets;
#[cfg(feature = "ebv")]
pub mod ebv;
pub mod layers;
pub mod plots;
pub mod projects;
pub mod session;
pub mod spatial_references;
pub mod tasks;
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.error, self.message)
    }
}

pub fn get_token(req: &HttpRequest) -> Result<SessionId> {
    let header = req
        .headers()
        .get(header::AUTHORIZATION)
        .ok_or(Error::Authorization {
            source: Box::new(Error::MissingAuthorizationHeader),
        })?;
    let scheme = Bearer::parse(header).map_err(|_| Error::Authorization {
        source: Box::new(Error::InvalidAuthorizationScheme),
    })?;
    SessionId::from_str(scheme.token()).map_err(|_err| Error::Authorization {
        source: Box::new(Error::InvalidUuid),
    })
}
