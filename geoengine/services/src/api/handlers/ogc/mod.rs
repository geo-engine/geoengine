#![allow(clippy::needless_for_each)] // TODO: remove when clippy is fixed for utoipa <https://github.com/juhaku/utoipa/issues/1420>

use crate::{contexts::ApplicationContext, error::Result, workflows::workflow::WorkflowId};
use actix_http::StatusCode;
use actix_web::{FromRequest, web};
use ogcapi_types::common::{Exception, LandingPage};
use url::Url;
use utoipa::OpenApi;

mod common;
mod tiles;

pub(crate) fn init_ogc_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(
        web::scope("/ogc/{processingGraphId}")
            .service(web::resource(["", "/"]).route(web::get().to(common::landing_page::<C>)))
            .service(web::resource("/conformance").route(web::get().to(common::conformance::<C>))),
    );
}

#[derive(OpenApi)]
#[openapi(
    paths(
        // Common
        common::landing_page,

        // Tiles

    ),
    components(
        schemas(
            // Common
            LandingPage,

            // Tiles
        )
    )
)]
pub struct OgcApiDoc;

type OgcApiResult<T> = Result<T, OgcException>;

#[derive(Debug)]
struct OgcException(Exception);

impl std::fmt::Display for OgcException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Exception> for OgcException {
    fn from(exception: Exception) -> Self {
        Self(exception)
    }
}

fn internal_server_error() -> OgcException {
    Exception::new_from_status(StatusCode::INTERNAL_SERVER_ERROR.into())
        .detail("Server was misconfigured with a bad base url")
        .into()
}

impl actix_web::error::ResponseError for OgcException {
    fn status_code(&self) -> StatusCode {
        self.0
            .status
            .and_then(|status| StatusCode::from_u16(status).ok())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
        actix_web::HttpResponse::build(self.status_code()).json(&self.0)
    }
}

fn ogc_base_url(processing_graph_id: WorkflowId) -> Result<Url> {
    let web_config = crate::config::get_config_element::<crate::config::Web>()?;
    let base = web_config.api_url()?;

    Ok(base.join(&format!("ogc/{processing_graph_id}/"))?)
}
