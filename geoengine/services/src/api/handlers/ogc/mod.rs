#![allow(clippy::needless_for_each)] // TODO: remove when clippy is fixed for utoipa <https://github.com/juhaku/utoipa/issues/1420>

use crate::{api::handlers::ogc::error::OgcApiError, contexts::ApplicationContext, error::Result};
use actix_web::{FromRequest, web};
use ogcapi_types::common::{Collection, Collections, LandingPage};
use utoipa::OpenApi;

mod common;
mod error;
mod tiles;
mod util;

pub(crate) fn init_ogc_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(
        web::scope("/ogc/{processingGraphId}")
            .service(web::resource(["", "/"]).route(web::get().to(common::landing_page::<C>)))
            .service(web::resource("/conformance").route(web::get().to(common::conformance::<C>)))
            .service(
                web::scope("/collections")
                    .service(
                        web::resource(["", "/"]).route(web::get().to(common::collections::<C>)),
                    )
                    .service(
                        web::resource("/{collectionId}")
                            .route(web::get().to(common::collection::<C>)),
                    ),
            ),
    );
}

#[derive(OpenApi)]
#[openapi(
    paths(
        // Common
        common::landing_page,
        common::conformance,
        common::collections,
        common::collection,

        // Tiles

    ),
    components(
        schemas(
            // Common
            LandingPage,
            Collections,
            Collection,

            // Tiles
        )
    )
)]
pub struct OgcApiDoc;

type OgcApiResult<T> = Result<T, OgcApiError>;
