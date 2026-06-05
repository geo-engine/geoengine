#![allow(clippy::needless_for_each)] // TODO: remove when clippy is fixed for utoipa <https://github.com/juhaku/utoipa/issues/1420>

use crate::api::handlers::ogc::common::CollectionsResponseFormat;
use crate::{api::handlers::ogc::error::OgcApiError, contexts::ApplicationContext, error::Result};
use actix_web::{FromRequest, web};
use ogcapi_types::common::{Collection, Collections, Conformance, LandingPage};
use ogcapi_types::tiles::{TileMatrixSet, TileMatrixSetItem, TileMatrixSets};
use utoipa::OpenApi;

mod common;
mod error;
mod tiles;
mod tms;
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
                    )
                    .service(
                        web::resource("/{collectionId}/tiles")
                            .route(web::get().to(tiles::collection_tilesets::<C>)),
                    )
                    .service(
                        web::resource("/{collectionId}/tiles/{tileMatrixSetId}")
                            .route(web::get().to(tiles::collection_tileset::<C>)),
                    )
                    .service(
                        web::resource(
                            "/{collectionId}/tiles/{tileMatrixSetId}/{tileMatrix}/{tileRow}/{tileCol}",
                        )
                        .route(web::get().to(tiles::tile::<C>)),
                    ),
            )
            .service(
                web::scope("/tileMatrixSets")
                    .service(
                        web::resource(["", "/"]).route(web::get().to(tms::tile_matrix_sets::<C>)),
                    )
                    .service(
                        web::resource("/{tileMatrixSetId}")
                            .route(web::get().to(tms::tile_matrix_set::<C>)),
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

        // Tile Matrix Sets
        tms::tile_matrix_sets,
        tms::tile_matrix_set,

        // Tiles
        tiles::collection_tilesets,
        tiles::collection_tileset,
        tiles::tile,

    ),
    components(
        schemas(
            // Common
            Collection,
            Collections,
            CollectionsResponseFormat,
            Conformance,
            LandingPage,

            // Tile Matrix Sets
            TileMatrixSet,
            TileMatrixSetItem,
            TileMatrixSets,

            // Tiles
            tiles::TileSetsResponse,
            tiles::TileSetListItemResponse,
            tiles::TileSetMetadataResponse,
            tiles::TemplatedTileLink,
        )
    )
)]
pub struct OgcApiDoc;

type OgcApiResult<T> = Result<T, OgcApiError>;
