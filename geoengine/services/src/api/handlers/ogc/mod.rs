#![allow(clippy::needless_for_each)] // TODO: remove when clippy is fixed for utoipa <https://github.com/juhaku/utoipa/issues/1420>

use crate::api::handlers::ogc::common::CollectionsResponseFormat;
use crate::{api::handlers::ogc::error::OgcApiError, contexts::ApplicationContext, error::Result};
use actix_web::{FromRequest, web};
use ogcapi_types::{
    common::{Collection, Collections, Conformance, LandingPage},
    tiles::{TileMatrixSet, TileMatrixSetItem, TileMatrixSets, TileSet, TileSetItem, TileSets},
};
use utoipa::OpenApi;

mod common;
mod error;
#[cfg(test)]
mod test_util;
mod tiles;
mod tms;
mod tms_spec;
mod util;

pub(crate) fn init_ogc_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    let mut scope = web::scope("/ogc/{dataConnectorId}/{layerId}");

    macro_rules! bind_routes {
        ($($method:ident $path:literal -> $handler:expr),* $(,)? ) => {
            $(paste::item! {
                scope = scope.route($path, web::[<$method:lower>]().to($handler));
            })*
        };
    }

    bind_routes!(
        HEAD "" -> common::landing_page_head::<C>,
        HEAD "/" -> common::landing_page_head::<C>,
        GET "" -> common::landing_page::<C>,
        GET "/" -> common::landing_page::<C>,
        GET "/conformance" -> common::conformance::<C>,
        GET "/collections" -> common::collections::<C>,
        GET "/collections/" -> common::collections::<C>,
        GET "/collections/{collectionId}" -> common::collection::<C>,
        GET "/collections/{collectionId}/" -> common::collection::<C>,
        GET "/collections/{collectionId}/map/tiles" -> tiles::collection_tilesets::<C>,
        GET "/collections/{collectionId}/map/tiles/" -> tiles::collection_tilesets::<C>,
        GET "/collections/{collectionId}/map/tiles/{tileMatrixSetId}" -> tiles::collection_tileset::<C>,
        GET "/collections/{collectionId}/map/tiles/{tileMatrixSetId}/{tileMatrix}/{tileRow}/{tileCol}" -> tiles::tile::<C>,
        GET "/tileMatrixSets" -> tms::tile_matrix_sets::<C>,
        GET "/tileMatrixSets/" -> tms::tile_matrix_sets::<C>,
        GET "/tileMatrixSets/{tileMatrixSetId}" -> tms::tile_matrix_set::<C>,
    );

    cfg.service(scope);
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
            TileSets,
            TileSetItem,
            TileSet,
        )
    )
)]
pub struct OgcApiDoc;

type OgcApiResult<T> = Result<T, OgcApiError>;
