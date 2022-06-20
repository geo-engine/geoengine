use actix_web::{web, FromRequest, Responder};

use crate::error::Result;

use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider, LayerId};
use crate::layers::storage::LayerDb;
use crate::util::user_input::UserInput;
use crate::{contexts::Context, layers::layer::LayerCollectionListOptions};

pub(crate) fn init_layer_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: Context,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/layers").route(web::get().to(list_root_collections_handler::<C>)))
        // TODO: add provider as param
        .service(web::resource("/layers/{id}").route(web::get().to(list_collection_handler::<C>)))
        // TODO: add provider as param
        .service(web::resource("/layer/{id}").route(web::get().to(layer_handler::<C>)));
}

async fn list_root_collections_handler<C: Context>(
    ctx: web::Data<C>,
    options: web::Query<LayerCollectionListOptions>,
) -> Result<impl Responder> {
    let db = ctx.layer_db_ref();
    let collection = db
        .root_collection_items(options.into_inner().validated()?)
        .await?;

    Ok(web::Json(collection))
}

async fn list_collection_handler<C: Context>(
    ctx: web::Data<C>,
    id: web::Path<LayerCollectionId>,
    options: web::Query<LayerCollectionListOptions>,
) -> Result<impl Responder> {
    let collection = ctx
        .layer_db_ref()
        .collection_items(&id.into_inner(), options.into_inner().validated()?)
        .await?;

    Ok(web::Json(collection))
}

async fn layer_handler<C: Context>(
    ctx: web::Data<C>,
    id: web::Path<LayerId>,
) -> Result<impl Responder> {
    let collection = ctx.layer_db_ref().get_layer(&id.into_inner()).await?;

    Ok(web::Json(collection))
}
