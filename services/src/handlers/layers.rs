use actix_web::{web, FromRequest, Responder};
use geoengine_datatypes::dataset::{DataProviderId, LayerId};

use crate::error::Result;

use crate::layers::layer::{CollectionItem, LayerCollectionListing, ProviderLayerCollectionId};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::layers::storage::{LayerProviderDb, LayerProviderListingOptions};
use crate::util::user_input::UserInput;
use crate::{contexts::Context, layers::layer::LayerCollectionListOptions};

pub(crate) fn init_layer_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: Context,
    C::Session: FromRequest,
{
    cfg.service(
        web::resource("/layers/collections")
            .route(web::get().to(list_root_collections_handler::<C>)),
    )
    .service(
        web::resource("/layers/collections/{provider}/{item}")
            .route(web::get().to(list_collection_handler::<C>)),
    )
    .service(web::resource("/layers/{provider}/{item}").route(web::get().to(layer_handler::<C>)));
}

async fn list_root_collections_handler<C: Context>(
    ctx: web::Data<C>,
    mut options: web::Query<LayerCollectionListOptions>,
) -> Result<impl Responder> {
    let mut providers = vec![];

    if options.offset == 0 && options.limit > 0 {
        providers.push(CollectionItem::Collection(LayerCollectionListing {
            id: ProviderLayerCollectionId {
                provider_id: crate::datasets::storage::DATASET_DB_LAYER_PROVIDER_ID,
                collection_id: LayerCollectionId(
                    crate::datasets::storage::DATASET_DB_ROOT_COLLECTION_ID.to_string(),
                ),
            },
            name: "Datasets".to_string(),
            description: "Basic Layers for all Datasets".to_string(),
            entry_label: None,
            properties: vec![],
        }));

        options.limit -= 1;
    }

    if options.offset <= 1 && options.limit > 1 {
        providers.push(CollectionItem::Collection(LayerCollectionListing {
            id: ProviderLayerCollectionId {
                provider_id: crate::layers::storage::INTERNAL_PROVIDER_ID,
                collection_id: LayerCollectionId(
                    crate::layers::storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID.to_string(),
                ),
            },
            name: "Layers".to_string(),
            description: "All available Geo Engine layers".to_string(),
            entry_label: None,
            properties: vec![],
        }));

        options.limit -= 1;
    }

    let external = ctx.layer_provider_db_ref();

    for provider_listing in external
        .list_layer_providers(
            LayerProviderListingOptions {
                offset: options.offset,
                limit: options.limit,
            }
            .validated()?,
        )
        .await?
    {
        // TODO: resolve providers in parallel
        let provider = external.layer_provider(provider_listing.id).await?;
        providers.push(CollectionItem::Collection(LayerCollectionListing {
            id: ProviderLayerCollectionId {
                provider_id: provider_listing.id,
                collection_id: provider.root_collection_id().await?,
            },
            name: provider_listing.name,
            description: provider_listing.description,
            entry_label: None,
            properties: vec![],
        }));
    }

    Ok(web::Json(providers))
}

async fn list_collection_handler<C: Context>(
    ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerCollectionId)>,
    options: web::Query<LayerCollectionListOptions>,
) -> Result<impl Responder> {
    let (provider, item) = path.into_inner();

    if provider == crate::datasets::storage::DATASET_DB_LAYER_PROVIDER_ID {
        let collection = ctx
            .dataset_db_ref()
            .collection_items(&item, options.into_inner().validated()?)
            .await?;

        return Ok(web::Json(collection));
    }

    if provider == crate::layers::storage::INTERNAL_PROVIDER_ID {
        let collection = ctx
            .layer_db_ref()
            .collection_items(&item, options.into_inner().validated()?)
            .await?;

        return Ok(web::Json(collection));
    }

    let collection = ctx
        .layer_provider_db_ref()
        .layer_provider(provider)
        .await?
        .collection_items(&item, options.into_inner().validated()?)
        .await?;

    Ok(web::Json(collection))
}

async fn layer_handler<C: Context>(
    ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId)>,
) -> Result<impl Responder> {
    let (provider, item) = path.into_inner();

    if provider == crate::datasets::storage::DATASET_DB_LAYER_PROVIDER_ID {
        let collection = ctx.dataset_db_ref().get_layer(&item).await?;

        return Ok(web::Json(collection));
    }

    if provider == crate::layers::storage::INTERNAL_PROVIDER_ID {
        let collection = ctx.layer_db_ref().get_layer(&item).await?;

        return Ok(web::Json(collection));
    }

    let collection = ctx
        .layer_provider_db_ref()
        .layer_provider(provider)
        .await?
        .get_layer(&item)
        .await?;

    Ok(web::Json(collection))
}
