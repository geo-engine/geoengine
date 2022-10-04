use crate::api::model::datatypes::{DataProviderId, LayerId};
use actix_web::{web, FromRequest, Responder};

use crate::error::Result;

use crate::layers::layer::{
    CollectionItem, LayerCollection, LayerCollectionListing, ProviderLayerCollectionId,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::layers::storage::{LayerProviderDb, LayerProviderListingOptions};
use crate::util::user_input::UserInput;
use crate::{contexts::Context, layers::layer::LayerCollectionListOptions};

pub const ROOT_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0x1c3b_8042_300b_485c_95b5_0147_d9dc_068d);

pub const ROOT_COLLECTION_ID: DataProviderId =
    DataProviderId::from_u128(0xf242_4474_ef24_4c18_ab84_6859_2e12_ce48);

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
        web::resource(r#"/layers/collections/{provider}/{collection:.+}"#)
            .route(web::get().to(list_collection_handler::<C>)),
    )
    .service(
        web::resource("/layers/{provider}/{layer:.+}").route(web::get().to(layer_handler::<C>)),
    );
}

/// List all layer collections
#[utoipa::path(
    tag = "Layers",
    get,
    path = "/layers/collections",
    responses(
        (status = 200, description = "OK", body = LayerCollection,
            example = json!({
                "id": {
                  "providerId": "1c3b8042-300b-485c-95b5-0147d9dc068d",
                  "collectionId": "f2424474-ef24-4c18-ab84-68592e12ce48"
                },
                "name": "Layer Providers",
                "description": "All available Geo Engine layer providers",
                "items": [
                  {
                    "type": "collection",
                    "id": {
                      "providerId": "ac50ed0d-c9a0-41f8-9ce8-35fc9e38299b",
                      "collectionId": "546073b6-d535-4205-b601-99675c9f6dd7"
                    },
                    "name": "Datasets",
                    "description": "Basic Layers for all Datasets"
                  },
                  {
                    "type": "collection",
                    "id": {
                      "providerId": "ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74",
                      "collectionId": "05102bb3-a855-4a37-8a8a-30026a91fef1"
                    },
                    "name": "Layers",
                    "description": "All available Geo Engine layers"
                  }
                ],
                "entryLabel": null,
                "properties": []
              })
        )
    ),
    params(
        LayerCollectionListOptions
    ),
    security(
        ("session_token" = [])
    )
)]
async fn list_root_collections_handler<C: Context>(
    _session: C::Session,
    ctx: web::Data<C>,
    options: web::Query<LayerCollectionListOptions>,
) -> Result<impl Responder> {
    let root_collection = get_layer_providers(options, ctx).await?;

    Ok(web::Json(root_collection))
}

async fn get_layer_providers<C: Context>(
    mut options: web::Query<LayerCollectionListOptions>,
    ctx: web::Data<C>,
) -> Result<LayerCollection> {
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
        let provider = match external.layer_provider(provider_listing.id).await {
            Ok(provider) => provider,
            Err(err) => {
                log::error!("Error loading provider: {err}");
                continue;
            }
        };

        let collection_id = match provider.root_collection_id().await {
            Ok(root) => root,
            Err(err) => {
                log::error!(
                    "Error loading provider {}, could not get root collection id: {err}",
                    provider_listing.id,
                );
                continue;
            }
        };

        providers.push(CollectionItem::Collection(LayerCollectionListing {
            id: ProviderLayerCollectionId {
                provider_id: provider_listing.id,
                collection_id,
            },
            name: provider_listing.name,
            description: provider_listing.description,
        }));
    }
    let root_collection = LayerCollection {
        id: ProviderLayerCollectionId {
            provider_id: ROOT_PROVIDER_ID,
            collection_id: LayerCollectionId(ROOT_COLLECTION_ID.to_string()),
        },
        name: "Layer Providers".to_string(),
        description: "All available Geo Engine layer providers".to_string(),
        items: providers,
        entry_label: None,
        properties: vec![],
    };
    Ok(root_collection)
}

/// List the contents of the collection of the given provider
#[utoipa::path(
    tag = "Layers",
    get,
    path = "/layers/collections/{provider}/{collection}",
    responses(
        (status = 200, description = "OK", body = LayerCollection,
            example = json!({
                "id": {
                  "providerId": "ac50ed0d-c9a0-41f8-9ce8-35fc9e38299b",
                  "collectionId": "546073b6-d535-4205-b601-99675c9f6dd7"
                },
                "name": "Datasets",
                "description": "Basic Layers for all Datasets",
                "items": [
                  {
                    "type": "layer",
                    "id": {
                      "providerId": "ac50ed0d-c9a0-41f8-9ce8-35fc9e38299b",
                      "layerId": "9ee3619e-d0f9-4ced-9c44-3d407c3aed69"
                    },
                    "name": "Land Cover",
                    "description": "Land Cover derived from MODIS/Terra+Aqua Land Cover"
                  },
                  {
                    "type": "layer",
                    "id": {
                      "providerId": "ac50ed0d-c9a0-41f8-9ce8-35fc9e38299b",
                      "layerId": "36574dc3-560a-4b09-9d22-d5945f2b8093"
                    },
                    "name": "NDVI",
                    "description": "NDVI data from MODIS"
                  }
                ],
                "entryLabel": null,
                "properties": []
              })
        )
    ),
    params(
        ("provider" = DataProviderId, description = "Data provider id"),
        ("collection" = LayerCollectionId, description = "Layer collection id"),
        LayerCollectionListOptions
    ),
    security(
        ("session_token" = [])
    )
)]
async fn list_collection_handler<C: Context>(
    ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerCollectionId)>,
    options: web::Query<LayerCollectionListOptions>,
) -> Result<impl Responder> {
    let (provider, item) = path.into_inner();

    if provider == ROOT_PROVIDER_ID && item == LayerCollectionId(ROOT_COLLECTION_ID.to_string()) {
        let collection = get_layer_providers(options, ctx).await?;
        return Ok(web::Json(collection));
    }

    if provider == crate::datasets::storage::DATASET_DB_LAYER_PROVIDER_ID {
        let collection = ctx
            .dataset_db_ref()
            .collection(&item, options.into_inner().validated()?)
            .await?;

        return Ok(web::Json(collection));
    }

    if provider == crate::layers::storage::INTERNAL_PROVIDER_ID {
        let collection = ctx
            .layer_db_ref()
            .collection(&item, options.into_inner().validated()?)
            .await?;

        return Ok(web::Json(collection));
    }

    let collection = ctx
        .layer_provider_db_ref()
        .layer_provider(provider)
        .await?
        .collection(&item, options.into_inner().validated()?)
        .await?;

    Ok(web::Json(collection))
}

/// Retrieves the layer of the given provider
#[utoipa::path(
    tag = "Layers",
    get,
    path = "/layers/{provider}/{layer}",
    responses(
        (status = 200, description = "OK", body = Layer,
            example = json!({
                "id": {
                  "providerId": "ac50ed0d-c9a0-41f8-9ce8-35fc9e38299b",
                  "layerId": "9ee3619e-d0f9-4ced-9c44-3d407c3aed69"
                },
                "name": "Land Cover",
                "description": "Land Cover derived from MODIS/Terra+Aqua Land Cover",
                "workflow": {
                  "type": "Raster",
                  "operator": {
                    "type": "GdalSource",
                    "params": {
                      "data": {
                        "type": "internal",
                        "datasetId": "9ee3619e-d0f9-4ced-9c44-3d407c3aed69"
                      }
                    }
                  }
                },
                "symbology": {
                  "type": "raster",
                  "opacity": 1,
                  "colorizer": {
                    "type": "palette",
                    "colors": {
                      "0": [
                        134,
                        201,
                        227,
                        255
                      ],
                      "1": [
                        30,
                        129,
                        62,
                        255
                      ],
                      "2": [
                        59,
                        194,
                        212,
                        255
                      ],
                      "3": [
                        157,
                        194,
                        63,
                        255
                      ],
                      "4": [
                        159,
                        225,
                        127,
                        255
                      ],
                      "5": [
                        125,
                        194,
                        127,
                        255
                      ],
                      "6": [
                        195,
                        127,
                        126,
                        255
                      ],
                      "7": [
                        188,
                        221,
                        190,
                        255
                      ],
                      "8": [
                        224,
                        223,
                        133,
                        255
                      ],
                      "9": [
                        226,
                        221,
                        7,
                        255
                      ],
                      "10": [
                        223,
                        192,
                        125,
                        255
                      ],
                      "11": [
                        66,
                        128,
                        189,
                        255
                      ],
                      "12": [
                        225,
                        222,
                        127,
                        255
                      ],
                      "13": [
                        253,
                        2,
                        0,
                        255
                      ],
                      "14": [
                        162,
                        159,
                        66,
                        255
                      ],
                      "15": [
                        255,
                        255,
                        255,
                        255
                      ],
                      "16": [
                        192,
                        192,
                        192,
                        255
                      ]
                    },
                    "noDataColor": [
                      0,
                      0,
                      0,
                      0
                    ],
                    "defaultColor": [
                      0,
                      0,
                      0,
                      0
                    ]
                  }
                },
                "properties": [],
                "metadata": {}
              })
        )
    ),
    params(
        ("provider" = DataProviderId, description = "Data provider id"),
        ("layer" = LayerCollectionId, description = "Layer id"),
    ),
    security(
        ("session_token" = [])
    )
)]
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
