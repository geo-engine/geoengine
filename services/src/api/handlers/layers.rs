use std::sync::Arc;

use super::tasks::TaskResponse;

use crate::api::model::datatypes::LayerId;
use crate::api::model::responses::IdResponse;
use crate::api::model::services::LayerProviderListing;
use crate::api::model::services::TypedDataProviderDefinition;
use crate::config::get_config_element;
use crate::contexts::ApplicationContext;
use crate::datasets::{
    RasterDatasetFromWorkflowParams, schedule_raster_dataset_from_workflow_task,
};
use crate::error::Error::NotImplemented;
use crate::error::Result;
use crate::layers::layer::{
    AddLayer, AddLayerCollection, CollectionItem, Layer, LayerCollection, LayerCollectionListing,
    ProviderLayerCollectionId, UpdateLayer, UpdateLayerCollection,
};
use crate::layers::listing::{
    LayerCollectionId, LayerCollectionProvider, ProviderCapabilities, SearchParameters,
};
use crate::layers::storage::{LayerDb, LayerProviderDb, LayerProviderListingOptions};
use crate::util::extractors::{ValidatedJson, ValidatedQuery};
use crate::util::workflows::validate_workflow;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;
use crate::{contexts::SessionContext, layers::layer::LayerCollectionListOptions};
use actix_web::{FromRequest, HttpResponse, Responder, web};
use geoengine_datatypes::dataset::DataProviderId;

use serde::{Deserialize, Serialize};
use utoipa::IntoParams;

pub const ROOT_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0x1c3b_8042_300b_485c_95b5_0147_d9dc_068d);

pub const ROOT_COLLECTION_ID: DataProviderId =
    DataProviderId::from_u128(0xf242_4474_ef24_4c18_ab84_6859_2e12_ce48);

pub(crate) fn init_layer_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(
        web::scope("/layers")
            .service(
                web::scope("/collections")
                    .service(
                        web::scope("/search")
                            .route(
                                "/autocomplete/{provider}/{collection}",
                                web::get().to(autocomplete_handler::<C>),
                            )
                            .route(
                                "/{provider}/{collection}",
                                web::get().to(search_handler::<C>),
                            ),
                    )
                    .route("", web::get().to(list_root_collections_handler::<C>))
                    .route(
                        "/{provider}/{collection}",
                        web::get().to(list_collection_handler::<C>),
                    ),
            )
            .service(
                web::scope("/{provider}")
                    .route(
                        "/capabilities",
                        web::get().to(provider_capabilities_handler::<C>),
                    )
                    .service(
                        web::scope("/{layer}")
                            .route(
                                "/workflowId",
                                web::post().to(layer_to_workflow_id_handler::<C>),
                            )
                            .route("/dataset", web::post().to(layer_to_dataset::<C>))
                            .route("", web::get().to(layer_handler::<C>)),
                    ),
            ),
    )
    .service(
        web::scope("/layerDb")
            .service(
                web::scope("/layers/{layer}")
                    .route("", web::put().to(update_layer::<C>))
                    .route("", web::delete().to(remove_layer::<C>)),
            )
            .service(
                web::scope("/collections/{collection}")
                    .service(
                        web::scope("/layers")
                            .route("", web::post().to(add_layer::<C>))
                            .service(
                                web::resource("/{layer}")
                                    .route(web::post().to(add_existing_layer_to_collection::<C>))
                                    .route(web::delete().to(remove_layer_from_collection::<C>)),
                            ),
                    )
                    .service(
                        web::scope("/collections")
                            .route("", web::post().to(add_collection::<C>))
                            .service(
                                web::resource("/{sub_collection}")
                                    .route(
                                        web::post().to(add_existing_collection_to_collection::<C>),
                                    )
                                    .route(
                                        web::delete().to(remove_collection_from_collection::<C>),
                                    ),
                            ),
                    )
                    .route("", web::put().to(update_collection::<C>))
                    .route("", web::delete().to(remove_collection::<C>)),
            )
            .service(
                web::scope("/providers")
                    .route("", web::post().to(add_provider::<C>))
                    .route("", web::get().to(list_providers::<C>))
                    .service(
                        web::resource("/{provider}")
                            .route(web::get().to(get_provider_definition::<C>))
                            .route(web::put().to(update_provider_definition::<C>))
                            .route(web::delete().to(delete_provider::<C>)),
                    ),
            ),
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
async fn list_root_collections_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    options: ValidatedQuery<LayerCollectionListOptions>,
) -> Result<impl Responder> {
    let root_collection = get_layer_providers(session, options, app_ctx).await?;

    Ok(web::Json(root_collection))
}

async fn get_layer_providers<C: ApplicationContext>(
    session: C::Session,
    mut options: ValidatedQuery<LayerCollectionListOptions>,
    app_ctx: web::Data<C>,
) -> Result<LayerCollection> {
    let mut providers = vec![];
    if options.offset == 0 && options.limit > 0 {
        providers.push(CollectionItem::Collection(LayerCollectionListing {
            r#type: Default::default(),
            id: ProviderLayerCollectionId {
                provider_id: crate::layers::storage::INTERNAL_PROVIDER_ID,
                collection_id: LayerCollectionId(
                    crate::layers::storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID.to_string(),
                ),
            },
            name: "Data Catalog".to_string(),
            description: "Catalog of data and workflows".to_string(),
            properties: Default::default(),
        }));

        options.limit -= 1;
    }

    let external = app_ctx.session_context(session).db();
    for provider_listing in external
        .list_layer_providers(LayerProviderListingOptions {
            offset: options.offset,
            limit: options.limit,
        })
        .await?
    {
        if provider_listing.priority <= -1000 {
            continue; // skip providers that are disabled
        }

        // TODO: resolve providers in parallel
        let provider = match external.load_layer_provider(provider_listing.id).await {
            Ok(provider) => provider,
            Err(err) => {
                tracing::error!("Error loading provider: {err:?}");
                continue;
            }
        };

        if !provider.capabilities().listing {
            continue; // skip providers that do not support listing
        }

        let collection_id = match provider.get_root_layer_collection_id().await {
            Ok(root) => root,
            Err(err) => {
                tracing::error!(
                    "Error loading provider {}, could not get root collection id: {err:?}",
                    provider_listing.id,
                );
                continue;
            }
        };

        providers.push(CollectionItem::Collection(LayerCollectionListing {
            r#type: Default::default(),
            id: ProviderLayerCollectionId {
                provider_id: provider_listing.id,
                collection_id,
            },
            name: provider.name().to_owned(),
            description: provider.description().to_owned(),
            properties: Default::default(),
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
        ("provider" = crate::api::model::datatypes::DataProviderId, description = "Data provider id"),
        ("collection" = LayerCollectionId, description = "Layer collection id"),
        LayerCollectionListOptions
    ),
    security(
        ("session_token" = [])
    )
)]
async fn list_collection_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerCollectionId)>,
    options: ValidatedQuery<LayerCollectionListOptions>,
    session: C::Session,
) -> Result<impl Responder> {
    let (provider, item) = path.into_inner();

    if provider == ROOT_PROVIDER_ID && item == LayerCollectionId(ROOT_COLLECTION_ID.to_string()) {
        let collection = get_layer_providers(session.clone(), options, app_ctx).await?;
        return Ok(web::Json(collection));
    }

    let db = app_ctx.session_context(session).db();

    if provider == crate::layers::storage::INTERNAL_PROVIDER_ID {
        let collection = db
            .load_layer_collection(&item, options.into_inner())
            .await?;

        return Ok(web::Json(collection));
    }

    let collection = db
        .load_layer_provider(provider)
        .await?
        .load_layer_collection(&item, options.into_inner())
        .await?;

    Ok(web::Json(collection))
}

// Returns the capabilities of the given provider
#[utoipa::path(
    tag = "Layers",
    get,
    path = "/layers/{provider}/capabilities",
    responses(
        (status = 200, description = "OK", body = ProviderCapabilities,
            example = json!({
                "listing": true,
                "search": {
                    "search_types": {
                        "fulltext": true,
                        "prefix": true
                    },
                    "autocomplete": true,
                    "filters": [],
                }
            })
        )
    ),
    params(
        ("provider" = crate::api::model::datatypes::DataProviderId, description = "Data provider id")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn provider_capabilities_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    path: web::Path<DataProviderId>,
    session: C::Session,
) -> Result<web::Json<ProviderCapabilities>> {
    let provider = path.into_inner();

    if provider == ROOT_PROVIDER_ID {
        return Err(NotImplemented {
            message: "Global search is not supported".to_string(),
        });
    }

    let db = app_ctx.session_context(session).db();

    let capabilities = match provider {
        crate::layers::storage::INTERNAL_PROVIDER_ID => LayerCollectionProvider::capabilities(&db),
        provider => db.load_layer_provider(provider).await?.capabilities(),
    };

    Ok(web::Json(capabilities))
}

/// Searches the contents of the collection of the given provider
#[utoipa::path(
    tag = "Layers",
    get,
    path = "/layers/collections/search/{provider}/{collection}",
    responses(
        (status = 200, description = "OK", body = LayerCollection,
            example = json!({
                "id": {
                    "providerId": "ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74",
                    "collectionId": "05102bb3-a855-4a37-8a8a-30026a91fef1"
                },
                "name": "Layers",
                "description": "All available Geo Engine layers",
                "items": [
                    {
                        "type": "collection",
                        "id": {
                            "providerId": "ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74",
                            "collectionId": "a29f77cc-51ce-466b-86ef-d0ab2170bc0a"
                        },
                        "name": "An empty collection",
                        "description": "There is nothing here",
                        "properties": []
                    },
                    {
                        "type": "layer",
                        "id": {
                            "providerId": "ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74",
                            "layerId": "b75db46e-2b9a-4a86-b33f-bc06a73cd711"
                        },
                        "name": "Ports in Germany",
                        "description": "Natural Earth Ports point filtered with Germany polygon",
                        "properties": []
                    }
                ],
                "entryLabel": null,
                "properties": []
            })
        )
    ),
    params(
        ("provider" = crate::api::model::datatypes::DataProviderId, description = "Data provider id", example = "ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74"),
        ("collection" = LayerCollectionId, description = "Layer collection id", example = "05102bb3-a855-4a37-8a8a-30026a91fef1"),
        SearchParameters
    ),
    security(
        ("session_token" = [])
    )
)]
async fn search_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerCollectionId)>,
    options: ValidatedQuery<SearchParameters>,
) -> Result<web::Json<LayerCollection>> {
    let (provider, collection) = path.into_inner();

    if provider == ROOT_PROVIDER_ID {
        return Err(NotImplemented {
            message: "Global search is not supported".to_string(),
        });
    }

    let db = app_ctx.session_context(session).db();

    let collection = match provider {
        crate::layers::storage::INTERNAL_PROVIDER_ID => {
            LayerCollectionProvider::search(&db, &collection, options.into_inner()).await?
        }
        provider => {
            db.load_layer_provider(provider)
                .await?
                .search(&collection, options.into_inner())
                .await?
        }
    };

    Ok(web::Json(collection))
}

/// Autocompletes the search on the contents of the collection of the given provider
#[utoipa::path(
    tag = "Layers",
    get,
    path = "/layers/collections/search/autocomplete/{provider}/{collection}",
    responses(
        (status = 200, description = "OK", body = Vec<String>,
            example = json!(["An empty collection", "Ports in Germany"])
        )
    ),
    params(
        ("provider" = crate::api::model::datatypes::DataProviderId, description = "Data provider id", example = "ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74"),
        ("collection" = LayerCollectionId, description = "Layer collection id", example = "05102bb3-a855-4a37-8a8a-30026a91fef1"),
        SearchParameters
    ),
    security(
        ("session_token" = [])
    )
)]
async fn autocomplete_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerCollectionId)>,
    options: ValidatedQuery<SearchParameters>,
) -> Result<web::Json<Vec<String>>> {
    let (provider, collection) = path.into_inner();

    if provider == ROOT_PROVIDER_ID {
        return Err(NotImplemented {
            message: "Global search is not supported".to_string(),
        });
    }

    let db = app_ctx.session_context(session).db();

    let suggestions = match provider {
        crate::layers::storage::INTERNAL_PROVIDER_ID => {
            LayerCollectionProvider::autocomplete_search(&db, &collection, options.into_inner())
                .await?
        }
        provider => {
            db.load_layer_provider(provider)
                .await?
                .autocomplete_search(&collection, options.into_inner())
                .await?
        }
    };

    Ok(web::Json(suggestions))
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
        ("provider" = crate::api::model::datatypes::DataProviderId, description = "Data provider id"),
        ("layer" = LayerCollectionId, description = "Layer id"),
    ),
    security(
        ("session_token" = [])
    )
)]
async fn layer_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId)>,
    session: C::Session,
) -> Result<impl Responder> {
    let (provider, item) = path.into_inner();

    let db = app_ctx.session_context(session).db();

    if provider == crate::layers::storage::INTERNAL_PROVIDER_ID {
        let collection = db.load_layer(&item.into()).await?;

        return Ok(web::Json(collection));
    }

    let collection = db
        .load_layer_provider(provider)
        .await?
        .load_layer(&item.into())
        .await?;

    Ok(web::Json(collection))
}

/// Registers a layer from a provider as a workflow and returns the workflow id
#[utoipa::path(
    tag = "Layers",
    post,
    path = "/layers/{provider}/{layer}/workflowId",
    responses(
        (status = 200, response = IdResponse::<WorkflowId>)
    ),
    params(
        ("provider" = crate::api::model::datatypes::DataProviderId, description = "Data provider id"),
        ("layer" = LayerCollectionId, description = "Layer id"),
    ),
    security(
        ("session_token" = [])
    )
)]
async fn layer_to_workflow_id_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId)>,
    session: C::Session,
) -> Result<web::Json<IdResponse<WorkflowId>>> {
    let (provider, item) = path.into_inner();

    let db = app_ctx.session_context(session.clone()).db();
    let layer = match provider {
        crate::layers::storage::INTERNAL_PROVIDER_ID => db.load_layer(&item.into()).await?,
        _ => {
            db.load_layer_provider(provider)
                .await?
                .load_layer(&item.into())
                .await?
        }
    };

    let db = app_ctx.session_context(session).db();
    let workflow_id = db.register_workflow(layer.workflow).await?;

    Ok(web::Json(IdResponse::from(workflow_id)))
}

/// Persist a raster layer from a provider as a dataset.
#[utoipa::path(
    tag = "Layers",
    post,
    path = "/layers/{provider}/{layer}/dataset",
    responses(
        (status = 200, description = "Id of created task", body = TaskResponse,
            example = json!({
                "taskId": "7f8a4cfe-76ab-4972-b347-b197e5ef0f3c"
            })
        )
    ),
    params(
        ("provider" = crate::api::model::datatypes::DataProviderId, description = "Data provider id"),
        ("layer" = LayerId, description = "Layer id"),
    ),
    security(
        ("session_token" = [])
    )
)]
async fn layer_to_dataset<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId)>,
) -> Result<impl Responder> {
    let ctx = Arc::new(app_ctx.into_inner().session_context(session));

    let (provider, item) = path.into_inner();
    let item: geoengine_datatypes::dataset::LayerId = item.into();

    let db = ctx.db();

    let layer = match provider {
        crate::layers::storage::INTERNAL_PROVIDER_ID => db.load_layer(&item).await?,
        _ => {
            db.load_layer_provider(provider)
                .await?
                .load_layer(&item)
                .await?
        }
    };

    let workflow_id = db.register_workflow(layer.workflow.clone()).await?;

    let from_workflow = RasterDatasetFromWorkflowParams {
        name: None,
        display_name: layer.name,
        description: Some(layer.description),
        query: None,
        as_cog: true,
    };

    let compression_num_threads =
        get_config_element::<crate::config::Gdal>()?.compression_num_threads;

    let task_id = schedule_raster_dataset_from_workflow_task(
        format!("layer {item}"),
        workflow_id,
        ctx,
        from_workflow,
        compression_num_threads,
    )
    .await?;

    Ok(web::Json(TaskResponse::new(task_id)))
}

/// Add a new layer to a collection
#[utoipa::path(
    tag = "Layers",
    post,
    path = "/layerDb/collections/{collection}/layers",
    params(
        ("collection" = LayerCollectionId, description = "Layer collection id", example = "05102bb3-a855-4a37-8a8a-30026a91fef1"),
    ),
    request_body = AddLayer,
    responses(
        (status = 200, response = IdResponse::<LayerId>)
    ),
    security(
        ("session_token" = [])
    )
)]
async fn add_layer<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    collection: web::Path<LayerCollectionId>,
    request: web::Json<AddLayer>,
) -> Result<web::Json<IdResponse<LayerId>>> {
    let request = request.into_inner();
    let add_layer = request;

    let ctx = app_ctx.session_context(session);

    validate_workflow(&add_layer.workflow, &ctx.execution_context()?).await?;

    let id = ctx.db().add_layer(add_layer, &collection).await?.into();

    Ok(web::Json(IdResponse { id }))
}

/// Update a layer
#[utoipa::path(
    tag = "Layers",
    put,
    path = "/layerDb/layers/{layer}",
    params(
        ("layer" = LayerId, description = "Layer id", example = "05102bb3-a855-4a37-8a8a-30026a91fef1"),
    ),
    request_body = UpdateLayer,
    responses(
        (status = 200)
    ),
    security(
        ("session_token" = [])
    )
)]
async fn update_layer<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    layer: web::Path<LayerId>,
    request: ValidatedJson<UpdateLayer>,
) -> Result<HttpResponse> {
    let layer = layer.into_inner().into();
    let request = request.into_inner();

    let ctx = app_ctx.session_context(session);

    validate_workflow(&request.workflow, &ctx.execution_context()?).await?;

    ctx.db().update_layer(&layer, request).await?;

    Ok(HttpResponse::Ok().finish())
}

/// Remove a collection
#[utoipa::path(
    tag = "Layers",
    delete,
    path = "/layerDb/layers/{layer}",
    params(
        ("layer" = LayerId, description = "Layer id"),
    ),
    responses(
        (status = 200, description = "OK")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn remove_layer<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    layer: web::Path<LayerId>,
) -> Result<HttpResponse> {
    let layer = layer.into_inner().into();

    app_ctx
        .session_context(session)
        .db()
        .remove_layer(&layer)
        .await?;

    Ok(HttpResponse::Ok().finish())
}

/// Add a new collection to an existing collection
#[utoipa::path(
    tag = "Layers",
    post,
    path = "/layerDb/collections/{collection}/collections",
    params(
        ("collection" = LayerCollectionId, description = "Layer collection id", example = "05102bb3-a855-4a37-8a8a-30026a91fef1"),
    ),
    request_body = AddLayerCollection,
    responses(
        (status = 200, response = IdResponse::<LayerCollectionId>)
    ),
    security(
        ("session_token" = [])
    )
)]
async fn add_collection<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    collection: web::Path<LayerCollectionId>,
    request: web::Json<AddLayerCollection>,
) -> Result<web::Json<IdResponse<LayerCollectionId>>> {
    let add_collection = request.into_inner();

    let id = app_ctx
        .into_inner()
        .session_context(session)
        .db()
        .add_layer_collection(add_collection, &collection)
        .await?;

    Ok(web::Json(IdResponse { id }))
}

/// Update a collection
#[utoipa::path(
    tag = "Layers",
    put,
    path = "/layerDb/collections/{collection}",
    params(
        ("collection" = LayerCollectionId, description = "Layer collection id", example = "05102bb3-a855-4a37-8a8a-30026a91fef1"),
    ),
    request_body = UpdateLayerCollection,
    responses(
        (status = 200)
    ),
    security(
        ("session_token" = [])
    )
)]
async fn update_collection<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    collection: web::Path<LayerCollectionId>,
    request: ValidatedJson<UpdateLayerCollection>,
) -> Result<HttpResponse> {
    let collection = collection.into_inner();
    let update = request.into_inner();

    app_ctx
        .into_inner()
        .session_context(session)
        .db()
        .update_layer_collection(&collection, update)
        .await?;

    Ok(HttpResponse::Ok().finish())
}

/// Remove a collection
#[utoipa::path(
    tag = "Layers",
    delete,
    path = "/layerDb/collections/{collection}",
    params(
        ("collection" = LayerCollectionId, description = "Layer collection id"),
    ),
    responses(
        (status = 200, description = "OK")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn remove_collection<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    collection: web::Path<LayerCollectionId>,
) -> Result<HttpResponse> {
    app_ctx
        .session_context(session)
        .db()
        .remove_layer_collection(&collection)
        .await?;

    Ok(HttpResponse::Ok().finish())
}

// TODO: reflect in the API docs that these ids are usually UUIDs in the layer db
#[derive(Debug, Serialize, Deserialize, IntoParams)]
struct RemoveLayerFromCollectionParams {
    collection: LayerCollectionId,
    layer: LayerId,
}

/// Remove a layer from a collection
#[utoipa::path(
    tag = "Layers",
    delete,
    path = "/layerDb/collections/{collection}/layers/{layer}",
    params(
        ("collection" = LayerCollectionId, description = "Layer collection id"),
        ("layer" = LayerId, description = "Layer id"),
    ),
    responses(
        (status = 200, description = "OK")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn remove_layer_from_collection<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<RemoveLayerFromCollectionParams>,
) -> Result<HttpResponse> {
    app_ctx
        .session_context(session)
        .db()
        .remove_layer_from_collection(&path.layer.clone().into(), &path.collection)
        .await?;

    Ok(HttpResponse::Ok().finish())
}

#[derive(Debug, Serialize, Deserialize, IntoParams)]
struct AddExistingLayerToCollectionParams {
    collection: LayerCollectionId,
    layer: LayerId,
}

/// Add an existing layer to a collection
#[utoipa::path(
    tag = "Layers",
    post,
    path = "/layerDb/collections/{collection}/layers/{layer}",
    params(
        ("collection" = LayerCollectionId, description = "Layer collection id"),
        ("layer" = LayerId, description = "Layer id"),
    ),
    responses(
        (status = 200, description = "OK")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn add_existing_layer_to_collection<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<AddExistingLayerToCollectionParams>,
) -> Result<HttpResponse> {
    app_ctx
        .session_context(session)
        .db()
        .add_layer_to_collection(&path.layer.clone().into(), &path.collection)
        .await?;

    Ok(HttpResponse::Ok().finish())
}

#[derive(Debug, Serialize, Deserialize, IntoParams)]
struct CollectionAndSubCollectionParams {
    collection: LayerCollectionId,
    sub_collection: LayerCollectionId,
}

/// Add an existing collection to a collection
#[utoipa::path(
    tag = "Layers",
    post,
    path = "/layerDb/collections/{parent}/collections/{collection}",
    params(
        ("parent" = LayerCollectionId, description = "Parent layer collection id", example = "05102bb3-a855-4a37-8a8a-30026a91fef1"),
        ("collection" = LayerId, description = "Layer collection id"),
    ),
    responses(
        (status = 200, description = "OK")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn add_existing_collection_to_collection<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<CollectionAndSubCollectionParams>,
) -> Result<HttpResponse> {
    app_ctx
        .session_context(session)
        .db()
        .add_collection_to_parent(&path.sub_collection, &path.collection)
        .await?;

    Ok(HttpResponse::Ok().finish())
}

/// Delete a collection from a collection
#[utoipa::path(
    tag = "Layers",
    delete,
    path = "/layerDb/collections/{parent}/collections/{collection}",
    params(
        ("parent" = LayerCollectionId, description = "Parent layer collection id", example = "05102bb3-a855-4a37-8a8a-30026a91fef1"),
        ("collection" = LayerId, description = "Layer collection id"),
    ),
    responses(
        (status = 200, description = "OK")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn remove_collection_from_collection<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<CollectionAndSubCollectionParams>,
) -> Result<HttpResponse> {
    app_ctx
        .session_context(session)
        .db()
        .remove_layer_collection_from_parent(&path.sub_collection, &path.collection)
        .await?;

    Ok(HttpResponse::Ok().finish())
}

/// Add a new provider
#[utoipa::path(
    tag = "Layers",
    post,
    path = "/layerDb/providers",
    params(),
    request_body = TypedDataProviderDefinition,
    responses(
        (status = 200, response = IdResponse::<DataProviderId>)
    ),
    security(
        ("session_token" = [])
    )
)]
async fn add_provider<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    request: web::Json<TypedDataProviderDefinition>,
) -> Result<web::Json<IdResponse<DataProviderId>>> {
    let provider = request.into_inner().into();

    let id = app_ctx
        .into_inner()
        .session_context(session)
        .db()
        .add_layer_provider(provider)
        .await?;

    Ok(web::Json(IdResponse { id }))
}

/// List all providers
#[utoipa::path(
    tag = "Layers",
    get,
    path = "/layerDb/providers",
    params(LayerProviderListingOptions),
    responses(
        (status = 200, description = "OK", body = Vec<LayerProviderListing>)
    ),
    security(
        ("session_token" = [])
    )
)]
async fn list_providers<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    options: ValidatedQuery<LayerProviderListingOptions>,
) -> Result<web::Json<Vec<LayerProviderListing>>> {
    let providers = app_ctx
        .into_inner()
        .session_context(session)
        .db()
        .list_layer_providers(options.into_inner())
        .await?;

    Ok(web::Json(providers.into_iter().map(Into::into).collect()))
}

/// Get an existing provider's definition
#[utoipa::path(
    tag = "Layers",
    get,
    path = "/layerDb/providers/{provider}",
    params(
        ("provider" = uuid::Uuid, description = "Layer provider id"),
    ),
    responses(
        (status = 200, description = "OK", body = TypedDataProviderDefinition)
    ),
    security(
        ("session_token" = [])
    )
)]
async fn get_provider_definition<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<uuid::Uuid>,
) -> Result<web::Json<TypedDataProviderDefinition>> {
    let id = DataProviderId(path.into_inner());

    let provider = app_ctx
        .into_inner()
        .session_context(session)
        .db()
        .get_layer_provider_definition(id)
        .await?
        .into();

    Ok(web::Json(provider))
}

/// Update an existing provider's definition
#[utoipa::path(
    tag = "Layers",
    put,
    path = "/layerDb/providers/{provider}",
    params(
        ("provider" = uuid::Uuid, description = "Layer provider id"),
    ),
    request_body = TypedDataProviderDefinition,
    responses(
        (status = 200, description = "OK")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn update_provider_definition<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<uuid::Uuid>,
    request: web::Json<TypedDataProviderDefinition>,
) -> Result<HttpResponse> {
    let id = DataProviderId(path.into_inner());
    let definition = request.into_inner().into();

    app_ctx
        .into_inner()
        .session_context(session)
        .db()
        .update_layer_provider_definition(id, definition)
        .await?;

    Ok(HttpResponse::Ok().finish())
}

/// Delete an existing provider
#[utoipa::path(
    tag = "Layers",
    delete,
    path = "/layerDb/providers/{provider}",
    params(
        ("provider" = uuid::Uuid, description = "Layer provider id"),
    ),
    responses(
        (status = 200, description = "OK")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn delete_provider<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    path: web::Path<uuid::Uuid>,
) -> Result<HttpResponse> {
    let id = DataProviderId(path.into_inner());

    app_ctx
        .into_inner()
        .session_context(session)
        .db()
        .delete_layer_provider(id)
        .await?;

    Ok(HttpResponse::Ok().finish())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        api::model::responses::ErrorResponse,
        contexts::{PostgresContext, Session, SessionId},
        datasets::{
            RasterDatasetFromWorkflowResult,
            dataset_listing_provider::{
                DatasetLayerListingCollection, DatasetLayerListingProviderDefinition,
            },
            external::aruna::ArunaDataProviderDefinition,
        },
        ge_context,
        layers::{layer::Layer, storage::INTERNAL_PROVIDER_ID},
        tasks::{TaskManager, TaskStatus, util::test::wait_for_task_to_finish},
        users::{UserAuth, UserSession},
        util::tests::{TestDataUploads, admin_login, read_body_string, send_test_request},
        workflows::workflow::Workflow,
    };
    use actix_web::{
        dev::ServiceResponse,
        http::header,
        test::{self, TestRequest, read_body_json},
    };
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::{
        primitives::{
            BandSelection, CacheHint, CacheTtlSeconds, Coordinate2D, RasterQueryRectangle,
            TimeGranularity, TimeInterval,
        },
        raster::{
            GeoTransform, Grid, GridBoundingBox2D, GridShape, RasterDataType, RasterTile2D,
            TilingSpecification,
        },
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };
    use geoengine_operators::{
        engine::{
            RasterBandDescriptors, RasterOperator, RasterResultDescriptor,
            SingleRasterOrVectorSource, SpatialGridDescriptor, TypedOperator, VectorOperator,
        },
        mock::{MockPointSource, MockPointSourceParams, MockRasterSource, MockRasterSourceParams},
        processing::{TimeShift, TimeShiftParams},
        source::{GdalSource, GdalSourceParameters},
        util::test::assert_eq_two_raster_operator_res_u8,
    };
    use uuid::Uuid;

    use std::sync::Arc;
    use tokio_postgres::NoTls;

    #[ge_context::test]
    async fn test_add_layer_to_collection(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let collection_id = ctx.db().get_root_layer_collection_id().await.unwrap();

        let req = TestRequest::post()
            .uri(&format!("/layerDb/collections/{collection_id}/layers"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(serde_json::json!({
                "name": "Foo",
                "description": "Bar",
                "workflow": {
                  "type": "Vector",
                  "operator": {
                    "type": "MockPointSource",
                    "params": {
                      "points": [
                        { "x": 0.0, "y": 0.1 },
                        { "x": 1.0, "y": 1.1 }
                      ]
                    }
                  }
                },
                "symbology": null,
            }));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let result: IdResponse<LayerId> = read_body_json(response).await;

        ctx.db()
            .load_layer(&result.id.clone().into())
            .await
            .unwrap();

        let collection = ctx
            .db()
            .load_layer_collection(&collection_id, LayerCollectionListOptions::default())
            .await
            .unwrap();

        assert!(collection.items.iter().any(|item| match item {
            CollectionItem::Layer(layer) => layer.id.layer_id == result.id.clone().into(),
            CollectionItem::Collection(_) => false,
        }));
    }

    #[ge_context::test]
    async fn test_add_existing_layer_to_collection(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let root_collection_id = ctx.db().get_root_layer_collection_id().await.unwrap();

        let layer_id = ctx
            .db()
            .add_layer(
                AddLayer {
                    name: "Layer Name".to_string(),
                    description: "Layer Description".to_string(),
                    workflow: Workflow {
                        operator: MockPointSource {
                            params: MockPointSourceParams::new(vec![
                                (0.0, 0.1).into(),
                                (1.0, 1.1).into(),
                            ]),
                        }
                        .boxed()
                        .into(),
                    },
                    symbology: None,
                    metadata: Default::default(),
                    properties: Default::default(),
                },
                &root_collection_id,
            )
            .await
            .unwrap();

        let collection_id = ctx
            .db()
            .add_layer_collection(
                AddLayerCollection {
                    name: "Foo".to_string(),
                    description: "Bar".to_string(),
                    properties: Default::default(),
                },
                &root_collection_id,
            )
            .await
            .unwrap();

        let req = TestRequest::post()
            .uri(&format!(
                "/layerDb/collections/{collection_id}/layers/{layer_id}"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let collection = ctx
            .db()
            .load_layer_collection(&collection_id, LayerCollectionListOptions::default())
            .await
            .unwrap();
        assert_eq!(collection.items.len(), 1);
    }

    #[ge_context::test]
    async fn test_add_layer_collection(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let collection_id = ctx.db().get_root_layer_collection_id().await.unwrap();

        let req = TestRequest::post()
            .uri(&format!("/layerDb/collections/{collection_id}/collections"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(serde_json::json!({
                "name": "Foo",
                "description": "Bar",
            }));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let result: IdResponse<LayerCollectionId> = read_body_json(response).await;

        ctx.db()
            .load_layer_collection(&result.id, LayerCollectionListOptions::default())
            .await
            .unwrap();
    }

    #[ge_context::test]
    async fn test_update_layer_collection(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let collection_id = ctx
            .db()
            .add_layer_collection(
                AddLayerCollection {
                    name: "Foo".to_string(),
                    description: "Bar".to_string(),
                    properties: Default::default(),
                },
                &ctx.db().get_root_layer_collection_id().await.unwrap(),
            )
            .await
            .unwrap();

        let req = TestRequest::put()
            .uri(&format!("/layerDb/collections/{collection_id}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(serde_json::json!({
                "name": "Foo new",
                "description": "Bar new",
            }));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let result = ctx
            .db()
            .load_layer_collection(&collection_id, LayerCollectionListOptions::default())
            .await
            .unwrap();

        assert_eq!(result.name, "Foo new");
        assert_eq!(result.description, "Bar new");
    }

    #[ge_context::test]
    async fn test_update_layer(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let add_layer = AddLayer {
            name: "Foo".to_string(),
            description: "Bar".to_string(),
            properties: Default::default(),
            workflow: Workflow {
                operator: TypedOperator::Vector(
                    MockPointSource {
                        params: MockPointSourceParams {
                            points: vec![Coordinate2D::new(1., 2.); 3],
                            spatial_bounds: geoengine_operators::mock::SpatialBoundsDerive::Derive,
                        },
                    }
                    .boxed(),
                ),
            },
            symbology: None,
            metadata: Default::default(),
        };

        let layer_id = ctx
            .db()
            .add_layer(
                add_layer.clone(),
                &ctx.db().get_root_layer_collection_id().await.unwrap(),
            )
            .await
            .unwrap();

        let update_layer = UpdateLayer {
            name: "Foo new".to_string(),
            description: "Bar new".to_string(),
            workflow: Workflow {
                operator: TypedOperator::Vector(
                    MockPointSource {
                        params: MockPointSourceParams {
                            points: vec![Coordinate2D::new(4., 5.); 3],
                            spatial_bounds: geoengine_operators::mock::SpatialBoundsDerive::Derive,
                        },
                    }
                    .boxed(),
                ),
            },
            symbology: None,
            metadata: Default::default(),
            properties: Default::default(),
        };

        let req = TestRequest::put()
            .uri(&format!("/layerDb/layers/{layer_id}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(serde_json::json!(update_layer.clone()));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let result = ctx.db().load_layer(&layer_id).await.unwrap();

        assert_eq!(result.name, update_layer.name);
        assert_eq!(result.description, update_layer.description);
        assert_eq!(result.workflow, update_layer.workflow);
        assert_eq!(result.symbology, update_layer.symbology);
        assert_eq!(result.metadata, update_layer.metadata);
        assert_eq!(result.properties, update_layer.properties);
    }

    #[ge_context::test]
    async fn it_checks_for_workflow_validity(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let collection_id = ctx.db().get_root_layer_collection_id().await.unwrap();

        let invalid_workflow_layer = serde_json::json!({
            "name": "Foo",
            "description": "Bar",
            "workflow":{
                "type": "Raster",
                "operator": {
                    "type": "GdalSource",
                    "params": {
                    "data": "example"
                    }
                }
            }
        });
        let req = TestRequest::post()
            .uri(&format!("/layerDb/collections/{collection_id}/layers"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(invalid_workflow_layer.clone());

        let response = send_test_request(req, app_ctx.clone()).await;

        ErrorResponse::assert(
            response,
            400,
            "UnknownDatasetName",
            "Dataset name 'example' does not exist",
        )
        .await;

        let add_layer = AddLayer {
            name: "Foo".to_string(),
            description: "Bar".to_string(),
            properties: Default::default(),
            workflow: Workflow {
                operator: TypedOperator::Vector(
                    MockPointSource {
                        params: MockPointSourceParams {
                            points: vec![Coordinate2D::new(1., 2.); 3],
                            spatial_bounds: geoengine_operators::mock::SpatialBoundsDerive::Derive,
                        },
                    }
                    .boxed(),
                ),
            },
            symbology: None,
            metadata: Default::default(),
        };

        let layer_id = ctx
            .db()
            .add_layer(
                add_layer.clone(),
                &ctx.db().get_root_layer_collection_id().await.unwrap(),
            )
            .await
            .unwrap();

        let req = TestRequest::put()
            .uri(&format!("/layerDb/layers/{layer_id}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(invalid_workflow_layer);
        let response = send_test_request(req, app_ctx.clone()).await;

        ErrorResponse::assert(
            response,
            400,
            "UnknownDatasetName",
            "Dataset name 'example' does not exist",
        )
        .await;
    }

    #[ge_context::test]
    async fn test_remove_layer(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let add_layer = AddLayer {
            name: "Foo".to_string(),
            description: "Bar".to_string(),
            properties: Default::default(),
            workflow: Workflow {
                operator: TypedOperator::Vector(
                    MockPointSource {
                        params: MockPointSourceParams {
                            points: vec![Coordinate2D::new(1., 2.); 3],
                            spatial_bounds: geoengine_operators::mock::SpatialBoundsDerive::Derive,
                        },
                    }
                    .boxed(),
                ),
            },
            symbology: None,
            metadata: Default::default(),
        };

        let layer_id = ctx
            .db()
            .add_layer(
                add_layer.clone(),
                &ctx.db().get_root_layer_collection_id().await.unwrap(),
            )
            .await
            .unwrap();

        let req = TestRequest::delete()
            .uri(&format!("/layerDb/layers/{layer_id}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let result = ctx.db().load_layer(&layer_id).await;

        assert!(result.is_err());
    }

    #[ge_context::test]
    async fn test_add_existing_collection_to_collection(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let root_collection_id = ctx.db().get_root_layer_collection_id().await.unwrap();

        let collection_a_id = ctx
            .db()
            .add_layer_collection(
                AddLayerCollection {
                    name: "Foo".to_string(),
                    description: "Foo".to_string(),
                    properties: Default::default(),
                },
                &root_collection_id,
            )
            .await
            .unwrap();

        let collection_b_id = ctx
            .db()
            .add_layer_collection(
                AddLayerCollection {
                    name: "Bar".to_string(),
                    description: "Bar".to_string(),
                    properties: Default::default(),
                },
                &root_collection_id,
            )
            .await
            .unwrap();

        let req = TestRequest::post()
            .uri(&format!(
                "/layerDb/collections/{collection_a_id}/collections/{collection_b_id}"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let collection_a = ctx
            .db()
            .load_layer_collection(&collection_a_id, LayerCollectionListOptions::default())
            .await
            .unwrap();

        assert_eq!(collection_a.items.len(), 1);
    }

    #[ge_context::test]
    async fn test_remove_layer_from_collection(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let root_collection_id = ctx.db().get_root_layer_collection_id().await.unwrap();

        let collection_id = ctx
            .db()
            .add_layer_collection(
                AddLayerCollection {
                    name: "Foo".to_string(),
                    description: "Bar".to_string(),
                    properties: Default::default(),
                },
                &root_collection_id,
            )
            .await
            .unwrap();

        let layer_id = ctx
            .db()
            .add_layer(
                AddLayer {
                    name: "Layer Name".to_string(),
                    description: "Layer Description".to_string(),
                    workflow: Workflow {
                        operator: MockPointSource {
                            params: MockPointSourceParams::new(vec![
                                (0.0, 0.1).into(),
                                (1.0, 1.1).into(),
                            ]),
                        }
                        .boxed()
                        .into(),
                    },
                    symbology: None,
                    metadata: Default::default(),
                    properties: Default::default(),
                },
                &collection_id,
            )
            .await
            .unwrap();

        let req = TestRequest::delete()
            .uri(&format!(
                "/layerDb/collections/{collection_id}/layers/{layer_id}"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(
            response.status().is_success(),
            "{:?}: {:?}",
            response.response().head(),
            response.response().body()
        );

        // layer should be gone
        ctx.db().load_layer(&layer_id).await.unwrap_err();
    }

    #[ge_context::test]
    async fn test_remove_collection(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let root_collection_id = ctx.db().get_root_layer_collection_id().await.unwrap();

        let collection_id = ctx
            .db()
            .add_layer_collection(
                AddLayerCollection {
                    name: "Foo".to_string(),
                    description: "Bar".to_string(),
                    properties: Default::default(),
                },
                &root_collection_id,
            )
            .await
            .unwrap();

        let req = TestRequest::delete()
            .uri(&format!("/layerDb/collections/{collection_id}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        ctx.db()
            .load_layer_collection(&collection_id, LayerCollectionListOptions::default())
            .await
            .unwrap_err();

        // try removing root collection id --> should fail

        let req = TestRequest::delete()
            .uri(&format!("/layerDb/collections/{root_collection_id}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_client_error(), "{response:?}");
    }

    #[ge_context::test]
    async fn test_remove_collection_from_collection(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let root_collection_id = ctx.db().get_root_layer_collection_id().await.unwrap();

        let collection_id = ctx
            .db()
            .add_layer_collection(
                AddLayerCollection {
                    name: "Foo".to_string(),
                    description: "Bar".to_string(),
                    properties: Default::default(),
                },
                &root_collection_id,
            )
            .await
            .unwrap();

        let req = TestRequest::delete()
            .uri(&format!(
                "/layerDb/collections/{root_collection_id}/collections/{collection_id}"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let root_collection = ctx
            .db()
            .load_layer_collection(&root_collection_id, LayerCollectionListOptions::default())
            .await
            .unwrap();

        assert!(
            !root_collection
                .items
                .iter()
                .any(|item| item.name() == "Foo"),
            "{root_collection:#?}"
        );
    }

    #[ge_context::test]
    async fn test_search_capabilities(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let req = TestRequest::get()
            .uri(&format!("/layers/{INTERNAL_PROVIDER_ID}/capabilities",))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");
    }

    #[ge_context::test]
    async fn test_search(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let root_collection_id = ctx.db().get_root_layer_collection_id().await.unwrap();

        let req = TestRequest::get()
            .uri(&format!(
                "/layers/collections/search/{INTERNAL_PROVIDER_ID}/{root_collection_id}?limit=5&offset=0&searchType=fulltext&searchString=x"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");
    }

    #[ge_context::test]
    async fn test_search_autocomplete(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let root_collection_id = ctx.db().get_root_layer_collection_id().await.unwrap();

        let req = TestRequest::get()
            .uri(&format!(
                "/layers/collections/search/autocomplete/{INTERNAL_PROVIDER_ID}/{root_collection_id}?limit=5&offset=0&searchType=fulltext&searchString=x"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");
    }

    fn default_dataset_layer_listing_provider_definition() -> DatasetLayerListingProviderDefinition
    {
        DatasetLayerListingProviderDefinition {
            id: DataProviderId::from_u128(0xcbb2_1ee3_d15d_45c5_a175_6696_4adf_4e85),
            name: "User Data Listing".to_string(),
            description: "User specific datasets grouped by tags.".to_string(),
            priority: None,
            collections: vec![
                DatasetLayerListingCollection {
                    name: "User Uploads".to_string(),
                    description: "Datasets uploaded by the user.".to_string(),
                    tags: vec!["upload".to_string()],
                },
                DatasetLayerListingCollection {
                    name: "Workflows".to_string(),
                    description: "Datasets created from workflows.".to_string(),
                    tags: vec!["workflow".to_string()],
                },
                DatasetLayerListingCollection {
                    name: "All Datasets".to_string(),
                    description: "All datasets".to_string(),
                    tags: vec!["*".to_string()],
                },
            ],
        }
    }

    #[ge_context::test]
    async fn test_get_provider_definition(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let dataset_listing_provider = default_dataset_layer_listing_provider_definition();

        ctx.db()
            .add_layer_provider(
                crate::layers::external::TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                    dataset_listing_provider.clone(),
                ),
            )
            .await.unwrap();

        let req = test::TestRequest::get()
            .uri("/layerDb/providers/cbb21ee3-d15d-45c5-a175-66964adf4e85")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let response_provider =
            serde_json::from_str::<TypedDataProviderDefinition>(&read_body_string(response).await)
                .unwrap();
        assert_eq!(
            response_provider,
            TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                dataset_listing_provider.into()
            )
        );
    }

    #[ge_context::test]
    async fn test_add_provider_definition(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let dataset_listing_provider = default_dataset_layer_listing_provider_definition();

        let req = test::TestRequest::post()
            .uri("/layerDb/providers")
            .set_json(serde_json::json!(
                &TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                    dataset_listing_provider.clone().into(),
                )
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        assert_eq!(
            ctx.db()
                .get_layer_provider_definition(
                    DataProviderId::from_u128(0xcbb2_1ee3_d15d_45c5_a175_6696_4adf_4e85),
                )
                .await
                .unwrap(),
            crate::layers::external::TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                dataset_listing_provider
            )
        );
    }

    #[ge_context::test]
    async fn test_update_provider_definition(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let dataset_listing_provider = default_dataset_layer_listing_provider_definition();

        ctx.db()
            .add_layer_provider(
                crate::layers::external::TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                    dataset_listing_provider,
                ),
            )
            .await.unwrap();

        let dataset_listing_provider = DatasetLayerListingProviderDefinition {
            id: DataProviderId::from_u128(0xcbb2_1ee3_d15d_45c5_a175_6696_4adf_4e85),
            name: "Updated User Data Listing".to_string(),
            description: "Updated User specific datasets grouped by tags.".to_string(),
            priority: Some(2),
            collections: vec![
                DatasetLayerListingCollection {
                    name: "Updated User Uploads".to_string(),
                    description: "Datasets uploaded by the user.".to_string(),
                    tags: vec!["upload".to_string()],
                },
                DatasetLayerListingCollection {
                    name: "Workflows".to_string(),
                    description: "Datasets created from workflows.".to_string(),
                    tags: vec!["workflow".to_string()],
                },
            ],
        };

        let req = test::TestRequest::put()
            .uri("/layerDb/providers/cbb21ee3-d15d-45c5-a175-66964adf4e85")
            .set_json(serde_json::json!(
                &TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                    dataset_listing_provider.clone().into(),
                )
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        assert_eq!(
            ctx.db()
                .get_layer_provider_definition(
                    DataProviderId::from_u128(0xcbb2_1ee3_d15d_45c5_a175_6696_4adf_4e85),
                )
                .await
                .unwrap(),
            crate::layers::external::TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                dataset_listing_provider
            )
        );
    }

    #[ge_context::test]
    async fn test_delete_provider_definition(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let dataset_listing_provider = default_dataset_layer_listing_provider_definition();

        ctx.db()
            .add_layer_provider(
                crate::layers::external::TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                    dataset_listing_provider,
                ),
            )
            .await.unwrap();

        let req = test::TestRequest::delete()
            .uri("/layerDb/providers/cbb21ee3-d15d-45c5-a175-66964adf4e85")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        assert!(
            ctx.db()
                .get_layer_provider_definition(DataProviderId::from_u128(
                    0xcbb2_1ee3_d15d_45c5_a175_6696_4adf_4e85
                ),)
                .await
                .is_err()
        );
    }

    #[ge_context::test]
    async fn test_cannot_add_existing_provider_definition(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let dataset_listing_provider = default_dataset_layer_listing_provider_definition();

        ctx.db()
            .add_layer_provider(
                crate::layers::external::TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                    dataset_listing_provider.clone(),
                ),
            )
            .await.unwrap();

        let req = test::TestRequest::post()
            .uri("/layerDb/providers")
            .set_json(serde_json::json!(
                &TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                    dataset_listing_provider.clone().into(),
                )
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_client_error(), "{response:?}");

        assert_eq!(
            response.response().error().unwrap().to_string(),
            crate::error::Error::ProviderIdAlreadyExists {
                provider_id: DataProviderId::from_u128(0xcbb2_1ee3_d15d_45c5_a175_6696_4adf_4e85)
            }
            .to_string()
        );
    }

    #[ge_context::test]
    async fn test_cannot_update_provider_id(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let dataset_listing_provider = default_dataset_layer_listing_provider_definition();

        ctx.db()
            .add_layer_provider(
                crate::layers::external::TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                    dataset_listing_provider,
                ),
            )
            .await.unwrap();

        let dataset_listing_provider = DatasetLayerListingProviderDefinition {
            id: DataProviderId::from_u128(0xcbb2_1ee3_d15d_45c5_a175_6696_4adf_4e86),
            name: "Updated User Data Listing".to_string(),
            description: "Updated User specific datasets grouped by tags.".to_string(),
            priority: Some(2),
            collections: vec![],
        };

        let req = test::TestRequest::put()
            .uri("/layerDb/providers/cbb21ee3-d15d-45c5-a175-66964adf4e85")
            .set_json(serde_json::json!(
                &TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                    dataset_listing_provider.clone().into(),
                )
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_client_error(), "{response:?}");

        assert_eq!(
            response.response().error().unwrap().to_string(),
            crate::error::Error::ProviderIdUnmodifiable.to_string()
        );
    }

    #[ge_context::test]
    async fn test_cannot_update_provider_type(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let dataset_listing_provider = default_dataset_layer_listing_provider_definition();

        ctx.db()
            .add_layer_provider(
                crate::layers::external::TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                    dataset_listing_provider,
                ),
            )
            .await.unwrap();

        let aruna_provider = ArunaDataProviderDefinition {
            id: DataProviderId::from_u128(0xcbb2_1ee3_d15d_45c5_a175_6696_4adf_4e85),
            name: "Aruna".to_string(),
            description: String::new(),
            priority: None,
            api_url: String::new(),
            project_id: String::new(),
            api_token: String::new(),
            filter_label: String::new(),
            cache_ttl: CacheTtlSeconds::default(),
        };

        let req = test::TestRequest::put()
            .uri("/layerDb/providers/cbb21ee3-d15d-45c5-a175-66964adf4e85")
            .set_json(serde_json::json!(
                &TypedDataProviderDefinition::ArunaDataProviderDefinition(
                    aruna_provider.clone().into(),
                )
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_client_error(), "{response:?}");

        assert_eq!(
            response.response().error().unwrap().to_string(),
            crate::error::Error::ProviderTypeUnmodifiable.to_string()
        );
    }

    #[ge_context::test]
    async fn test_cannot_update_non_existing_provider(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;

        let session_id = session.id();

        let dataset_listing_provider = default_dataset_layer_listing_provider_definition();

        let req = test::TestRequest::put()
            .uri("/layerDb/providers/cbb21ee3-d15d-45c5-a175-66964adf4e85")
            .set_json(serde_json::json!(
                &TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(
                    dataset_listing_provider.clone().into(),
                )
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_client_error(), "{response:?}");

        assert_eq!(
            response.response().error().unwrap().to_string(),
            "A permission error occurred: Permission Owner for resource provider:cbb21ee3-d15d-45c5-a175-66964adf4e85 denied..".to_string()
        );
    }

    #[ge_context::test]
    async fn test_cannot_get_non_existing_provider_definition(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;

        let session_id = session.id();

        let req = test::TestRequest::get()
            .uri("/layerDb/providers/cbb21ee3-d15d-45c5-a175-66964adf4e85")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_client_error(), "{response:?}");

        assert_eq!(
            response.response().error().unwrap().to_string(),
            "TokioPostgres".to_string()
        );
    }

    struct MockRasterWorkflowLayerDescription {
        workflow: Workflow,
        tiling_specification: TilingSpecification,
        query_rectangle: RasterQueryRectangle,
        collection_name: String,
        collection_description: String,
        layer_name: String,
        layer_description: String,
    }

    impl MockRasterWorkflowLayerDescription {
        fn new(has_time: bool, time_shift_millis: i32) -> Self {
            let data: Vec<RasterTile2D<u8>> = vec![
                RasterTile2D {
                    time: TimeInterval::new_unchecked(1_671_868_800_000, 1_671_955_200_000),
                    tile_position: [-1, 0].into(),
                    band: 0,
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                    properties: Default::default(),
                    cache_hint: CacheHint::default(),
                },
                RasterTile2D {
                    time: TimeInterval::new_unchecked(1_671_955_200_000, 1_672_041_600_000),
                    tile_position: [-1, 0].into(),
                    band: 0,
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                    properties: Default::default(),
                    cache_hint: CacheHint::default(),
                },
            ];

            let result_descriptor = RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                time: if has_time {
                    Some(TimeInterval::new_unchecked(
                        1_671_868_800_000,
                        1_672_041_600_000,
                    ))
                } else {
                    None
                },
                spatial_grid: SpatialGridDescriptor::source_from_parts(
                    GeoTransform::test_default(),
                    GridBoundingBox2D::new_min_max(-2, 0, 0, 2).unwrap(),
                ),
                bands: RasterBandDescriptors::new_single_band(),
            };

            let raster_source = MockRasterSource {
                params: MockRasterSourceParams {
                    data,
                    result_descriptor,
                },
            }
            .boxed();

            let workflow = if time_shift_millis == 0 {
                Workflow {
                    operator: raster_source.into(),
                }
            } else {
                Workflow {
                    operator: TypedOperator::Raster(Box::new(TimeShift {
                        params: TimeShiftParams::Relative {
                            granularity: TimeGranularity::Millis,
                            value: time_shift_millis,
                        },
                        sources: SingleRasterOrVectorSource {
                            source: raster_source.into(),
                        },
                    })),
                }
            };

            let tiling_specification = TilingSpecification {
                tile_size_in_pixels: GridShape::new([2, 2]),
            };

            let query_rectangle = RasterQueryRectangle::new(
                GridBoundingBox2D::new_min_max(-2, -1, 0, 1).unwrap(),
                TimeInterval::new_unchecked(
                    1_671_868_800_000 - i64::from(time_shift_millis),
                    1_672_041_600_000 - i64::from(time_shift_millis),
                ),
                BandSelection::first(),
            );

            MockRasterWorkflowLayerDescription {
                workflow,
                tiling_specification,
                query_rectangle,
                collection_name: "Test Collection Name".to_string(),
                collection_description: "Test Collection Description".to_string(),
                layer_name: "Test Layer Name".to_string(),
                layer_description: "Test Layer Description".to_string(),
            }
        }

        async fn create_layer_in_context(&self, app_ctx: &PostgresContext<NoTls>) -> Layer {
            let session = admin_login(app_ctx).await;
            let ctx = app_ctx.session_context(session.clone());

            let root_collection_id = ctx.db().get_root_layer_collection_id().await.unwrap();

            let collection_id = ctx
                .db()
                .add_layer_collection(
                    AddLayerCollection {
                        name: self.collection_name.clone(),
                        description: self.collection_description.clone(),
                        properties: Default::default(),
                    },
                    &root_collection_id,
                )
                .await
                .unwrap();

            let layer_id = ctx
                .db()
                .add_layer(
                    AddLayer {
                        name: self.layer_name.clone(),
                        description: self.layer_description.clone(),
                        workflow: self.workflow.clone(),
                        symbology: None,
                        metadata: Default::default(),
                        properties: Default::default(),
                    },
                    &collection_id,
                )
                .await
                .unwrap();

            ctx.db().load_layer(&layer_id).await.unwrap()
        }
    }

    async fn send_dataset_creation_test_request(
        app_ctx: PostgresContext<NoTls>,
        layer: Layer,
        session_id: SessionId,
    ) -> ServiceResponse {
        let layer_id = layer.id.layer_id;
        let provider_id = layer.id.provider_id;

        // create dataset from workflow
        let req = TestRequest::post()
            .uri(&format!("/layers/{provider_id}/{layer_id}/dataset"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .append_header((header::CONTENT_TYPE, mime::APPLICATION_JSON));
        send_test_request(req, app_ctx.clone()).await
    }

    async fn create_dataset_request_with_result_success(
        ctx: PostgresContext<NoTls>,
        layer: Layer,
        session: UserSession,
    ) -> RasterDatasetFromWorkflowResult {
        let res = send_dataset_creation_test_request(ctx.clone(), layer, session.id()).await;
        assert_eq!(res.status(), 200, "{:?}", res.response());

        let task_response =
            serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

        let task_manager = Arc::new(ctx.session_context(session).tasks());
        wait_for_task_to_finish(task_manager.clone(), task_response.task_id).await;

        let status = task_manager
            .get_task_status(task_response.task_id)
            .await
            .unwrap();

        if let TaskStatus::Completed { info, .. } = status {
            info.as_any_arc()
                .downcast::<RasterDatasetFromWorkflowResult>()
                .unwrap()
                .as_ref()
                .clone()
        } else {
            panic!("Task must be completed");
        }
    }

    async fn raster_layer_to_dataset_success(
        app_ctx: PostgresContext<NoTls>,
        mock_source: MockRasterWorkflowLayerDescription,
    ) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let layer = mock_source.create_layer_in_context(&app_ctx).await;
        let response =
            create_dataset_request_with_result_success(app_ctx, layer, ctx.session().clone()).await;

        // automatically deletes uploads on drop
        let _test_uploads = TestDataUploads {
            uploads: vec![response.upload],
        };

        // query the layer
        let workflow_operator = mock_source.workflow.operator.get_raster().unwrap();

        // query the newly created dataset
        let dataset_operator = GdalSource {
            params: GdalSourceParameters::new(response.dataset.into()),
        }
        .boxed();

        assert_eq_two_raster_operator_res_u8(
            &ctx.execution_context().unwrap(),
            &ctx.query_context(Uuid::new_v4(), Uuid::new_v4()).unwrap(),
            workflow_operator,
            dataset_operator,
            mock_source.query_rectangle,
            false,
        )
        .await;
    }

    fn test_raster_layer_to_dataset_success_tiling_spec() -> TilingSpecification {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, 0);
        mock_source.tiling_specification
    }

    #[ge_context::test(tiling_spec = "test_raster_layer_to_dataset_success_tiling_spec")]
    async fn test_raster_layer_to_dataset_success(app_ctx: PostgresContext<NoTls>) {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, 0);
        raster_layer_to_dataset_success(app_ctx, mock_source).await;
    }

    fn test_raster_layer_with_timeshift_to_dataset_success_tiling_spec() -> TilingSpecification {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, 1_000);
        mock_source.tiling_specification
    }

    #[ge_context::test(
        tiling_spec = "test_raster_layer_with_timeshift_to_dataset_success_tiling_spec"
    )]
    async fn test_raster_layer_with_timeshift_to_dataset_success(app_ctx: PostgresContext<NoTls>) {
        let mock_source: MockRasterWorkflowLayerDescription =
            MockRasterWorkflowLayerDescription::new(true, 1_000);
        raster_layer_to_dataset_success(app_ctx, mock_source).await;
    }

    fn test_raster_layer_to_dataset_no_time_interval_tiling_spec() -> TilingSpecification {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, 0);
        mock_source.tiling_specification
    }

    #[ge_context::test(tiling_spec = "test_raster_layer_to_dataset_no_time_interval_tiling_spec")]
    async fn test_raster_layer_to_dataset_no_time_interval(app_ctx: PostgresContext<NoTls>) {
        let mock_source = MockRasterWorkflowLayerDescription::new(false, 0);

        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let layer = mock_source.create_layer_in_context(&app_ctx).await;

        let res = send_dataset_creation_test_request(app_ctx, layer, session_id).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        let task_response =
            serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

        let task_manager = Arc::new(ctx.tasks());
        wait_for_task_to_finish(task_manager.clone(), task_response.task_id).await;

        let status = task_manager
            .get_task_status(task_response.task_id)
            .await
            .unwrap();

        let error_res = if let TaskStatus::Failed { error, .. } = status {
            error
                .clone()
                .into_any_arc()
                .downcast::<crate::error::Error>()
                .unwrap()
        } else {
            panic!("Task must fail");
        };

        let crate::error::Error::LayerResultDescriptorMissingFields { field: f, cause: c } =
            error_res.as_ref()
        else {
            panic!("Error must be LayerResultDescriptorMissingFields")
        };

        assert_eq!(f, "time");
        assert_eq!(c, "is None");
    }
}
