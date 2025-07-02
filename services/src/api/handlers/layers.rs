use crate::api::model::datatypes::{DataProviderId, LayerId};
use crate::api::model::responses::IdResponse;
use crate::config::get_config_element;
use crate::contexts::ApplicationContext;
use crate::datasets::{RasterDatasetFromWorkflow, schedule_raster_dataset_from_workflow_task};
use crate::error::Error::NotImplemented;
use crate::error::{Error, Result};
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
use geoengine_datatypes::primitives::{BandSelection, QueryRectangle};
use geoengine_operators::engine::WorkflowOperatorPath;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::IntoParams;

use super::tasks::TaskResponse;

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
                log::error!("Error loading provider: {err:?}");
                continue;
            }
        };

        if !provider.capabilities().listing {
            continue; // skip providers that do not support listing
        }

        let collection_id = match provider.get_root_layer_collection_id().await {
            Ok(root) => root,
            Err(err) => {
                log::error!(
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
            provider_id: ROOT_PROVIDER_ID.into(),
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

    if provider == crate::layers::storage::INTERNAL_PROVIDER_ID.into() {
        let collection = db
            .load_layer_collection(&item, options.into_inner())
            .await?;

        return Ok(web::Json(collection));
    }

    let collection = db
        .load_layer_provider(provider.into())
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
        ("provider" = DataProviderId, description = "Data provider id")
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

    let capabilities = match provider.into() {
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
        ("provider" = DataProviderId, description = "Data provider id", example = "ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74"),
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

    let collection = match provider.into() {
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
        ("provider" = DataProviderId, description = "Data provider id", example = "ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74"),
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

    let suggestions = match provider.into() {
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
        ("provider" = DataProviderId, description = "Data provider id"),
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

    if provider == crate::layers::storage::INTERNAL_PROVIDER_ID.into() {
        let collection = db.load_layer(&item.into()).await?;

        return Ok(web::Json(collection));
    }

    let collection = db
        .load_layer_provider(provider.into())
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
        ("provider" = DataProviderId, description = "Data provider id"),
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
    let layer = match provider.into() {
        crate::layers::storage::INTERNAL_PROVIDER_ID => db.load_layer(&item.into()).await?,
        _ => {
            db.load_layer_provider(provider.into())
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
        ("provider" = DataProviderId, description = "Data provider id"),
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

    let layer = match provider.into() {
        crate::layers::storage::INTERNAL_PROVIDER_ID => db.load_layer(&item).await?,
        _ => {
            db.load_layer_provider(provider.into())
                .await?
                .load_layer(&item)
                .await?
        }
    };

    let workflow_id = db.register_workflow(layer.workflow.clone()).await?;

    let execution_context = ctx.execution_context()?;

    let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

    let raster_operator = layer
        .workflow
        .operator
        .clone()
        .get_raster()?
        .initialize(workflow_operator_path_root, &execution_context)
        .await?;

    let result_descriptor = raster_operator.result_descriptor();

    let qr = QueryRectangle {
        spatial_bounds: result_descriptor.bbox.ok_or(
            Error::LayerResultDescriptorMissingFields {
                field: "bbox".to_string(),
                cause: "is None".to_string(),
            },
        )?,
        time_interval: result_descriptor
            .time
            .ok_or(Error::LayerResultDescriptorMissingFields {
                field: "time".to_string(),
                cause: "is None".to_string(),
            })?,
        spatial_resolution: result_descriptor.resolution.ok_or(
            Error::LayerResultDescriptorMissingFields {
                field: "spatial_resolution".to_string(),
                cause: "is None".to_string(),
            },
        )?,
        attributes: BandSelection::first(), // TODO: add to API
    };

    let from_workflow = RasterDatasetFromWorkflow {
        name: None,
        display_name: layer.name,
        description: Some(layer.description),
        query: qr.into(),
        as_cog: true,
    };

    let compression_num_threads =
        get_config_element::<crate::config::Gdal>()?.compression_num_threads;

    let task_id = schedule_raster_dataset_from_workflow_task(
        format!("layer {item}"),
        workflow_id,
        layer.workflow,
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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::api::model::responses::ErrorResponse;
    use crate::config::get_config_element;
    use crate::contexts::PostgresContext;
    use crate::contexts::SessionId;
    use crate::datasets::RasterDatasetFromWorkflowResult;
    use crate::ge_context;
    use crate::layers::layer::Layer;
    use crate::layers::storage::INTERNAL_PROVIDER_ID;
    use crate::tasks::util::test::wait_for_task_to_finish;
    use crate::tasks::{TaskManager, TaskStatus};
    use crate::users::{UserAuth, UserSession};
    use crate::util::tests::admin_login;
    use crate::util::tests::{
        MockQueryContext, TestDataUploads, read_body_string, send_test_request,
    };
    use crate::{contexts::Session, workflows::workflow::Workflow};
    use actix_web::dev::ServiceResponse;
    use actix_web::{http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::primitives::{CacheHint, Coordinate2D};
    use geoengine_datatypes::primitives::{
        RasterQueryRectangle, SpatialPartition2D, TimeGranularity, TimeInterval,
    };
    use geoengine_datatypes::raster::{
        GeoTransform, Grid, GridShape, RasterDataType, RasterTile2D, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{
        ExecutionContext, InitializedRasterOperator, RasterBandDescriptors, RasterOperator,
        RasterResultDescriptor, SingleRasterOrVectorSource, TypedOperator,
    };
    use geoengine_operators::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_operators::processing::{TimeShift, TimeShiftParams};
    use geoengine_operators::source::{GdalSource, GdalSourceParameters};
    use geoengine_operators::util::raster_stream_to_geotiff::{
        GdalGeoTiffDatasetMetadata, GdalGeoTiffOptions, raster_stream_to_geotiff_bytes,
    };
    use geoengine_operators::{
        engine::VectorOperator,
        mock::{MockPointSource, MockPointSourceParams},
    };
    use std::sync::Arc;
    use tokio_postgres::NoTls;

    #[ge_context::test]
    async fn test_add_layer_to_collection(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let collection_id = ctx.db().get_root_layer_collection_id().await.unwrap();

        let req = test::TestRequest::post()
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

        let result: IdResponse<LayerId> = test::read_body_json(response).await;

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
                            params: MockPointSourceParams {
                                points: vec![(0.0, 0.1).into(), (1.0, 1.1).into()],
                            },
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

        let req = test::TestRequest::post()
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

        let req = test::TestRequest::post()
            .uri(&format!("/layerDb/collections/{collection_id}/collections"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(serde_json::json!({
                "name": "Foo",
                "description": "Bar",
            }));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let result: IdResponse<LayerCollectionId> = test::read_body_json(response).await;

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

        let req = test::TestRequest::put()
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
                        },
                    }
                    .boxed(),
                ),
            },
            symbology: None,
            metadata: Default::default(),
            properties: Default::default(),
        };

        let req = test::TestRequest::put()
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
        let req = test::TestRequest::post()
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

        let req = test::TestRequest::put()
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

        let req = test::TestRequest::delete()
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

        let req = test::TestRequest::post()
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
                            params: MockPointSourceParams {
                                points: vec![(0.0, 0.1).into(), (1.0, 1.1).into()],
                            },
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

        let req = test::TestRequest::delete()
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

        let req = test::TestRequest::delete()
            .uri(&format!("/layerDb/collections/{collection_id}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        ctx.db()
            .load_layer_collection(&collection_id, LayerCollectionListOptions::default())
            .await
            .unwrap_err();

        // try removing root collection id --> should fail

        let req = test::TestRequest::delete()
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

        let req = test::TestRequest::delete()
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

        let req = test::TestRequest::get()
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

        let req = test::TestRequest::get()
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

        let req = test::TestRequest::get()
            .uri(&format!(
                "/layers/collections/search/autocomplete/{INTERNAL_PROVIDER_ID}/{root_collection_id}?limit=5&offset=0&searchType=fulltext&searchString=x"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");
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
        fn new(
            has_time: bool,
            has_bbox: bool,
            has_resolution: bool,
            time_shift_millis: i32,
        ) -> Self {
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

            let raster_source = MockRasterSource {
                params: MockRasterSourceParams {
                    data,
                    result_descriptor: RasterResultDescriptor {
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
                        bbox: if has_bbox {
                            Some(SpatialPartition2D::new_unchecked(
                                (0., 2.).into(),
                                (2., 0.).into(),
                            ))
                        } else {
                            None
                        },
                        resolution: if has_resolution {
                            Some(GeoTransform::test_default().spatial_resolution())
                        } else {
                            None
                        },
                        bands: RasterBandDescriptors::new_single_band(),
                    },
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
                origin_coordinate: (0., 0.).into(),
                tile_size_in_pixels: GridShape::new([2, 2]),
            };

            let query_rectangle = RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new((0., 2.).into(), (2., 0.).into()).unwrap(),
                time_interval: TimeInterval::new_unchecked(
                    1_671_868_800_000 + i64::from(time_shift_millis),
                    1_672_041_600_000 + i64::from(time_shift_millis),
                ),
                spatial_resolution: GeoTransform::test_default().spatial_resolution(),
                attributes: BandSelection::first(),
            };

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
        let req = test::TestRequest::post()
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

    async fn raster_operator_to_geotiff_bytes<C: SessionContext>(
        ctx: &C,
        operator: Box<dyn RasterOperator>,
        query_rectangle: RasterQueryRectangle,
    ) -> geoengine_operators::util::Result<Vec<Vec<u8>>> {
        let exe_ctx = ctx.execution_context().unwrap();
        let query_ctx = ctx.mock_query_context().unwrap();

        let initialized_operator = operator
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();
        let query_processor = initialized_operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        raster_stream_to_geotiff_bytes(
            query_processor,
            query_rectangle,
            query_ctx,
            GdalGeoTiffDatasetMetadata {
                no_data_value: Some(0.),
                spatial_reference: SpatialReference::epsg_4326(),
            },
            GdalGeoTiffOptions {
                compression_num_threads: get_config_element::<crate::config::Gdal>()
                    .unwrap()
                    .compression_num_threads,
                as_cog: true,
                force_big_tiff: false,
            },
            None,
            Box::pin(futures::future::pending()),
            exe_ctx.tiling_specification(),
        )
        .await
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
        let workflow_result = raster_operator_to_geotiff_bytes(
            &ctx,
            workflow_operator,
            mock_source.query_rectangle.clone(),
        )
        .await
        .unwrap();

        // query the newly created dataset
        let dataset_operator = GdalSource {
            params: GdalSourceParameters {
                data: response.dataset.into(),
            },
        }
        .boxed();
        let dataset_result = raster_operator_to_geotiff_bytes(
            &ctx,
            dataset_operator,
            mock_source.query_rectangle.clone(),
        )
        .await
        .unwrap();

        assert_eq!(workflow_result.as_slice(), dataset_result.as_slice());
    }

    fn test_raster_layer_to_dataset_success_tiling_spec() -> TilingSpecification {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, true, true, 0);
        mock_source.tiling_specification
    }

    #[ge_context::test(tiling_spec = "test_raster_layer_to_dataset_success_tiling_spec")]
    async fn test_raster_layer_to_dataset_success(app_ctx: PostgresContext<NoTls>) {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, true, true, 0);
        raster_layer_to_dataset_success(app_ctx, mock_source).await;
    }

    fn test_raster_layer_with_timeshift_to_dataset_success_tiling_spec() -> TilingSpecification {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, true, true, 1_000);
        mock_source.tiling_specification
    }

    #[ge_context::test(
        tiling_spec = "test_raster_layer_with_timeshift_to_dataset_success_tiling_spec"
    )]
    async fn test_raster_layer_with_timeshift_to_dataset_success(app_ctx: PostgresContext<NoTls>) {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, true, true, 1_000);
        raster_layer_to_dataset_success(app_ctx, mock_source).await;
    }

    fn test_raster_layer_to_dataset_no_time_interval_tiling_spec() -> TilingSpecification {
        let mock_source = MockRasterWorkflowLayerDescription::new(false, true, true, 0);
        mock_source.tiling_specification
    }

    #[ge_context::test(tiling_spec = "test_raster_layer_to_dataset_no_time_interval_tiling_spec")]
    async fn test_raster_layer_to_dataset_no_time_interval(app_ctx: PostgresContext<NoTls>) {
        let mock_source = MockRasterWorkflowLayerDescription::new(false, true, true, 0);

        let session = admin_login(&app_ctx).await;

        let session_id = session.id();

        let layer = mock_source.create_layer_in_context(&app_ctx).await;

        let res = send_dataset_creation_test_request(app_ctx, layer, session_id).await;

        ErrorResponse::assert(
            res,
            400,
            "LayerResultDescriptorMissingFields",
            "Result Descriptor field 'time' is None",
        )
        .await;
    }

    fn test_raster_layer_to_dataset_no_bounding_box_tiling_spec() -> TilingSpecification {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, false, true, 0);
        mock_source.tiling_specification
    }

    #[ge_context::test(tiling_spec = "test_raster_layer_to_dataset_no_bounding_box_tiling_spec")]
    async fn test_raster_layer_to_dataset_no_bounding_box(app_ctx: PostgresContext<NoTls>) {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, false, true, 0);

        let session = admin_login(&app_ctx).await;

        let session_id = session.id();

        let layer = mock_source.create_layer_in_context(&app_ctx).await;

        let res = send_dataset_creation_test_request(app_ctx, layer, session_id).await;

        ErrorResponse::assert(
            res,
            400,
            "LayerResultDescriptorMissingFields",
            "Result Descriptor field 'bbox' is None",
        )
        .await;
    }

    fn test_raster_layer_to_dataset_no_spatial_resolution_tiling_spec() -> TilingSpecification {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, true, false, 0);
        mock_source.tiling_specification
    }

    #[ge_context::test(
        tiling_spec = "test_raster_layer_to_dataset_no_spatial_resolution_tiling_spec"
    )]
    async fn test_raster_layer_to_dataset_no_spatial_resolution(app_ctx: PostgresContext<NoTls>) {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, true, false, 0);

        let session = admin_login(&app_ctx).await;

        let session_id = session.id();

        let layer = mock_source.create_layer_in_context(&app_ctx).await;

        let res = send_dataset_creation_test_request(app_ctx, layer, session_id).await;

        ErrorResponse::assert(
            res,
            400,
            "LayerResultDescriptorMissingFields",
            "Result Descriptor field 'spatial_resolution' is None",
        )
        .await;
    }
}
