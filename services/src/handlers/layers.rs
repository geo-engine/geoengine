use crate::api::model::datatypes::{DataProviderId, LayerId};
use crate::datasets::{schedule_raster_dataset_from_workflow_task, RasterDatasetFromWorkflow};
use crate::error::{Error, Result};
use crate::handlers::tasks::TaskResponse;
use crate::layers::layer::{
    AddLayer, AddLayerCollection, CollectionItem, LayerCollection, LayerCollectionListing,
    ProviderLayerCollectionId,
};
use crate::layers::listing::{
    DatasetLayerCollectionProvider, LayerCollectionId, LayerCollectionProvider,
};
use crate::layers::storage::{LayerDb, LayerProviderDb, LayerProviderListingOptions};
use crate::util::config::get_config_element;
use crate::util::user_input::UserInput;
use crate::util::IdResponse;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;
use crate::{contexts::Context, layers::layer::LayerCollectionListOptions};
use actix_web::{web, FromRequest, HttpResponse, Responder};
use geoengine_datatypes::primitives::QueryRectangle;
use serde::{Deserialize, Serialize};
use utoipa::IntoParams;

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
        web::scope("/layers")
            .service(
                web::scope("/collections")
                    .route("", web::get().to(list_root_collections_handler::<C>))
                    .route(
                        r#"/{provider}/{collection}"#,
                        web::get().to(list_collection_handler::<C>),
                    ),
            )
            .route(
                "/{provider}/{layer:.*}/workflowId",
                web::post().to(layer_to_workflow_id_handler::<C>),
            )
            .route(
                "/{provider}/{layer:.*}/dataset",
                web::post().to(layer_to_dataset::<C>),
            )
            .route("/{provider}/{layer}", web::get().to(layer_handler::<C>)),
    )
    .service(
        web::scope("/layerDb").service(
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
                                .route(web::post().to(add_existing_collection_to_collection::<C>))
                                .route(web::delete().to(remove_collection_from_collection::<C>)),
                        ),
                )
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
async fn list_root_collections_handler<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    options: web::Query<LayerCollectionListOptions>,
) -> Result<impl Responder> {
    let root_collection = get_layer_providers(session, options, ctx).await?;

    Ok(web::Json(root_collection))
}

async fn get_layer_providers<C: Context>(
    session: C::Session,
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
    let external = ctx.db(session);
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
        let provider = match external.load_layer_provider(provider_listing.id).await {
            Ok(provider) => provider,
            Err(err) => {
                log::error!("Error loading provider: {err}");
                continue;
            }
        };

        let collection_id = match provider.get_root_layer_collection_id().await {
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
    session: C::Session,
) -> Result<impl Responder> {
    let (provider, item) = path.into_inner();

    if provider == ROOT_PROVIDER_ID && item == LayerCollectionId(ROOT_COLLECTION_ID.to_string()) {
        let collection = get_layer_providers(session.clone(), options, ctx).await?;
        return Ok(web::Json(collection));
    }

    let db = ctx.db(session);

    if provider == crate::datasets::storage::DATASET_DB_LAYER_PROVIDER_ID {
        let collection = db
            .load_dataset_layer_collection(&item, options.into_inner().validated()?)
            .await?;

        return Ok(web::Json(collection));
    }

    if provider == crate::layers::storage::INTERNAL_PROVIDER_ID {
        let collection = db
            .load_layer_collection(&item, options.into_inner().validated()?)
            .await?;

        return Ok(web::Json(collection));
    }

    let collection = db
        .load_layer_provider(provider)
        .await?
        .load_layer_collection(&item, options.into_inner().validated()?)
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
    session: C::Session,
) -> Result<impl Responder> {
    let (provider, item) = path.into_inner();

    let db = ctx.db(session);

    if provider == crate::datasets::storage::DATASET_DB_LAYER_PROVIDER_ID {
        let collection = db.load_dataset_layer(&item).await?;

        return Ok(web::Json(collection));
    }

    if provider == crate::layers::storage::INTERNAL_PROVIDER_ID {
        let collection = db.load_layer(&item).await?;

        return Ok(web::Json(collection));
    }

    let collection = db
        .load_layer_provider(provider)
        .await?
        .load_layer(&item)
        .await?;

    Ok(web::Json(collection))
}

/// Registers a layer from a provider as a workflow and returns the workflow id
#[utoipa::path(
    tag = "Layers",
    post,
    path = "/layers/{provider}/{layer}/workflowId",
    responses(
        (status = 200, response = crate::api::model::responses::IdResponse)
    ),
    params(
        ("provider" = DataProviderId, description = "Data provider id"),
        ("layer" = LayerCollectionId, description = "Layer id"),
    ),
    security(
        ("session_token" = [])
    )
)]
async fn layer_to_workflow_id_handler<C: Context>(
    ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId)>,
    session: C::Session,
) -> Result<web::Json<IdResponse<WorkflowId>>> {
    let (provider, item) = path.into_inner();

    let db = ctx.db(session);
    let layer = match provider {
        crate::datasets::storage::DATASET_DB_LAYER_PROVIDER_ID => {
            db.load_dataset_layer(&item).await?
        }
        crate::layers::storage::INTERNAL_PROVIDER_ID => db.load_layer(&item).await?,
        _ => {
            db.load_layer_provider(provider)
                .await?
                .load_layer(&item)
                .await?
        }
    };

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
        ("layer" = LayerCollectionId, description = "Layer id"),
    ),
    security(
        ("session_token" = [])
    )
)]
async fn layer_to_dataset<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId)>,
) -> Result<impl Responder> {
    let (provider, item) = path.into_inner();

    let db = ctx.db(session.clone());

    let layer = match provider {
        crate::datasets::storage::DATASET_DB_LAYER_PROVIDER_ID => {
            db.load_dataset_layer(&item).await?
        }
        crate::layers::storage::INTERNAL_PROVIDER_ID => db.load_layer(&item).await?,
        _ => {
            db.load_layer_provider(provider)
                .await?
                .load_layer(&item)
                .await?
        }
    };

    let execution_context = ctx.execution_context(session.clone())?;

    let raster_operator = layer
        .workflow
        .operator
        .clone()
        .get_raster()?
        .initialize(&execution_context)
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
    };

    let from_workflow = RasterDatasetFromWorkflow {
        name: layer.name,
        description: Some(layer.description),
        query: qr,
        as_cog: true,
    };

    let compression_num_threads =
        get_config_element::<crate::util::config::Gdal>()?.compression_num_threads;

    let task_id = schedule_raster_dataset_from_workflow_task(
        layer.workflow,
        session,
        ctx.into_inner(),
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
        (status = 200, response = crate::api::model::responses::IdResponse)
    ),
    security(
        ("session_token" = [])
    )
)]
async fn add_layer<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    collection: web::Path<LayerCollectionId>,
    request: web::Json<AddLayer>,
) -> Result<web::Json<IdResponse<LayerId>>> {
    let request = request.into_inner();

    let add_layer = request.validated()?;

    let id = ctx.db(session).add_layer(add_layer, &collection).await?;

    Ok(web::Json(IdResponse { id }))
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
        (status = 200, response = crate::api::model::responses::IdResponse)
    ),
    security(
        ("session_token" = [])
    )
)]
async fn add_collection<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    collection: web::Path<LayerCollectionId>,
    request: web::Json<AddLayerCollection>,
) -> Result<web::Json<IdResponse<LayerCollectionId>>> {
    let add_collection = request.into_inner().validated()?;

    let id = ctx
        .db(session)
        .add_layer_collection(add_collection, &collection)
        .await?;

    Ok(web::Json(IdResponse { id }))
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
async fn remove_collection<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    collection: web::Path<LayerCollectionId>,
) -> Result<HttpResponse> {
    ctx.db(session).remove_layer_collection(&collection).await?;

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
async fn remove_layer_from_collection<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    path: web::Path<RemoveLayerFromCollectionParams>,
) -> Result<HttpResponse> {
    ctx.db(session)
        .remove_layer_from_collection(&path.layer, &path.collection)
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
async fn add_existing_layer_to_collection<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    path: web::Path<AddExistingLayerToCollectionParams>,
) -> Result<HttpResponse> {
    ctx.db(session)
        .add_layer_to_collection(&path.layer, &path.collection)
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
async fn add_existing_collection_to_collection<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    path: web::Path<CollectionAndSubCollectionParams>,
) -> Result<HttpResponse> {
    ctx.db(session)
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
async fn remove_collection_from_collection<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    path: web::Path<CollectionAndSubCollectionParams>,
) -> Result<HttpResponse> {
    ctx.db(session)
        .remove_layer_collection_from_parent(&path.sub_collection, &path.collection)
        .await?;

    Ok(HttpResponse::Ok().finish())
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use crate::contexts::{SessionId, SimpleContext, SimpleSession};
    use crate::datasets::RasterDatasetFromWorkflowResult;
    use crate::handlers::ErrorResponse;
    use crate::layers::layer::Layer;
    use crate::tasks::util::test::wait_for_task_to_finish;
    use crate::tasks::{TaskManager, TaskStatus};
    use crate::util::config::get_config_element;
    use crate::util::tests::{read_body_string, TestDataUploads};
    use crate::{
        contexts::{InMemoryContext, Session},
        util::tests::send_test_request,
        workflows::workflow::Workflow,
    };
    use actix_web::dev::ServiceResponse;
    use actix_web::{http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::primitives::{
        Measurement, RasterQueryRectangle, SpatialPartition2D, TimeGranularity, TimeInterval,
    };
    use geoengine_datatypes::raster::{
        GeoTransform, Grid, GridShape, RasterDataType, RasterTile2D, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{
        ExecutionContext, InitializedRasterOperator, RasterOperator, RasterResultDescriptor,
        SingleRasterOrVectorSource, TypedOperator,
    };
    use geoengine_operators::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_operators::processing::{TimeShift, TimeShiftParams};
    use geoengine_operators::source::{GdalSource, GdalSourceParameters};
    use geoengine_operators::util::raster_stream_to_geotiff::{
        raster_stream_to_geotiff_bytes, GdalGeoTiffDatasetMetadata, GdalGeoTiffOptions,
    };
    use geoengine_operators::{
        engine::VectorOperator,
        mock::{MockPointSource, MockPointSourceParams},
    };

    use super::*;

    #[tokio::test]
    async fn test_add_layer_to_collection() {
        let ctx = InMemoryContext::test_default();

        let session = ctx.default_session_ref().await.clone();
        let session_id = session.id();

        let collection_id = ctx
            .db(session.clone())
            .get_root_layer_collection_id()
            .await
            .unwrap();

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
        let response = send_test_request(req, ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let result: IdResponse<LayerId> = test::read_body_json(response).await;

        ctx.db(session.clone())
            .load_layer(&result.id)
            .await
            .unwrap();

        let collection = ctx
            .db(session.clone())
            .load_layer_collection(
                &collection_id,
                LayerCollectionListOptions::default().validated().unwrap(),
            )
            .await
            .unwrap();

        assert!(collection.items.iter().any(|item| match item {
            CollectionItem::Layer(layer) => layer.id.layer_id == result.id,
            CollectionItem::Collection(_) => false,
        }));
    }

    #[tokio::test]
    async fn test_add_existing_layer_to_collection() {
        let ctx = InMemoryContext::test_default();

        let session = ctx.default_session_ref().await.clone();
        let session_id = session.id();

        let root_collection_id = ctx
            .db(session.clone())
            .get_root_layer_collection_id()
            .await
            .unwrap();

        let layer_id = ctx
            .db(session.clone())
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
                }
                .validated()
                .unwrap(),
                &root_collection_id,
            )
            .await
            .unwrap();

        let collection_id = ctx
            .db(session.clone())
            .add_layer_collection(
                AddLayerCollection {
                    name: "Foo".to_string(),
                    description: "Bar".to_string(),
                }
                .validated()
                .unwrap(),
                &root_collection_id,
            )
            .await
            .unwrap();

        let req = test::TestRequest::post()
            .uri(&format!(
                "/layerDb/collections/{collection_id}/layers/{layer_id}"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let collection = ctx
            .db(session.clone())
            .load_layer_collection(
                &collection_id,
                LayerCollectionListOptions::default().validated().unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(collection.items.len(), 1);
    }

    #[tokio::test]
    async fn test_add_layer_collection() {
        let ctx = InMemoryContext::test_default();

        let session = ctx.default_session_ref().await.clone();
        let session_id = session.id();

        let collection_id = ctx
            .db(session.clone())
            .get_root_layer_collection_id()
            .await
            .unwrap();

        let req = test::TestRequest::post()
            .uri(&format!("/layerDb/collections/{collection_id}/collections"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(serde_json::json!({
                "name": "Foo",
                "description": "Bar",
            }));
        let response = send_test_request(req, ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let result: IdResponse<LayerCollectionId> = test::read_body_json(response).await;

        ctx.db(session.clone())
            .load_layer_collection(
                &result.id,
                LayerCollectionListOptions::default().validated().unwrap(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_add_existing_collection_to_collection() {
        let ctx = InMemoryContext::test_default();

        let session = ctx.default_session_ref().await.clone();
        let session_id = session.id();

        let root_collection_id = ctx
            .db(session.clone())
            .get_root_layer_collection_id()
            .await
            .unwrap();

        let collection_a_id = ctx
            .db(session.clone())
            .add_layer_collection(
                AddLayerCollection {
                    name: "Foo".to_string(),
                    description: "Foo".to_string(),
                }
                .validated()
                .unwrap(),
                &root_collection_id,
            )
            .await
            .unwrap();

        let collection_b_id = ctx
            .db(session.clone())
            .add_layer_collection(
                AddLayerCollection {
                    name: "Bar".to_string(),
                    description: "Bar".to_string(),
                }
                .validated()
                .unwrap(),
                &root_collection_id,
            )
            .await
            .unwrap();

        let req = test::TestRequest::post()
            .uri(&format!(
                "/layerDb/collections/{collection_a_id}/collections/{collection_b_id}"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let collection_a = ctx
            .db(session.clone())
            .load_layer_collection(
                &collection_a_id,
                LayerCollectionListOptions::default().validated().unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(collection_a.items.len(), 1);
    }

    #[tokio::test]
    async fn test_remove_layer_from_collection() {
        let ctx = InMemoryContext::test_default();

        let session = ctx.default_session_ref().await.clone();
        let session_id = session.id();

        let root_collection_id = ctx
            .db(session.clone())
            .get_root_layer_collection_id()
            .await
            .unwrap();

        let collection_id = ctx
            .db(session.clone())
            .add_layer_collection(
                AddLayerCollection {
                    name: "Foo".to_string(),
                    description: "Bar".to_string(),
                }
                .validated()
                .unwrap(),
                &root_collection_id,
            )
            .await
            .unwrap();

        let layer_id = ctx
            .db(session.clone())
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
                }
                .validated()
                .unwrap(),
                &collection_id,
            )
            .await
            .unwrap();

        let req = test::TestRequest::delete()
            .uri(&format!(
                "/layerDb/collections/{collection_id}/layers/{layer_id}"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, ctx.clone()).await;

        assert!(
            response.status().is_success(),
            "{:?}: {:?}",
            response.response().head(),
            response.response().body()
        );

        // layer should be gone
        ctx.db(session.clone())
            .load_layer(&layer_id)
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_remove_collection() {
        let ctx = InMemoryContext::test_default();

        let session = ctx.default_session_ref().await.clone();
        let session_id = session.id();

        let root_collection_id = ctx
            .db(session.clone())
            .get_root_layer_collection_id()
            .await
            .unwrap();

        let collection_id = ctx
            .db(session.clone())
            .add_layer_collection(
                AddLayerCollection {
                    name: "Foo".to_string(),
                    description: "Bar".to_string(),
                }
                .validated()
                .unwrap(),
                &root_collection_id,
            )
            .await
            .unwrap();

        let req = test::TestRequest::delete()
            .uri(&format!("/layerDb/collections/{collection_id}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        ctx.db(session.clone())
            .load_layer_collection(
                &collection_id,
                LayerCollectionListOptions::default().validated().unwrap(),
            )
            .await
            .unwrap_err();

        // try removing root collection id --> should fail

        let req = test::TestRequest::delete()
            .uri(&format!("/layers/collections/{root_collection_id}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, ctx.clone()).await;

        assert!(response.status().is_client_error(), "{response:?}");
    }

    #[tokio::test]
    async fn test_remove_collection_from_collection() {
        let ctx = InMemoryContext::test_default();

        let session = ctx.default_session_ref().await.clone();
        let session_id = session.id();

        let root_collection_id = ctx
            .db(session.clone())
            .get_root_layer_collection_id()
            .await
            .unwrap();

        let collection_id = ctx
            .db(session.clone())
            .add_layer_collection(
                AddLayerCollection {
                    name: "Foo".to_string(),
                    description: "Bar".to_string(),
                }
                .validated()
                .unwrap(),
                &root_collection_id,
            )
            .await
            .unwrap();

        let req = test::TestRequest::delete()
            .uri(&format!(
                "/layerDb/collections/{root_collection_id}/collections/{collection_id}"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let root_collection = ctx
            .db(session.clone())
            .load_layer_collection(
                &root_collection_id,
                LayerCollectionListOptions::default().validated().unwrap(),
            )
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
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                    properties: Default::default(),
                },
                RasterTile2D {
                    time: TimeInterval::new_unchecked(1_671_955_200_000, 1_672_041_600_000),
                    tile_position: [-1, 0].into(),
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                    properties: Default::default(),
                },
            ];

            let raster_source = MockRasterSource {
                params: MockRasterSourceParams {
                    data,
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Unitless,
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

        async fn create_layer_in_context(&self, ctx: &InMemoryContext) -> Layer {
            let session = ctx.default_session_ref().await.clone();

            let root_collection_id = ctx
                .db(session.clone())
                .get_root_layer_collection_id()
                .await
                .unwrap();

            let collection_id = ctx
                .db(session.clone())
                .add_layer_collection(
                    AddLayerCollection {
                        name: self.collection_name.clone(),
                        description: self.collection_description.clone(),
                    }
                    .validated()
                    .unwrap(),
                    &root_collection_id,
                )
                .await
                .unwrap();

            let layer_id = ctx
                .db(session.clone())
                .add_layer(
                    AddLayer {
                        name: self.layer_name.clone(),
                        description: self.layer_description.clone(),
                        workflow: self.workflow.clone(),
                        symbology: None,
                    }
                    .validated()
                    .unwrap(),
                    &collection_id,
                )
                .await
                .unwrap();

            ctx.db(session.clone()).load_layer(&layer_id).await.unwrap()
        }
    }

    async fn send_dataset_creation_test_request<C: SimpleContext>(
        ctx: &C,
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
        send_test_request(req, ctx.clone()).await
    }

    async fn create_dataset_request_with_result_success<C: SimpleContext>(
        ctx: &C,
        layer: Layer,
        session: SimpleSession,
    ) -> RasterDatasetFromWorkflowResult {
        let res = send_dataset_creation_test_request(ctx, layer, session.id()).await;
        assert_eq!(res.status(), 200, "{:?}", res.response());

        let task_response =
            serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

        let task_manager = Arc::new(ctx.tasks(session));
        wait_for_task_to_finish(task_manager.clone(), task_response.task_id).await;

        let status = task_manager
            .get_task_status(task_response.task_id)
            .await
            .unwrap();

        let response = if let TaskStatus::Completed { info, .. } = status {
            info.as_any_arc()
                .downcast::<RasterDatasetFromWorkflowResult>()
                .unwrap()
                .as_ref()
                .clone()
        } else {
            panic!("Task must be completed");
        };

        response
    }

    async fn raster_operator_to_geotiff_bytes<C: Context>(
        ctx: &C,
        session: C::Session,
        operator: Box<dyn RasterOperator>,
        query_rectangle: RasterQueryRectangle,
    ) -> geoengine_operators::util::Result<Vec<Vec<u8>>> {
        let exe_ctx = ctx.execution_context(session.clone()).unwrap();
        let query_ctx = ctx.query_context(session.clone()).unwrap();

        let initialized_operator = operator.initialize(&exe_ctx).await.unwrap();
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
                compression_num_threads: get_config_element::<crate::util::config::Gdal>()
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

    async fn raster_layer_to_dataset_success(mock_source: MockRasterWorkflowLayerDescription) {
        let ctx = InMemoryContext::new_with_context_spec(
            mock_source.tiling_specification,
            TestDefault::test_default(),
        );

        let session = ctx.default_session_ref().await.clone();

        let layer = mock_source.create_layer_in_context(&ctx).await;
        let response =
            create_dataset_request_with_result_success(&ctx, layer, session.clone()).await;

        // automatically deletes uploads on drop
        let _test_uploads = TestDataUploads {
            uploads: vec![response.upload],
        };

        // query the layer
        let workflow_operator = mock_source.workflow.operator.get_raster().unwrap();
        let workflow_result = raster_operator_to_geotiff_bytes(
            &ctx,
            session.clone(),
            workflow_operator,
            mock_source.query_rectangle,
        )
        .await
        .unwrap();

        // query the newly created dataset
        let dataset_id: geoengine_datatypes::dataset::DatasetId = response.dataset.into();
        let dataset_operator = GdalSource {
            params: GdalSourceParameters {
                data: dataset_id.into(),
            },
        }
        .boxed();
        let dataset_result = raster_operator_to_geotiff_bytes(
            &ctx,
            session,
            dataset_operator,
            mock_source.query_rectangle,
        )
        .await
        .unwrap();

        assert_eq!(workflow_result.as_slice(), dataset_result.as_slice());
    }

    #[tokio::test]
    async fn test_raster_layer_to_dataset_success() {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, true, true, 0);
        raster_layer_to_dataset_success(mock_source).await;
    }

    #[tokio::test]
    async fn test_raster_layer_with_timeshift_to_dataset_success() {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, true, true, 1_000);
        raster_layer_to_dataset_success(mock_source).await;
    }

    #[tokio::test]
    async fn test_raster_layer_to_dataset_no_time_interval() {
        let mock_source = MockRasterWorkflowLayerDescription::new(false, true, true, 0);
        let ctx = InMemoryContext::new_with_context_spec(
            mock_source.tiling_specification,
            TestDefault::test_default(),
        );

        let session_id = ctx.default_session_ref().await.id();

        let layer = mock_source.create_layer_in_context(&ctx).await;

        let res = send_dataset_creation_test_request(&ctx, layer, session_id).await;

        ErrorResponse::assert(
            res,
            400,
            "LayerResultDescriptorMissingFields",
            "Result Descriptor field 'time' is None",
        )
        .await;
    }

    #[tokio::test]
    async fn test_raster_layer_to_dataset_no_bounding_box() {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, false, true, 0);
        let ctx = InMemoryContext::new_with_context_spec(
            mock_source.tiling_specification,
            TestDefault::test_default(),
        );

        let session_id = ctx.default_session_ref().await.id();

        let layer = mock_source.create_layer_in_context(&ctx).await;

        let res = send_dataset_creation_test_request(&ctx, layer, session_id).await;

        ErrorResponse::assert(
            res,
            400,
            "LayerResultDescriptorMissingFields",
            "Result Descriptor field 'bbox' is None",
        )
        .await;
    }

    #[tokio::test]
    async fn test_raster_layer_to_dataset_no_spatial_resolution() {
        let mock_source = MockRasterWorkflowLayerDescription::new(true, true, false, 0);
        let ctx = InMemoryContext::new_with_context_spec(
            mock_source.tiling_specification,
            TestDefault::test_default(),
        );

        let session_id = ctx.default_session_ref().await.id();

        let layer = mock_source.create_layer_in_context(&ctx).await;

        let res = send_dataset_creation_test_request(&ctx, layer, session_id).await;

        ErrorResponse::assert(
            res,
            400,
            "LayerResultDescriptorMissingFields",
            "Result Descriptor field 'spatial_resolution' is None",
        )
        .await;
    }
}
