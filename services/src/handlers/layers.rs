use crate::api::model::datatypes::{DataProviderId, LayerId};
use crate::contexts::AdminSession;
use crate::error::Result;
use crate::layers::layer::{
    AddLayer, AddLayerCollection, CollectionItem, LayerCollection, LayerCollectionListing,
    ProviderLayerCollectionId,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::layers::storage::{LayerDb, LayerProviderDb, LayerProviderListingOptions};
use crate::util::user_input::UserInput;
use crate::util::IdResponse;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;
use crate::{contexts::Context, layers::layer::LayerCollectionListOptions};
use actix_web::{web, Either, FromRequest, HttpResponse, Responder};
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
                        r#"/{provider}/{collection:.+}"#,
                        web::get().to(list_collection_handler::<C>),
                    ),
            )
            .route(
                "/{provider}/{layer:.*}/workflowId",
                web::post().to(layer_to_workflow_id_handler::<C>),
            )
            .route("/{provider}/{layer:.+}", web::get().to(layer_handler::<C>)),
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
    _session: Either<AdminSession, C::Session>,
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

/// Registers a layer from a provider as a workflow and returns the workflow id
#[utoipa::path(
    tag = "Layers",
    post,
    path = "/layers/{provider}/{layer}/workflowId",
    responses(
        (status = 200, description = "OK", body = IdResponse<WorkflowId>,
            example = json!({
                "id": "36574dc3-560a-4b09-9d22-d5945f2b8093"
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
async fn layer_to_workflow_id_handler<C: Context>(
    ctx: web::Data<C>,
    path: web::Path<(DataProviderId, LayerId)>,
) -> Result<web::Json<WorkflowId>> {
    let (provider, item) = path.into_inner();

    let layer = match provider {
        crate::datasets::storage::DATASET_DB_LAYER_PROVIDER_ID => {
            ctx.dataset_db_ref().get_layer(&item).await?
        }
        crate::layers::storage::INTERNAL_PROVIDER_ID => ctx.layer_db_ref().get_layer(&item).await?,
        _ => {
            ctx.layer_provider_db_ref()
                .layer_provider(provider)
                .await?
                .get_layer(&item)
                .await?
        }
    };

    let workflow_id = ctx.workflow_registry().register(layer.workflow).await?;

    Ok(web::Json(workflow_id))
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
        (status = 200, description = "OK", body = IdResponse<LayerId>,
            example = json!({
                "id": "36574dc3-560a-4b09-9d22-d5945f2b8093"
            })
        )
    ),
    security(
        ("session_token" = [])
    )
)]
async fn add_layer<C: Context>(
    _session: AdminSession, // TODO: allow normal users to add layers to their stuff
    ctx: web::Data<C>,
    collection: web::Path<LayerCollectionId>,
    request: web::Json<AddLayer>,
) -> Result<web::Json<IdResponse<LayerId>>> {
    let request = request.into_inner();

    let add_layer = request.validated()?;

    let id = ctx.layer_db_ref().add_layer(add_layer, &collection).await?;

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
        (status = 200, description = "OK", body = IdResponse<LayerCollectionId>,
            example = json!({
                "id": "36574dc3-560a-4b09-9d22-d5945f2b8093"
            })
        )
    ),
    security(
        ("session_token" = [])
    )
)]
async fn add_collection<C: Context>(
    _session: AdminSession, // TODO: allow normal users to add collections to their stuff
    ctx: web::Data<C>,
    collection: web::Path<LayerCollectionId>,
    request: web::Json<AddLayerCollection>,
) -> Result<web::Json<IdResponse<LayerCollectionId>>> {
    let add_collection = request.into_inner().validated()?;

    let id = ctx
        .layer_db_ref()
        .add_collection(add_collection, &collection)
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
    _session: AdminSession, // TODO: allow normal users to remove their collections
    ctx: web::Data<C>,
    collection: web::Path<LayerCollectionId>,
) -> Result<HttpResponse> {
    ctx.layer_db_ref().remove_collection(&collection).await?;

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
    _session: AdminSession, // TODO: allow normal users to remove their collections
    ctx: web::Data<C>,
    path: web::Path<RemoveLayerFromCollectionParams>,
) -> Result<HttpResponse> {
    ctx.layer_db_ref()
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
    _session: AdminSession, // TODO: allow normal users to remove their collections
    ctx: web::Data<C>,
    path: web::Path<AddExistingLayerToCollectionParams>,
) -> Result<HttpResponse> {
    ctx.layer_db_ref()
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
    _session: AdminSession, // TODO: allow normal users to remove their collections
    ctx: web::Data<C>,
    path: web::Path<CollectionAndSubCollectionParams>,
) -> Result<HttpResponse> {
    ctx.layer_db_ref()
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
    _session: AdminSession, // TODO: allow normal users to remove their collections
    ctx: web::Data<C>,
    path: web::Path<CollectionAndSubCollectionParams>,
) -> Result<HttpResponse> {
    ctx.layer_db_ref()
        .remove_collection_from_parent(&path.sub_collection, &path.collection)
        .await?;

    Ok(HttpResponse::Ok().finish())
}

#[cfg(test)]
mod tests {

    use crate::{
        contexts::{InMemoryContext, Session},
        util::tests::send_test_request,
        workflows::workflow::Workflow,
    };
    use actix_web::{http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::{
        engine::VectorOperator,
        mock::{MockPointSource, MockPointSourceParams},
    };

    use super::*;

    #[tokio::test]
    async fn test_add_layer_to_collection() {
        let ctx = InMemoryContext::test_default();

        let admin_session_id = AdminSession::default().id();

        let collection_id = ctx.layer_db_ref().root_collection_id().await.unwrap();

        let req = test::TestRequest::post()
            .uri(&format!("/layerDb/collections/{collection_id}/layers"))
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session_id.to_string()),
            ))
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

        ctx.layer_db_ref().get_layer(&result.id).await.unwrap();

        let collection = ctx
            .layer_db_ref()
            .collection(
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

        let admin_session_id = AdminSession::default().id();

        let root_collection_id = ctx.layer_db_ref().root_collection_id().await.unwrap();

        let layer_id = ctx
            .layer_db_ref()
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
            .layer_db_ref()
            .add_collection(
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
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session_id.to_string()),
            ));
        let response = send_test_request(req, ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let collection = ctx
            .layer_db_ref()
            .collection(
                &collection_id,
                LayerCollectionListOptions::default().validated().unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(collection.items.len(), 1);
    }

    #[tokio::test]
    async fn test_add_collection() {
        let ctx = InMemoryContext::test_default();

        let admin_session_id = AdminSession::default().id();

        let collection_id = ctx.layer_db_ref().root_collection_id().await.unwrap();

        let req = test::TestRequest::post()
            .uri(&format!("/layerDb/collections/{collection_id}/collections"))
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session_id.to_string()),
            ))
            .set_json(serde_json::json!({
                "name": "Foo",
                "description": "Bar",
            }));
        let response = send_test_request(req, ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let result: IdResponse<LayerCollectionId> = test::read_body_json(response).await;

        ctx.layer_db_ref()
            .collection(
                &result.id,
                LayerCollectionListOptions::default().validated().unwrap(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_add_existing_collection_to_collection() {
        let ctx = InMemoryContext::test_default();

        let admin_session_id = AdminSession::default().id();

        let root_collection_id = ctx.layer_db_ref().root_collection_id().await.unwrap();

        let collection_a_id = ctx
            .layer_db_ref()
            .add_collection(
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
            .layer_db_ref()
            .add_collection(
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
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session_id.to_string()),
            ));
        let response = send_test_request(req, ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let collection_a = ctx
            .layer_db_ref()
            .collection(
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

        let root_collection_id = ctx.layer_db_ref().root_collection_id().await.unwrap();

        let collection_id = ctx
            .layer_db_ref()
            .add_collection(
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
            .layer_db_ref()
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

        let admin_session_id = AdminSession::default().id();

        let req = test::TestRequest::delete()
            .uri(&format!(
                "/layerDb/collections/{collection_id}/layers/{layer_id}"
            ))
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session_id.to_string()),
            ));
        let response = send_test_request(req, ctx.clone()).await;

        assert!(
            response.status().is_success(),
            "{:?}: {:?}",
            response.response().head(),
            response.response().body()
        );

        // layer should be gone
        ctx.layer_db_ref().get_layer(&layer_id).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_remove_collection() {
        let ctx = InMemoryContext::test_default();

        let root_collection_id = ctx.layer_db_ref().root_collection_id().await.unwrap();

        let collection_id = ctx
            .layer_db_ref()
            .add_collection(
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

        let admin_session_id = AdminSession::default().id();

        let req = test::TestRequest::delete()
            .uri(&format!("/layerDb/collections/{collection_id}"))
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session_id.to_string()),
            ));
        let response = send_test_request(req, ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        ctx.layer_db_ref()
            .collection(
                &collection_id,
                LayerCollectionListOptions::default().validated().unwrap(),
            )
            .await
            .unwrap_err();

        // try removing root collection id --> should fail

        let req = test::TestRequest::delete()
            .uri(&format!("/layers/collections/{root_collection_id}"))
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session_id.to_string()),
            ));
        let response = send_test_request(req, ctx.clone()).await;

        assert!(response.status().is_client_error(), "{response:?}");
    }

    #[tokio::test]
    async fn test_remove_collection_from_collection() {
        let ctx = InMemoryContext::test_default();

        let root_collection_id = ctx.layer_db_ref().root_collection_id().await.unwrap();

        let collection_id = ctx
            .layer_db_ref()
            .add_collection(
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

        let admin_session_id = AdminSession::default().id();

        let req = test::TestRequest::delete()
            .uri(&format!(
                "/layerDb/collections/{root_collection_id}/collections/{collection_id}"
            ))
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session_id.to_string()),
            ));
        let response = send_test_request(req, ctx.clone()).await;

        assert!(response.status().is_success(), "{response:?}");

        let root_collection = ctx
            .layer_db_ref()
            .collection(
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
}
