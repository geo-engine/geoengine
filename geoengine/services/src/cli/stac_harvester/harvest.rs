use std::{collections::HashMap, io::Read, str::FromStr, time::Instant};

use anyhow::Context;
use chrono::Timelike;
use futures::StreamExt;
use geoengine_datatypes::{
    dataset::NamedData,
    primitives::{DateTime, TimeInstance, TimeInterval},
    raster::{GeoTransform, GridBoundingBox2D, GridIdx2D},
    spatial_reference::{SpatialReference, SpatialReferenceAuthority, SpatialReferenceOption},
};
use tracing::{debug, error, info, warn};

use crate::datasets::external::stac::{
    StacDataProviderDefinition, StacProviderDataset, StacProviderDatasetBand, common,
};
use crate::util::retry::{RetryPolicy, retry_http};
use crate::{
    api::{
        handlers::{
            datasets::AddDatasetTile,
            permissions::{
                DatasetResource, DatasetResourceTypeTag, LayerCollectionResource,
                LayerCollectionResourceTypeTag, LayerResource, LayerResourceTypeTag,
                PermissionRequest, Resource,
            },
        },
        model::{
            datatypes::{
                GdalConfigOption, GridBoundingBox2D as ApiGridBoundingBox2D,
                GridIdx2D as ApiGridIdx2D, LayerId, SpatialGridDefinition, TimeGranularity,
                TimeStep,
            },
            operators::{
                GdalDatasetParameters, GdalMultiBand, GdalMultiBandTypeTag, RasterBandDescriptor,
                RasterBandDescriptors, RasterResultDescriptor, RegularTimeDimension,
                SpatialGridDescriptor, SpatialGridDescriptorState, TimeDescriptor, TimeDimension,
            },
            responses::IdResponse,
            services::{
                AddDataset, CreateDataset, DataPath, DatasetDefinition, MetaDataDefinition,
            },
        },
    },
    datasets::{DatasetName, upload::VolumeName},
    layers::{
        layer::{AddLayer, AddLayerCollection, CollectionItem, LayerCollection},
        listing::LayerCollectionId,
        storage::{INTERNAL_LAYER_DB_ROOT_COLLECTION_ID, INTERNAL_PROVIDER_ID},
    },
    permissions::{Permission, Role},
    workflows::workflow::Workflow,
};
use geoengine_api_client::apis::configuration::Configuration as ApiConfig;
use geoengine_operators::{
    engine::{RasterOperator, TypedOperator},
    source::{MultiBandGdalSource, MultiBandGdalSourceParameters},
};

// ---------------------------------------------------------------------------
// Harvest
// ---------------------------------------------------------------------------

/// Harvest tiles from a STAC collection using a predefined dataset mapping.
#[derive(Debug, clap::Parser)]
pub struct StacHarvest {
    /// Path to the `StacDataProviderDefinition` JSON file (or `-` for stdin)
    #[arg(long, value_parser = parse_mapping_file)]
    pub mapping: StacDataProviderDefinition,

    /// Time range start to import (optional)
    #[arg(long)]
    pub time_start: Option<String>,

    /// Time range end to import (optional)
    #[arg(long)]
    pub time_end: Option<String>,

    /// Bounding box to import: minx miny maxx maxy (optional)
    #[clap(short, long, value_parser, num_args = 1.., value_delimiter = ' ')]
    pub bbox: Option<Vec<f64>>,

    /// Geo Engine API URL
    #[arg(long, default_value = "http://localhost:3030/api")]
    pub geo_engine_url: String,

    /// Geo Engine API email
    #[arg(long, default_value = "admin@localhost")]
    pub geo_engine_email: String,

    /// Geo Engine API password
    #[arg(long, default_value = "adminadmin")]
    pub geo_engine_password: String,

    /// Volume on the server
    #[arg(long, default_value = "geodata")]
    pub volume_name: String,

    /// Verbose output
    #[arg(long, default_value_t = false)]
    pub verbose: bool,

    /// Number of pages to prefetch while processing the current page
    #[arg(long, default_value_t = 2)]
    pub prefetch_pages: usize,

    /// Z-index property name
    #[arg(long, default_value = "updated")]
    pub z_index_property_name: Option<String>,

    /// No data value override
    #[arg(long)]
    pub no_data_value: Option<f64>,

    /// GDAL retry count
    #[arg(long)]
    pub gdal_retries: Option<usize>,

    /// Filter item fields to reduce response size
    #[arg(long, default_value_t = false)]
    pub filter_item_fields: bool,
}

/// Parse a JSON file path (or `-` for stdin) into a `StacDataProviderDefinition`.
fn parse_mapping_file(s: &str) -> Result<StacDataProviderDefinition, String> {
    let json = if s == "-" {
        let mut input = String::new();
        std::io::stdin()
            .read_to_string(&mut input)
            .map_err(|e| format!("Failed to read mapping from stdin: {e}"))?;
        input
    } else {
        std::fs::read_to_string(s).map_err(|e| format!("Failed to read mapping from '{s}': {e}"))?
    };
    let api_def: crate::api::model::services::StacDataProviderDefinition =
        serde_json::from_str(&json).map_err(|e| format!("Invalid mapping JSON: {e}"))?;
    Ok(api_def.into())
}

// ---------------------------------------------------------------------------
// Harvest Implementation
// ---------------------------------------------------------------------------

pub(super) async fn harvest_tiles(params: StacHarvest) -> Result<(), anyhow::Error> {
    let start_time = Instant::now();

    let provider_def = &params.mapping;

    info!(
        "Harvesting STAC collection '{}' at {} with {} dataset(s)",
        provider_def.collection_name,
        provider_def.api_url,
        provider_def.datasets.len()
    );

    let api_config = create_api_config(
        &params.geo_engine_url,
        &params.geo_engine_email,
        &params.geo_engine_password,
    )
    .await?;

    // Separate reqwest client for STAC API calls (not Geo Engine)
    let stac_client = reqwest::Client::new();

    let created_datasets = setup_datasets(&api_config, &params, provider_def).await?;

    info!(
        "Created {} new dataset(s) out of {}",
        created_datasets.len(),
        provider_def.datasets.len()
    );

    let stac_api_url = provider_def.api_url.trim_end_matches('/').to_string();
    let items_url = format!(
        "{}/collections/{}/items",
        stac_api_url, provider_def.collection_name
    );

    let query_params = build_stac_query_params(&params, provider_def.page_limit as usize);

    let (tiles_by_dataset, items_processed) = process_item_stream(
        &stac_client,
        &items_url,
        &query_params,
        provider_def,
        &params,
        &start_time,
    )
    .await?;

    info!("Processed {} items total", items_processed);

    upload_tiles_to_datasets(&api_config, &params, &tiles_by_dataset).await?;

    create_harvest_layer_collections(&api_config, provider_def, &created_datasets, &params).await?;

    let elapsed = start_time.elapsed();
    info!("Harvest completed in {:.2?}", elapsed);

    Ok(())
}

// ---------------------------------------------------------------------------
// Item Processing (Harvest)
// ---------------------------------------------------------------------------

/// Process a STAC item and add tiles to the appropriate dataset from the mapping.
///
/// Only assets whose EPSG code matches the dataset's projection are included.
/// Items with a different EPSG are silently skipped — all datasets must be
/// predefined in the mapping.
fn process_harvest_item(
    item: &stac::Item,
    provider_def: &StacDataProviderDefinition,
    tiles_by_dataset: &mut HashMap<String, Vec<AddDatasetTile>>,
    params: &StacHarvest,
) -> Result<(), anyhow::Error> {
    let Some(datetime) = item.properties.datetime else {
        return Ok(());
    };

    let date_without_time = datetime
        .with_hour(0)
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_nanosecond(0))
        .context("Failed to set time to zero")?;
    let date_without_time: DateTime = date_without_time.into();
    let time: TimeInstance = date_without_time.into();

    let z_index = match params.z_index_property_name.as_deref() {
        Some("updated") => item
            .properties
            .updated
            .as_deref()
            .and_then(|updated| chrono::DateTime::parse_from_rfc3339(updated).ok())
            .map_or_else(|| datetime.timestamp_millis(), |dt| dt.timestamp_millis()),
        _ => 0,
    };

    for dataset in &provider_def.datasets {
        for (band_idx, band_def) in dataset.bands.iter().enumerate() {
            if let Some((dataset_name, tile)) = try_create_tile_for_band(
                item,
                dataset,
                band_idx,
                band_def,
                (time, z_index),
                provider_def,
                params,
            ) {
                tiles_by_dataset.entry(dataset_name).or_default().push(tile);
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Dataset and Layer Creation (Harvest)
// ---------------------------------------------------------------------------

fn dataset_name_for_harvest(collection_name: &str, dataset: &StacProviderDataset) -> String {
    let cleaned_name: String = collection_name
        .chars()
        .map(|c| {
            if geoengine_datatypes::dataset::is_invalid_name_char(c) {
                '_'
            } else {
                c
            }
        })
        .collect();

    let resolution_str = format!("{}", dataset.resolution.x);
    let clean_resolution: String = resolution_str
        .chars()
        .map(|c| {
            if geoengine_datatypes::dataset::is_invalid_name_char(c) {
                '_'
            } else {
                c
            }
        })
        .collect();

    format!(
        "{}_EPSG{}_{:?}_{}",
        cleaned_name,
        dataset.projection.code(),
        dataset.data_type,
        clean_resolution
    )
}

async fn dataset_exists_api(
    api_config: &ApiConfig,
    dataset_name: &str,
) -> Result<bool, anyhow::Error> {
    // A missing dataset is signalled with HTTP 400 (`CannotLoadDataset`) and
    // conventionally 404 — a definitive "doesn't exist" answer. Configure the
    // retry policy to stop on those codes so they are not retried.
    let policy = RetryPolicy::new().stop_on_status(&[400, 404]);

    let result = retry_http(
        || geoengine_api_client::apis::datasets_api::get_dataset_handler(api_config, dataset_name),
        &format!("Check dataset existence for '{dataset_name}'"),
        &policy,
        apis_error_status,
    )
    .await;

    match result {
        Ok(_) => Ok(true),
        Err(geoengine_api_client::apis::Error::ResponseError(resp))
            if resp.status == reqwest::StatusCode::BAD_REQUEST
                || resp.status == reqwest::StatusCode::NOT_FOUND =>
        {
            Ok(false)
        }
        Err(e) => Err(anyhow::anyhow!(
            "Failed to check dataset '{dataset_name}': {e}"
        )),
    }
}

/// Extract an HTTP status code from a `geoengine_api_client` API error, so a
/// [`RetryPolicy`] can match on it.
fn apis_error_status<T>(e: &geoengine_api_client::apis::Error<T>) -> Option<u16> {
    match e {
        geoengine_api_client::apis::Error::ResponseError(resp) => Some(resp.status.as_u16()),
        _ => None,
    }
}

async fn create_dataset_api(
    api_config: &ApiConfig,
    dataset_name: &str,
    dataset: &StacProviderDataset,
    volume_name: &str,
) -> Result<(), anyhow::Error> {
    let geo_engine_url = &api_config.base_path;
    let session_id = api_config.bearer_access_token.as_deref().unwrap_or("");
    let client = &api_config.client;

    let bands: Vec<RasterBandDescriptor> = dataset
        .bands
        .iter()
        .map(|b| b.band_descriptor.clone().into())
        .collect();

    let dt_gt: GeoTransform = dataset.spatial_grid.geo_transform();
    let api_gt: crate::api::model::datatypes::GeoTransform = dt_gt.into();

    let create_dataset_req = CreateDataset {
        data_path: DataPath::Volume(VolumeName(volume_name.to_string())),
        definition: DatasetDefinition {
            properties: AddDataset {
                name: Some(
                    DatasetName::from_str(dataset_name)
                        .map_err(|e| anyhow::anyhow!("Failed to create dataset name: {e}"))?,
                ),
                display_name: dataset_name.to_string(),
                description: format!("{dataset_name} harvested from STAC"),
                source_operator: "MultiBandGdalSource".to_string(),
                symbology: None,
                provenance: None,
                tags: None,
            },
            meta_data: MetaDataDefinition::GdalMultiBand(GdalMultiBand {
                r#type: GdalMultiBandTypeTag::GdalMultiBandTypeTag,
                result_descriptor: RasterResultDescriptor {
                    data_type: dataset.data_type.into(),
                    spatial_reference: SpatialReferenceOption::SpatialReference(dataset.projection)
                        .into(),
                    time: TimeDescriptor {
                        bounds: None,
                        dimension: TimeDimension::Regular(RegularTimeDimension {
                            origin: TimeInstance::from_millis_unchecked(0).into(),
                            step: TimeStep {
                                granularity: TimeGranularity::Days,
                                step: 1,
                            },
                        }),
                    },
                    spatial_grid: SpatialGridDescriptor {
                        spatial_grid: SpatialGridDefinition {
                            geo_transform: api_gt,
                            grid_bounds: ApiGridBoundingBox2D {
                                top_left_idx: ApiGridIdx2D { x_idx: 0, y_idx: 0 },
                                bottom_right_idx: ApiGridIdx2D { x_idx: 1, y_idx: 1 },
                            },
                        },
                        descriptor: SpatialGridDescriptorState::Source,
                    },
                    bands: RasterBandDescriptors::new(bands).context("Invalid band descriptors")?,
                },
            }),
        },
    };

    let response = retry_http(
        || async {
            client
                .post(format!("{geo_engine_url}/dataset"))
                .header("Content-Type", "application/json")
                .header("Authorization", format!("Bearer {session_id}"))
                .json(&create_dataset_req)
                .send()
                .await
        },
        &format!("Create dataset '{dataset_name}'"),
        &RetryPolicy::new(),
        |e| e.status().map(|s| s.as_u16()),
    )
    .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Failed to create dataset '{dataset_name}': HTTP {status}: {body}");
    }

    let created_name = if let Ok(json) = response.json::<serde_json::Value>().await {
        json.get("datasetName")
            .and_then(|v| v.as_str())
            .unwrap_or(dataset_name)
            .to_string()
    } else {
        dataset_name.to_string()
    };

    share_dataset_api(api_config, &created_name).await?;

    Ok(())
}

async fn share_dataset_api(
    api_config: &ApiConfig,
    dataset_name: &str,
) -> Result<(), anyhow::Error> {
    let session_id = api_config.bearer_access_token.as_deref().unwrap_or("");

    let permissions = vec![
        PermissionRequest {
            resource: Resource::Dataset(DatasetResource {
                id: DatasetName::new(None, dataset_name.to_string()),
                r#type: DatasetResourceTypeTag::DatasetResourceTypeTag,
            }),
            role_id: Role::registered_user_role_id(),
            permission: Permission::Read,
        },
        PermissionRequest {
            resource: Resource::Dataset(DatasetResource {
                id: DatasetName::new(None, dataset_name.to_string()),
                r#type: DatasetResourceTypeTag::DatasetResourceTypeTag,
            }),
            role_id: Role::anonymous_role_id(),
            permission: Permission::Read,
        },
    ];

    for permission in &permissions {
        retry_http(
            || async {
                api_config
                    .client
                    .put(format!("{}/permissions", api_config.base_path))
                    .header("Content-Type", "application/json")
                    .header("Authorization", format!("Bearer {session_id}"))
                    .json(permission)
                    .send()
                    .await
            },
            &format!("Add permission for dataset '{dataset_name}'"),
            &RetryPolicy::new(),
            |e| e.status().map(|s| s.as_u16()),
        )
        .await?;
    }

    Ok(())
}

async fn create_harvest_layer_collections(
    api_config: &ApiConfig,
    provider_def: &StacDataProviderDefinition,
    created_datasets: &[(usize, StacProviderDataset)],
    params: &StacHarvest,
) -> Result<(), anyhow::Error> {
    let geo_engine_url = &api_config.base_path;
    let session_id = api_config.bearer_access_token.as_deref().unwrap_or("");
    let client = &api_config.client;

    let root_collection_id = create_layer_collection_api(
        api_config,
        &LayerCollectionId(INTERNAL_LAYER_DB_ROOT_COLLECTION_ID.to_string()),
        &provider_def.collection_name,
        &format!(
            "{} datasets harvested from STAC",
            provider_def.collection_name
        ),
        params,
    )
    .await?;

    let temp_collection_id = create_layer_collection_api(
        api_config,
        &root_collection_id,
        "_layers",
        "All dataset layers (internal)",
        params,
    )
    .await?;

    for (_idx, dataset) in created_datasets {
        let dataset_name = dataset_name_for_harvest(&provider_def.collection_name, dataset);
        let layer_name = format!(
            "EPSG:{} {:?} {}m",
            dataset.projection.authority(),
            dataset.data_type,
            dataset.resolution.x
        );

        let add_layer = AddLayer {
            name: layer_name.clone(),
            description: format!("Dataset: {dataset_name}"),
            workflow: Workflow::Legacy {
                operator: TypedOperator::Raster(
                    MultiBandGdalSource {
                        params: MultiBandGdalSourceParameters::new(NamedData {
                            namespace: None,
                            provider: None,
                            name: dataset_name.clone(),
                        }),
                    }
                    .boxed(),
                ),
            },
            symbology: None,
            properties: vec![],
            metadata: Default::default(),
        };

        let response: IdResponse<LayerId> = retry_http(
            || async {
                client
                    .post(format!(
                        "{geo_engine_url}/layerDb/collections/{temp_collection_id}/layers"
                    ))
                    .header("Content-Type", "application/json")
                    .header("Authorization", format!("Bearer {session_id}"))
                    .json(&add_layer)
                    .send()
                    .await?
                    .json()
                    .await
            },
            &format!("Create layer '{layer_name}'"),
            &RetryPolicy::new(),
            |e| e.status().map(|s| s.as_u16()),
        )
        .await?;

        share_layer_api(api_config, &response.id).await?;
    }

    Ok(())
}

async fn create_layer_collection_api(
    api_config: &ApiConfig,
    parent_id: &LayerCollectionId,
    name: &str,
    description: &str,
    params: &StacHarvest,
) -> Result<LayerCollectionId, anyhow::Error> {
    let geo_engine_url = &api_config.base_path;
    let session_id = api_config.bearer_access_token.as_deref().unwrap_or("");
    let client = &api_config.client;

    if let Some(existing_id) = find_child_collection_by_name(api_config, parent_id, name).await? {
        if params.verbose {
            info!("Found existing layer collection '{name}'");
        }
        return Ok(existing_id);
    }

    let add_collection = AddLayerCollection {
        name: name.to_string(),
        description: description.to_string(),
        properties: vec![],
    };

    let response: IdResponse<LayerCollectionId> = retry_http(
        || async {
            client
                .post(format!(
                    "{geo_engine_url}/layerDb/collections/{parent_id}/collections"
                ))
                .header("Content-Type", "application/json")
                .header("Authorization", format!("Bearer {session_id}"))
                .json(&add_collection)
                .send()
                .await?
                .json()
                .await
        },
        &format!("Create layer collection '{name}'"),
        &RetryPolicy::new(),
        |e| e.status().map(|s| s.as_u16()),
    )
    .await?;

    share_layer_collection_api(api_config, &response.id).await?;

    Ok(response.id)
}

async fn find_child_collection_by_name(
    api_config: &ApiConfig,
    parent_id: &LayerCollectionId,
    child_name: &str,
) -> Result<Option<LayerCollectionId>, anyhow::Error> {
    let geo_engine_url = &api_config.base_path;
    let session_id = api_config.bearer_access_token.as_deref().unwrap_or("");
    let client = &api_config.client;

    let mut offset: u32 = 0;
    let limit: u32 = 20;

    loop {
        let response: LayerCollection = retry_http(
            || async {
                client
                    .get(format!(
                        "{geo_engine_url}/layers/collections/{INTERNAL_PROVIDER_ID}/{parent_id}"
                    ))
                    .query(&[("offset", offset), ("limit", limit)])
                    .header("Authorization", format!("Bearer {session_id}"))
                    .send()
                    .await?
                    .json()
                    .await
            },
            &format!("List child collections of {parent_id}"),
            &RetryPolicy::new(),
            |e| e.status().map(|s| s.as_u16()),
        )
        .await?;

        for item in &response.items {
            if let CollectionItem::Collection(collection) = item
                && collection.name == child_name
            {
                return Ok(Some(collection.id.collection_id.clone()));
            }
        }

        if response.items.len() < limit as usize {
            return Ok(None);
        }

        offset += limit;
    }
}

async fn share_layer_collection_api(
    api_config: &ApiConfig,
    collection_id: &LayerCollectionId,
) -> Result<(), anyhow::Error> {
    let geo_engine_url = &api_config.base_path;
    let session_id = api_config.bearer_access_token.as_deref().unwrap_or("");
    let client = &api_config.client;

    let permissions = vec![
        PermissionRequest {
            resource: Resource::LayerCollection(LayerCollectionResource {
                id: collection_id.clone(),
                r#type: LayerCollectionResourceTypeTag::LayerCollectionResourceTypeTag,
            }),
            role_id: Role::registered_user_role_id(),
            permission: Permission::Read,
        },
        PermissionRequest {
            resource: Resource::LayerCollection(LayerCollectionResource {
                id: collection_id.clone(),
                r#type: LayerCollectionResourceTypeTag::LayerCollectionResourceTypeTag,
            }),
            role_id: Role::anonymous_role_id(),
            permission: Permission::Read,
        },
    ];

    for permission in &permissions {
        retry_http(
            || async {
                client
                    .put(format!("{geo_engine_url}/permissions"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", format!("Bearer {session_id}"))
                    .json(permission)
                    .send()
                    .await
            },
            &format!("Share collection with role {}", permission.role_id),
            &RetryPolicy::new(),
            |e| e.status().map(|s| s.as_u16()),
        )
        .await?;
    }

    Ok(())
}

async fn share_layer_api(api_config: &ApiConfig, layer_id: &LayerId) -> Result<(), anyhow::Error> {
    let geo_engine_url = &api_config.base_path;
    let session_id = api_config.bearer_access_token.as_deref().unwrap_or("");
    let client = &api_config.client;

    let permissions = vec![
        PermissionRequest {
            resource: Resource::Layer(LayerResource {
                id: layer_id.clone(),
                r#type: LayerResourceTypeTag::LayerResourceTypeTag,
            }),
            role_id: Role::registered_user_role_id(),
            permission: Permission::Read,
        },
        PermissionRequest {
            resource: Resource::Layer(LayerResource {
                id: layer_id.clone(),
                r#type: LayerResourceTypeTag::LayerResourceTypeTag,
            }),
            role_id: Role::anonymous_role_id(),
            permission: Permission::Read,
        },
    ];

    for permission in &permissions {
        retry_http(
            || async {
                client
                    .put(format!("{geo_engine_url}/permissions"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", format!("Bearer {session_id}"))
                    .json(permission)
                    .send()
                    .await
            },
            &format!("Share layer with role {}", permission.role_id),
            &RetryPolicy::new(),
            |e| e.status().map(|s| s.as_u16()),
        )
        .await?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum QueryState {
    FirstPage {
        query_url: String,
        query_params: Vec<(String, String)>,
    },
    NextPage {
        next_url: String,
    },
    Finished,
}

fn create_page_stream(
    initial_query_state: QueryState,
    client: reqwest::Client,
    _verbose: bool,
    prefetch_buffer: usize,
) -> impl futures::Stream<Item = Result<stac::ItemCollection, anyhow::Error>> {
    let page_stream = futures::stream::unfold(
        (client, initial_query_state),
        move |(client, state)| async move {
            if matches!(state, QueryState::Finished) {
                return None;
            }

            debug!("Fetching page: {state:?}");

            let result = query_item_collection_internal(&client, &state).await;

            match result {
                Ok((item_collection, new_state)) => {
                    if item_collection.items.is_empty() {
                        None
                    } else {
                        Some((Ok(item_collection), (client, new_state)))
                    }
                }
                Err(e) => {
                    error!("Error fetching page: {e:#}");
                    Some((Err(e), (client, QueryState::Finished)))
                }
            }
        },
    );
    page_stream
        .map(|result| async move { result })
        .buffered(prefetch_buffer)
}

async fn query_item_collection_internal(
    client: &reqwest::Client,
    query_state: &QueryState,
) -> Result<(stac::ItemCollection, QueryState), anyhow::Error> {
    match query_state {
        QueryState::FirstPage {
            query_url,
            query_params,
        } => {
            let item_collection: stac::ItemCollection = retry_http(
                || async {
                    client
                        .get(query_url)
                        .query(&query_params)
                        .send()
                        .await?
                        .json()
                        .await
                },
                "Query STAC first page",
                &RetryPolicy::new(),
                |e| e.status().map(|s| s.as_u16()),
            )
            .await?;

            let new_state = item_collection
                .links
                .iter()
                .find(|link| link.rel == "next")
                .map_or(QueryState::Finished, |link| QueryState::NextPage {
                    next_url: link.href.clone(),
                });

            Ok((item_collection, new_state))
        }
        QueryState::NextPage { next_url } => {
            let item_collection: stac::ItemCollection = retry_http(
                || async { client.get(next_url).send().await?.json().await },
                "Query STAC next page",
                &RetryPolicy::new(),
                |e| e.status().map(|s| s.as_u16()),
            )
            .await?;

            let new_state = item_collection
                .links
                .iter()
                .find(|link| link.rel == "next")
                .map_or(QueryState::Finished, |link| QueryState::NextPage {
                    next_url: link.href.clone(),
                });

            Ok((item_collection, new_state))
        }
        QueryState::Finished => anyhow::bail!("No more pages to query"),
    }
}

// ---------------------------------------------------------------------------
// Authentication helper
// ---------------------------------------------------------------------------

async fn create_api_config(
    geo_engine_url: &str,
    geo_engine_email: &str,
    geo_engine_password: &str,
) -> Result<ApiConfig, anyhow::Error> {
    use geoengine_api_client::models;

    let config = ApiConfig {
        base_path: geo_engine_url.to_string(),
        ..ApiConfig::default()
    };

    let credentials = models::UserCredentials::new(
        geo_engine_email.to_string(),
        geo_engine_password.to_string(),
    );

    let session = retry_http(
        || geoengine_api_client::apis::session_api::login_handler(&config, credentials.clone()),
        "Login to Geo Engine",
        &RetryPolicy::new(),
        apis_error_status,
    )
    .await
    .context("Failed to authenticate")?;

    Ok(ApiConfig {
        bearer_access_token: Some(session.id.to_string()),
        ..config
    })
}

// ---------------------------------------------------------------------------
// Harvest Helper Functions
// ---------------------------------------------------------------------------

/// Create datasets that don't already exist on the Geo Engine server.
async fn setup_datasets(
    api_config: &ApiConfig,
    params: &StacHarvest,
    provider_def: &StacDataProviderDefinition,
) -> Result<Vec<(usize, StacProviderDataset)>, anyhow::Error> {
    let mut created_datasets: Vec<(usize, StacProviderDataset)> = Vec::new();

    for (idx, dataset) in provider_def.datasets.iter().enumerate() {
        let dataset_name = dataset_name_for_harvest(&provider_def.collection_name, dataset);

        if params.verbose {
            info!("Checking dataset '{}'", dataset_name);
        }

        if !dataset_exists_api(api_config, &dataset_name).await? {
            create_dataset_api(api_config, &dataset_name, dataset, &params.volume_name).await?;
            created_datasets.push((idx, dataset.clone()));
        }
    }

    Ok(created_datasets)
}

/// Build the query parameters for the STAC items API request.
fn build_stac_query_params(params: &StacHarvest, page_limit: usize) -> Vec<(String, String)> {
    let mut query_params: Vec<(String, String)> = Vec::new();

    if let Some(bbox) = &params.bbox
        && bbox.len() == 4
    {
        query_params.push((
            "bbox".to_string(),
            format!("{},{},{},{}", bbox[0], bbox[1], bbox[2], bbox[3]),
        ));
    }

    if params.time_start.is_some() || params.time_end.is_some() {
        query_params.push((
            "datetime".to_string(),
            format!(
                "{}/{}",
                params.time_start.as_deref().unwrap_or(""),
                params.time_end.as_deref().unwrap_or("")
            ),
        ));
    }

    query_params.push(("limit".to_string(), page_limit.to_string()));

    if params.filter_item_fields {
        query_params.push((
            "fields".to_string(),
            "stac_version,properties.datetime,properties.updated,assets.*.title,assets.*.href,assets.*.data_type,assets.*.bands,assets.*.proj:code,assets.*.proj:shape,assets.*.proj:transform"
                .to_string(),
        ));
    }

    query_params
}

/// Fetch pages of STAC items, process them, and collect tiles grouped by dataset.
async fn process_item_stream(
    client: &reqwest::Client,
    items_url: &str,
    query_params: &[(String, String)],
    provider_def: &StacDataProviderDefinition,
    params: &StacHarvest,
    start_time: &Instant,
) -> Result<(HashMap<String, Vec<AddDatasetTile>>, u64), anyhow::Error> {
    let initial_query_state = QueryState::FirstPage {
        query_url: items_url.to_string(),
        query_params: query_params.to_vec(),
    };

    let page_stream = create_page_stream(
        initial_query_state,
        client.clone(),
        params.verbose,
        params.prefetch_pages,
    );

    let mut tiles_by_dataset: HashMap<String, Vec<AddDatasetTile>> = HashMap::new();
    let mut items_processed: u64 = 0;
    let mut items_per_sec: f64;

    futures::pin_mut!(page_stream);
    while let Some(result) = page_stream.next().await {
        let item_collection = result?;

        for item in &item_collection.items {
            process_harvest_item(item, provider_def, &mut tiles_by_dataset, params).unwrap_or_else(
                |e| {
                    if params.verbose {
                        warn!("Skipping item {}: {}", item.id, e);
                    }
                },
            );

            items_processed += 1;
        }

        if params.verbose {
            let elapsed = start_time.elapsed().as_secs_f64();
            items_per_sec = if elapsed > 0.0 {
                items_processed as f64 / elapsed
            } else {
                0.0
            };

            if let Some(number_matched) = item_collection
                .additional_fields
                .get("numberMatched")
                .and_then(serde_json::Value::as_u64)
            {
                let progress =
                    (items_processed as f64 / number_matched as f64 * 100.0).clamp(0.0, 100.0);
                let remaining = number_matched.saturating_sub(items_processed);
                let eta_secs = if items_per_sec > 0.0 {
                    remaining as f64 / items_per_sec
                } else {
                    f64::INFINITY
                };
                let eta_str = if eta_secs.is_finite() {
                    format_duration(eta_secs as u64)
                } else {
                    "unknown".to_string()
                };
                println!(
                    "[{progress:.1}%] Processed {items_processed}/{number_matched} items ({items_per_sec:.1} items/s, ETA: {eta_str})"
                );
            } else {
                println!("Processed {items_processed} items ({items_per_sec:.1} items/s)");
            }
        }
    }

    Ok((tiles_by_dataset, items_processed))
}

/// Upload collected tiles to the Geo Engine server in batches.
async fn upload_tiles_to_datasets(
    api_config: &ApiConfig,
    params: &StacHarvest,
    tiles_by_dataset: &HashMap<String, Vec<AddDatasetTile>>,
) -> Result<(), anyhow::Error> {
    let geo_engine_url = &api_config.base_path;
    let session_id = api_config.bearer_access_token.as_deref().unwrap_or("");
    let client = &api_config.client;

    for (dataset_name, tiles) in tiles_by_dataset {
        if tiles.is_empty() {
            continue;
        }

        if params.verbose {
            info!("Adding {} tiles to dataset '{}'", tiles.len(), dataset_name);
        }

        let batch_size = 100;
        for chunk in tiles.chunks(batch_size) {
            let response = retry_http(
                || async {
                    client
                        .post(format!("{geo_engine_url}/dataset/{dataset_name}/tiles"))
                        .header("Content-Type", "application/json")
                        .header("Authorization", format!("Bearer {session_id}"))
                        .json(chunk)
                        .send()
                        .await
                },
                &format!("Add tiles to dataset '{dataset_name}'"),
                &RetryPolicy::new(),
                |e| e.status().map(|s| s.as_u16()),
            )
            .await
            .with_context(|| format!("Failed to add tiles to dataset '{dataset_name}'"))?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                warn!("Failed to add tiles to dataset '{dataset_name}' (HTTP {status}): {body}");
                // Continue with remaining tiles; some conflicts (e.g. z-index) are expected
            }
        }
    }

    Ok(())
}

/// Try to create a tile for a single band of a single dataset from a STAC item asset.
///
/// Returns `None` if no matching asset exists, the data types don't match, the EPSG code
/// doesn't match the dataset's projection, the resolution doesn't match, or any required
/// metadata is missing.
fn try_create_tile_for_band(
    item: &stac::Item,
    dataset: &StacProviderDataset,
    band_idx: usize,
    band_def: &StacProviderDatasetBand,
    item_time: (TimeInstance, i64), // (time, z_index)
    provider_def: &StacDataProviderDefinition,
    params: &StacHarvest,
) -> Option<(String, AddDatasetTile)> {
    let (time, z_index) = item_time;
    let (_asset_key, asset) = item
        .assets
        .iter()
        .find(|(_, a)| a.title.as_deref() == Some(&band_def.asset_band.asset_title))?;

    // Check data type matches
    if let Some(asset_dt) = common::data_type_from_asset_v1_1_0_fallback(asset)
        && asset_dt != dataset.data_type
    {
        return None;
    }

    // Extract the item's actual EPSG code from the asset
    let item_epsg = common::epsg_code_from_fields(
        common::StacExtensionMajorVersion::V2,
        &asset.additional_fields,
    )
    .or_else(|| {
        // Also try to extract from serialized properties as fallback
        let props_val = serde_json::to_value(&item.properties)
            .ok()
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default();
        common::epsg_code_from_fields(common::StacExtensionMajorVersion::V2, &props_val)
    })?;

    // Only process assets whose EPSG matches the dataset's projection
    if dataset.projection != SpatialReference::new(SpatialReferenceAuthority::Epsg, item_epsg) {
        return None;
    }

    let actual_dataset_name = dataset_name_for_harvest(&provider_def.collection_name, dataset);

    let geo_transform = common::geo_transform_from_fields(&asset.additional_fields)?;

    if (geo_transform.x_pixel_size().abs() - dataset.resolution.x).abs() > 1e-9
        || (geo_transform.y_pixel_size().abs() - dataset.resolution.y).abs() > 1e-9
    {
        return None;
    }

    let (height, width) = common::proj_shape_from_fields(&asset.additional_fields)?;

    let rasterband_channel = common::rasterband_channel_for_dataset_band(
        asset,
        band_def.asset_band.band_name.as_deref(),
    )?;

    let grid_bounds = GridBoundingBox2D::new(
        GridIdx2D::new([0, 0]),
        GridIdx2D::new([(width as isize) - 1, (height as isize) - 1]),
    )
    .ok()?;

    let spatial_partition = geo_transform.grid_to_spatial_bounds(&grid_bounds);

    let file_path = common::gdal_file_path(&asset.href)?;

    let gdal_config_options =
        common::gdal_config_options_for_file_path(&file_path, provider_def.s3_config.as_ref());

    let tile = AddDatasetTile {
        time: TimeInterval::new(time, time + i64::from(24 * 60 * 60 * 1000))
            .ok()?
            .into(),
        spatial_partition: spatial_partition.into(),
        band: band_idx as u32,
        z_index,
        params: GdalDatasetParameters {
            file_path,
            rasterband_channel,
            geo_transform: geo_transform.into(),
            width,
            height,
            file_not_found_handling: crate::api::model::operators::FileNotFoundHandling::Error,
            no_data_value: params.no_data_value,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: gdal_config_options.map(|opts| {
                opts.into_iter()
                    .map(|(k, v)| GdalConfigOption::from((k, v)))
                    .collect()
            }),
            allow_alphaband_as_mask: false,
        },
    };

    Some((actual_dataset_name, tile))
}

fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}h{}m{}s", secs / 3600, (secs % 3600) / 60, secs % 60)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::primitives::SpatialResolution;
    use geoengine_datatypes::raster::RasterDataType;

    #[test]
    fn test_dataset_name_for_harvest() {
        let collection = "sentinel-2-l2a";
        let dataset = StacProviderDataset {
            name: "Test".to_string(),
            description: String::new(),
            data_type: RasterDataType::U16,
            resolution: SpatialResolution::new_unchecked(10.0, 10.0),
            projection: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632),
            spatial_grid: geoengine_operators::engine::SpatialGridDescriptor::source_from_parts(
                GeoTransform::new((0.0, 0.0).into(), 10.0, -10.0),
                GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([0, 0])).unwrap(),
            ),
            bands: vec![],
        };

        let name = dataset_name_for_harvest(collection, &dataset);
        assert!(name.contains("sentinel-2-l2a"));
        assert!(name.contains("EPSG"));
        assert!(name.contains("U16"));
    }

    #[test]
    fn test_process_harvest_item_produces_correct_tiles() {
        let api_mapping: crate::api::model::services::StacDataProviderDefinition =
            serde_json::from_str(include_str!(
                "../../../../test_data/stac_responses/expected-mapping-code-de.json"
            ))
            .expect("valid mapping fixture");
        let mapping: StacDataProviderDefinition = api_mapping.into();

        let items: stac::ItemCollection = serde_json::from_str(include_str!(
            "../../../../test_data/stac_responses/items/code-de-harvest-test.json"
        ))
        .expect("valid items fixture");

        let params = StacHarvest {
            mapping: mapping.clone(),
            time_start: None,
            time_end: None,
            bbox: None,
            geo_engine_url: String::new(),
            geo_engine_email: String::new(),
            geo_engine_password: String::new(),
            volume_name: String::new(),
            verbose: false,
            prefetch_pages: 1,
            z_index_property_name: Some("updated".to_string()),
            no_data_value: None,
            gdal_retries: None,
            filter_item_fields: true,
        };

        let mut tiles_by_dataset: HashMap<String, Vec<AddDatasetTile>> = HashMap::new();

        // Process the first item from the fixture
        let item = &items.items[0];

        process_harvest_item(item, &mapping, &mut tiles_by_dataset, &params)
            .expect("item processing should succeed");

        // The mapping has 2 datasets (10m and 20m), the item has assets for both
        assert_eq!(
            tiles_by_dataset.len(),
            2,
            "tiles should be produced for 2 datasets (10m and 20m)"
        );

        // Check each dataset has valid tiles
        for (dataset_name, tiles) in &tiles_by_dataset {
            assert!(
                !tiles.is_empty(),
                "dataset {dataset_name} should have tiles"
            );
            for tile in tiles {
                assert!(tile.band < 10, "band index should be reasonable");
                assert!(
                    tile.params.width > 0 && tile.params.height > 0,
                    "tile dimensions should be positive"
                );
                assert!(
                    tile.params
                        .file_path
                        .to_string_lossy()
                        .starts_with("/vsis3/"),
                    "file path should be a VSI path: {}",
                    tile.params.file_path.display()
                );
            }
        }

        // 10m dataset should have 4 tiles (B02, B03, B04, B08)
        let total_tiles_10m: usize = tiles_by_dataset
            .iter()
            .filter(|(name, _)| name.contains("10"))
            .map(|(_, tiles)| tiles.len())
            .sum();
        assert_eq!(
            total_tiles_10m, 4,
            "first item should produce 4 tiles for 10m bands"
        );

        // 20m dataset should have 2 tiles (B11, B12)
        let total_tiles_20m: usize = tiles_by_dataset
            .iter()
            .filter(|(name, _)| name.contains("20"))
            .map(|(_, tiles)| tiles.len())
            .sum();
        assert_eq!(
            total_tiles_20m, 2,
            "first item should produce 2 tiles for 20m bands"
        );
    }

    #[test]
    fn test_process_harvest_landsat_item_produces_correct_tiles() {
        let api_mapping: crate::api::model::services::StacDataProviderDefinition =
            serde_json::from_str(include_str!(
                "../../../../test_data/stac_responses/expected-mapping-landsat-c2-l1.json"
            ))
            .expect("valid Landsat mapping fixture");
        let mapping: StacDataProviderDefinition = api_mapping.into();

        let items: stac::ItemCollection = serde_json::from_str(include_str!(
            "../../../../test_data/stac_responses/items/landsat-c2-l1-harvest-test.json"
        ))
        .expect("valid Landsat items fixture");

        let params = StacHarvest {
            mapping: mapping.clone(),
            time_start: None,
            time_end: None,
            bbox: None,
            geo_engine_url: String::new(),
            geo_engine_email: String::new(),
            geo_engine_password: String::new(),
            volume_name: String::new(),
            verbose: false,
            prefetch_pages: 1,
            z_index_property_name: Some("updated".to_string()),
            no_data_value: None,
            gdal_retries: None,
            filter_item_fields: true,
        };

        let mut tiles_by_dataset: HashMap<String, Vec<AddDatasetTile>> = HashMap::new();

        let item = &items.items[0];

        process_harvest_item(item, &mapping, &mut tiles_by_dataset, &params)
            .expect("Landsat item processing should succeed");

        // Mapping has 1 dataset (30m), the item has assets for it
        assert_eq!(
            tiles_by_dataset.len(),
            1,
            "tiles should be produced for 1 dataset (30m)"
        );

        for (dataset_name, tiles) in &tiles_by_dataset {
            assert!(
                !tiles.is_empty(),
                "dataset {dataset_name} should have tiles"
            );
            for tile in tiles {
                assert!(tile.band < 10, "band index should be reasonable");
                assert!(
                    tile.params.width > 0 && tile.params.height > 0,
                    "tile dimensions should be positive"
                );
                assert!(
                    tile.params
                        .file_path
                        .to_string_lossy()
                        .starts_with("/vsis3/"),
                    "file path should be a VSI path: {}",
                    tile.params.file_path.display()
                );
            }
        }

        // 30m dataset should have 4 tiles (Blue, Green, Red, NIR)
        let total_tiles_30m: usize = tiles_by_dataset
            .iter()
            .filter(|(name, _)| name.contains("30"))
            .map(|(_, tiles)| tiles.len())
            .sum();
        assert_eq!(
            total_tiles_30m, 4,
            "first item should produce 4 tiles for 30m bands"
        );
    }
}
