use std::{
    collections::HashMap,
    io::Read,
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::Context;
use chrono::Timelike;
use futures::StreamExt;
use geoengine_datatypes::{
    dataset::NamedData,
    primitives::{DateTime, TimeInstance, TimeInterval},
    raster::{GeoTransform, GridBoundingBox2D, GridIdx2D, RasterDataType},
    spatial_reference::{SpatialReference, SpatialReferenceAuthority, SpatialReferenceOption},
};
use tracing::{debug, error, info, warn};

use crate::datasets::external::stac::{StacDataProviderDefinition, StacProviderDataset, common};
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
                GridIdx2D as ApiGridIdx2D, LayerId, Measurement, SpatialGridDefinition,
                TimeGranularity, TimeStep, UnitlessMeasurement, UnitlessMeasurementTypeTag,
            },
            operators::{
                GdalDatasetParameters, GdalMultiBand, GdalMultiBandTypeTag, RasterBandDescriptor,
                RasterBandDescriptors, RasterResultDescriptor, RegularTimeDimension,
                SpatialGridDescriptor, SpatialGridDescriptorState, TimeDescriptor, TimeDimension,
            },
            responses::{ErrorResponse, IdResponse},
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

    /// Import limit (page size)
    #[arg(long)]
    pub limit: Option<usize>,

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
    serde_json::from_str(&json).map_err(|e| format!("Invalid mapping JSON: {e}"))
}

// ---------------------------------------------------------------------------
// Harvest Implementation
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
pub(super) async fn harvest_tiles(params: StacHarvest) -> Result<(), anyhow::Error> {
    let start_time = Instant::now();

    let provider_def = &params.mapping;

    info!(
        "Harvesting STAC collection '{}' at {} with {} dataset(s)",
        provider_def.collection_name,
        provider_def.api_url,
        provider_def.datasets.len()
    );

    let (client, session_id) = login_geo_engine(
        &params.geo_engine_url,
        &params.geo_engine_email,
        &params.geo_engine_password,
    )
    .await?;

    let mut created_datasets: Vec<(usize, StacProviderDataset)> = Vec::new();

    for (idx, dataset) in provider_def.datasets.iter().enumerate() {
        let dataset_name = dataset_name_for_harvest(&provider_def.collection_name, dataset);

        if params.verbose {
            info!("Checking dataset '{}'", dataset_name);
        }

        if !dataset_exists_api(&client, &params.geo_engine_url, &session_id, &dataset_name).await? {
            create_dataset_api(
                &client,
                &params.geo_engine_url,
                &session_id,
                &dataset_name,
                dataset,
                &params.volume_name,
            )
            .await?;
            created_datasets.push((idx, dataset.clone()));
        }
    }

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

    if let Some(limit) = params.limit {
        query_params.push(("limit".to_string(), limit.to_string()));
    }

    if params.filter_item_fields {
        query_params.push((
            "fields".to_string(),
            "stac_version,properties.datetime,properties.updated,assets.*.title,assets.*.href,assets.*.data_type,assets.*.bands,assets.*.proj:code,assets.*.proj:shape,assets.*.proj:transform"
                .to_string(),
        ));
    }

    let initial_query_state = QueryState::FirstPage {
        query_url: items_url.clone(),
        query_params,
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
            process_harvest_item(item, provider_def, &mut tiles_by_dataset, &params)
                .unwrap_or_else(|e| {
                    if params.verbose {
                        warn!("Skipping item {}: {}", item.id, e);
                    }
                });

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

    info!("Processed {} items total", items_processed);

    for (dataset_name, tiles) in &tiles_by_dataset {
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
                        .post(format!(
                            "{}/dataset/{}/tiles",
                            params.geo_engine_url, dataset_name
                        ))
                        .header("Content-Type", "application/json")
                        .header("Authorization", format!("Bearer {session_id}"))
                        .json(chunk)
                        .send()
                        .await
                },
                &format!("Add tiles to dataset '{dataset_name}'"),
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

    create_harvest_layer_collections(
        &client,
        &params.geo_engine_url,
        &session_id,
        provider_def,
        &created_datasets,
        &params,
    )
    .await?;

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
#[allow(clippy::too_many_lines)]
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
            let Some((_asset_key, asset)) = item
                .assets
                .iter()
                .find(|(_, a)| a.title.as_deref() == Some(&band_def.asset_title))
            else {
                continue;
            };

            // Check data type matches
            if let Some(asset_dt) = data_type_from_asset_v1_1_0_fallback(asset)
                && asset_dt != dataset.data_type
            {
                continue;
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
            });

            let Some(item_epsg) = item_epsg else {
                continue;
            };

            // Only process assets whose EPSG matches the dataset's projection
            if dataset.projection
                != SpatialReference::new(SpatialReferenceAuthority::Epsg, item_epsg)
            {
                continue;
            }

            let actual_dataset_name =
                dataset_name_for_harvest(&provider_def.collection_name, dataset);

            let Some(geo_transform) = common::geo_transform_from_fields(&asset.additional_fields)
            else {
                continue;
            };

            if (geo_transform.x_pixel_size().abs() - dataset.resolution.x).abs() > 1e-9
                || (geo_transform.y_pixel_size().abs() - dataset.resolution.y).abs() > 1e-9
            {
                continue;
            }

            let Some((height, width)) = common::proj_shape_from_fields(&asset.additional_fields)
            else {
                continue;
            };

            let Some(rasterband_channel) =
                common::rasterband_channel_for_dataset_band(asset, band_def.band_name.as_deref())
            else {
                continue;
            };

            let grid_bounds = GridBoundingBox2D::new(
                GridIdx2D::new([0, 0]),
                GridIdx2D::new([(width as isize) - 1, (height as isize) - 1]),
            )
            .context("Failed to create grid bounds")?;

            let spatial_partition = geo_transform.grid_to_spatial_bounds(&grid_bounds);

            let Some(file_path) = common::gdal_file_path(&asset.href) else {
                continue;
            };

            let gdal_config_options = common::gdal_config_options_for_file_path(
                &file_path,
                provider_def.s3_config.as_ref(),
            );

            let tile = AddDatasetTile {
                time: TimeInterval::new(time, time + i64::from(24 * 60 * 60 * 1000))
                    .context("Failed to create time interval")?
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
                    file_not_found_handling:
                        crate::api::model::operators::FileNotFoundHandling::Error,
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

            tiles_by_dataset
                .entry(actual_dataset_name)
                .or_default()
                .push(tile);
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
    client: &reqwest::Client,
    geo_engine_url: &str,
    session_id: &str,
    dataset_name: &str,
) -> Result<bool, anyhow::Error> {
    let response = retry_http(
        || async {
            client
                .get(format!("{geo_engine_url}/dataset/{dataset_name}"))
                .header("Authorization", format!("Bearer {session_id}"))
                .send()
                .await
        },
        &format!("Check dataset existence for '{dataset_name}'"),
    )
    .await?;

    if response.status().is_success() {
        return Ok(true);
    }

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if status == reqwest::StatusCode::BAD_REQUEST
        && let Ok(error_response) = serde_json::from_str::<ErrorResponse>(&body)
        && error_response.error == "CannotLoadDataset"
    {
        return Ok(false);
    }

    anyhow::bail!("Failed to check dataset '{dataset_name}': HTTP {status}: {body}");
}

async fn create_dataset_api(
    client: &reqwest::Client,
    geo_engine_url: &str,
    session_id: &str,
    dataset_name: &str,
    dataset: &StacProviderDataset,
    volume_name: &str,
) -> Result<(), anyhow::Error> {
    let bands: Vec<RasterBandDescriptor> = dataset
        .bands
        .iter()
        .map(|b| RasterBandDescriptor {
            name: b.band_name.clone().unwrap_or_else(|| b.asset_title.clone()),
            measurement: Measurement::Unitless(UnitlessMeasurement {
                r#type: UnitlessMeasurementTypeTag::UnitlessMeasurementTypeTag,
            }),
        })
        .collect();

    // Get GeoTransform from the spatial grid descriptor for the API
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

    share_dataset_api(client, geo_engine_url, session_id, &created_name).await?;

    Ok(())
}

async fn share_dataset_api(
    client: &reqwest::Client,
    geo_engine_url: &str,
    session_id: &str,
    dataset_name: &str,
) -> Result<(), anyhow::Error> {
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
                client
                    .put(format!("{geo_engine_url}/permissions"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", format!("Bearer {session_id}"))
                    .json(permission)
                    .send()
                    .await
            },
            &format!("Add permission for dataset '{dataset_name}'"),
        )
        .await?;
    }

    Ok(())
}

async fn create_harvest_layer_collections(
    client: &reqwest::Client,
    geo_engine_url: &str,
    session_id: &str,
    provider_def: &StacDataProviderDefinition,
    created_datasets: &[(usize, StacProviderDataset)],
    params: &StacHarvest,
) -> Result<(), anyhow::Error> {
    let root_collection_id = create_layer_collection_api(
        client,
        geo_engine_url,
        session_id,
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
        client,
        geo_engine_url,
        session_id,
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
        )
        .await?;

        share_layer_api(client, geo_engine_url, session_id, &response.id).await?;
    }

    Ok(())
}

async fn create_layer_collection_api(
    client: &reqwest::Client,
    geo_engine_url: &str,
    session_id: &str,
    parent_id: &LayerCollectionId,
    name: &str,
    description: &str,
    params: &StacHarvest,
) -> Result<LayerCollectionId, anyhow::Error> {
    if let Some(existing_id) =
        find_child_collection_by_name(client, geo_engine_url, session_id, parent_id, name).await?
    {
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
    )
    .await?;

    share_layer_collection_api(client, geo_engine_url, session_id, &response.id).await?;

    Ok(response.id)
}

async fn find_child_collection_by_name(
    client: &reqwest::Client,
    geo_engine_url: &str,
    session_id: &str,
    parent_id: &LayerCollectionId,
    child_name: &str,
) -> Result<Option<LayerCollectionId>, anyhow::Error> {
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
    client: &reqwest::Client,
    geo_engine_url: &str,
    session_id: &str,
    collection_id: &LayerCollectionId,
) -> Result<(), anyhow::Error> {
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
        )
        .await?;
    }

    Ok(())
}

async fn share_layer_api(
    client: &reqwest::Client,
    geo_engine_url: &str,
    session_id: &str,
    layer_id: &LayerId,
) -> Result<(), anyhow::Error> {
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
        )
        .await?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

const MAX_RETRIES: u32 = 10;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;

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
// HTTP retry helper
// ---------------------------------------------------------------------------

async fn retry_http<F, Fut, T, E>(mut operation: F, operation_name: &str) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 0;
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(err) => {
                attempt += 1;
                if attempt >= MAX_RETRIES {
                    error!("{operation_name} failed after {MAX_RETRIES} attempts: {err}");
                    return Err(err);
                }
                let delay = Duration::from_millis(INITIAL_RETRY_DELAY_MS * 2_u64.pow(attempt - 1));
                warn!(
                    "{operation_name} failed (attempt {attempt}/{MAX_RETRIES}): {err}. Retrying in {delay:?}..."
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Authentication helper
// ---------------------------------------------------------------------------

async fn login_geo_engine(
    geo_engine_url: &str,
    geo_engine_email: &str,
    geo_engine_password: &str,
) -> Result<(reqwest::Client, String), anyhow::Error> {
    let client = reqwest::Client::new();

    let response = retry_http(
        || async {
            client
                .post(format!("{geo_engine_url}/login"))
                .header("Content-Type", "application/json")
                .json(&serde_json::json!({
                    "email": geo_engine_email,
                    "password": geo_engine_password,
                }))
                .send()
                .await
        },
        "Login to Geo Engine",
    )
    .await
    .context("Failed to authenticate")?;

    let json = response
        .json::<serde_json::Value>()
        .await
        .context("Failed to parse auth response")?;

    let session_id = json["id"]
        .as_str()
        .context("No session id in response")?
        .to_string();

    Ok((client, session_id))
}

fn data_type_from_asset_v1_1_0_fallback(asset: &stac::Asset) -> Option<RasterDataType> {
    common::data_type_from_asset_v1_1_0(asset).or_else(|| {
        asset
            .additional_fields
            .get("data_type")
            .and_then(|v| v.as_str())
            .and_then(common::raster_data_type_from_stac_data_type_str)
    })
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
        let mapping: StacDataProviderDefinition = serde_json::from_str(include_str!(
            "../../../../test_data/stac_responses/expected-mapping-code-de.json"
        ))
        .expect("valid mapping fixture");

        let items: stac::ItemCollection = serde_json::from_str(include_str!(
            "../../../../test_data/stac_responses/items/code-de-harvest-test.json"
        ))
        .expect("valid items fixture");

        let params = StacHarvest {
            mapping: mapping.clone(),
            time_start: None,
            time_end: None,
            bbox: None,
            limit: None,
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
        let mapping: StacDataProviderDefinition = serde_json::from_str(include_str!(
            "../../../../test_data/stac_responses/expected-mapping-landsat-c2-l1.json"
        ))
        .expect("valid Landsat mapping fixture");

        let items: stac::ItemCollection = serde_json::from_str(include_str!(
            "../../../../test_data/stac_responses/items/landsat-c2-l1-harvest-test.json"
        ))
        .expect("valid Landsat items fixture");

        let params = StacHarvest {
            mapping: mapping.clone(),
            time_start: None,
            time_end: None,
            bbox: None,
            limit: None,
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
