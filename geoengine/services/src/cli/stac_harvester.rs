//! New STAC harvester CLI that separates mapping generation from tile harvesting.
//!
//! # Subcommands
//!
//! - `discover-mapping`: Probes a STAC collection and items API to generate a
//!   `StacDataProviderDefinition` JSON that maps STAC assets to Geo Engine datasets.
//! - `harvest`: Reads a `StacDataProviderDefinition` and harvests tiles into Geo Engine,
//!   creating datasets, tiles, and layer collections.
//!
//! The mapping JSON matches the format of `StacDataProviderDefinition` as used by the
//! STAC provider and the EDV bootstrap scripts.

#![allow(clippy::print_stdout)]

use std::{
    collections::HashMap,
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::Context;
use chrono::Timelike;
use clap::{Parser, Subcommand, ValueEnum};
use futures::StreamExt;
use geoengine_datatypes::{
    dataset::{DataProviderId, NamedData},
    primitives::{DateTime, SpatialResolution, TimeInstance, TimeInterval},
    raster::{GeoTransform, GridBoundingBox2D, GridIdx2D, RasterDataType},
    spatial_reference::{SpatialReference, SpatialReferenceAuthority, SpatialReferenceOption},
    util::Identifier,
};
use ordered_float::OrderedFloat;
use tracing::{debug, error, info, warn};

use crate::datasets::external::stac::{
    StacDataProviderDefinition, StacProviderDataset, StacProviderDatasetBand, StacProviderS3Config,
};
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
    datasets::{DatasetName, external::stac::common, upload::VolumeName},
    layers::{
        layer::{AddLayer, AddLayerCollection, CollectionItem, LayerCollection},
        listing::LayerCollectionId,
        storage::{INTERNAL_LAYER_DB_ROOT_COLLECTION_ID, INTERNAL_PROVIDER_ID},
    },
    permissions::{Permission, Role},
    workflows::workflow::Workflow,
};
use geoengine_datatypes::primitives::{
    RegularTimeDimension as DtRegularTimeDimension, TimeDimension as DtTimeDimension,
};
use geoengine_operators::{
    engine::{RasterOperator, SpatialGridDescriptor as GeoOpSpatialGridDescriptor, TypedOperator},
    source::{MultiBandGdalSource, MultiBandGdalSourceParameters},
};

// ---------------------------------------------------------------------------
// Main CLI struct
// ---------------------------------------------------------------------------

/// STAC harvester for Geo Engine
#[derive(Debug, Parser)]
pub struct StacHarvester {
    #[clap(subcommand)]
    pub command: StacHarvesterCommand,
}

#[derive(Debug, Subcommand)]
pub enum StacHarvesterCommand {
    /// Probe a STAC API to auto-discover the dataset mapping
    DiscoverMapping(StacDiscoverMapping),
    /// Harvest tiles using a predefined dataset mapping
    Harvest(StacHarvest),
}

// ---------------------------------------------------------------------------
// Discover Mapping
// ---------------------------------------------------------------------------

/// Probe a STAC collection and sample items to auto-discover the dataset mapping.
#[derive(Debug, Parser)]
pub struct StacDiscoverMapping {
    /// STAC API URL
    #[arg(long)]
    stac_url: String,

    /// STAC collection to scan
    #[arg(long, default_value = "sentinel-2-l2a")]
    stac_collection: String,

    /// S3 endpoint (if assets are hosted on S3-compatible storage)
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// S3 access key
    #[arg(long)]
    s3_access_key: Option<String>,

    /// S3 secret key
    #[arg(long)]
    s3_secret_key: Option<String>,

    /// Number of sample items to probe (default: 5)
    #[arg(long, default_value_t = 5)]
    sample_items: usize,

    /// Output file for the mapping JSON (default: stdout)
    #[arg(long)]
    output: Option<PathBuf>,

    /// Filter STAC item fields to reduce response size
    #[arg(long, default_value_t = false)]
    filter_item_fields: bool,

    /// File types to import
    #[arg(long, value_enum, num_args = 1.., value_delimiter = ' ', default_values_t = [ImportFileType::Cog])]
    file_types: Vec<ImportFileType>,

    /// Filter by EPSG codes (only include datasets for these codes)
    #[clap(long, value_parser, num_args = 0.., value_delimiter = ' ')]
    epsgs: Vec<u32>,

    /// Verbose output
    #[arg(long, default_value_t = false)]
    verbose: bool,

    /// Time dimension granularity (default: days)
    #[arg(long, default_value = "days")]
    time_granularity: String,

    /// Time dimension step (default: 1)
    #[arg(long, default_value_t = 1)]
    time_step: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ImportFileType {
    Cog,
    Jp2,
}

// ---------------------------------------------------------------------------
// Harvest
// ---------------------------------------------------------------------------

/// Harvest tiles from a STAC collection using a predefined dataset mapping.
#[derive(Debug, Parser)]
pub struct StacHarvest {
    /// Path to the StacDataProviderDefinition JSON file
    #[arg(long)]
    mapping: String,

    /// STAC API URL (overrides the one in the mapping, if any)
    #[arg(long)]
    stac_url: Option<String>,

    /// STAC collection name (overrides the one in the mapping, if any)
    #[arg(long)]
    stac_collection: Option<String>,

    /// Time range start to import (optional)
    #[arg(long)]
    time_start: Option<String>,

    /// Time range end to import (optional)
    #[arg(long)]
    time_end: Option<String>,

    /// Bounding box to import: minx miny maxx maxy (optional)
    #[clap(short, long, value_parser, num_args = 1.., value_delimiter = ' ')]
    bbox: Option<Vec<f64>>,

    /// Import limit (page size)
    #[arg(long)]
    limit: Option<usize>,

    /// Geo Engine API URL
    #[arg(long, default_value = "http://localhost:3030/api")]
    geo_engine_url: String,

    /// Geo Engine API email
    #[arg(long, default_value = "admin@localhost")]
    geo_engine_email: String,

    /// Geo Engine API password
    #[arg(long, default_value = "adminadmin")]
    geo_engine_password: String,

    /// Volume on the server
    #[arg(long, default_value = "geodata")]
    volume_name: String,

    /// Verbose output
    #[arg(long, default_value_t = false)]
    verbose: bool,

    /// Number of pages to prefetch while processing the current page
    #[arg(long, default_value_t = 2)]
    prefetch_pages: usize,

    /// Filter datasets by EPSG codes
    #[clap(long, value_parser, num_args = 0.., value_delimiter = ' ')]
    epsgs: Vec<u32>,

    /// Z-index property name
    #[arg(long, default_value = "updated")]
    z_index_property_name: Option<String>,

    /// No data value override
    #[arg(long)]
    no_data_value: Option<f64>,

    /// GDAL retry count
    #[arg(long)]
    gdal_retries: Option<usize>,

    /// Filter item fields to reduce response size
    #[arg(long, default_value_t = false)]
    filter_item_fields: bool,
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

/// Run the STAC harvester
pub async fn stac_harvester(params: StacHarvester) -> Result<(), anyhow::Error> {
    match params.command {
        StacHarvesterCommand::DiscoverMapping(discover) => discover_mapping(discover).await,
        StacHarvesterCommand::Harvest(harvest) => harvest_tiles(harvest).await,
    }
}

// ---------------------------------------------------------------------------
// Discover Mapping Implementation
// ---------------------------------------------------------------------------

async fn discover_mapping(params: StacDiscoverMapping) -> Result<(), anyhow::Error> {
    let client = reqwest::Client::new();

    info!(
        "Discovering mapping for STAC collection '{}' at {}",
        params.stac_collection, params.stac_url
    );

    let collection_url = format!(
        "{}/collections/{}",
        params.stac_url.trim_end_matches('/'),
        params.stac_collection
    );

    let collection: stac::Collection = stac_api_request_parse(&client, &collection_url)
        .await
        .context("Failed to fetch STAC collection")?;

    // Scan collection-level item_assets for bands (partial information)
    let mut dataset_bands: HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>> = HashMap::new();

    for (_asset_key, asset) in &collection.item_assets {
        if !matches_selected_file_types_static(asset.r#type.as_deref(), &params.file_types) {
            continue;
        }

        if let Ok(Some(bands)) =
            scan_item_asset_common(&collection.version, asset, &collection.summaries)
        {
            merge_dataset_bands(&mut dataset_bands, bands);
        }
    }

    if params.verbose {
        info!(
            "Found {} data type/resolution combinations from collection metadata",
            dataset_bands.len()
        );
    }

    // Sample items to discover EPSG codes and additional band/resolution info
    let items_url = format!(
        "{}/collections/{}/items",
        params.stac_url.trim_end_matches('/'),
        params.stac_collection
    );

    let mut query_params = Vec::new();
    if params.filter_item_fields {
        query_params.push((
            "fields".to_string(),
            "stac_version,stac_extensions,properties.datetime,properties.updated,properties.proj:epsg,properties.proj:code,assets.*.title,assets.*.href,assets.*.data_type,assets.*.bands,assets.*.eo:bands,assets.*.raster:bands,assets.*.proj:epsg,assets.*.proj:code,assets.*.proj:transform,assets.*.proj:shape,assets.*.gsd,assets.*.type".to_string(),
        ));
    }
    query_params.push(("limit".to_string(), format!("{}", params.sample_items)));

    let items_response: stac::ItemCollection =
        stac_api_request_with_params(&client, &items_url, &query_params)
            .await
            .context("Failed to fetch sample items")?;

    if items_response.items.is_empty() {
        anyhow::bail!("No items found in the collection. Cannot discover mapping.");
    }

    info!(
        "Probing {} sample item(s) to discover EPSG codes and additional bands",
        items_response.items.len()
    );

    let mut discovered_datasets: HashMap<DatasetKey, DiscoveredDatasetInfo> = HashMap::new();
    let mut sample_band_info: HashMap<PartialDatasetKey, Vec<(String, String)>> = HashMap::new();

    for item in &items_response.items {
        let item_epsg = common::epsg_code_from_item(item, common::StacExtensionMajorVersion::V2);

        for (asset_key, asset) in &item.assets {
            if !matches_selected_file_types_static(asset.r#type.as_deref(), &params.file_types) {
                continue;
            }

            let Some(geo_transform) = common::geo_transform_from_fields(&asset.additional_fields)
            else {
                continue;
            };

            let data_type = common::data_type_from_asset_v1_1_0(asset)
                .or_else(|| data_type_from_asset_v1_0_0_fallback(asset));
            let Some(data_type) = data_type else {
                continue;
            };

            let epsg = common::epsg_code_from_fields(
                common::StacExtensionMajorVersion::V2,
                &asset.additional_fields,
            )
            .or(item_epsg);
            let Some(epsg) = epsg else {
                continue;
            };

            if !params.epsgs.is_empty() && !params.epsgs.contains(&epsg) {
                continue;
            }

            let resolution: OrderedFloat<f64> = geo_transform.x_pixel_size().abs().into();

            let dataset_key = DatasetKey {
                epsg,
                data_type,
                resolution,
            };
            let partial_key = PartialDatasetKey {
                data_type,
                resolution,
            };

            let asset_title = asset.title.as_deref().unwrap_or(asset_key).to_string();
            let band_names = common::band_names_from_asset_v1_1_0(asset)
                .unwrap_or_else(|_| vec![asset_title.clone()]);

            let entry = sample_band_info.entry(partial_key.clone()).or_default();
            for bn in &band_names {
                if !entry.iter().any(|(t, _)| t == &asset_title) {
                    entry.push((asset_title.clone(), bn.clone()));
                }
            }

            let info_entry =
                discovered_datasets
                    .entry(dataset_key)
                    .or_insert(DiscoveredDatasetInfo {
                        geo_transform: Some(geo_transform),
                        proj_shape: common::proj_shape_from_fields(&asset.additional_fields),
                        srs: SpatialReference::new(SpatialReferenceAuthority::Epsg, epsg),
                        asset_count: 0,
                    });
            info_entry.asset_count += 1;
        }
    }

    if discovered_datasets.is_empty() {
        anyhow::bail!(
            "No matching assets found in sample items. Check your --file-types and --epsgs filters."
        );
    }

    // Build the StacDataProviderDefinition
    let time_dimension = parse_time_dimension(&params.time_granularity, params.time_step)
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    let s3_config = params
        .s3_endpoint
        .as_ref()
        .map(|endpoint| StacProviderS3Config {
            endpoint: endpoint.clone(),
            access_key: params.s3_access_key.clone(),
            secret_key: params.s3_secret_key.clone(),
        });

    let mut datasets: Vec<StacProviderDataset> = Vec::new();

    for (dataset_key, info) in &discovered_datasets {
        let partial_key = PartialDatasetKey {
            data_type: dataset_key.data_type,
            resolution: dataset_key.resolution,
        };

        let mut bands: Vec<StacProviderDatasetBand> = Vec::new();

        // Use bands from collection-level scan
        if let Some(descriptors) = dataset_bands.get(&partial_key) {
            for desc in descriptors {
                bands.push(StacProviderDatasetBand {
                    asset_title: desc.name.clone(),
                    band_name: None,
                });
            }
        }

        // Enrich with sample item band info
        if let Some(sample_bands) = sample_band_info.get(&partial_key) {
            for (asset_title, band_name) in sample_bands {
                if !bands.iter().any(|b| b.asset_title == *asset_title) {
                    bands.push(StacProviderDatasetBand {
                        asset_title: asset_title.clone(),
                        band_name: Some(band_name.clone()),
                    });
                }
            }
        }

        if bands.is_empty() {
            warn!("No bands found for dataset {:?}, skipping", dataset_key);
            continue;
        }

        bands.sort_by(|a, b| a.asset_title.cmp(&b.asset_title));

        let spatial_grid = if let (Some(gt), Some((height, width))) =
            (info.geo_transform, info.proj_shape)
        {
            GeoOpSpatialGridDescriptor::source_from_parts(
                gt,
                GridBoundingBox2D::new(
                    GridIdx2D::new([0, 0]),
                    GridIdx2D::new([(width as isize) - 1, (height as isize) - 1]),
                )
                .unwrap_or_else(|_| {
                    GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([0, 0])).unwrap()
                }),
            )
        } else {
            GeoOpSpatialGridDescriptor::source_from_parts(
                GeoTransform::new(
                    (0.0, 0.0).into(),
                    dataset_key.resolution.into_inner(),
                    -dataset_key.resolution.into_inner(),
                ),
                GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([0, 0])).unwrap(),
            )
        };

        let dataset_name = format!(
            "{} EPSG:{} {:?} {}m",
            params.stac_collection, dataset_key.epsg, dataset_key.data_type, dataset_key.resolution
        );

        datasets.push(StacProviderDataset {
            name: dataset_name,
            description: format!(
                "Auto-discovered from STAC collection '{}'",
                params.stac_collection
            ),
            data_type: dataset_key.data_type,
            resolution: SpatialResolution::new_unchecked(
                dataset_key.resolution.into_inner(),
                dataset_key.resolution.into_inner(),
            ),
            projection: info.srs,
            spatial_grid,
            bands,
        });
    }

    let provider_def = StacDataProviderDefinition {
        name: format!("{} from STAC", params.stac_collection),
        id: DataProviderId::new(),
        description: format!(
            "Auto-discovered mapping for STAC collection '{}' at {}",
            params.stac_collection, params.stac_url
        ),
        priority: Some(50),
        api_url: params.stac_url.clone(),
        collection_name: params.stac_collection.clone(),
        s3_config,
        time_dimension,
        datasets,
    };

    let json = serde_json::to_string_pretty(&provider_def)
        .context("Failed to serialize mapping to JSON")?;

    if let Some(output_path) = &params.output {
        std::fs::write(output_path, &json)
            .with_context(|| format!("Failed to write mapping to {}", output_path.display()))?;
        println!("Mapping written to {}", output_path.display());
    } else {
        println!("{json}");
    }

    Ok(())
}

struct DiscoveredDatasetInfo {
    geo_transform: Option<GeoTransform>,
    proj_shape: Option<(usize, usize)>,
    srs: SpatialReference,
    asset_count: usize,
}

// ---------------------------------------------------------------------------
// Harvest Implementation
// ---------------------------------------------------------------------------

async fn harvest_tiles(params: StacHarvest) -> Result<(), anyhow::Error> {
    let start_time = Instant::now();

    let mapping_json = if params.mapping == "-" {
        let mut input = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)
            .context("Failed to read mapping from stdin")?;
        input
    } else {
        std::fs::read_to_string(&params.mapping)
            .with_context(|| format!("Failed to read mapping from {}", params.mapping))?
    };

    let mut provider_def: StacDataProviderDefinition = serde_json::from_str(&mapping_json)
        .context("Failed to parse StacDataProviderDefinition from JSON")?;

    if let Some(stac_url) = &params.stac_url {
        provider_def.api_url = stac_url.clone();
    }
    if let Some(stac_collection) = &params.stac_collection {
        provider_def.collection_name = stac_collection.clone();
    }

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
            "stac_version,properties.datetime,properties.updated,assets.*.title,assets.*.href,assets.*.data_type,assets.*.bands,assets.*.proj:code,assets.*.proj:shape,assets.*.proj:transform".to_string(),
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
    let mut dynamic_datasets: HashMap<String, StacProviderDataset> = HashMap::new();
    let mut items_processed: u64 = 0;
    let mut items_per_sec: f64;

    futures::pin_mut!(page_stream);
    while let Some(result) = page_stream.next().await {
        let item_collection = result?;

        for item in &item_collection.items {
            process_harvest_item_dynamic(
                item,
                &provider_def,
                &mut tiles_by_dataset,
                &mut dynamic_datasets,
                &params,
            )
            .await
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
                    "[{:.1}%] Processed {}/{} items ({:.1} items/s, ETA: {})",
                    progress, items_processed, number_matched, items_per_sec, eta_str
                );
            } else {
                println!(
                    "Processed {} items ({:.1} items/s)",
                    items_processed, items_per_sec
                );
            }
        }
    }

    info!("Processed {} items total", items_processed);

    // Create any dynamic (per-EPSG) datasets that were discovered during item processing
    for (dyn_dataset_name, dyn_dataset) in &dynamic_datasets {
        if !dataset_exists_api(
            &client,
            &params.geo_engine_url,
            &session_id,
            dyn_dataset_name,
        )
        .await?
        {
            if params.verbose {
                info!("Creating dynamic dataset '{}'", dyn_dataset_name);
            }
            create_dataset_api(
                &client,
                &params.geo_engine_url,
                &session_id,
                dyn_dataset_name,
                dyn_dataset,
                &params.volume_name,
            )
            .await?;
        }
        // Also ensure tiles_by_dataset has an entry for this dataset
        tiles_by_dataset
            .entry(dyn_dataset_name.clone())
            .or_default();
    }

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
                        .header("Authorization", format!("Bearer {}", session_id))
                        .json(chunk)
                        .send()
                        .await
                },
                &format!("Add tiles to dataset '{}'", dataset_name),
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
        &provider_def,
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

#[allow(dead_code)]
#[allow(clippy::too_many_lines)]
async fn process_harvest_item(
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
            .map(|dt| dt.timestamp_millis())
            .unwrap_or_else(|| datetime.timestamp_millis()),
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
            if let Some(asset_dt) = data_type_from_asset_v1_1_0_fallback(asset) {
                if asset_dt != dataset.data_type {
                    continue;
                }
            }

            if !common::proj_code_matches_dataset(&asset.additional_fields, dataset.projection) {
                continue;
            }

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
                    retry: params.gdal_retries.map(|max_retries| {
                        crate::api::model::operators::GdalRetryOptions { max_retries }
                    }),
                },
            };

            let dataset_name = dataset_name_for_harvest(&provider_def.collection_name, dataset);
            tiles_by_dataset.entry(dataset_name).or_default().push(tile);
        }
    }

    Ok(())
}

/// Like `process_harvest_item`, but handles items whose EPSG code differs from
/// the mapping's projection by dynamically creating per-EPSG dataset variants.
/// Items that pass the `--epsgs` filter but have a different EPSG than the
/// mapping will be grouped into separate datasets named with their actual EPSG.
#[allow(clippy::too_many_lines)]
async fn process_harvest_item_dynamic(
    item: &stac::Item,
    provider_def: &StacDataProviderDefinition,
    tiles_by_dataset: &mut HashMap<String, Vec<AddDatasetTile>>,
    dynamic_datasets: &mut HashMap<String, StacProviderDataset>,
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
            .map(|dt| dt.timestamp_millis())
            .unwrap_or_else(|| datetime.timestamp_millis()),
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
            if let Some(asset_dt) = data_type_from_asset_v1_1_0_fallback(asset) {
                if asset_dt != dataset.data_type {
                    continue;
                }
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

            // Apply --epsgs filter if provided
            if !params.epsgs.is_empty() && !params.epsgs.contains(&item_epsg) {
                continue;
            }

            // Determine the actual projection and dataset name for this item
            let (_actual_projection, actual_dataset_name) = if dataset.projection
                == SpatialReference::new(SpatialReferenceAuthority::Epsg, item_epsg)
            {
                // EPSG matches the mapping — use the dataset as-is
                (
                    dataset.projection,
                    dataset_name_for_harvest(&provider_def.collection_name, dataset),
                )
            } else {
                // EPSG differs — create a per-EPSG variant
                let per_epsg_projection =
                    SpatialReference::new(SpatialReferenceAuthority::Epsg, item_epsg);

                // Build a modified dataset with the item's EPSG
                let per_epsg_dataset = StacProviderDataset {
                    projection: per_epsg_projection,
                    ..dataset.clone()
                };

                let dyn_name =
                    dataset_name_for_harvest(&provider_def.collection_name, &per_epsg_dataset);

                // Register this dynamic dataset so it gets created
                dynamic_datasets
                    .entry(dyn_name.clone())
                    .or_insert_with(|| StacProviderDataset {
                        projection: SpatialReference::new(
                            SpatialReferenceAuthority::Epsg,
                            item_epsg,
                        ),
                        spatial_grid: dataset.spatial_grid,
                        ..dataset.clone()
                    });

                (per_epsg_projection, dyn_name)
            };

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
                    retry: params.gdal_retries.map(|max_retries| {
                        crate::api::model::operators::GdalRetryOptions { max_retries }
                    }),
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
    {
        if error_response.error == "CannotLoadDataset" {
            return Ok(false);
        }
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
// STAC API helpers
// ---------------------------------------------------------------------------

async fn stac_api_request_parse<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
) -> Result<T, anyhow::Error> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("Failed to fetch {url}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("STAC API returned HTTP {status}: {body}");
    }

    response
        .json()
        .await
        .with_context(|| format!("Failed to parse response from {url}"))
}

async fn stac_api_request_with_params(
    client: &reqwest::Client,
    url: &str,
    params: &[(String, String)],
) -> Result<stac::ItemCollection, anyhow::Error> {
    let response = client
        .get(url)
        .query(params)
        .send()
        .await
        .with_context(|| format!("Failed to fetch {url}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("STAC API returned HTTP {status}: {body}");
    }

    response
        .json()
        .await
        .with_context(|| format!("Failed to parse response from {url}"))
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
                .map(|link| QueryState::NextPage {
                    next_url: link.href.clone(),
                })
                .unwrap_or(QueryState::Finished);

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
                .map(|link| QueryState::NextPage {
                    next_url: link.href.clone(),
                })
                .unwrap_or(QueryState::Finished);

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

// ---------------------------------------------------------------------------
// Collection scanning helpers
// ---------------------------------------------------------------------------

fn scan_item_asset_common(
    collection_version: &stac::Version,
    asset: &stac::ItemAsset,
    collection_summaries: &Option<serde_json::Map<String, serde_json::Value>>,
) -> Result<Option<HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>>>, String> {
    match collection_version {
        stac::Version::v1_0_0 => scan_item_asset_v1_0_0_common(asset),
        stac::Version::v1_1_0 => scan_item_asset_v1_1_0_common(asset, collection_summaries),
        _ => Err(format!("Unsupported STAC version: {collection_version}")),
    }
}

fn scan_item_asset_v1_0_0_common(
    asset: &stac::ItemAsset,
) -> Result<Option<HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>>>, String> {
    let mut dataset_bands: HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>> = HashMap::new();

    let Some(raster_bands) = asset.additional_fields.get("raster:bands") else {
        return Ok(None);
    };
    let raster_bands: Vec<stac_extensions::raster::Band> =
        serde_json::from_value(raster_bands.clone())
            .map_err(|e| format!("invalid raster:bands: {e}"))?;

    let band_count = raster_bands.len();

    let eo_bands = asset
        .additional_fields
        .get("eo:bands")
        .and_then(|v| serde_json::from_value::<Vec<common::EoBand>>(v.clone()).ok());

    if let Some(ref eo_bands_vec) = eo_bands {
        if band_count != eo_bands_vec.len() {
            return Ok(None);
        }
    } else if band_count != 1 {
        return Ok(None);
    }

    for (index, raster_band) in raster_bands.into_iter().enumerate() {
        let data_type = raster_band
            .data_type
            .ok_or_else(|| "Missing data_type in raster band".to_string())?;
        let raster_data_type = common::raster_data_type_from_stac_data_type(&data_type)
            .ok_or_else(|| format!("Unsupported data type: {data_type:?}"))?;

        let geo_transform = common::geo_transform_from_fields(&asset.additional_fields)
            .ok_or_else(|| "Missing proj:transform".to_string())?;
        let resolution: OrderedFloat<f64> = geo_transform.x_pixel_size().into();

        let band_name = if let Some(ref eo_bands_vec) = eo_bands {
            common::v1_0_0_band_name(
                asset.title.as_deref(),
                Some(&eo_bands_vec[index]),
                band_count,
            )
        } else {
            common::v1_0_0_band_name(asset.title.as_deref(), None, 1)
        };

        dataset_bands
            .entry(PartialDatasetKey {
                data_type: raster_data_type,
                resolution,
            })
            .or_default()
            .push(RasterBandDescriptor {
                name: band_name,
                measurement: Measurement::Unitless(UnitlessMeasurement {
                    r#type: UnitlessMeasurementTypeTag::UnitlessMeasurementTypeTag,
                }),
            });
    }

    Ok(Some(dataset_bands))
}

fn scan_item_asset_v1_1_0_common(
    asset: &stac::ItemAsset,
    collection_summaries: &Option<serde_json::Map<String, serde_json::Value>>,
) -> Result<Option<HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>>>, String> {
    let mut dataset_bands: HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>> = HashMap::new();

    let data_type = asset
        .additional_fields
        .get("data_type")
        .ok_or_else(|| "Missing data_type in asset additional fields".to_string())?
        .as_str()
        .ok_or_else(|| "data_type is not a string".to_string())?;

    let raster_data_type = common::raster_data_type_from_stac_data_type_str(data_type)
        .ok_or_else(|| format!("Unsupported data_type: {data_type}"))?;

    let band_names = common::band_names_from_item_asset_v1_1_0(asset)?;

    let resolution = asset
        .additional_fields
        .get("gsd")
        .and_then(serde_json::Value::as_f64)
        .or_else(|| {
            common::geo_transform_from_fields(&asset.additional_fields)
                .map(|gt| gt.x_pixel_size().abs())
        })
        .or_else(|| {
            collection_summaries
                .as_ref()
                .and_then(|s| s.get("gsd"))
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_f64())
        })
        .ok_or_else(|| "Missing attribute `gsd` or `proj:transform`".to_string())?;

    for band_name in band_names {
        dataset_bands
            .entry(PartialDatasetKey {
                data_type: raster_data_type,
                resolution: resolution.into(),
            })
            .or_default()
            .push(RasterBandDescriptor {
                name: band_name.clone(),
                measurement: Measurement::Unitless(UnitlessMeasurement {
                    r#type: UnitlessMeasurementTypeTag::UnitlessMeasurementTypeTag,
                }),
            });
    }

    Ok(Some(dataset_bands))
}

fn data_type_from_asset_v1_0_0_fallback(asset: &stac::Asset) -> Option<RasterDataType> {
    asset
        .additional_fields
        .get("raster:bands")
        .and_then(|v| v.as_array())
        .and_then(|bands| bands.first())
        .and_then(|band| band.get("data_type"))
        .and_then(|v| v.as_str())
        .and_then(common::raster_data_type_from_stac_data_type_str)
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

fn data_type_from_asset_v1_1_0_fallback(asset: &stac::Asset) -> Option<RasterDataType> {
    common::data_type_from_asset_v1_1_0(asset).or_else(|| {
        asset
            .additional_fields
            .get("data_type")
            .and_then(|v| v.as_str())
            .and_then(common::raster_data_type_from_stac_data_type_str)
    })
}

fn matches_selected_file_types_static(
    media_type: Option<&str>,
    file_types: &[ImportFileType],
) -> bool {
    file_types.iter().any(|file_type| match file_type {
        ImportFileType::Cog => common::is_cog_media_type(media_type),
        ImportFileType::Jp2 => common::is_jp2_media_type(media_type),
    })
}

fn merge_dataset_bands(
    dataset_bands: &mut HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>>,
    additions: HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>>,
) {
    for (partial_key, band_descriptors) in additions {
        let existing_bands = dataset_bands.entry(partial_key).or_default();
        for descriptor in band_descriptors {
            if existing_bands.iter().all(|b| b.name != descriptor.name) {
                existing_bands.push(descriptor);
            }
        }
    }
}

fn parse_time_dimension(granularity: &str, step: u64) -> Result<DtTimeDimension, String> {
    let dt_granularity = match granularity.to_lowercase().as_str() {
        "days" | "day" => geoengine_datatypes::primitives::TimeGranularity::Days,
        "months" | "month" => geoengine_datatypes::primitives::TimeGranularity::Months,
        "years" | "year" => geoengine_datatypes::primitives::TimeGranularity::Years,
        "hours" | "hour" => geoengine_datatypes::primitives::TimeGranularity::Hours,
        other => return Err(format!("Unsupported time granularity: {other}")),
    };

    let step_u32: u32 = step
        .try_into()
        .map_err(|_| format!("step {step} exceeds u32 range"))?;

    Ok(DtTimeDimension::Regular(
        DtRegularTimeDimension::new_with_epoch_origin(geoengine_datatypes::primitives::TimeStep {
            granularity: dt_granularity,
            step: step_u32,
        }),
    ))
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// A key that uniquely identifies a Geo Engine dataset derived from STAC assets.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct DatasetKey {
    epsg: u32,
    data_type: RasterDataType,
    resolution: OrderedFloat<f64>,
}

/// Partial dataset key without EPSG (used during collection scanning).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct PartialDatasetKey {
    data_type: RasterDataType,
    resolution: OrderedFloat<f64>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_time_dimension_days() {
        let td = parse_time_dimension("days", 1).expect("should parse");
        assert_eq!(
            td,
            DtTimeDimension::Regular(DtRegularTimeDimension::new_with_epoch_origin(
                geoengine_datatypes::primitives::TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Days,
                    step: 1,
                }
            ))
        );
    }

    #[test]
    fn test_parse_time_dimension_months() {
        let td = parse_time_dimension("months", 2).expect("should parse");
        assert_eq!(
            td,
            DtTimeDimension::Regular(DtRegularTimeDimension::new_with_epoch_origin(
                geoengine_datatypes::primitives::TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Months,
                    step: 2,
                }
            ))
        );
    }

    #[test]
    fn test_parse_time_dimension_invalid() {
        assert!(parse_time_dimension("invalid", 1).is_err());
    }

    #[test]
    fn test_dataset_name_for_harvest() {
        let collection = "sentinel-2-l2a";
        let dataset = StacProviderDataset {
            name: "Test".to_string(),
            description: String::new(),
            data_type: RasterDataType::U16,
            resolution: SpatialResolution::new_unchecked(10.0, 10.0),
            projection: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632),
            spatial_grid: GeoOpSpatialGridDescriptor::source_from_parts(
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
    fn test_merge_dataset_bands() {
        let mut map = HashMap::new();
        let key = PartialDatasetKey {
            data_type: RasterDataType::U16,
            resolution: OrderedFloat(10.0),
        };

        let bands1 = vec![RasterBandDescriptor {
            name: "B04".to_string(),
            measurement: Measurement::Unitless(UnitlessMeasurement {
                r#type: UnitlessMeasurementTypeTag::UnitlessMeasurementTypeTag,
            }),
        }];
        let mut additions1 = HashMap::new();
        additions1.insert(key.clone(), bands1);
        merge_dataset_bands(&mut map, additions1);

        let bands2 = vec![RasterBandDescriptor {
            name: "B08".to_string(),
            measurement: Measurement::Unitless(UnitlessMeasurement {
                r#type: UnitlessMeasurementTypeTag::UnitlessMeasurementTypeTag,
            }),
        }];
        let mut additions2 = HashMap::new();
        additions2.insert(key.clone(), bands2);
        merge_dataset_bands(&mut map, additions2);

        let bands3 = vec![RasterBandDescriptor {
            name: "B04".to_string(),
            measurement: Measurement::Unitless(UnitlessMeasurement {
                r#type: UnitlessMeasurementTypeTag::UnitlessMeasurementTypeTag,
            }),
        }];
        let mut additions3 = HashMap::new();
        additions3.insert(key, bands3);
        merge_dataset_bands(&mut map, additions3);

        assert_eq!(map.len(), 1);
        let merged = map.values().next().unwrap();
        assert_eq!(merged.len(), 2);
        assert!(merged.iter().any(|b| b.name == "B04"));
        assert!(merged.iter().any(|b| b.name == "B08"));
    }
}
