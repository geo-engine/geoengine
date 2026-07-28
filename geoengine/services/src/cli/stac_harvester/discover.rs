use std::{collections::HashMap, path::PathBuf};

use anyhow::Context;
use clap::{Parser, ValueEnum};
use geoengine_datatypes::{
    dataset::DataProviderId,
    primitives::SpatialResolution,
    raster::{GeoTransform, GridBoundingBox2D, GridIdx2D, RasterDataType},
    spatial_reference::{SpatialReference, SpatialReferenceAuthority},
    util::Identifier,
};
use ordered_float::OrderedFloat;
use tracing::{info, warn};

use crate::api::model::datatypes::{Measurement, UnitlessMeasurement, UnitlessMeasurementTypeTag};
use crate::api::model::operators::RasterBandDescriptor;
use crate::datasets::external::stac::{
    StacDataProviderDefinition, StacProviderDataset, StacProviderDatasetBand, StacProviderS3Config,
    common,
};
use geoengine_datatypes::primitives::{
    RegularTimeDimension as DtRegularTimeDimension, TimeDimension as DtTimeDimension,
};
use geoengine_operators::engine::SpatialGridDescriptor as GeoOpSpatialGridDescriptor;

// ---------------------------------------------------------------------------
// Discover Mapping
// ---------------------------------------------------------------------------

/// Probe a STAC collection and sample items to auto-discover the dataset mapping.
#[derive(Debug, Parser)]
pub struct StacDiscoverMapping {
    /// STAC API URL
    #[arg(long)]
    pub stac_url: String,

    /// STAC collection to scan
    #[arg(long, default_value = "sentinel-2-l2a")]
    pub stac_collection: String,

    /// S3 endpoint (if assets are hosted on S3-compatible storage)
    #[arg(long)]
    pub s3_endpoint: Option<String>,

    /// S3 access key
    #[arg(long)]
    pub s3_access_key: Option<String>,

    /// S3 secret key
    #[arg(long)]
    pub s3_secret_key: Option<String>,

    /// Number of sample items to probe (default: 5)
    #[arg(long, default_value_t = 5)]
    pub sample_items: usize,

    /// Output file for the mapping JSON (default: stdout)
    #[arg(long)]
    pub output: Option<PathBuf>,

    /// Filter STAC item fields to reduce response size
    #[arg(long, default_value_t = false)]
    pub filter_item_fields: bool,

    /// File types to import
    #[arg(long, value_enum, num_args = 1.., value_delimiter = ' ', default_values_t = [ImportFileType::Cog])]
    pub file_types: Vec<ImportFileType>,

    /// Filter by EPSG codes (only include datasets for these codes)
    #[clap(long, value_parser, num_args = 0.., value_delimiter = ' ')]
    pub epsgs: Vec<u32>,

    /// Verbose output
    #[arg(long, default_value_t = false)]
    pub verbose: bool,

    /// Bounding box to probe: minx miny maxx maxy (optional, defaults to UTM 32N area)
    #[clap(short, long, value_parser, num_args = 1.., value_delimiter = ' ', default_value = "6.0 47.0 12.0 55.0")]
    pub bbox: Option<Vec<f64>>,

    /// Use the full projected CRS extent for grid bounds instead of the first asset's shape.
    /// For UTM projections, this computes grid indices covering the entire zone (easting 0-1,000,000,
    /// northing 0-10,000,000) so that multi-tile datasets have correct global raster bounds.
    #[arg(long, default_value_t = false)]
    pub full_projection_grid: bool,

    /// Time dimension granularity (default: days)
    #[arg(long, default_value = "days")]
    pub time_granularity: String,

    /// Time dimension step (default: 1)
    #[arg(long, default_value_t = 1)]
    pub time_step: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ImportFileType {
    Cog,
    Jp2,
}

// ---------------------------------------------------------------------------
// Discover Mapping Implementation
// ---------------------------------------------------------------------------

pub(super) async fn discover_mapping(params: StacDiscoverMapping) -> Result<(), anyhow::Error> {
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

    let dataset_bands = scan_collection_bands(&collection, &params.file_types);

    if params.verbose {
        info!(
            "Found {} data type/resolution combinations from collection metadata",
            dataset_bands.len()
        );
    }

    let items_response = fetch_sample_items(&client, &params).await?;

    if items_response.items.is_empty() {
        anyhow::bail!("No items found in the collection. Cannot discover mapping.");
    }

    info!(
        "Probing {} sample item(s) to discover EPSG codes and additional bands",
        items_response.items.len()
    );

    let (discovered_datasets, sample_band_info) =
        process_sample_assets(&items_response, &params.file_types, &params.epsgs);

    if discovered_datasets.is_empty() {
        anyhow::bail!(
            "No matching assets found in sample items. Check your --file-types and --epsgs filters."
        );
    }

    let time_dimension = parse_time_dimension(&params.time_granularity, params.time_step)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let s3_config = params
        .s3_endpoint
        .as_ref()
        .map(|endpoint| StacProviderS3Config {
            endpoint: endpoint.clone(),
            access_key: params.s3_access_key.clone(),
            secret_key: params.s3_secret_key.clone(),
        });

    let datasets = build_datasets(
        &discovered_datasets,
        &dataset_bands,
        &sample_band_info,
        params.full_projection_grid,
        &params.stac_collection,
    );

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
    asset_count: u32,
}

// ---------------------------------------------------------------------------
// Discover Helper Functions
// ---------------------------------------------------------------------------

/// Scan the collection-level `item_assets` for band metadata (data type, resolution, band names).
fn scan_collection_bands(
    collection: &stac::Collection,
    file_types: &[ImportFileType],
) -> HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>> {
    let mut dataset_bands: HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>> = HashMap::new();

    for (_asset_key, asset) in &collection.item_assets {
        if !matches_selected_file_types_static(asset.r#type.as_deref(), file_types) {
            continue;
        }

        if let Ok(Some(bands)) =
            scan_collection_item_asset(&collection.version, asset, collection.summaries.as_ref())
        {
            merge_dataset_bands(&mut dataset_bands, bands);
        }
    }

    dataset_bands
}

/// Build query parameters and fetch sample items from the STAC items API.
async fn fetch_sample_items(
    client: &reqwest::Client,
    params: &StacDiscoverMapping,
) -> Result<stac::ItemCollection, anyhow::Error> {
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
    if let Some(bbox) = &params.bbox
        && bbox.len() == 4
    {
        query_params.push((
            "bbox".to_string(),
            format!("{},{},{},{}", bbox[0], bbox[1], bbox[2], bbox[3]),
        ));
    }

    query_params.push(("limit".to_string(), format!("{}", params.sample_items)));

    stac_api_request_with_params(client, &items_url, &query_params)
        .await
        .context("Failed to fetch sample items")
}

/// Process sample items to discover unique datasets (by EPSG, data type, resolution)
/// and their associated bands.
type DiscoveredDatasets = HashMap<DatasetKey, DiscoveredDatasetInfo>;
type SampleBandInfo = HashMap<PartialDatasetKey, Vec<(String, String)>>;

fn process_sample_assets(
    items_response: &stac::ItemCollection,
    file_types: &[ImportFileType],
    epsgs: &[u32],
) -> (DiscoveredDatasets, SampleBandInfo) {
    let mut discovered_datasets: HashMap<DatasetKey, DiscoveredDatasetInfo> = HashMap::new();
    let mut sample_band_info: HashMap<PartialDatasetKey, Vec<(String, String)>> = HashMap::new();

    for item in &items_response.items {
        let item_epsg = common::epsg_code_from_item(item, common::StacExtensionMajorVersion::V2);

        for (asset_key, asset) in &item.assets {
            if !matches_selected_file_types_static(asset.r#type.as_deref(), file_types) {
                continue;
            }

            let Some(geo_transform) = common::geo_transform_from_fields(&asset.additional_fields)
            else {
                continue;
            };

            let data_type = common::data_type_from_asset_v1_1_0(asset)
                .or_else(|| common::data_type_from_asset_v1_0_0_fallback(asset));
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

            if !epsgs.is_empty() && !epsgs.contains(&epsg) {
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

    (discovered_datasets, sample_band_info)
}

/// Build the `StacProviderDataset` list from discovered datasets, collection bands,
/// and sample band information.
fn build_datasets(
    discovered_datasets: &HashMap<DatasetKey, DiscoveredDatasetInfo>,
    dataset_bands: &HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>>,
    sample_band_info: &HashMap<PartialDatasetKey, Vec<(String, String)>>,
    full_projection_grid: bool,
    stac_collection: &str,
) -> Vec<StacProviderDataset> {
    let mut datasets: Vec<StacProviderDataset> = Vec::new();

    for (dataset_key, info) in discovered_datasets {
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

        let spatial_grid = build_dataset_spatial_grid(info, dataset_key, full_projection_grid);

        let dataset_name = format!(
            "{} EPSG:{} {:?} {}m",
            stac_collection, dataset_key.epsg, dataset_key.data_type, dataset_key.resolution
        );

        datasets.push(StacProviderDataset {
            name: dataset_name,
            description: format!("Auto-discovered from STAC collection '{stac_collection}'"),
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

    datasets
}

/// Build the spatial grid descriptor for a discovered dataset, optionally using the
/// full projected CRS extent instead of the first asset's shape.
fn build_dataset_spatial_grid(
    info: &DiscoveredDatasetInfo,
    dataset_key: &DatasetKey,
    full_projection_grid: bool,
) -> GeoOpSpatialGridDescriptor {
    let default_grid = || {
        GeoOpSpatialGridDescriptor::source_from_parts(
            GeoTransform::new(
                (0.0, 0.0).into(),
                dataset_key.resolution.into_inner(),
                -dataset_key.resolution.into_inner(),
            ),
            zero_size_grid(),
        )
    };

    if full_projection_grid {
        if let Some(gt) = info.geo_transform {
            let grid_bounds = projection_grid_bounds(gt, dataset_key.epsg)
                .unwrap_or_else(|| fallback_grid_bounds(info));
            GeoOpSpatialGridDescriptor::source_from_parts(gt, grid_bounds)
        } else {
            default_grid()
        }
    } else if let (Some(gt), Some((height, width))) = (info.geo_transform, info.proj_shape) {
        let grid_bounds = asset_shape_bounds(height, width).unwrap_or_else(|()| zero_size_grid());
        GeoOpSpatialGridDescriptor::source_from_parts(gt, grid_bounds)
    } else {
        default_grid()
    }
}

/// Fallback grid bounds: use the first asset's shape, or a single-pixel grid.
fn fallback_grid_bounds(info: &DiscoveredDatasetInfo) -> GridBoundingBox2D {
    if let Some((height, width)) = info.proj_shape {
        asset_shape_bounds(height, width).unwrap_or_else(|()| zero_size_grid())
    } else {
        zero_size_grid()
    }
}

/// Build grid bounds from an asset's `(height, width)` shape.
/// Returns `Err` if dimensions are zero (causing negative indices).
fn asset_shape_bounds(height: usize, width: usize) -> Result<GridBoundingBox2D, ()> {
    GridBoundingBox2D::new(
        GridIdx2D::new([0, 0]),
        GridIdx2D::new([
            width.saturating_sub(1) as isize,
            height.saturating_sub(1) as isize,
        ]),
    )
    .map_err(|_| ())
}

/// A single-pixel grid at the origin, used as a safe fallback when no shape info is available.
fn zero_size_grid() -> GridBoundingBox2D {
    GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([0, 0]))
        .expect("zero-size grid bounds should always be valid")
}

/// Compute grid bounds that cover the full projected CRS extent for the given
/// geo-transform and EPSG code. Currently handles UTM projections (zones 32601–32660
/// and 32701–32760) with known extents. Returns `None` for unsupported CRS types.
fn projection_grid_bounds(gt: GeoTransform, epsg: u32) -> Option<GridBoundingBox2D> {
    let (min_x, max_x, min_y, max_y) = projection_extent(epsg)?;

    let ox = gt.origin_coordinate.x;
    let oy = gt.origin_coordinate.y;
    let ps_x = gt.x_pixel_size();
    let ps_y = gt.y_pixel_size();

    // For north-up images ps_y < 0 and origin is top-left.
    // Pixel index i = (coord - origin) / pixel_size.
    let min_x_idx = ((min_x - ox) / ps_x).floor() as isize;
    let max_x_idx = ((max_x - ox) / ps_x).ceil() as isize - 1;

    let (min_y_idx, max_y_idx) = if ps_y < 0.0 {
        // ps_y negative: top of area (max_y) → smallest row index
        let top = ((max_y - oy) / ps_y).ceil() as isize;
        // bottom of area (min_y) → largest row index
        let bottom = ((min_y - oy) / ps_y).floor() as isize;
        (top, bottom)
    } else {
        let top = ((max_y - oy) / ps_y).floor() as isize;
        let bottom = ((min_y - oy) / ps_y).ceil() as isize - 1;
        (top, bottom)
    };

    GridBoundingBox2D::new(
        GridIdx2D::new([min_x_idx, min_y_idx]),
        GridIdx2D::new([max_x_idx, max_y_idx]),
    )
    .ok()
}

/// Return the projected extent `(min_x, max_x, min_y, max_y)` for a given EPSG code.
/// Known extents for UTM zones; other CRS types return `None`.
fn projection_extent(epsg: u32) -> Option<(f64, f64, f64, f64)> {
    // UTM northern hemisphere zones EPSG:32601 – 32660
    if (32601..=32660).contains(&epsg) {
        return Some((0.0, 1_000_000.0, 0.0, 10_000_000.0));
    }
    // UTM southern hemisphere zones EPSG:32701 – 32760
    if (32701..=32760).contains(&epsg) {
        return Some((0.0, 1_000_000.0, 0.0, 10_000_000.0));
    }
    None
}

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
// Collection scanning helpers
// ---------------------------------------------------------------------------

fn scan_collection_item_asset(
    collection_version: &stac::Version,
    asset: &stac::ItemAsset,
    collection_summaries: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<Option<HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>>>, String> {
    match collection_version {
        stac::Version::v1_0_0 => scan_collection_item_asset_v1_0_0(asset),
        stac::Version::v1_1_0 => scan_collection_item_asset_v1_1_0(asset, collection_summaries),
        _ => {
            // For unknown STAC versions, try v1.1.0 first (more common), fall back to v1.0.0
            scan_collection_item_asset_v1_1_0(asset, collection_summaries)
                .or_else(|_| scan_collection_item_asset_v1_0_0(asset))
                .or(Ok(None))
        }
    }
}

fn scan_collection_item_asset_v1_0_0(
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

fn scan_collection_item_asset_v1_1_0(
    asset: &stac::ItemAsset,
    collection_summaries: Option<&serde_json::Map<String, serde_json::Value>>,
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
                .and_then(|s| s.get("gsd"))
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(serde_json::Value::as_f64)
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

#[cfg(test)]
mod tests {
    use super::*;
    use httptest::{Expectation, Server, all_of, matchers::request, responders};

    const COLLECTION_PATH: &str = "/v1/collections/sentinel-2-l2a";
    const ITEMS_PATH: &str = "/v1/collections/sentinel-2-l2a/items";

    fn stac_collection_json() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../../../test_data/stac_responses/collections/code-de-minimal.json"
        ))
        .expect("valid collection fixture")
    }

    fn stac_items_json() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../../../test_data/stac_responses/items/code-de-harvest-test.json"
        ))
        .expect("valid items fixture")
    }

    fn expected_mapping_json() -> serde_json::Value {
        let mut mapping: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../test_data/stac_responses/expected-mapping-code-de.json"
        ))
        .expect("valid expected mapping fixture");
        // Remove the id since it is auto-generated
        mapping.as_object_mut().unwrap().remove("id");
        mapping
    }

    #[tokio::test]
    async fn test_discover_mapping_produces_expected_mapping() {
        let stac_server = Server::run();

        // Mock collection endpoint
        stac_server.expect(
            Expectation::matching(request::method_path("GET", COLLECTION_PATH))
                .times(1)
                .respond_with(responders::json_encoded(stac_collection_json())),
        );

        // Mock items endpoint
        stac_server.expect(
            Expectation::matching(all_of![request::method("GET"), request::path(ITEMS_PATH),])
                .times(1)
                .respond_with(responders::json_encoded(stac_items_json())),
        );

        let output_path = std::env::temp_dir().join("test_discover_mapping_output.json");

        let params = StacDiscoverMapping {
            stac_url: stac_server.url_str("/v1").trim_end_matches('/').to_string(),
            stac_collection: "sentinel-2-l2a".to_string(),
            s3_endpoint: None,
            s3_access_key: None,
            s3_secret_key: None,
            sample_items: 2,
            output: Some(output_path.clone()),
            filter_item_fields: true,
            file_types: vec![ImportFileType::Jp2],
            epsgs: vec![],
            bbox: None,
            full_projection_grid: false,
            verbose: false,
            time_granularity: "days".to_string(),
            time_step: 1,
        };

        discover_mapping(params)
            .await
            .expect("discover mapping should succeed");

        assert!(output_path.exists(), "output mapping file should exist");

        let output_content =
            std::fs::read_to_string(&output_path).expect("should read output file");
        let mut output_json: serde_json::Value =
            serde_json::from_str(&output_content).expect("output should be valid JSON");

        // Normalize dynamic fields before comparison
        let output_obj = output_json.as_object_mut().unwrap();
        output_obj.remove("id");
        output_obj.insert(
            "apiUrl".to_string(),
            serde_json::json!("https://stac.test/v1"),
        );
        output_obj.insert("description".to_string(), serde_json::json!("Auto-discovered mapping for STAC collection 'sentinel-2-l2a' at https://stac.test/v1"));

        // Sort datasets by name for deterministic comparison (HashMap order)
        if let Some(datasets) = output_obj
            .get_mut("datasets")
            .and_then(|d| d.as_array_mut())
        {
            datasets.sort_by(|a, b| {
                a["name"]
                    .as_str()
                    .unwrap_or("")
                    .cmp(b["name"].as_str().unwrap_or(""))
            });
        }

        let expected = expected_mapping_json();

        pretty_assertions::assert_eq!(expected, output_json);

        // Clean up
        let _ = std::fs::remove_file(&output_path);
    }

    // -----------------------------------------------------------------------
    // Landsat C2 L1 discover test
    // -----------------------------------------------------------------------

    fn landsat_collection_json() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../../../test_data/stac_responses/collections/landsat-c2-l1-minimal.json"
        ))
        .expect("valid Landsat collection fixture")
    }

    fn landsat_items_json() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../../../test_data/stac_responses/items/landsat-c2-l1-harvest-test.json"
        ))
        .expect("valid Landsat items fixture")
    }

    fn expected_landsat_mapping_json() -> serde_json::Value {
        let mut mapping: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../test_data/stac_responses/expected-mapping-landsat-c2-l1.json"
        ))
        .expect("valid Landsat expected mapping fixture");
        mapping.as_object_mut().unwrap().remove("id");
        mapping
    }

    const LANDSAT_COLLECTION_PATH: &str = "/v1/collections/landsat-c2-l1";
    const LANDSAT_ITEMS_PATH: &str = "/v1/collections/landsat-c2-l1/items";

    #[tokio::test]
    async fn test_discover_landsat_mapping_produces_expected_mapping() {
        let stac_server = Server::run();

        stac_server.expect(
            Expectation::matching(request::method_path("GET", LANDSAT_COLLECTION_PATH))
                .times(1)
                .respond_with(responders::json_encoded(landsat_collection_json())),
        );

        stac_server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(LANDSAT_ITEMS_PATH)
            ])
            .times(1)
            .respond_with(responders::json_encoded(landsat_items_json())),
        );

        let output_path = std::env::temp_dir().join("test_discover_landsat_mapping_output.json");

        let params = StacDiscoverMapping {
            stac_url: stac_server.url_str("/v1").trim_end_matches('/').to_string(),
            stac_collection: "landsat-c2-l1".to_string(),
            s3_endpoint: None,
            s3_access_key: None,
            s3_secret_key: None,
            sample_items: 1,
            output: Some(output_path.clone()),
            filter_item_fields: true,
            file_types: vec![ImportFileType::Cog],
            epsgs: vec![],
            bbox: None,
            full_projection_grid: false,
            verbose: false,
            time_granularity: "days".to_string(),
            time_step: 1,
        };

        discover_mapping(params)
            .await
            .expect("Landsat discover mapping should succeed");

        assert!(output_path.exists(), "output mapping file should exist");

        let output_content =
            std::fs::read_to_string(&output_path).expect("should read output file");
        let mut output_json: serde_json::Value =
            serde_json::from_str(&output_content).expect("output should be valid JSON");

        let output_obj = output_json.as_object_mut().unwrap();
        output_obj.remove("id");
        output_obj.insert(
            "apiUrl".to_string(),
            serde_json::json!("https://stac.test/v1"),
        );
        output_obj.insert("description".to_string(), serde_json::json!("Auto-discovered mapping for STAC collection 'landsat-c2-l1' at https://stac.test/v1"));

        if let Some(datasets) = output_obj
            .get_mut("datasets")
            .and_then(|d| d.as_array_mut())
        {
            datasets.sort_by(|a, b| {
                a["name"]
                    .as_str()
                    .unwrap_or("")
                    .cmp(b["name"].as_str().unwrap_or(""))
            });
        }

        let expected = expected_landsat_mapping_json();

        pretty_assertions::assert_eq!(expected, output_json);

        let _ = std::fs::remove_file(&output_path);
    }
}
