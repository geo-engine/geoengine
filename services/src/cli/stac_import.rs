#![allow(clippy::print_stdout)]

use gdal::raster::{self, RasterBand};
use ordered_float::OrderedFloat;
use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
    str::FromStr,
};

use anyhow::Context;
use chrono::Timelike;
use clap::Parser;
use geoengine_datatypes::{
    operations::reproject::{CoordinateProjection, CoordinateProjector, Reproject},
    primitives::{AxisAlignedRectangle, DateTime, TimeInstance, TimeInterval},
    raster::{GdalGeoTransform, GeoTransform},
    spatial_reference::{SpatialReference, SpatialReferenceAuthority, SpatialReferenceOption},
    util::well_known_data::HAMBURG_EPSG_900_913,
};
use serde::Deserialize;
use stac::Asset;
// use stac_extensions::raster::

use crate::{
    api::{
        handlers::{
            datasets::AddDatasetTile,
            permissions::{DatasetResource, PermissionRequest},
        },
        model::{
            datatypes::{
                GridBoundingBox2D, GridIdx2D, Measurement, RasterDataType, SpatialGridDefinition,
                TimeGranularity, TimeStep, UnitlessMeasurement,
            },
            operators::{
                GdalDatasetParameters, GdalMultiBand, RasterBandDescriptor, RasterBandDescriptors,
                RasterResultDescriptor, RegularTimeDimension, SpatialGridDescriptor,
                SpatialGridDescriptorState, TimeDescriptor, TimeDimension,
            },
            services::{
                AddDataset, CreateDataset, DataPath, DatasetDefinition, MetaDataDefinition,
            },
        },
    },
    datasets::{DatasetName, dataset_listing_provider, upload::VolumeName},
    permissions::{Permission, Role},
    util::sentinel_2_utm_zones::UtmZone,
};

const EXTERNAL_VOLUME_NAME: &str = "external";

/// Checks if the Geo Engine server is alive
#[derive(Debug, Parser)]
pub struct StacImport {
    /// STAC API URL
    #[arg(long, default_value = "https://earth-search.aws.element84.com/v1")]
    stac_url: String,

    // collection to import from
    #[arg(long, default_value = "sentinel-2-l2a")]
    stac_collection: String,

    // import limit
    #[arg(long, default_value = None)]
    limit: Option<usize>,

    // time range start to import
    #[arg(long, default_value = "2020-01-01T00:00:00Z")]
    time_start: String,

    // time range end to import
    #[arg(long, default_value = "2020-12-31T23:59:59Z")]
    time_end: String,

    // bbox to import: minx miny maxx maxy
    #[clap(short, long, value_parser, num_args = 1.., value_delimiter = ' ', default_values = &["0.3184423382324359", "-0.0884814721075339", "20.784002046916676", "72.0970954730969"])]
    bbox: Vec<f64>,

    // // bands to import
    // #[clap(short, long, value_parser, num_args = 1.., value_delimiter = ' ', default_values = &["aot", "nir08", "rededge1", "rededge2", "rededge3", "scl", "swir16", "swir22"])]
    // bands: Vec<String>,

    // epsg codes of the items to import
    #[clap(short, long, value_parser, num_args = 1.., value_delimiter = ' ', default_values = &["32630"])]
    epsgs: Vec<u32>,

    // // time range end to import
    // #[arg(long, default_value_t = true)]
    // create_dataset: bool,
    /// Geo Engine API URL
    #[arg(long, default_value = "http://localhost:3030/api")]
    geo_engine_url: String,

    /// Geo Engine API email
    #[arg(long, default_value = "admin@localhost")]
    geo_engine_email: String,

    /// Geo Engine API password
    #[arg(long, default_value = "adminadmin")]
    geo_engine_password: String,

    /// volume on the server
    #[arg(long, default_value = "geodata")]
    volume_name: String,

    #[arg(long, default_value = "Sentinal2")]
    dataset_name_prefix: String,
    // TODO: time granularity (validity of items)

    // /// Parent layer collection ID
    // #[arg(long, default_value_t = INTERNAL_LAYER_DB_ROOT_COLLECTION_ID)]
    // parent_layer_collection_id: Uuid,

    // /// Name of the layer collection to create/use
    // #[arg(long, default_value = "FORCE")]
    // layer_collection_name: String,

    // /// whether to share the layers with registered and anonymous users
    // #[arg(long, value_enum, default_value_t = LayerShareMode::Share)]
    // share_layers: LayerShareMode,
}

pub async fn stac_import(params: StacImport) -> Result<(), anyhow::Error> {
    let (client, session_id) = login(
        &params.geo_engine_url,
        &params.geo_engine_email,
        &params.geo_engine_password,
    )
    .await?;

    let query_params = create_query_params(&params);

    let bands_by_data_type = scan_collection(&params, &client)
        .await
        .context("Failed to scan collection")?;

    println!("Scanned collection, found bands for data types:");
    for (data_type, bands) in &bands_by_data_type {
        println!(
            "  {:?}: {} bands",
            data_type,
            bands
                .iter()
                .map(|b| b.name.as_str())
                .collect::<Vec<&str>>()
                .join(", ")
        );
    }

    // keep track of created datasets to create them on-the-fly when an items occurs that gives us the data needed to create it
    let mut created_datasets: HashSet<DatasetKey> = HashSet::new();

    let mut query_state = QueryState::FirstPage {
        query_url: format!(
            "{}/collections/{}/items",
            params.stac_url, params.stac_collection
        ),
        query_params,
    };

    // process page by page and insert all tiles as they come
    loop {
        let (item_collection, new_query_state) =
            query_item_collection(&client, &query_state).await?;

        if matches!(new_query_state, QueryState::Finished) || item_collection.items.is_empty() {
            break;
        }

        query_state = new_query_state;

        let dataset_tiles = process_items(
            &params,
            &client,
            &session_id,
            &bands_by_data_type,
            &mut created_datasets,
            item_collection,
        )
        .await?;

        for (dataset_key, tiles) in &dataset_tiles {
            let dataset_name = dataset_key.dataset_name(&params.stac_collection);
            println!("Adding {} tiles to dataset {}", tiles.len(), dataset_name,);

            let response = client
                .post(format!(
                    "{}/dataset/{}/tiles",
                    params.geo_engine_url, dataset_name
                ))
                .header("Content-Type", "application/json")
                .header("Authorization", format!("Bearer {session_id}"))
                .json(&tiles)
                .send()
                .await
                .context("Failed to send add tiles request")?;

            if !response.status().is_success() {
                let error_text = response.text().await.unwrap_or_default();

                // TODO: retry or continue instead of exit?

                return Err(anyhow::anyhow!(
                    "Failed to add tile to dataset: {error_text}"
                ));
            }
        }
    }

    println!("Dataset tiles added successfully");

    Ok(())
}

async fn process_items(
    params: &StacImport,
    client: &reqwest::Client,
    session_id: &String,
    bands_by_data_type: &HashMap<RasterDataType, Vec<RasterBandDescriptor>>,
    created_datasets: &mut HashSet<DatasetKey>,
    item_collection: stac::ItemCollection,
) -> Result<HashMap<DatasetKey, Vec<AddDatasetTile>>, anyhow::Error> {
    let mut dataset_tiles = HashMap::new();

    for item in item_collection.items {
        let mut date = item.properties.datetime.unwrap(); // TODO: handle Option
        date = date
            .with_hour(0)
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_nanosecond(0))
            .context("Failed to set time to zero")?;
        let date: DateTime = date.into();
        let time: TimeInstance = date.into();

        let epsg = item
            .properties
            .additional_fields
            .get("proj:epsg")
            .and_then(|v| v.as_u64())
            .context("Missing proj:epsg in item properties")? as u32;

        for (asset_key, asset) in &item.assets {
            if let Err(err) = process_item(
                params,
                client,
                session_id,
                bands_by_data_type,
                created_datasets,
                asset_key,
                asset,
                epsg,
                date,
                time,
                &mut dataset_tiles,
            )
            .await
            {
                eprintln!(
                    "Skipping asset {} of item {}: {:#}",
                    asset_key, item.id, err
                );
            }
        }
    }

    Ok(dataset_tiles)
}

async fn process_item(
    params: &StacImport,
    client: &reqwest::Client,
    session_id: &String,
    bands_by_data_type: &HashMap<RasterDataType, Vec<RasterBandDescriptor>>,
    created_datasets: &mut HashSet<DatasetKey>,
    asset_key: &str,
    asset: &Asset,
    epsg: u32,
    date: DateTime,
    time: TimeInstance,
    dataset_tiles: &mut HashMap<DatasetKey, Vec<AddDatasetTile>>,
) -> Result<(), anyhow::Error> {
    if asset.r#type != Some("image/tiff; application=geotiff; profile=cloud-optimized".to_string())
    {
        anyhow::bail!("non-geotiff asset");
    }

    let geo_transform = geo_transform_from_fields(&asset.additional_fields)
        .ok_or(anyhow::anyhow!("missing proj:transform"))?;

    let data_type = data_type_from_asset(asset).context("unknown data type")?;

    let dataset_key = DatasetKey {
        epsg,
        data_type,
        resolution: geo_transform.x_pixel_size().into(),
    };

    if !created_datasets.contains(&dataset_key) {
        // create dataset on-the-fly

        dbg!(&dataset_key);
        dbg!(&created_datasets);

        // TODO: if dataset already exists on server, skip creation, but check compatibility?
        create_dataset(
            params,
            client,
            session_id,
            &dataset_key,
            &bands_by_data_type,
            geo_transform,
        )
        .await
        .context(format!("failed to create dataset {:?}", dataset_key))?;

        created_datasets.insert(dataset_key.clone());
    }

    let eo_bands: Vec<EoBand> = serde_json::from_value(
        asset
            .additional_fields
            .get("eo:bands")
            .ok_or(anyhow::anyhow!("Missing eo:bands in asset"))?
            .clone(),
    )
    .context("Failed to parse eo:bands")?;

    let dataset_bands = bands_by_data_type
        .get(&dataset_key.data_type)
        .ok_or(anyhow::anyhow!("unknown dataset key: {:?}", dataset_key))?;

    // if multiple bands for asset, prefix band names with asset key
    let prefix = if eo_bands.len() > 1 {
        format!("{}_", asset_key)
    } else {
        "".to_string()
    };

    for eo_band in &eo_bands {
        process_band(
            asset_key,
            asset,
            date,
            time,
            dataset_tiles,
            geo_transform,
            &dataset_key,
            dataset_bands,
            &prefix,
            eo_band,
        )
        .context(format!("Failed to process band {}", eo_band.name))?;
    }

    Ok(())
}

fn process_band(
    asset_key: &str,
    asset: &Asset,
    date: DateTime,
    time: TimeInstance,
    dataset_tiles: &mut HashMap<DatasetKey, Vec<AddDatasetTile>>,
    geo_transform: GeoTransform,
    dataset_key: &DatasetKey,
    dataset_bands: &Vec<RasterBandDescriptor>,
    prefix: &String,
    eo_band: &EoBand,
) -> anyhow::Result<()> {
    let band_name = format!("{}{}", prefix, eo_band.name);

    let band_index = dataset_bands
        .iter()
        .position(|b| b.name == band_name.as_str())
        .ok_or(anyhow::anyhow!("unknown band: {}", band_name))?;

    let tile_file = asset.href.clone();

    let proj_shape = asset
        .additional_fields
        .get("proj:shape")
        .ok_or(anyhow::anyhow!("missing proj:shape"))?;

    let proj_shape = proj_shape
        .as_array()
        .ok_or(anyhow::anyhow!("proj:shape is not an array"))?;

    let (height, width) = (
        proj_shape[0].as_u64().unwrap() as usize,
        proj_shape[1].as_u64().unwrap() as usize,
    );

    let grid_bounds = geoengine_datatypes::raster::GridBoundingBox2D::new(
        GridIdx2D { x_idx: 0, y_idx: 0 },
        GridIdx2D {
            x_idx: (width - 1) as isize,
            y_idx: (height - 1) as isize,
        },
    )
    .unwrap();

    let spatial_partition = geo_transform.grid_to_spatial_bounds(&grid_bounds);

    println!(
        "Importing tile: date: {}, band: {}, href: {}",
        date, asset_key, asset.href
    );

    let tile = AddDatasetTile {
        time: TimeInterval::new(time, time + i64::from(24 * 60 * 60 * 1000)) // TODO
            .unwrap()
            .into(),
        spatial_partition: spatial_partition.into(),
        band: band_index as u32,
        z_index: 0, // TODO: collect all possibly overlapping items...
        params: GdalDatasetParameters {
            file_path: format!("/vsicurl/{}", tile_file).into(),
            rasterband_channel: 1, // TODO
            geo_transform: geo_transform.into(),
            width: width,
            height: height,
            file_not_found_handling: crate::api::model::operators::FileNotFoundHandling::Error,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: false,
        },
    };

    dataset_tiles
        .entry(dataset_key.clone())
        .or_insert_with(Vec::new)
        .push(tile);

    Ok(())
}

async fn query_item_collection(
    client: &reqwest::Client,
    query_state: &QueryState,
) -> Result<(stac::ItemCollection, QueryState), anyhow::Error> {
    println!("Querying STAC items: {:?}", query_state);

    match query_state {
        QueryState::FirstPage {
            query_url,
            query_params,
        } => {
            let item_collection: stac::ItemCollection = client
                .get(query_url)
                .query(&query_params)
                .send()
                .await?
                .json()
                .await?;

            let new_query_state = if let Some(next_link) =
                item_collection.links.iter().find(|link| link.rel == "next")
            {
                QueryState::NextPage {
                    next_url: next_link.href.clone(),
                }
            } else {
                QueryState::Finished
            };

            Ok((item_collection, new_query_state))
        }
        QueryState::NextPage { next_url } => {
            let item_collection: stac::ItemCollection =
                client.get(next_url).send().await?.json().await?;

            let new_query_state = if let Some(next_link) =
                item_collection.links.iter().find(|link| link.rel == "next")
            {
                QueryState::NextPage {
                    next_url: next_link.href.clone(),
                }
            } else {
                QueryState::Finished
            };

            Ok((item_collection, new_query_state))
        }
        QueryState::Finished => anyhow::bail!("No more pages to query"),
    }
}

fn create_query_params(params: &StacImport) -> Vec<(String, String)> {
    let mut query_params = vec![
        (
            "bbox".to_owned(),
            format!(
                "{},{},{},{}", // array-brackets are not used in standard but required here for unknkown reason
                params.bbox[0], params.bbox[1], params.bbox[2], params.bbox[3]
            ),
        ), // TODO: order coordinates depending on projection
        (
            "datetime".to_owned(),
            format!("{}/{}", params.time_start, params.time_end),
        ),
    ];

    if let Some(limit) = params.limit {
        query_params.push(("limit".to_owned(), limit.to_string()));
    }

    query_params
}

async fn create_dataset(
    params: &StacImport,
    client: &reqwest::Client,
    session_id: &str,
    dataset_key: &DatasetKey,
    scanned_collection: &HashMap<RasterDataType, Vec<RasterBandDescriptor>>,
    geo_transform: GeoTransform,
) -> Result<(), anyhow::Error> {
    let dataset_name_str = &dataset_key.dataset_name(&params.stac_collection);

    println!(
        "Creating dataset: {} for {:?}",
        dataset_name_str, dataset_key
    );

    let bands = scanned_collection
        .get(&dataset_key.data_type)
        .context("Failed to get bands for dataset")?;

    let create_dataset = CreateDataset {
        data_path: DataPath::Volume(VolumeName(EXTERNAL_VOLUME_NAME.to_string())),
        definition: DatasetDefinition {
            properties: AddDataset {
                name: Some(
                    DatasetName::from_str(dataset_name_str)
                        .context("Failed to create dataset name")?,
                ),
                display_name: dataset_name_str.to_string(),
                description: format!(
                    "{dataset_name_str} imported from STAC {}",
                    params.stac_collection
                ),
                source_operator: "MultiBandGdalSource".to_string(),
                symbology: None,
                provenance: None,
                tags: None,
            },
            meta_data: MetaDataDefinition::GdalMultiBand(GdalMultiBand {
                r#type: crate::api::model::operators::GdalMultiBandTypeTag::GdalMultiBandTypeTag,
                result_descriptor: RasterResultDescriptor {
                    data_type: dataset_key.data_type,
                    spatial_reference: SpatialReferenceOption::SpatialReference(
                        SpatialReference::new(SpatialReferenceAuthority::Epsg, dataset_key.epsg),
                    )
                    .into(),
                    time: TimeDescriptor {
                        bounds: None, // TODO: from params
                        dimension: TimeDimension::Regular(RegularTimeDimension {
                            // TODO: irregular?
                            origin: TimeInstance::from_millis(0).unwrap().into(), // TODO
                            step: TimeStep {
                                granularity: TimeGranularity::Days, // TODO
                                step: 1,                            // TODO
                            },
                        }),
                    },
                    spatial_grid: SpatialGridDescriptor {
                        spatial_grid: SpatialGridDefinition {
                            geo_transform: geo_transform.into(),
                            grid_bounds: GridBoundingBox2D {
                                top_left_idx: GridIdx2D { x_idx: 0, y_idx: 0 },
                                bottom_right_idx: GridIdx2D { x_idx: 1, y_idx: 1 }, // TODO, but will be overridden when adding tiles anyway
                            }, // TODO from  query bbox and asset proj:shape??
                        },
                        descriptor: SpatialGridDescriptorState::Source,
                    },
                    bands: RasterBandDescriptors::new(bands.clone())
                        .context(format!("Failed to create band descriptors {:?}", bands))?,
                },
            }),
        },
    };

    let response = client
        .post(format!("{}/dataset", params.geo_engine_url))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {session_id}"))
        .json(&create_dataset)
        .send()
        .await
        .context("Failed to send dataset creation request")?;

    let dataset_name = if let Ok(json) = response.json::<serde_json::Value>().await {
        if let Some(id) = json.get("datasetName").and_then(|v| v.as_str()) {
            println!("Dataset added successfully");
            id.to_string()
        } else {
            anyhow::bail!("Failed to get dataset id from response: {json:?}");
        }
    } else {
        println!();
        anyhow::bail!("Failed to parse dataset creation response as JSON");
    };

    let permissions = vec![
        PermissionRequest {
            resource: crate::api::handlers::permissions::Resource::Dataset(
                DatasetResource {
                    id: DatasetName::new(None, dataset_name.clone()),
                    r#type: crate::api::handlers::permissions::DatasetResourceTypeTag::DatasetResourceTypeTag
                },
            ),
            role_id: Role::registered_user_role_id(),
            permission: Permission::Read,
        },
        PermissionRequest {
            resource: crate::api::handlers::permissions::Resource::Dataset(
                DatasetResource {
                    id: DatasetName::new(None, dataset_name.clone()),
                    r#type: crate::api::handlers::permissions::DatasetResourceTypeTag::DatasetResourceTypeTag
                },
            ),
            role_id: Role::anonymous_role_id(),
            permission: Permission::Read,
        },
    ];

    for permission in &permissions {
        let response = client
            .put(format!("{}/permissions", params.geo_engine_url))
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {session_id}"))
            .json(&permission)
            .send()
            .await
            .context("Failed to add permission")?;

        println!(
            "Dataset '{}' shared with role {}: {}",
            dataset_name,
            permission.role_id,
            response.text().await.unwrap_or_default()
        );
    }

    Ok(())
}

#[derive(Debug)]
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

/// STAC collection is split up by epsg, data_type, resolution/grid to produce uniform Geo Engine datasets
/// Bands are grouped by data type
/// Different epsg and resolutions produce different datasets with the same bands
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct DatasetKey {
    epsg: u32,
    data_type: RasterDataType,
    resolution: OrderedFloat<f64>,
}

impl DatasetKey {
    fn dataset_name(&self, collection_name: &str) -> String {
        let cleaned_name = collection_name
            .chars()
            .map(|c| {
                if geoengine_datatypes::dataset::is_invalid_name_char(c) {
                    '_'
                } else {
                    c
                }
            })
            .collect::<String>();

        format!(
            "{}_EPSG{}_{:?}_{}",
            cleaned_name, self.epsg, self.data_type, self.resolution
        )
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct ProjTransform {
    origin_x: OrderedFloat<f64>,
    origin_y: OrderedFloat<f64>,
    pixel_size_x: OrderedFloat<f64>,
    pixel_size_y: OrderedFloat<f64>,
}

impl From<GeoTransform> for ProjTransform {
    fn from(gt: GeoTransform) -> Self {
        ProjTransform {
            origin_x: OrderedFloat(gt.origin_coordinate.x),
            origin_y: OrderedFloat(gt.origin_coordinate.y),
            pixel_size_x: OrderedFloat(gt.x_pixel_size()),
            pixel_size_y: OrderedFloat(gt.y_pixel_size()),
        }
    }
}

impl From<ProjTransform> for crate::api::model::datatypes::GeoTransform {
    fn from(pt: ProjTransform) -> Self {
        crate::api::model::datatypes::GeoTransform {
            origin_coordinate: crate::api::model::datatypes::Coordinate2D {
                x: pt.origin_x.into_inner(),
                y: pt.origin_y.into_inner(),
            },
            x_pixel_size: pt.pixel_size_x.into_inner(),
            y_pixel_size: pt.pixel_size_y.into_inner(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct EoBand {
    name: String,
    description: String,
    // ...
}

struct KeyedAssetBand<'a> {
    key: String,
    band_index: usize,
    is_multi_band_asset: bool,
    asset: &'a stac::ItemAsset,
}

// Partial data for dataset, gathered from collection API and used as blueprint for creating datasets when scanning items
// which give us the remining info (spatial grid)
struct DatasetPlaceholder {
    // data_path: DataPath,
    // properties: AddDataset,
    // time_descriptor: TimeDescriptor,
    data_type: RasterDataType,
    bands: RasterBandDescriptors,
}

async fn scan_collection(
    params: &StacImport,
    client: &reqwest::Client,
) -> anyhow::Result<HashMap<RasterDataType, Vec<RasterBandDescriptor>>> {
    let collection: stac::Collection = client
        .get(format!(
            "{}/collections/{}",
            params.stac_url, params.stac_collection
        ))
        .send()
        .await?
        .json()
        .await?;

    // create datasets by grouping items by (epsg, data_type, resolution/grid)
    // because datasets must have uniform epsg, data_type, resolution

    let mut dataset_bands: HashMap<RasterDataType, Vec<RasterBandDescriptor>> = HashMap::new();

    for (asset_key, asset) in &collection.item_assets {
        if let Err(err) = scan_item_asset(asset_key, asset, &mut dataset_bands)
            .context(format!("Failed to scan item asset {}", asset_key))
        {
            eprintln!("Skipping asset {}: {:#}", asset_key, err);
        }
    }
    Ok(dataset_bands)
}

fn scan_item_asset(
    asset_key: &str,
    asset: &stac::ItemAsset,
    dataset_bands: &mut HashMap<RasterDataType, Vec<RasterBandDescriptor>>,
) -> anyhow::Result<()> {
    if asset.r#type != Some("image/tiff; application=geotiff; profile=cloud-optimized".to_string())
    {
        anyhow::bail!("Skipping non-geotiff asset: {}", asset_key);
    }

    let raster_bands: Vec<stac_extensions::raster::Band> = asset
        .additional_fields
        .get("raster:bands")
        .ok_or(anyhow::anyhow!("Missing raster:bands"))
        .and_then(|bands| {
            serde_json::from_value(bands.clone())
                .map_err(|e| anyhow::anyhow!("invalid raster:bands: {}", e))
        })?;

    let eo_bands: Vec<EoBand> = asset
        .additional_fields
        .get("eo:bands")
        .ok_or(anyhow::anyhow!("Missing eo:bands in asset"))
        .and_then(|eo_bands| {
            serde_json::from_value(eo_bands.clone())
                .map_err(|e| anyhow::anyhow!("invalid eo:bands: {}", e))
        })?;

    if raster_bands.len() != eo_bands.len() {
        anyhow::bail!("Skipping asset with mismatched raster:bands and eo:bands length",);
    }

    // if multiple bands for asset, prefix band names with asset key
    let prefix = if raster_bands.len() > 1 {
        format!("{}_", asset_key)
    } else {
        "".to_string()
    };

    for (raster_band, eo_band) in raster_bands.into_iter().zip(eo_bands.into_iter()) {
        let data_type = raster_band
            .data_type
            .ok_or(anyhow::anyhow!("Missing data_type in raster band"))?;

        let band_name = format!("{}{}", prefix, eo_band.name);

        dataset_bands
                .entry(raster_data_type_from_stac_data_type(&data_type)?)
                .or_insert_with(Vec::new)
                .push(RasterBandDescriptor {
                    name: band_name,
                    // TODO: unit from raster_band.unit
                    measurement: Measurement::Unitless(UnitlessMeasurement { r#type: crate::api::model::datatypes::UnitlessMeasurementTypeTag::UnitlessMeasurementTypeTag }),
                });
    }

    Ok(())
}

// fn spatial_grid_definitions_from_fields(fields: serde_json::Map<String, serde_json::Value>) -> Option<SpatialGridDefinition> {
//     let geo_transform = geo_transform_from_fields(fields.clone())?;
//     let Some(proj_shape) = fields.get("proj:shape") else {
//         return None;
//     };
//     let proj_shape_array = proj_shape.as_array().unwrap();
//     let (height, width) = (
//         proj_shape_array[0].as_u64().unwrap() as usize,
//         proj_shape_array[1].as_u64().unwrap() as usize,
//     );

//     let grid_bounds =  geoengine_datatypes::raster::GridBoundingBox2D::new(
//         GridIdx2D { x_idx: 0, y_idx: 0 },
//         GridIdx2D { x_idx: (width - 1) as isize, y_idx: (height - 1) as isize},
//     ).unwrap();

//     Some(SpatialGridDefinition {
//         geo_transform,
//         grid_bounds,
//     }.into())
// }

fn geo_transform_from_fields(
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Option<GeoTransform> {
    let Some(proj_transform) = fields.get("proj:transform") else {
        return None;
    };
    let proj_transform_array = proj_transform.as_array().unwrap();
    let proj_transform_values: Vec<f64> = proj_transform_array
        .iter()
        .filter_map(|v| v.as_f64())
        .collect();
    let gdal_geotransform: GdalGeoTransform = [
        proj_transform_values[2], // g[0] = a2 (x origin)
        proj_transform_values[0], // g[1] = a0 (pixel width)
        proj_transform_values[1], // g[2] = a1 (rotation)
        proj_transform_values[5], // g[3] = a5 (y origin)
        proj_transform_values[3], // g[4] = a3 (rotation)
        proj_transform_values[4], // g[5] = a4 (pixel height, negative)
    ];
    let geo_transform: GeoTransform = gdal_geotransform.into();
    Some(geo_transform)
}

fn data_type_from_asset(asset: &Asset) -> anyhow::Result<RasterDataType> {
    let data_type_str = asset
        .additional_fields
        .get("raster:bands")
        .and_then(|v| v.as_array())
        .and_then(|bands| bands.first())
        .and_then(|band| band.get("data_type"))
        .and_then(|v| v.as_str())
        .ok_or(anyhow::anyhow!("Missing data_type in raster:bands[0]"))?;

    raster_data_type_from_stac_data_type_str(data_type_str)
}

fn raster_data_type_from_stac_data_type_str(data_type_str: &str) -> anyhow::Result<RasterDataType> {
    Ok(match data_type_str.to_lowercase().as_str() {
        "uint8" => RasterDataType::U8,
        "uint16" => RasterDataType::U16,
        "uint32" => RasterDataType::U32,
        "int16" => RasterDataType::I16,
        "int32" => RasterDataType::I32,
        "float32" => RasterDataType::F32,
        "float64" => RasterDataType::F64,
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported raster data type: {}",
                data_type_str
            ));
        }
    })
}

fn raster_data_type_from_stac_data_type(
    data_type: &stac_extensions::raster::DataType,
) -> anyhow::Result<RasterDataType> {
    Ok(match data_type {
        stac_extensions::raster::DataType::UInt8 => RasterDataType::U8,
        stac_extensions::raster::DataType::UInt16 => RasterDataType::U16,
        stac_extensions::raster::DataType::UInt32 => RasterDataType::U32,
        stac_extensions::raster::DataType::Int16 => RasterDataType::I16,
        stac_extensions::raster::DataType::Int32 => RasterDataType::I32,
        stac_extensions::raster::DataType::Float32 => RasterDataType::F32,
        stac_extensions::raster::DataType::Float64 => RasterDataType::F64,
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported raster data type: {:?}",
                data_type
            ));
        }
    })
}

async fn login(
    geo_engine_url: &str,
    geo_engine_email: &str,
    geo_engine_password: &str,
) -> anyhow::Result<(reqwest::Client, String)> {
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{geo_engine_url}/login"))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "email": geo_engine_email,
            "password": geo_engine_password,
        }))
        .send()
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

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::test_data;

    #[test]
    fn it_parses_stac_json() {
        // read stac.json from crate root
        let stac_json = std::fs::read_to_string(test_data!("../stac.json")).unwrap();
        // let feature_collection: FeatureCollection =
        //     serde_json::from_str(&stac_json).expect("Failed to parse stac.json");
        // println!("{:?}", feature_collection);

        let feature_collection: stac::ItemCollection =
            serde_json::from_str(&stac_json).expect("Failed to parse stac.json");
        // println!("{:?}", feature_collection)

        println!("Feature Collection");

        feature_collection
            .additional_fields
            .iter()
            .for_each(|(key, value)| {
                println!("    {}: {}", key, value);
            });

        feature_collection.items.iter().for_each(|item| {
            println!("Item ID: {}", item.id);
            println!("  Fields:");
            item.assets.iter().for_each(|(key, asset)| {
                println!("  Asset Key: {}", key);
                println!("    Title: {:?}", asset.title);
                println!("    Type: {:?}", asset.r#type);
                println!("    Href: {}", asset.href);
                if let Some(bands) = asset.additional_fields.get("raster:bands") {
                    if let Some(bands_array) = bands.as_array() {
                        bands_array.iter().for_each(|band| {
                            if let Some(name) = band.get("name").and_then(|n| n.as_str()) {
                                println!("    Raster Band Name: {}", name);
                            }
                        });
                    }
                }
            });
        });
    }

    #[test]
    fn it_parses_raster_bands() {
        let json = r#" [
            {
              "nodata": 0,
              "data_type": "uint16",
              "bits_per_sample": 15,
              "spatial_resolution": 20,
              "unit": "cm",
              "scale": 0.001,
              "offset": 0
            }
          ]"#;

        let _raster_bands: Vec<stac_extensions::raster::Band> =
            serde_json::from_str(json).expect("Failed to parse raster bands");
    }

    #[tokio::test]
    async fn it_imports() {
        let params: StacImport = StacImport::parse_from(["stac-import"]);
        stac_import(params).await.unwrap();
    }
}
