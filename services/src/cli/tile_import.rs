#![allow(clippy::print_stdout)]

use crate::api::handlers::datasets::AddDatasetTile;
use crate::api::handlers::permissions::{
    LayerCollectionResource, LayerResource, PermissionRequest,
};
use crate::api::model::datatypes::LayerId;
use crate::api::model::operators::{GdalDatasetParameters, GdalMultiBand};
use crate::api::model::responses::IdResponse;
use crate::api::model::services::{
    AddDataset, CreateDataset, DataPath, DatasetDefinition, MetaDataDefinition,
};
use crate::datasets::DatasetName;
use crate::datasets::upload::VolumeName;
use crate::layers::layer::{AddLayer, AddLayerCollection};
use crate::layers::listing::LayerCollectionId;
use crate::layers::storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID;
use crate::permissions::{Permission, Role};
use crate::workflows::workflow::Workflow;
use anyhow::Context;
use chrono::{NaiveDate, TimeZone};
use clap::Parser;
use gdal::{Dataset as GdalDataset, Metadata};
use geoengine_datatypes::dataset::NamedData;
use geoengine_datatypes::primitives::{
    Coordinate2D, DateTime, Measurement, SpatialPartition2D, TimeInstance, TimeInterval,
};
use geoengine_datatypes::raster::GdalGeoTransform;
use geoengine_operators::engine::{RasterBandDescriptor, RasterOperator, RasterResultDescriptor};
use geoengine_operators::source::{
    GdalDatasetGeoTransform, MultiBandGdalSource, MultiBandGdalSourceParameters,
};
use geoengine_operators::util::gdal::{
    measurement_from_rasterband, raster_descriptor_from_dataset,
};
use regex::Regex;
use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use uuid::Uuid;

/// Checks if the Geo Engine server is alive
#[derive(Debug, Parser)]
pub struct TileImport {
    /// where the files are scanned
    #[arg(long, default_value = "/home/michael/geodata/force/marburg")]
    local_data_dir: String,

    /// volume on the server that points to the path of the same files
    #[arg(long, default_value = "force")]
    volume_name: String,

    /// file extension to look for
    #[arg(long, default_value = "tif")]
    file_extension: String,

    /// whether to scan directories recursively
    #[arg(long, value_enum, default_value_t = FileScanMode::Recursive)]
    scan_mode: FileScanMode,

    /// regex to extract time from file names
    #[arg(long, default_value = r"(\d{8})_LEVEL2")]
    time_regex: String,

    /// strftime format for time
    #[arg(long, default_value = "%Y%m%d")]
    time_format: String,

    /// duration of each tile in seconds
    #[arg(long, default_value_t = 60 * 60 * 24)]
    tile_duration_seconds: u32,

    /// regex to extract product name from file names
    #[arg(long, default_value = r"LEVEL2_(\w+_\w+)\.tif")]
    product_name_regex: String,

    /// Geo Engine API URL
    #[arg(long, default_value = "http://localhost:3030/api")]
    geo_engine_url: String,

    /// Geo Engine API email
    #[arg(long, default_value = "admin@localhost")]
    geo_engine_email: String,

    /// Geo Engine API password
    #[arg(long, default_value = "adminadmin")]
    geo_engine_password: String,

    /// Parent layer collection ID
    #[arg(long, default_value_t = INTERNAL_LAYER_DB_ROOT_COLLECTION_ID)]
    parent_layer_collection_id: Uuid,

    /// Name of the layer collection to create/use
    #[arg(long, default_value = "FORCE")]
    layer_collection_name: String,

    /// whether to share the layers with registered and anonymous users
    #[arg(long, value_enum, default_value_t = LayerShareMode::Share)]
    share_layers: LayerShareMode,
}

#[derive(clap::ValueEnum, Clone, Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum FileScanMode {
    NonRecursive,
    #[default]
    Recursive,
}

#[derive(clap::ValueEnum, Clone, Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum LayerShareMode {
    DoNotShare,
    #[default]
    Share,
}

/// A simple importer of tiled datasets that have
///  * multiple time steps
///  * each time step has one or more corresponding files
///  * each file of the dataset has the same bands
///  * the time and product name are encoded in each file name
///
/// One example is the FORCE dataset where the rasters are stored in multiple tiles for each time step and the files look like this
/// `/geodata/force/marburg/X0059_Y0049/20000124_LEVEL2_LND07_BOA.tif`
///
/// The datasets are inserted into a Geo Engine instance via its REST API.
/// The files are scanned from a given local directory.
/// On the server, the files are expected to be available via a volume mapping.
pub async fn tile_import(params: TileImport) -> Result<(), anyhow::Error> {
    let time_regex = Regex::new(&params.time_regex).context("Invalid time regex")?;
    let product_regex = Regex::new(&params.product_name_regex).context("Invalid product regex")?;

    let (client, session_id) = login(
        &params.geo_engine_url,
        &params.geo_engine_email,
        &params.geo_engine_password,
    )
    .await?;

    let layer_collection_id = create_layer_collection_if_not_exists(
        &session_id,
        &client,
        &params.geo_engine_url,
        params.parent_layer_collection_id,
        &params.layer_collection_name,
        params.share_layers == LayerShareMode::Share,
    )
    .await?;

    let mut files = Vec::new();
    collect_files(
        Path::new(&params.local_data_dir),
        &params.file_extension,
        params.scan_mode == FileScanMode::Recursive,
        &mut files,
    )?;

    println!("Found {} files", files.len());

    let products =
        extract_products_from_files(&time_regex, &params.time_format, &product_regex, files);

    for product in products {
        let num_timesteps = product
            .1
            .iter()
            .map(|f| f.time)
            .collect::<std::collections::HashSet<_>>()
            .len();
        println!(
            "Found Product: {} ({} files, {num_timesteps} time steps)",
            product.0,
            product.1.len()
        );

        let dataset_name = add_dataset_and_tiles_to_geoengine(
            &session_id,
            &client,
            &product.0,
            &product.1,
            params.tile_duration_seconds,
            &params.geo_engine_url,
            &params.local_data_dir,
            &params.volume_name,
        )
        .await?;

        if let Some(dataset_name) = dataset_name {
            add_dataset_to_collection(
                &session_id,
                &client,
                &dataset_name,
                &layer_collection_id,
                &product.0,
                &params.geo_engine_url,
                params.share_layers == LayerShareMode::Share,
            )
            .await?;
        }
    }

    Ok(())
}

async fn add_dataset_to_collection(
    session_id: &str,
    client: &reqwest::Client,
    dataset_name: &str,
    layer_collection_id: &str,
    layer_name: &str,
    geo_engine_url: &str,
    share_layer: bool,
) -> anyhow::Result<()> {
    let add_layer = AddLayer {
        name: layer_name.to_string(),
        description: String::new(),
        workflow: Workflow {
            operator: geoengine_operators::engine::TypedOperator::Raster(
                MultiBandGdalSource {
                    params: MultiBandGdalSourceParameters {
                        data: NamedData {
                            namespace: None,
                            provider: None,
                            name: dataset_name.to_string(),
                        },
                        overview_level: None,
                    },
                }
                .boxed(),
            ),
        },
        symbology: None, // TODO: add symbology
        properties: vec![],
        metadata: Default::default(),
    };

    let response: IdResponse<LayerId> = client
        .post(format!(
            "{geo_engine_url}/layerDb/collections/{layer_collection_id}/layers"
        ))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {session_id}"))
        .json(&add_layer)
        .send()
        .await
        .context("Failed to add layer to collection")?
        .json()
        .await
        .context("Failed to parse layer response")?;

    if share_layer {
        let permissions = vec![
            PermissionRequest {
                resource: crate::api::handlers::permissions::Resource::Layer(
                    LayerResource {
                        id:response.id.clone(),
                        r#type: crate::api::handlers::permissions::LayerResourceTypeTag::LayerResourceTypeTag
                 },
                ),
                role_id: Role::registered_user_role_id(),
                permission: Permission::Read,
            },
            PermissionRequest {
                resource: crate::api::handlers::permissions::Resource::Layer(
                    LayerResource {
                        id:response.id.clone(),
                         r#type: crate::api::handlers::permissions::LayerResourceTypeTag::LayerResourceTypeTag 
                    },
                ),
                role_id: Role::anonymous_role_id(),
                permission: Permission::Read,
            },
        ];

        for permission in &permissions {
            let response = client
                .put(format!("{geo_engine_url}/permissions"))
                .header("Content-Type", "application/json")
                .header("Authorization", format!("Bearer {session_id}"))
                .json(&permission)
                .send()
                .await
                .context("Failed to add permission")?;

            println!(
                "Layer '{}' shared with role {}: {}",
                layer_name,
                permission.role_id,
                response.text().await.unwrap_or_default()
            );
        }
    }

    Ok(())
}

async fn create_layer_collection_if_not_exists(
    session_id: &str,
    client: &reqwest::Client,
    geo_engine_url: &str,
    parent_layer_collection_id: Uuid,
    layer_collection_name: &str,
    share_layer: bool,
) -> anyhow::Result<String> {
    let add_collection = AddLayerCollection {
        name: layer_collection_name.to_string(),
        description: format!("Layer collection for {layer_collection_name}",),
        properties: vec![],
    };

    let response: IdResponse<LayerCollectionId> = client
        .post(format!(
            "{geo_engine_url}/layerDb/collections/{parent_layer_collection_id}/collections"
        ))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {session_id}"))
        .json(&add_collection)
        .send()
        .await
        .context("Failed to add layer collection")?
        .json()
        .await
        .context("Failed to parse layer collection response")?;

    // TODO: handle case where collection already exists, but there is no API to get collection by name

    println!(
        "Layer collection '{}' created with id {}",
        layer_collection_name, response.id.0
    );

    if share_layer {
        let permissions = vec![PermissionRequest {
            resource: crate::api::handlers::permissions::Resource::LayerCollection(
                LayerCollectionResource {
                    id: response.id.clone(),
                    r#type: crate::api::handlers::permissions::LayerCollectionResourceTypeTag::LayerCollectionResourceTypeTag,
                },
            ),
            role_id: Role::registered_user_role_id(),
            permission: Permission::Read,
        }, PermissionRequest {
            resource: crate::api::handlers::permissions::Resource::LayerCollection(
                LayerCollectionResource {
                    id: response.id.clone(),
                    r#type: crate::api::handlers::permissions::LayerCollectionResourceTypeTag::LayerCollectionResourceTypeTag,
                },
            ),
            role_id: Role::anonymous_role_id(),
            permission: Permission::Read,
        }];

        for permission in &permissions {
            let response = client
                .put(format!("{geo_engine_url}/permissions"))
                .header("Content-Type", "application/json")
                .header("Authorization", format!("Bearer {session_id}"))
                .json(&permission)
                .send()
                .await
                .context("Failed to add permission")?;

            println!(
                "Layer collection '{}' shared with role {}: {}",
                layer_collection_name,
                permission.role_id,
                response.text().await.unwrap_or_default()
            );
        }
    }

    Ok(response.id.0)
}

struct ProductFile {
    product_name: String,
    path: PathBuf,
    time: TimeInstance,
    geo_transform: GdalDatasetGeoTransform,
    spatial_partition: SpatialPartition2D,
    width: usize,
    height: usize,
    result_descriptor: RasterResultDescriptor,
}

fn naive_date_to_time_instance(date: NaiveDate) -> anyhow::Result<TimeInstance> {
    let time: chrono::DateTime<chrono::Utc> = chrono::Utc.from_utc_datetime(
        &date
            .and_hms_opt(0, 0, 0)
            .with_context(|| format!("Failed to create datetime from date: {}", date))?,
    );
    let time: DateTime = time.into();
    Ok(time.into())
}

fn collect_files(
    dir: &Path,
    extension: &str,
    recursive: bool,
    files: &mut Vec<PathBuf>,
) -> anyhow::Result<()> {
    let entries =
        fs::read_dir(dir).with_context(|| format!("Failed to read directory {:?}", dir))?;

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() && recursive {
            collect_files(&path, extension, recursive, files)?;
        } else if path.is_file()
            && let Some(ext) = path.extension().and_then(|e| e.to_str())
            && ext.eq_ignore_ascii_case(extension)
        {
            files.push(path);
        }
    }

    Ok(())
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn add_dataset_and_tiles_to_geoengine(
    session_id: &str,
    client: &reqwest::Client,
    product_name: &str,
    files: &[ProductFile],
    tile_duration_seconds: u32,
    geo_engine_url: &str,
    local_data_dir: &str,
    volume_name: &str,
) -> anyhow::Result<Option<String>> {
    let create_dataset = CreateDataset {
        data_path: DataPath::Volume(VolumeName(volume_name.to_string())),
        definition: DatasetDefinition {
            properties: AddDataset {
                name: Some(
                    DatasetName::from_str(product_name).context("Failed to create dataset name")?,
                ),
                display_name: format!("{product_name} Dataset"),
                description: format!("Dataset for {product_name}"),
                source_operator: "MultiBandGdalSource".to_string(),
                symbology: None,
                provenance: None,
                tags: None,
            },
            meta_data: MetaDataDefinition::GdalMultiBand(GdalMultiBand {
                r#type: crate::api::model::operators::GdalMultiBandTypeTag::GdalMultiBandTypeTag,
                result_descriptor: files[0].result_descriptor.clone().into(),
            }),
        },
    };

    let response = client
        .post(format!("{geo_engine_url}/dataset"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {session_id}"))
        .json(&create_dataset)
        .send()
        .await
        .context("Failed to send dataset creation request")?;

    let dataset_name = if let Ok(json) = response.json::<serde_json::Value>().await {
        if let Some(id) = json.get("datasetName").and_then(|v| v.as_str()) {
            println!("Dataset added successfully for product: {product_name}, id: {id}");
            id.to_string()
        } else {
            println!("Failed to get dataset id from response: {json:?}");
            return Ok(None);
        }
    } else {
        println!("Failed to parse dataset creation response as JSON");
        return Ok(None);
    };

    let mut tiles = Vec::new();

    for file in files {
        for (band_idx, _band) in file.result_descriptor.bands.iter().enumerate() {
            let tile = AddDatasetTile {
                time: TimeInterval::new(
                    file.time,
                    file.time + i64::from(tile_duration_seconds * 1000),
                )
                .context("Failed to create time interval")?
                .into(),
                spatial_partition: file.spatial_partition.into(),
                band: band_idx as u32,
                z_index: 0, // TODO: implement z-index calculation
                params: GdalDatasetParameters {
                    file_path: file
                        .path
                        .strip_prefix(local_data_dir)
                        .with_context(|| {
                            format!("Failed to strip local data dir from path {:?}", file.path)
                        })?
                        .to_path_buf(),
                    rasterband_channel: band_idx + 1,
                    geo_transform: file.geo_transform.into(),
                    width: file.width,
                    height: file.height,
                    file_not_found_handling:
                        crate::api::model::operators::FileNotFoundHandling::Error,
                    no_data_value: None,
                    properties_mapping: None,
                    gdal_open_options: None,
                    gdal_config_options: None,
                    allow_alphaband_as_mask: false,
                },
            };

            tiles.push(tile);
        }
    }

    let response = client
        .post(format!("{geo_engine_url}/dataset/{dataset_name}/tiles"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {session_id}"))
        .json(&tiles)
        .send()
        .await
        .context("Failed to send add tiles request")?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        println!("Failed to add tile to dataset: {error_text}");
        return Ok(Some(dataset_name));
    }

    println!("Dataset tiles added successfully for product: {product_name}");

    Ok(Some(dataset_name))
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

fn extract_products_from_files(
    time_regex: &Regex,
    time_format: &str,
    product_regex: &Regex,
    files: Vec<PathBuf>,
) -> HashMap<String, Vec<ProductFile>> {
    let product_files = files
        .into_iter()
        .filter_map(|f| {
            match extract_product_from_file(time_regex, time_format, product_regex, f.clone()) {
                Ok(product_file) => Some(product_file),
                Err(e) => {
                    println!("Skipped file {}: {}", f.as_os_str().to_string_lossy(), e);
                    None
                }
            }
        })
        .collect::<Vec<_>>();

    let mut products = HashMap::new();

    for file in product_files {
        let product_name = file.product_name.clone();
        products
            .entry(product_name)
            .or_insert_with(Vec::new)
            .push(file);
    }

    products
}

fn extract_product_from_file(
    time_regex: &Regex,
    time_format: &str,
    product_regex: &Regex,
    file_path: PathBuf,
) -> anyhow::Result<ProductFile> {
    let filename = file_path
        .file_name()
        .and_then(|f| f.to_str())
        .with_context(|| format!("Invalid filename: {:?}", file_path))?;

    let time_match = time_regex
        .captures(filename)
        .with_context(|| format!("Time regex did not match filename: {}", filename))?;

    let product_match = product_regex
        .captures(filename)
        .with_context(|| format!("Product regex did not match filename: {}", filename))?;

    let time_str = time_match
        .get(1)
        .context("No time capture group in regex match")?
        .as_str();

    let product_str = product_match
        .get(1)
        .context("No product capture group in regex match")?
        .as_str();

    let naive_date = NaiveDate::parse_from_str(time_str, time_format)
        .with_context(|| format!("Failed to parse time '{}'", time_str))?;

    let time = naive_date_to_time_instance(naive_date)?;

    let gdal_dataset = GdalDataset::open(&file_path)
        .with_context(|| format!("Failed to open dataset {:?}", file_path))?;

    let geo_transform: GdalGeoTransform = gdal_dataset
        .geo_transform()
        .context("Failed to get geo-transform")?;
    let geo_transform: GdalDatasetGeoTransform = geo_transform.into();

    let (width, height) = gdal_dataset.raster_size();

    let spatial_partition = SpatialPartition2D::new(
        geo_transform.origin_coordinate,
        geo_transform.origin_coordinate
            + Coordinate2D::new(
                width as f64 * geo_transform.x_pixel_size,
                height as f64 * geo_transform.y_pixel_size,
            ),
    )
    .context("Failed to create spatial partition")?;

    // TODO: collect units for all bands
    let mut result_descriptor = raster_descriptor_from_dataset(&gdal_dataset, 1)
        .context("Could not get raster descriptor")?;

    let measurements = (1..=gdal_dataset.raster_count())
        .map(|band| measurement_from_rasterband(&gdal_dataset, band))
        .collect::<Vec<_>>();

    result_descriptor.bands = measurements
        .into_iter()
        .enumerate()
        .map(|(idx, measurement)| {
            let band = gdal_dataset
                .rasterband(idx + 1)
                .with_context(|| format!("Failed to get raster band {}", idx + 1))?;

            let description = band
                .description()
                .unwrap_or_else(|_| format!("band {}", idx + 1));

            Ok(RasterBandDescriptor::new(
                description,
                measurement.unwrap_or(Measurement::Unitless),
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .try_into()
        .context("Failed to convert raster bands")?;

    Ok(ProductFile {
        product_name: product_str.to_string(),
        path: file_path,
        time,
        geo_transform,
        spatial_partition,
        width,
        height,
        result_descriptor,
    })
}
