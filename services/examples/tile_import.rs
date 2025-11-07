#![allow(clippy::print_stdout)]

use chrono::{NaiveDate, TimeZone};
use gdal::Dataset as GdalDataset;
use geoengine_datatypes::dataset::{NamedData};
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
use geoengine_services::api::handlers::datasets::DatasetTile;
use geoengine_services::api::handlers::permissions::{
    LayerCollectionResource, LayerResource, PermissionRequest,
};
use geoengine_services::api::model::datatypes::LayerId;
use geoengine_services::api::model::operators::{GdalDatasetParameters, GdalMultiBand};
use geoengine_services::api::model::responses::IdResponse;
use geoengine_services::api::model::services::{
    AddDataset, CreateDataset, DataPath, DatasetDefinition, MetaDataDefinition,
};
use geoengine_services::datasets::DatasetName;
use geoengine_services::datasets::upload::VolumeName;
use geoengine_services::layers::layer::{AddLayer, AddLayerCollection};
use geoengine_services::layers::listing::LayerCollectionId;
use geoengine_services::layers::storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID;
use geoengine_services::permissions::{Permission, Role};
use geoengine_services::workflows::workflow::Workflow;
use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use uuid::Uuid;

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
/// If the files on the Geo Engine instance are in a different file path, specify the remote data dir.
fn main() {
    // TODO: turn into cli arguments
    const LOCAL_DATA_DIR: &str = "/home/michael/geodata/force/marburg"; // where the files are scanned
    const REMOTE_DATA_DIR: Option<&str> = None; // Some("/geodata/force/marburg"); // path of the same files on the Geo Engine instance, if different
    const FILE_EXTENSION: &str = "tif";
    const RECURSIVE_SCAN: bool = true;
    const TIME_REGEX: &str = r"(\d{8})_LEVEL2";
    const TIME_FORMAT: &str = "%Y%m%d";
    const TILE_DURATION_SECONDS: u32 = 60 * 60 * 24; // TODO: allow other units than seconds and support validity until next tile in time series
    const PRODUCT_NAME_REGEX: &str = r"LEVEL2_(\w+_\w+)\.tif";
    const GEO_ENGINE_URL: &str = "http://localhost:3030/api";
    const GEO_ENGINE_EMAIL: &str = "admin@localhost";
    const GEO_ENGINE_PASSWORD: &str = "adminadmin";
    const PARENT_LAYER_COLLECTION_ID: Uuid = INTERNAL_LAYER_DB_ROOT_COLLECTION_ID;
    const LAYER_COLLECTION_NAME: &str = "FORCE";
    const SHARE_LAYERS: bool = true;

    let time_regex = Regex::new(TIME_REGEX).expect("Invalid time regex");
    let product_regex = Regex::new(PRODUCT_NAME_REGEX).expect("Invalid product regex");

    let (client, session_id) = login(GEO_ENGINE_URL, GEO_ENGINE_EMAIL, GEO_ENGINE_PASSWORD);

    let layer_collection_id = create_layer_collection_if_not_exists(
        &session_id,
        &client,
        GEO_ENGINE_URL,
        PARENT_LAYER_COLLECTION_ID,
        LAYER_COLLECTION_NAME,
        SHARE_LAYERS,
    );

    let mut files = Vec::new();
    collect_files(
        Path::new(LOCAL_DATA_DIR),
        FILE_EXTENSION,
        RECURSIVE_SCAN,
        &mut files,
    );

    println!("Found {} files", files.len());

    let products = extract_products_from_files(&time_regex, TIME_FORMAT, &product_regex, files);

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
            TILE_DURATION_SECONDS,
            GEO_ENGINE_URL,
            LOCAL_DATA_DIR,
            REMOTE_DATA_DIR,
        );

        if let Some(dataset_name) = dataset_name {
            add_dataset_to_collection(
                &session_id,
                &client,
                &dataset_name,
                &layer_collection_id,
                &product.0,
                GEO_ENGINE_URL,
                SHARE_LAYERS,
            );
        }
    }
}

fn add_dataset_to_collection(
    session_id: &str,
    client: &reqwest::blocking::Client,
    dataset_name: &str,
    layer_collection_id: &str,
    layer_name: &str,
    geo_engine_url: &str,
    share_layer: bool,
) {
    let add_layer = AddLayer {
        name: layer_name.to_string(),
        description: "".to_string(),
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
        .expect("Failed to add layer to collection")
        .json()
        .expect("Failed to parse response");

    if share_layer {
        let permissions = vec![
            PermissionRequest {
                resource: geoengine_services::api::handlers::permissions::Resource::Layer(
                    LayerResource {
                        id:response.id.clone(), 
                        r#type: geoengine_services::api::handlers::permissions::LayerResourceTypeTag::LayerResourceTypeTag
                 },
                ),
                role_id: Role::registered_user_role_id(),
                permission: Permission::Read,
            },
            PermissionRequest {
                resource: geoengine_services::api::handlers::permissions::Resource::Layer(
                    LayerResource {
                        id:response.id.clone(),
                         r#type: geoengine_services::api::handlers::permissions::LayerResourceTypeTag::LayerResourceTypeTag 
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
                .expect("Failed to add permission");

            println!(
                "Layer '{}' shared with role {}: {}",
                layer_name, permission.role_id, response.text().unwrap_or_default()
            );
        }
    }
}

fn create_layer_collection_if_not_exists(
    session_id: &str,
    client: &reqwest::blocking::Client,
    geo_engine_url: &str,
    parent_layer_collection_id: Uuid,
    layer_collection_name: &str,
    share_layer: bool,
) -> String {
    let add_collection = AddLayerCollection {
        name: layer_collection_name.to_string(),
        description: format!("Layer collection for {}", layer_collection_name),
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
        .expect("Failed to add dataset")
        .json()
        .expect("Failed to parse response");

    // TODO: handle case where collection already exists, but there is no API to get collection by name

    println!(
        "Layer collection '{}' created with id {}",
        layer_collection_name, response.id.0
    );

    if share_layer {
        let permissions = vec![PermissionRequest {
            resource: geoengine_services::api::handlers::permissions::Resource::LayerCollection(
                LayerCollectionResource {
                    id: response.id.clone(),
                    r#type: geoengine_services::api::handlers::permissions::LayerCollectionResourceTypeTag::LayerCollectionResourceTypeTag,
                },
            ),
            role_id: Role::registered_user_role_id(),
            permission: Permission::Read,
        }, PermissionRequest {
            resource: geoengine_services::api::handlers::permissions::Resource::LayerCollection(
                LayerCollectionResource {
                    id: response.id.clone(),
                    r#type: geoengine_services::api::handlers::permissions::LayerCollectionResourceTypeTag::LayerCollectionResourceTypeTag,
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
                .expect("Failed to add permission");

            println!(
                "Layer collection '{}' shared with role {}: {}",
                layer_collection_name, permission.role_id, response.text().unwrap_or_default()
            );
        }
    }

    response.id.0
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

fn naive_date_to_time_instance(date: NaiveDate) -> TimeInstance {
    let time: chrono::DateTime<chrono::Utc> = chrono::Utc.from_utc_datetime(
        &date
            .and_hms_opt(0, 0, 0)
            .expect("Failed to create datetime"),
    );
    let time: DateTime = time.into();
    time.into()
}

fn collect_files(dir: &Path, extension: &str, recursive: bool, files: &mut Vec<PathBuf>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() && recursive {
                collect_files(&path, extension, recursive, files);
            } else if path.is_file()
                && let Some(ext) = path.extension().and_then(|e| e.to_str())
                && ext.eq_ignore_ascii_case(extension)
            {
                files.push(path);
            }
        }
    }
}

fn add_dataset_and_tiles_to_geoengine(
    session_id: &str,
    client: &reqwest::blocking::Client,
    product_name: &str,
    files: &[ProductFile],
    tile_duration_seconds: u32,
    geo_engine_url: &str,
    local_data_dir: &str,
    remote_data_dir: Option<&str>,
) -> Option<String> {
    let create_dataset = CreateDataset {
        data_path: DataPath::Volume(VolumeName("test_data".to_string())),
        definition: DatasetDefinition {
            properties: AddDataset {
                name: Some(
                    DatasetName::from_str(product_name).expect("Failed to create dataset name"),
                ),
                display_name: format!("{product_name} Dataset"),
                description: format!("Dataset for {product_name}"),
                source_operator: "MultiBandGdalSource".to_string(),
                symbology: None,
                provenance: None,
                tags: None,
            },
            meta_data: MetaDataDefinition::GdalMultiBand(GdalMultiBand {
                r#type: geoengine_services::api::model::operators::GdalMultiBandTypeTag::GdalMultiBandTypeTag,
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
        .expect("Failed to add dataset");

    let dataset_name = if let Ok(json) = response.json::<serde_json::Value>() {
        if let Some(id) = json.get("datasetName").and_then(|v| v.as_str()) {
            println!("Dataset added successfully for product: {product_name}, id: {id}");
            id.to_string()
        } else {
            println!("Failed to get dataset id from response: {json:?}");
            return None;
        }
    } else {
        println!("Failed to parse dataset creation response as JSON");
        return None;
    };

    let mut tiles = Vec::new();

    for file in files {
        for (band_idx, _band) in file.result_descriptor.bands.iter().enumerate() {
            let tile = DatasetTile {
                time: TimeInterval::new(file.time, file.time + i64::from(tile_duration_seconds))
                    .expect("Failed to create time interval")
                    .into(),
                spatial_partition: file.spatial_partition.into(),
                band: band_idx as u32,
                z_index: 0, // TODO: implement z-index calculation
                params: GdalDatasetParameters {
                    file_path: {
                        let new_path = if let Some(remote) = remote_data_dir {
                            match file.path.strip_prefix(local_data_dir) {
                                Ok(suffix) => Path::new(remote).join(suffix),
                                Err(_) => file.path.clone(),
                            }
                        } else {
                            file.path.clone()
                        };
                        new_path
                    },
                    rasterband_channel: band_idx + 1,
                    geo_transform: file.geo_transform.into(),
                    width: file.width,
                    height: file.height,
                    file_not_found_handling:
                        geoengine_services::api::model::operators::FileNotFoundHandling::Error,
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
        .expect("Failed to add dataset");

    if !response.status().is_success() {
        println!(
            "Failed to add tile to dataset: {}",
            response.text().unwrap_or_default()
        );
        return dataset_name.into();
    }

    println!("Dataset tiles added successfully for product: {product_name}");

    dataset_name.into()
}

fn login(
    geo_engine_url: &str,
    geo_engine_email: &str,
    geo_engine_password: &str,
) -> (reqwest::blocking::Client, String) {
    let client = reqwest::blocking::Client::new();
    let session_id = client
        .post(format!("{geo_engine_url}/login"))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "email": geo_engine_email,
            "password": geo_engine_password,
        }))
        .send()
        .expect("Failed to authenticate")
        .json::<serde_json::Value>()
        .expect("Failed to parse auth response")["id"]
        .as_str()
        .expect("No session id in response")
        .to_string();
    (client, session_id)
}

fn extract_products_from_files(
    time_regex: &Regex,
    time_format: &str,
    product_regex: &Regex,
    files: Vec<PathBuf>,
) -> HashMap<String, Vec<ProductFile>> {
    let product_files = files
        .into_iter()
        .filter_map(|f| extract_product_from_file(time_regex, time_format, product_regex, f))
        .collect::<Vec<_>>();

    let mut products = HashMap::new();

    for file in product_files {
        let product_name = file.product_name.clone();
        let entry = products.entry(product_name).or_insert_with(Vec::new);
        entry.push(file);
    }

    products
}

fn extract_product_from_file(
    time_regex: &Regex,
    time_format: &str,
    product_regex: &Regex,
    file_path: PathBuf,
) -> Option<ProductFile> {
    let filename = file_path.file_name().and_then(|f| f.to_str()).unwrap_or("");
    let time_match = time_regex.captures(filename);
    let product_match = product_regex.captures(filename);

    let (Some(time), Some(product)) = (time_match, product_match) else {
        println!("Skipped file {}", file_path.as_os_str().to_string_lossy());
        return None;
    };

    let time_str = time.get(1).map_or("", |m| m.as_str());
    let product_str = product.get(1).map_or("", |m| m.as_str());

    let time = naive_date_to_time_instance(
        NaiveDate::parse_from_str(time_str, time_format).expect("Failed to parse time"),
    );

    let gdal_dataset = GdalDataset::open(&file_path).expect("Failed to open dataset");

    let geo_transform: GdalGeoTransform = gdal_dataset
        .geo_transform()
        .expect("Failed to get geo-transform");
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
    .expect("Failed to create spatial partition");

    // TODO: collect units for all bands
    let mut result_descriptor =
        raster_descriptor_from_dataset(&gdal_dataset, 1).expect("Could not get raster descriptor");

    let measurements = (1..=gdal_dataset.raster_count())
        .map(|band| measurement_from_rasterband(&gdal_dataset, band))
        .collect::<Vec<_>>();

    result_descriptor.bands = measurements
        .into_iter()
        .enumerate()
        .map(|(idx, measurement)| {
            RasterBandDescriptor::new(
                format!("band {idx}"),
                measurement.unwrap_or(Measurement::Unitless),
            )
        })
        .collect::<Vec<_>>()
        .try_into()
        .expect("Failed to convert raster bands");

    Some(ProductFile {
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
