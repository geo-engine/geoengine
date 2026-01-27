#![allow(clippy::print_stdout)]

use std::{
    str::FromStr,
};

use anyhow::Context;
use chrono::{Timelike};
use clap::Parser;
use geoengine_datatypes::{
    operations::reproject::{CoordinateProjection, CoordinateProjector, Reproject},
    primitives::{AxisAlignedRectangle, DateTime, TimeInstance, TimeInterval},
    raster::{GdalGeoTransform, GeoTransform},
    spatial_reference::{SpatialReference, SpatialReferenceAuthority},
};
use stac::Asset;

use crate::{
    api::{
        handlers::{datasets::AddDatasetTile, permissions::{DatasetResource, PermissionRequest}},
        model::{
            datatypes::{
                GridBoundingBox2D, GridIdx2D, Measurement, RasterDataType, SpatialGridDefinition,
                TimeGranularity, TimeStep, UnitlessMeasurement,
            },
            operators::{
                GdalDatasetParameters, GdalMultiBand, RasterBandDescriptor, RasterBandDescriptors, RasterResultDescriptor, RegularTimeDimension, SpatialGridDescriptor, SpatialGridDescriptorState, TimeDescriptor, TimeDimension
            },
            services::{
                AddDataset, CreateDataset, DataPath, DatasetDefinition, MetaDataDefinition,
            },
        },
    }, datasets::{DatasetName, upload::VolumeName}, permissions::{Permission, Role}, util::sentinel_2_utm_zones::UtmZone
};

const EXTERNAL_VOLUME_NAME: &str = "external";

/// Checks if the Geo Engine server is alive
#[derive(Debug, Parser)]
pub struct StacImport {
    // /// where the files are scanned
    // #[arg(long, default_value = "/home/michael/geodata/force/marburg")]
    // local_data_dir: String,

    // /// volume on the server that points to the path of the same files
    // #[arg(long, default_value = "geodata")]
    // volume_name: String,

    // /// directory on the server, relative to the volume, where the files are located
    // #[arg(long, default_value = "force/marburg")]
    // remote_data_dir: String,

    // /// file extension to look for
    // #[arg(long, default_value = "tif")]
    // file_extension: String,

    // /// whether to scan directories recursively
    // #[arg(long, value_enum, default_value_t = FileScanMode::Recursive)]
    // scan_mode: FileScanMode,

    // /// regex to extract time from file names
    // #[arg(long, default_value = r"(\d{8})_LEVEL2")]
    // time_regex: String,

    // /// strftime format for time
    // #[arg(long, default_value = "%Y%m%d")]
    // time_format: String,

    // /// duration of each tile in seconds
    // #[arg(long, default_value_t = 60 * 60 * 24)]
    // tile_duration_seconds: u32,

    // /// regex to extract product name from file names
    // #[arg(long, default_value = r"LEVEL2_(\w+_\w+)\.tif")]
    // product_name_regex: String,
    /// Geo Engine API URL
    #[arg(long, default_value = "http://localhost:3030/api")]
    geo_engine_url: String,

    /// Geo Engine API email
    #[arg(long, default_value = "admin@localhost")]
    geo_engine_email: String,

    /// Geo Engine API password
    #[arg(long, default_value = "adminadmin")]
    geo_engine_password: String,
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

    let limit = 1;

    let stac_api = "https://earth-search.aws.element84.com/v1";
    let geo_engine_url = params.geo_engine_url;

    let collection_id = "sentinel-2-l2a";
    let tile_duration_seconds: u32 = 60 * 60 * 24;

    let dataset_name = "Sentinal2-UTM32N_10m";

    let zone = UtmZone::from_str("UTM32N").unwrap();

    let t_start = "2020-01-01T00:00:00Z";
    let t_end = "2020-12-31T23:59:59Z";

    let create_dataset = true;

    let bands = vec![
        "aot", // 20m
        // "blue", // 10m
        // "coastal", // 60m
        // "green", // 10m
        // "nir", // 10m
        "nir08", // 20m
        // "nir09", // 60m
        // "red", // 10m
        "rededge1", // 20m
        "rededge2", // 20m
        "rededge3", // 20m
        "scl", // 20m
        "swir16", // 20m
        "swir22", // 20m
    ];

    let native_spatial_ref =
        SpatialReference::new(SpatialReferenceAuthority::Epsg, zone.epsg_code());
    let epsg_4326_ref = SpatialReference::epsg_4326();
    let projector = CoordinateProjector::from_known_srs(native_spatial_ref, epsg_4326_ref)?;
    let native_bounds = zone.native_extent();

    // request all features in zone in order to be able to determine the temporal validity of individual tile
    let bbox = native_bounds.reproject(&projector).unwrap();

    let params = vec![
        (
            "bbox".to_owned(),
            format!(
                "{},{},{},{}", // array-brackets are not used in standard but required here for unknkown reason
                bbox.lower_left().x,
                bbox.lower_left().y,
                bbox.upper_right().x,
                bbox.upper_right().y
            ),
        ), // TODO: order coordinates depending on projection
        ("datetime".to_owned(), format!("{}/{}", t_start, t_end)),
        ("limit".to_owned(), limit.to_string()),
    ];

    let res = client
        .get(format!("{stac_api}/collections/{collection_id}/items"))
        .query(&params)
        .send()
        .await?;

    // Parse as generic Value first to see structure
    let json: serde_json::Value = res.json().await?;

    // Then try to convert to FeatureCollection
    let feature_collection: stac::ItemCollection =
        serde_json::from_value(json).context("Failed to convert JSON to FeatureCollection")?;
    // println!("{:?}", feature_collection);

    if create_dataset {
        let probe_item = feature_collection.items.first().unwrap();
        println!("Probing first item for dataset creation: {:?}", probe_item);

        let probe_asset = probe_item.assets.first().unwrap().1;
        println!("Probing first asset for dataset creation: {:?}", probe_asset);

        let spatial_reference: geoengine_datatypes::spatial_reference::SpatialReferenceOption =
            SpatialReference::new(
                SpatialReferenceAuthority::Epsg,
                dbg!(probe_item
                    .properties.additional_fields
                    .get("proj:epsg"))
                    .and_then(|v| v.as_u64()).unwrap() as u32,
            )
            .into();

        let create_dataset = CreateDataset {
            data_path: DataPath::Volume(VolumeName(EXTERNAL_VOLUME_NAME.to_string())),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: Some(
                        DatasetName::from_str(dataset_name)
                            .context("Failed to create dataset name")?,
                    ),
                    display_name: dataset_name.to_string(),
                    description: format!("{dataset_name} imported from STAC {stac_api}"),
                    source_operator: "MultiBandGdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: None,
                },
                meta_data: MetaDataDefinition::GdalMultiBand(GdalMultiBand {
                    r#type:
                        crate::api::model::operators::GdalMultiBandTypeTag::GdalMultiBandTypeTag,
                    result_descriptor: RasterResultDescriptor {
                        data_type: data_type(probe_asset)?,
                        spatial_reference: spatial_reference.into(),
                        time: TimeDescriptor {
                            bounds: None, // TODO: from params
                            dimension: TimeDimension::Regular(RegularTimeDimension {
                                // TODO: irregula?
                                origin: TimeInstance::from_millis(0).unwrap().into(), // TODO
                                step: TimeStep {
                                    granularity: TimeGranularity::Days, // TODO
                                    step: 1,                            // TODO
                                },
                            }),
                        },
                        spatial_grid: SpatialGridDescriptor {
                            spatial_grid: SpatialGridDefinition {
                                geo_transform: geo_transform_from_asset(probe_asset)
                                    .ok_or(anyhow::anyhow!("Missing proj:transform in asset"))?
                                    .into(),
                                grid_bounds: GridBoundingBox2D {
                                    top_left_idx: GridIdx2D { x_idx: 0, y_idx: 0 },
                                    bottom_right_idx: GridIdx2D { x_idx: 1, y_idx: 1 }, // TODO
                                }, // TODO from  query bbox and asset proj:shape??
                            },
                            descriptor: SpatialGridDescriptorState::Source,
                        },
                        bands: RasterBandDescriptors::new(bands
                            .iter()
                            .map(|band_name| RasterBandDescriptor {
                                name: band_name.to_string(), // TODO: get band name from probe item
                                measurement: Measurement::Unitless(UnitlessMeasurement { r#type: crate::api::model::datatypes::UnitlessMeasurementTypeTag::UnitlessMeasurementTypeTag }),
                            })
                            .collect::<Vec<_>>()).unwrap()    
                    },
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
                println!("Dataset added successfully");
                id.to_string()
            } else {
                return Err(anyhow::anyhow!(
                    "Failed to get dataset id from response: {json:?}"
                )); 
            }
        } else {
            println!();
            return Err(anyhow::anyhow!(
                "Failed to parse dataset creation response as JSON"
            ));
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
                .put(format!("{geo_engine_url}/permissions"))
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
    }

    

    let mut tiles = vec![];

    for item in feature_collection.items {
        let mut date = item.properties.datetime.unwrap(); // TODO: handle Option
        date = date
            .with_hour(0)
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_nanosecond(0))
            .context("Failed to set time to zero")?;
        let date: DateTime = date.into();
        let time: TimeInstance = date.into();


        for (asset_key, asset) in &item.assets {
            let Some(band_index) = bands.iter().position(|&b| b == asset_key.as_str()) else {
                println!("Skipping unknown band: {}", asset_key);
                continue;
            };

            let tile_file = asset.href.clone();

            let geo_transform = match geo_transform_from_asset(asset) {
                Some(value) => value,
                None => {
                    println!("Skipping missing proj:transform: {}", asset_key);
                    continue;
                }
            };

            let Some(proj_shape) = asset.additional_fields.get("proj:shape") else {
                println!("Skipping missing proj:shape: {}", asset_key);
                continue;
            };

            let proj_shape = proj_shape.as_array().unwrap(); // TODO: handle Option
            let (height, width) = (
                proj_shape[0].as_u64().unwrap() as usize,
                proj_shape[1].as_u64().unwrap() as usize,
            );                    
    
            let grid_bounds =  geoengine_datatypes::raster::GridBoundingBox2D::new(
                GridIdx2D { x_idx: 0, y_idx: 0 },
                GridIdx2D { x_idx: (width - 1) as isize, y_idx: (height - 1) as isize},
            ).unwrap();

            let spatial_partition = 
                geo_transform.grid_to_spatial_bounds(&grid_bounds);

            println!(
                "Importing tile: date: {}, band: {}, href: {}",
                date, asset_key, asset.href
            );

            let tile = AddDatasetTile {
                time: TimeInterval::new(time, time + i64::from(tile_duration_seconds * 1000))
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
                    file_not_found_handling:
                        crate::api::model::operators::FileNotFoundHandling::Error,
                    no_data_value: None,
                    properties_mapping: None,
                    gdal_open_options: None,
                    gdal_config_options: None,
                    allow_alphaband_as_mask: false,
                },
            };
            dbg!(&tile);
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
        return Err(anyhow::anyhow!("Failed to add tile to dataset: {error_text}"));
    }

    println!("Dataset tiles added successfully");

    Ok(())
}

fn geo_transform_from_asset(asset: &Asset) -> Option<GeoTransform> {
    let Some(proj_transform) = asset.additional_fields.get("proj:transform") else {
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

fn data_type(asset: &Asset) -> anyhow::Result<RasterDataType> {
    let data_type_str = asset
        .additional_fields
        .get("raster:bands")
        .and_then(|v| v.as_array())
        .and_then(|bands| bands.first())
        .and_then(|band| band.get("data_type"))
        .and_then(|v| v.as_str())
        .ok_or(anyhow::anyhow!(
            "Missing data_type in raster:bands[0]"
        ))?;

    Ok(match data_type_str {
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
}
