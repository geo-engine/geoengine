#![allow(clippy::print_stdout)]

use ordered_float::OrderedFloat;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    time::Instant,
};

use anyhow::Context;
use chrono::Timelike;
use clap::Parser;
use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{DateTime, TimeInstance, TimeInterval},
    raster::{GdalGeoTransform, GeoTransform},
    spatial_reference::{SpatialReference, SpatialReferenceAuthority, SpatialReferenceOption},
};
use serde::Deserialize;
use stac::Asset;

use crate::{
    api::{
        handlers::{
            datasets::AddDatasetTile,
            permissions::{
                DatasetResource, LayerCollectionResource, LayerResource, PermissionRequest,
            },
        },
        model::{
            datatypes::{
                GridBoundingBox2D, GridIdx2D, LayerId, Measurement, RasterDataType,
                SpatialGridDefinition, TimeGranularity, TimeStep, UnitlessMeasurement,
            },
            operators::{
                GdalDatasetParameters, GdalMultiBand, RasterBandDescriptor, RasterBandDescriptors,
                RasterResultDescriptor, RegularTimeDimension, SpatialGridDescriptor,
                SpatialGridDescriptorState, TimeDescriptor, TimeDimension,
            },
            responses::IdResponse,
            services::{
                AddDataset, CreateDataset, DataPath, DatasetDefinition, MetaDataDefinition,
            },
        },
    },
    datasets::{DatasetName, upload::VolumeName},
    layers::{
        layer::{AddLayer, AddLayerCollection},
        listing::LayerCollectionId,
        storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
    },
    permissions::{Permission, Role},
    workflows::workflow::Workflow,
};
use geoengine_datatypes::dataset::NamedData;
use geoengine_operators::{
    engine::{RasterOperator, TypedOperator},
    source::{MultiBandGdalSource, MultiBandGdalSourceParameters},
};

const EXTERNAL_VOLUME_NAME: &str = "external";

/// STAC catalog importer for Geo Engine
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
    #[arg(long, default_value = "2020-12-25T00:00:00Z")]
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

    #[arg(long, default_value = "Sentinel2")]
    dataset_name_prefix: String,

    // #[arg(long, default_value = None)]
    // z_index_property_name: Option<String>,
    #[arg(long, default_value_t = false)]
    verbose: bool,

    /// Number of pages to prefetch while processing the current page
    #[arg(long, default_value_t = 2)]
    prefetch_pages: usize,

    // handle missing bands:
    // no eo:bands and only single-band -> use item key as band name
    // no raster:bands and role visual and three bands -> use u8 bands
    // if this parameter is false, assets with missing eo:bands or raster:bands will be skipped
    #[arg(long, default_value_t = false)]
    missing_bands_handling: bool,
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

/// Example call for Sentinel 2 from Element 84:
/// `cargo run --bin geoengine-cli stac-import --limit 267 --missing-bands-handling --verbose`
pub async fn stac_import(params: StacImport) -> Result<(), anyhow::Error> {
    let mut importer = StacImporter::new(params).await?;
    importer.run().await
}

struct StacImporter {
    params: StacImport,
    client: reqwest::Client,
    session_id: String,
    bands: HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>>,
    created_datasets: HashSet<DatasetKey>,
    time_range: TimeInterval,
}

impl StacImporter {
    async fn new(params: StacImport) -> Result<Self, anyhow::Error> {
        let (client, session_id) = login(
            &params.geo_engine_url,
            &params.geo_engine_email,
            &params.geo_engine_password,
        )
        .await?;

        let bands = scan_collection(&params, &client)
            .await
            .context("Failed to scan collection")?;

        Ok(Self {
            time_range: TimeInterval::new(
                DateTime::from_str(&params.time_start)?,
                DateTime::from_str(&params.time_end)?,
            )?,
            params,
            client,
            session_id,
            bands,
            created_datasets: HashSet::new(),
        })
    }

    async fn run(&mut self) -> Result<(), anyhow::Error> {
        if self.params.verbose {
            println!("Scanned collection, found bands for data type and resolution:");
            for (partial_dataset_key, bands) in &self.bands {
                println!(
                    "  {:?}, {}: {}",
                    partial_dataset_key.data_type,
                    partial_dataset_key.resolution,
                    bands
                        .iter()
                        .map(|b| b.name.as_str())
                        .collect::<Vec<&str>>()
                        .join(", ")
                );
            }
        }

        let query_params = create_query_params(&self.params);

        let initial_query_state = QueryState::FirstPage {
            query_url: format!(
                "{}/collections/{}/items",
                self.params.stac_url, self.params.stac_collection
            ),
            query_params,
        };

        let pages = Self::create_page_stream(
            initial_query_state,
            self.client.clone(),
            self.params.verbose,
            self.params.prefetch_pages,
        );

        self.process_pages(pages).await?;

        if self.params.verbose {
            println!("Dataset tiles added successfully");
        }

        self.create_collections_and_layers().await?;

        Ok(())
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

                println!("Fetching page: {state:?}");

                let start = Instant::now();
                let result = query_item_collection(&client, &state).await;
                let elapsed = start.elapsed();

                println!("Page fetched in {:.2}s", elapsed.as_secs_f64());

                match result {
                    Ok((item_collection, new_state)) => {
                        if item_collection.items.is_empty() {
                            None
                        } else {
                            Some((Ok(item_collection), (client, new_state)))
                        }
                    }
                    Err(e) => {
                        // TODO: abort or retry
                        println!("Error fetching page: {e:#}");
                        Some((Err(e), (client, QueryState::Finished)))
                    }
                }
            },
        );

        page_stream
            .map(|result| async move { result })
            .buffered(prefetch_buffer)
    }

    async fn process_pages(
        &mut self,
        buffered_stream: impl futures::Stream<Item = Result<stac::ItemCollection, anyhow::Error>>,
    ) -> Result<(), anyhow::Error> {
        // Process pages as they arrive from the buffered stream
        futures::pin_mut!(buffered_stream);
        while let Some(result) = buffered_stream.next().await {
            let item_collection = result?;

            let dataset_tiles = self.process_items(item_collection).await?;

            for (dataset_key, tiles) in &dataset_tiles {
                let dataset_name = dataset_key.dataset_name(&self.params.stac_collection);

                println!("Adding {} tiles to dataset {}", tiles.len(), dataset_name,);

                let response = self
                    .client
                    .post(format!(
                        "{}/dataset/{}/tiles",
                        self.params.geo_engine_url, dataset_name
                    ))
                    .header("Content-Type", "application/json")
                    .header("Authorization", format!("Bearer {}", self.session_id))
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

            self.print_progress(dataset_tiles);
        }

        Ok(())
    }

    async fn process_items(
        &mut self,
        item_collection: stac::ItemCollection,
    ) -> Result<HashMap<DatasetKey, Vec<AddDatasetTile>>, anyhow::Error> {
        let mut dataset_tiles = HashMap::new();

        for item in item_collection.items {
            let Some(datetime) = item.properties.datetime else {
                if self.params.verbose {
                    println!("Skipping item {}: missing datetime", item.id);
                }
                continue;
            };

            // TODO: make mapping of item datetime to tile time validity configurable
            let date_without_time = datetime
                .with_hour(0)
                .and_then(|d| d.with_minute(0))
                .and_then(|d| d.with_second(0))
                .and_then(|d| d.with_nanosecond(0))
                .context("Failed to set time to zero")?;
            let date_without_time: DateTime = date_without_time.into();
            let time: TimeInstance = date_without_time.into();

            let epsg = item
                .properties
                .additional_fields
                .get("proj:epsg")
                .and_then(serde_json::Value::as_u64)
                .context("Missing proj:epsg in item properties")? as u32;

            // TODO: provide other ways to compute z-index, e.g. from date updated
            // let z_index = if let Some(z_index_property_name) = &self.params.z_index_property_name {
            //     item.properties
            //         .additional_fields
            //         .get(z_index_property_name)
            //         .and_then(|v| {
            //             v.as_u64()
            //                 .map(|n| n as u32)
            //                 .or_else(|| v.as_str().and_then(|s| s.parse::<u32>().ok()))
            //         })
            //         .ok_or(anyhow::anyhow!(
            //             "Missing or invalid z index property for item {}",
            //             item.id
            //         ))?
            // } else {
            //     0
            // };

            // TODO: make z-index computation configurable
            let z_index = chrono::DateTime::parse_from_rfc3339(&item.properties.updated.unwrap())
                .unwrap()
                .timestamp_millis();

            for (asset_key, asset) in &item.assets {
                match self
                    .process_item_asset(asset_key, asset, epsg, time, z_index)
                    .await
                {
                    Ok(tiles) => {
                        for (dataset_key, tile) in tiles {
                            dataset_tiles
                                .entry(dataset_key)
                                .or_insert_with(Vec::new)
                                .push(tile);
                        }
                    }
                    Err(err) => {
                        if self.params.verbose {
                            eprintln!(
                                "Skipping asset {} of item {}: {:#}",
                                asset_key, item.id, err
                            );
                        }
                    }
                }
            }
        }

        Ok(dataset_tiles)
    }

    async fn process_item_asset(
        &mut self,
        asset_key: &str,
        asset: &Asset,
        epsg: u32,
        time: TimeInstance,
        z_index: i64,
    ) -> Result<Vec<(DatasetKey, AddDatasetTile)>, anyhow::Error> {
        if asset.r#type
            != Some("image/tiff; application=geotiff; profile=cloud-optimized".to_string())
        {
            anyhow::bail!("non-geotiff asset");
        }

        let geo_transform = geo_transform_from_fields(&asset.additional_fields)
            .ok_or(anyhow::anyhow!("missing proj:transform"))?;

        let data_type = if let Ok(data_type) = data_type_from_asset(asset) {
            data_type
        } else if self.params.missing_bands_handling
            && asset
                .roles
                .iter()
                .any(|roles| roles.contains(&"visual".to_string()))
        {
            // if no data type can be determined but asset has role visual, assume u8 data type with one band per visual asset
            RasterDataType::U8
        } else {
            anyhow::bail!("Failed to determine data type from asset");
        };

        let dataset_key = DatasetKey {
            epsg,
            data_type,
            resolution: geo_transform.x_pixel_size().into(),
        };

        if !self.created_datasets.contains(&dataset_key) {
            // create dataset on-the-fly
            // TODO: if dataset already exists on server, skip creation, but check compatibility?
            self.create_dataset(&dataset_key, geo_transform)
                .await
                .context(format!("failed to create dataset {:?}", dataset_key))?;

            self.created_datasets.insert(dataset_key.clone());

            debug_assert!(
                self.created_datasets.contains(&dataset_key),
                "Dataset should have been marked as created"
            );
        }

        let eo_bands: Result<Vec<EoBand>, _> = serde_json::from_value(
            asset
                .additional_fields
                .get("eo:bands")
                .ok_or(anyhow::anyhow!("Missing eo:bands in asset"))?
                .clone(),
        );

        let eo_bands = if let Ok(eo_bands) = eo_bands {
            eo_bands
        } else if self.params.missing_bands_handling {
            handle_missing_eo_bands_for_asset(asset_key, asset)?
        } else {
            anyhow::bail!("Failed to parse eo:bands");
        };

        let dataset_bands = self
            .bands
            .get(&PartialDatasetKey {
                data_type: dataset_key.data_type,
                resolution: dataset_key.resolution,
            })
            .ok_or(anyhow::anyhow!("unknown dataset key: {:?}", dataset_key))?;

        // if multiple bands for asset, prefix band names with asset key
        let prefix = if eo_bands.len() > 1 {
            format!("{}_", asset_key)
        } else {
            "".to_string()
        };

        let processor = AssetBandProcessor {
            asset,
            time,
            geo_transform,
            dataset_bands,
            prefix: &prefix,
            z_index,
        };

        let mut tiles = Vec::new();
        for (band_idx, eo_band) in eo_bands.iter().enumerate() {
            let tile = processor
                .process_band(band_idx, eo_band)
                .context(format!("Failed to process band {}", eo_band.name))?;
            tiles.push((dataset_key.clone(), tile));
        }

        Ok(tiles)
    }

    async fn create_dataset(
        &self,
        dataset_key: &DatasetKey,
        geo_transform: GeoTransform,
    ) -> Result<(), anyhow::Error> {
        let dataset_name_str = &dataset_key.dataset_name(&self.params.stac_collection);

        let bands = self
            .bands
            .get(&PartialDatasetKey {
                data_type: dataset_key.data_type,
                resolution: dataset_key.resolution,
            })
            .context(format!(
                "Failed to get bands for dataset: {:?}",
                dataset_key
            ))?;

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
                        "{dataset_name_str} imported from STAC collection {} from {}",
                        self.params.stac_collection, self.params.stac_url
                    ),
                    source_operator: "MultiBandGdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: None,
                },
                meta_data: MetaDataDefinition::GdalMultiBand(GdalMultiBand {
                    r#type:
                        crate::api::model::operators::GdalMultiBandTypeTag::GdalMultiBandTypeTag,
                    result_descriptor: RasterResultDescriptor {
                        data_type: dataset_key.data_type,
                        spatial_reference: SpatialReferenceOption::SpatialReference(
                            SpatialReference::new(
                                SpatialReferenceAuthority::Epsg,
                                dataset_key.epsg,
                            ),
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

        let response = self
            .client
            .post(format!("{}/dataset", self.params.geo_engine_url))
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", self.session_id))
            .json(&create_dataset)
            .send()
            .await
            .context("Failed to send dataset creation request")?;

        let dataset_name = if let Ok(json) = response.json::<serde_json::Value>().await {
            if let Some(id) = json.get("datasetName").and_then(|v| v.as_str()) {
                // println!(
                //     "Creatied dataset: {} for {:?}",
                //     dataset_name_str, dataset_key
                // );
                id.to_string()
            } else {
                anyhow::bail!("Failed to get dataset id from response: {json:?}");
            }
        } else {
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
            let response = self
                .client
                .put(format!("{}/permissions", self.params.geo_engine_url))
                .header("Content-Type", "application/json")
                .header("Authorization", format!("Bearer {}", self.session_id))
                .json(&permission)
                .send()
                .await
                .context("Failed to add permission")?;

            if self.params.verbose {
                println!(
                    "Dataset '{}' shared with role {}: {}",
                    dataset_name,
                    permission.role_id,
                    response.text().await.unwrap_or_default()
                );
            }
        }

        Ok(())
    }

    fn print_progress(&self, dataset_tiles: HashMap<DatasetKey, Vec<AddDatasetTile>>) {
        let min_date = dataset_tiles
            .values()
            .flatten()
            .map(|tile| tile.time.start)
            .min();

        if let Some(min_date) = min_date {
            let start_millis = self.time_range.start().inner() as f64;
            let end_millis = self.time_range.end().inner() as f64;
            let current_millis = min_date.inner() as f64;

            // Items are received in descending order (from end to start)
            let progress = ((end_millis - current_millis) / (end_millis - start_millis) * 100.0)
                .max(0.0)
                .min(100.0);

            println!(
                "[{:.1}%] Processed items down to date: {} in range {}/{} ",
                progress,
                DateTime::try_from(geoengine_datatypes::primitives::TimeInstance::from(
                    min_date
                ))
                .unwrap(),
                DateTime::try_from(geoengine_datatypes::primitives::TimeInstance::from(
                    self.time_range.start()
                ))
                .unwrap(),
                DateTime::try_from(geoengine_datatypes::primitives::TimeInstance::from(
                    self.time_range.end()
                ))
                .unwrap(),
            );
        }
    }

    async fn create_collections_and_layers(&self) -> anyhow::Result<()> {
        // Create root collection for STAC collection
        let root_collection_id = self
            .create_layer_collection(
                &INTERNAL_LAYER_DB_ROOT_COLLECTION_ID.to_string(),
                &self.params.stac_collection,
                &format!(
                    "{} datasets imported from STAC",
                    self.params.stac_collection
                ),
            )
            .await?;

        // Step 1: Create all layers first and store their IDs
        let mut layer_ids: HashMap<DatasetKey, LayerId> = HashMap::new();

        // Create one layer per dataset in a temporary collection
        let temp_collection_id = self
            .create_layer_collection(
                &root_collection_id,
                "_layers",
                "All dataset layers (internal)",
            )
            .await?;

        for dataset_key in &self.created_datasets {
            let layer_id = self.create_layer(&temp_collection_id, dataset_key).await?;
            layer_ids.insert(dataset_key.clone(), layer_id);
        }

        // Step 2: Create the three top-level collections
        let by_datatype_id = self
            .create_layer_collection(
                &root_collection_id,
                "By Data Type",
                "Organized by data type",
            )
            .await?;
        let by_resolution_id = self
            .create_layer_collection(
                &root_collection_id,
                "By Resolution",
                "Organized by resolution",
            )
            .await?;
        let by_epsg_id = self
            .create_layer_collection(&root_collection_id, "By EPSG", "Organized by EPSG code")
            .await?;

        // Step 3: Create hierarchies with both second-level options
        use Attribute::*;
        // Under "By Data Type": DataType -> {Resolution, Epsg} -> layers
        self.create_hierarchy_with_branches(
            &by_datatype_id,
            &layer_ids,
            DataType,
            &[Resolution, Epsg],
        )
        .await?;
        // Under "By Resolution": Resolution -> {DataType, Epsg} -> layers
        self.create_hierarchy_with_branches(
            &by_resolution_id,
            &layer_ids,
            Resolution,
            &[DataType, Epsg],
        )
        .await?;
        // Under "By EPSG": Epsg -> {DataType, Resolution} -> layers
        self.create_hierarchy_with_branches(&by_epsg_id, &layer_ids, Epsg, &[DataType, Resolution])
            .await?;

        Ok(())
    }

    async fn create_layer_collection(
        &self,
        parent_id: &str,
        name: &str,
        description: &str,
    ) -> anyhow::Result<String> {
        let add_collection = AddLayerCollection {
            name: name.to_string(),
            description: description.to_string(),
            properties: vec![],
        };

        let response: IdResponse<LayerCollectionId> = self
            .client
            .post(format!(
                "{}/layerDb/collections/{}/collections",
                self.params.geo_engine_url, parent_id
            ))
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", self.session_id))
            .json(&add_collection)
            .send()
            .await
            .context("Failed to add layer collection")?
            .json()
            .await
            .context("Failed to parse layer collection response")?;

        // Share with all users
        self.share_layer_collection(&response.id).await?;

        if self.params.verbose {
            println!(
                "Created layer collection '{}' with id {}",
                name, response.id.0
            );
        }

        Ok(response.id.0)
    }

    async fn create_layer(
        &self,
        collection_id: &str,
        dataset_key: &DatasetKey,
    ) -> anyhow::Result<LayerId> {
        let dataset_name = dataset_key.dataset_name(&self.params.stac_collection);
        let layer_name = format!(
            "EPSG:{} {:?} res:{}",
            dataset_key.epsg, dataset_key.data_type, dataset_key.resolution
        );

        let add_layer = AddLayer {
            name: layer_name.clone(),
            description: format!("Dataset: {}", dataset_name),
            workflow: Workflow {
                operator: TypedOperator::Raster(
                    MultiBandGdalSource {
                        params: MultiBandGdalSourceParameters {
                            data: NamedData {
                                namespace: None,
                                provider: None,
                                name: dataset_name.clone(),
                            },
                            overview_level: None,
                        },
                    }
                    .boxed(),
                ),
            },
            symbology: None,
            properties: vec![],
            metadata: Default::default(),
        };

        let response: IdResponse<LayerId> = self
            .client
            .post(format!(
                "{}/layerDb/collections/{}/layers",
                self.params.geo_engine_url, collection_id
            ))
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", self.session_id))
            .json(&add_layer)
            .send()
            .await
            .context("Failed to add layer to collection")?
            .json()
            .await
            .context("Failed to parse layer response")?;

        // Share with all users
        self.share_layer(&response.id).await?;

        if self.params.verbose {
            println!(
                "Created layer '{}' in collection {}",
                layer_name, collection_id
            );
        }

        Ok(response.id)
    }

    async fn add_existing_layer_to_collection(
        &self,
        collection_id: &str,
        layer_id: &LayerId,
    ) -> anyhow::Result<()> {
        self.client
            .post(format!(
                "{}/layerDb/collections/{}/layers/{}",
                self.params.geo_engine_url, collection_id, layer_id.0
            ))
            .header("Authorization", format!("Bearer {}", self.session_id))
            .send()
            .await
            .context("Failed to add existing layer to collection")?;

        if self.params.verbose {
            println!("Added layer {} to collection {}", layer_id.0, collection_id);
        }

        Ok(())
    }

    async fn create_hierarchy_with_branches(
        &self,
        root_id: &str,
        layer_ids: &HashMap<DatasetKey, LayerId>,
        first_attr: Attribute,
        second_attrs: &[Attribute; 2],
    ) -> anyhow::Result<()> {
        // Group by first attribute
        let grouped_level1 =
            self.group_by_attribute(layer_ids.keys().cloned().collect(), first_attr);

        // Process each value of the first attribute
        for (value1, keys1) in grouped_level1 {
            // Create collection for this value of first attribute (e.g., "U16", "EPSG:32630")
            let name1 = first_attr.format_value(&value1);
            let desc1 = format!("{} datasets", name1);
            let collection1 = self
                .create_layer_collection(root_id, &name1, &desc1)
                .await?;

            // Create branches for each possible second attribute
            for &second_attr in second_attrs {
                // Create label collection for this second attribute (e.g., "By Resolution")
                let label_name = format!("By {}", second_attr.capitalize_name());
                let label_desc =
                    format!("Organized by {} for {}", second_attr.display_name(), name1);
                let label_collection = self
                    .create_layer_collection(&collection1, &label_name, &label_desc)
                    .await?;

                // Group by second attribute
                let grouped_level2 = self.group_by_attribute(keys1.clone(), second_attr);

                // Process each value of the second attribute
                for (value2, keys2) in grouped_level2 {
                    let name2 = second_attr.format_value(&value2);
                    let desc2 = format!("Layers: {} + {}", name1, name2);
                    let collection2 = self
                        .create_layer_collection(&label_collection, &name2, &desc2)
                        .await?;

                    // Add layers at the leaf level (all layers with these two attribute values)
                    for key in &keys2 {
                        if let Some(layer_id) = layer_ids.get(key) {
                            self.add_existing_layer_to_collection(&collection2, layer_id)
                                .await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn group_by_attribute(
        &self,
        keys: Vec<DatasetKey>,
        attribute: Attribute,
    ) -> HashMap<AttributeValue, Vec<DatasetKey>> {
        let mut grouped: HashMap<AttributeValue, Vec<DatasetKey>> = HashMap::new();
        for key in keys {
            let value = match attribute {
                Attribute::DataType => AttributeValue::DataType(key.data_type),
                Attribute::Resolution => AttributeValue::Resolution(key.resolution),
                Attribute::Epsg => AttributeValue::Epsg(key.epsg),
            };
            grouped
                .entry(value)
                .or_insert_with(Vec::new)
                .push(key.clone());
        }
        grouped
    }

    async fn share_layer_collection(
        &self,
        collection_id: &LayerCollectionId,
    ) -> anyhow::Result<()> {
        let permissions = vec![
            PermissionRequest {
                resource: crate::api::handlers::permissions::Resource::LayerCollection(
                    LayerCollectionResource {
                        id: collection_id.clone(),
                        r#type: crate::api::handlers::permissions::LayerCollectionResourceTypeTag::LayerCollectionResourceTypeTag,
                    },
                ),
                role_id: Role::registered_user_role_id(),
                permission: Permission::Read,
            },
            PermissionRequest {
                resource: crate::api::handlers::permissions::Resource::LayerCollection(
                    LayerCollectionResource {
                        id: collection_id.clone(),
                        r#type: crate::api::handlers::permissions::LayerCollectionResourceTypeTag::LayerCollectionResourceTypeTag,
                    },
                ),
                role_id: Role::anonymous_role_id(),
                permission: Permission::Read,
            },
        ];

        for permission in &permissions {
            self.client
                .put(format!("{}/permissions", self.params.geo_engine_url))
                .header("Content-Type", "application/json")
                .header("Authorization", format!("Bearer {}", self.session_id))
                .json(&permission)
                .send()
                .await
                .context("Failed to add permission")?;
        }

        Ok(())
    }

    async fn share_layer(&self, layer_id: &LayerId) -> anyhow::Result<()> {
        let permissions = vec![
            PermissionRequest {
                resource: crate::api::handlers::permissions::Resource::Layer(LayerResource {
                    id: layer_id.clone(),
                    r#type: crate::api::handlers::permissions::LayerResourceTypeTag::LayerResourceTypeTag,
                }),
                role_id: Role::registered_user_role_id(),
                permission: Permission::Read,
            },
            PermissionRequest {
                resource: crate::api::handlers::permissions::Resource::Layer(LayerResource {
                    id: layer_id.clone(),
                    r#type: crate::api::handlers::permissions::LayerResourceTypeTag::LayerResourceTypeTag,
                }),
                role_id: Role::anonymous_role_id(),
                permission: Permission::Read,
            },
        ];

        for permission in &permissions {
            self.client
                .put(format!("{}/permissions", self.params.geo_engine_url))
                .header("Content-Type", "application/json")
                .header("Authorization", format!("Bearer {}", self.session_id))
                .json(&permission)
                .send()
                .await
                .context("Failed to add permission")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Attribute {
    DataType,
    Resolution,
    Epsg,
}

impl Attribute {
    fn display_name(&self) -> &str {
        match self {
            Attribute::DataType => "data type",
            Attribute::Resolution => "resolution",
            Attribute::Epsg => "EPSG",
        }
    }

    fn capitalize_name(&self) -> &str {
        match self {
            Attribute::DataType => "Data Type",
            Attribute::Resolution => "Resolution",
            Attribute::Epsg => "EPSG",
        }
    }

    fn format_value(&self, value: &AttributeValue) -> String {
        match (self, value) {
            (Attribute::DataType, AttributeValue::DataType(dt)) => format!("{:?}", dt),
            (Attribute::Resolution, AttributeValue::Resolution(res)) => format!("res:{}", res),
            (Attribute::Epsg, AttributeValue::Epsg(epsg)) => format!("EPSG:{}", epsg),
            _ => panic!("Mismatched attribute and value"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum AttributeValue {
    DataType(RasterDataType),
    Resolution(OrderedFloat<f64>),
    Epsg(u32),
}

fn handle_missing_eo_bands_for_asset(
    asset_key: &str,
    asset: &Asset,
) -> anyhow::Result<Vec<EoBand>> {
    let raster_bands: Vec<stac_extensions::raster::Band> = asset
        .additional_fields
        .get("raster:bands")
        .ok_or(anyhow::anyhow!("Missing raster:bands"))
        .and_then(|bands| {
            serde_json::from_value(bands.clone())
                .map_err(|e| anyhow::anyhow!("invalid raster:bands: {}", e))
        })?;

    handle_missing_eo_bands(asset_key, &raster_bands)
}

struct AssetBandProcessor<'a> {
    asset: &'a Asset,
    time: TimeInstance,
    geo_transform: GeoTransform,
    dataset_bands: &'a [RasterBandDescriptor],
    prefix: &'a str,
    z_index: i64,
}

impl<'a> AssetBandProcessor<'a> {
    fn process_band(&self, band_idx: usize, eo_band: &EoBand) -> anyhow::Result<AddDatasetTile> {
        let band_name = format!("{}{}", self.prefix, eo_band.name);

        let band_index = self
            .dataset_bands
            .iter()
            .position(|b| b.name == band_name.as_str())
            .ok_or(anyhow::anyhow!("unknown band: {}", band_name))?;

        let tile_file = self.asset.href.clone();

        let proj_shape = self
            .asset
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

        let spatial_partition = self.geo_transform.grid_to_spatial_bounds(&grid_bounds);

        // println!(
        //     "Importing tile: date: {}, band: {}, href: {}",
        //     self.date, self.asset_key, self.asset.href
        // );

        let tile = AddDatasetTile {
            time: TimeInterval::new(self.time, self.time + i64::from(24 * 60 * 60 * 1000)) // TODO
                .unwrap()
                .into(),
            spatial_partition: spatial_partition.into(),
            band: band_index as u32,
            z_index: self.z_index,
            params: GdalDatasetParameters {
                file_path: format!("/vsicurl/{}", tile_file).into(),
                rasterband_channel: band_idx + 1, // gdal channels are 1-based
                geo_transform: self.geo_transform.into(),
                width,
                height,
                file_not_found_handling: crate::api::model::operators::FileNotFoundHandling::Error,
                no_data_value: None,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: false,
            },
        };

        Ok(tile)
    }
}

async fn query_item_collection(
    client: &reqwest::Client,
    query_state: &QueryState,
) -> Result<(stac::ItemCollection, QueryState), anyhow::Error> {
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

/// STAC collection is split up by epsg, data_type, resolution/grid to produce uniform Geo Engine datasets
/// Bands are grouped by data type
/// Different epsg and resolutions produce different datasets with the same bands
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct DatasetKey {
    epsg: u32,
    data_type: RasterDataType,
    resolution: OrderedFloat<f64>,
}

// partial dataset key used for grouping bands by data type and resolution, because epsgs are only retrieved when querying the actual items and not on collection level
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct PartialDatasetKey {
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
    // description: String,
    // ...
}

async fn scan_collection(
    params: &StacImport,
    client: &reqwest::Client,
) -> anyhow::Result<HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>>> {
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

    let mut dataset_bands: HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>> = HashMap::new();

    for (asset_key, asset) in &collection.item_assets {
        if let Err(err) = scan_item_asset(
            asset_key,
            asset,
            &mut dataset_bands,
            params.missing_bands_handling,
        )
        .context(format!("Failed to scan item asset {}", asset_key))
        {
            if params.verbose {
                eprintln!("Skipping asset {}: {:#}", asset_key, err);
            }
        }
    }
    Ok(dataset_bands)
}

fn scan_item_asset(
    asset_key: &str,
    asset: &stac::ItemAsset,
    dataset_bands: &mut HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>>,
    missing_bands_handling: bool,
) -> anyhow::Result<()> {
    if asset.r#type != Some("image/tiff; application=geotiff; profile=cloud-optimized".to_string())
    {
        anyhow::bail!("Skipping non-geotiff asset: {}", asset_key);
    }

    let raster_bands: anyhow::Result<Vec<stac_extensions::raster::Band>> = asset
        .additional_fields
        .get("raster:bands")
        .ok_or(anyhow::anyhow!("Missing raster:bands"))
        .and_then(|bands| {
            serde_json::from_value(bands.clone())
                .map_err(|e| anyhow::anyhow!("invalid raster:bands: {}", e))
        });

    let eo_bands: anyhow::Result<Vec<EoBand>> = asset
        .additional_fields
        .get("eo:bands")
        .ok_or(anyhow::anyhow!("Missing eo:bands in asset"))
        .and_then(|eo_bands| {
            serde_json::from_value(eo_bands.clone())
                .map_err(|e| anyhow::anyhow!("invalid eo:bands: {}", e))
        });

    let (raster_bands, eo_bands) = if missing_bands_handling {
        handle_missing_bands(raster_bands, eo_bands, asset, asset_key)?
    } else {
        (raster_bands?, eo_bands?)
    };

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
        let raster_data_type = raster_data_type_from_stac_data_type(&data_type)?;

        let geo_transform = geo_transform_from_fields(&asset.additional_fields)
            .ok_or(anyhow::anyhow!("missing proj:transform"))?;
        let resoution = geo_transform.x_pixel_size().into();

        let band_name = format!("{}{}", prefix, eo_band.name);

        dataset_bands
                .entry(PartialDatasetKey {
                    data_type: raster_data_type,
                    resolution: resoution,
                })
                .or_insert_with(Vec::new)
                .push(RasterBandDescriptor {
                    name: band_name,
                    // TODO: unit from raster_band.unit
                    measurement: Measurement::Unitless(UnitlessMeasurement { r#type: crate::api::model::datatypes::UnitlessMeasurementTypeTag::UnitlessMeasurementTypeTag }),
                });
    }

    Ok(())
}

fn handle_missing_bands(
    raster_bands_result: anyhow::Result<Vec<stac_extensions::raster::Band>>,
    eo_bands_result: anyhow::Result<Vec<EoBand>>,
    asset: &stac::ItemAsset,
    asset_key: &str,
) -> anyhow::Result<(Vec<stac_extensions::raster::Band>, Vec<EoBand>)> {
    match (raster_bands_result, eo_bands_result) {
        (Ok(raster_bands), Ok(eo_bands)) => Ok((raster_bands, eo_bands)),
        (Err(_), Ok(eo_bands)) => {
            let raster_bands =
                handle_missing_raster_bands(asset, &eo_bands).with_context(|| {
                    format!(
                        "Missing raster:bands and cannot create defaults for asset {}",
                        asset_key
                    )
                })?;
            Ok((raster_bands, eo_bands))
        }
        (Ok(raster_bands), Err(_)) => {
            let eo_bands =
                handle_missing_eo_bands(asset_key, &raster_bands).with_context(|| {
                    format!(
                        "Missing eo:bands and cannot create defaults for asset {}",
                        asset_key
                    )
                })?;
            Ok((raster_bands, eo_bands))
        }
        (Err(_), Err(_)) => anyhow::bail!(
            "Missing both raster:bands and eo:bands for asset {}",
            asset_key
        ),
    }
}

/// Handle missing raster:bands by creating defaults for visual assets
/// Returns Ok(bands) if defaults can be created, Err otherwise
fn handle_missing_raster_bands(
    asset: &stac::ItemAsset,
    eo_bands: &[EoBand],
) -> anyhow::Result<Vec<stac_extensions::raster::Band>> {
    // Check if asset has visual role by looking at additional_fields
    let has_visual_role = asset.roles.iter().any(|r| r == "visual");

    if !has_visual_role {
        anyhow::bail!("Asset does not have visual role");
    }

    if eo_bands.len() != 3 {
        anyhow::bail!(
            "Asset has visual role but does not have exactly 3 eo:bands (found {})",
            eo_bands.len()
        );
    }

    // Create 3 default U8 bands for visual assets (RGB)
    Ok(vec![
        stac_extensions::raster::Band {
            data_type: Some(stac_extensions::raster::DataType::UInt8),
            ..Default::default()
        },
        stac_extensions::raster::Band {
            data_type: Some(stac_extensions::raster::DataType::UInt8),
            ..Default::default()
        },
        stac_extensions::raster::Band {
            data_type: Some(stac_extensions::raster::DataType::UInt8),
            ..Default::default()
        },
    ])
}

/// Handle missing eo:bands by creating defaults based on the number of raster bands
/// Returns Ok(bands) if defaults can be created, Err otherwise
fn handle_missing_eo_bands(
    asset_key: &str,
    raster_bands: &[stac_extensions::raster::Band],
) -> anyhow::Result<Vec<EoBand>> {
    match raster_bands.len() {
        1 => {
            // Use asset key as band name for single-band assets without eo:bands
            Ok(vec![EoBand {
                name: asset_key.to_string(),
            }])
        }
        _ => anyhow::bail!(
            "Cannot create default eo:bands for {} raster bands",
            raster_bands.len()
        ),
    }
}

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
}
