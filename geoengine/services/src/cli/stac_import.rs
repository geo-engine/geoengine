#![allow(clippy::print_stdout)]

use ordered_float::OrderedFloat;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::Context;
use chrono::Timelike;
use clap::{Parser, ValueEnum};
use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{DateTime, TimeInstance, TimeInterval},
    raster::{GdalGeoTransform, GeoTransform},
    spatial_reference::{SpatialReference, SpatialReferenceAuthority, SpatialReferenceOption},
};
use serde::Deserialize;
use stac::Asset;
use tracing::{debug, error, info, warn};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;

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
                GdalConfigOption, GridBoundingBox2D, GridIdx2D, LayerId, Measurement,
                RasterDataType, SpatialGridDefinition, TimeGranularity, TimeStep,
                UnitlessMeasurement,
            },
            operators::{
                GdalDatasetParameters, GdalMultiBand, RasterBandDescriptor, RasterBandDescriptors,
                RasterResultDescriptor, RegularTimeDimension, SpatialGridDescriptor,
                SpatialGridDescriptorState, TimeDescriptor, TimeDimension,
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
use geoengine_datatypes::dataset::NamedData;
use geoengine_operators::{
    engine::{RasterOperator, TypedOperator},
    source::{MultiBandGdalSource, MultiBandGdalSourceParameters},
};

const MAX_RETRIES: u32 = 10;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;

// TODO: make the filter depend on stac version and stac extension versions?
// TODO: also filter fields of collections api?
const STAC_ITEMS_FIELDS_FILTER: &[&str] = &[
    "id",
    "type",
    "geometry",
    "bbox",
    "links",
    "stac_version",
    "stac_extensions",
    "properties.datetime",
    "properties.updated",
    "properties.proj:epsg",
    "properties.proj:code",
    "assets.*.title",
    "assets.*.type",
    "assets.*.data_type",
    "assets.*.gsd",
    "assets.*.href",
    "assets.*.bands",
    "assets.*.eo:bands",
    "assets.*.raster:bands",
    "assets.*.proj:epsg",
    "assets.*.proj:transform",
    "assets.*.proj:shape",
    "assets.*.proj:code",
];

/// STAC catalog importer for Geo Engine
#[derive(Debug, Parser)]
pub struct StacImport {
    /// STAC API URL
    #[arg(long)]
    stac_url: String,

    // collection to import from
    #[arg(long, default_value = "sentinel-2-l2a")]
    stac_collection: String,

    // import limit (page size)
    #[arg(long, default_value = None)]
    limit: Option<usize>,

    /// Time range start to import (optional)
    /// Example: 2020-12-25T00:00:00Z
    #[arg(long)]
    time_start: Option<String>,

    /// Time range end to import (optional)
    /// Example: 2020-12-31T23:59:59Z
    #[arg(long)]
    time_end: Option<String>,

    /// Bounding box to import: minx miny maxx maxy (optional)
    /// Example: 0.3184423382324359 -0.0884814721075339 20.784002046916676 72.0970954730969
    #[clap(short, long, value_parser, num_args = 1.., value_delimiter = ' ')]
    bbox: Option<Vec<f64>>,

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

    // TODO: time granularity (validity of items)
    /// Filter datasets by EPSG codes (only create and insert tiles for these EPSG codes)
    #[clap(long, value_parser, num_args = 0.., value_delimiter = ' ')]
    epsgs: Vec<u32>,

    /// feature/item property to use as z-index for tiles (if not provided, z-index will be set to 0 or computed from item properties in a hardcoded way)
    /// must be date time property
    #[arg(long, default_value = "updated")]
    z_index_property_name: Option<String>,

    #[arg(long)]
    s3_endpoint: Option<String>,

    #[arg(long)]
    s3_access_key: Option<String>,

    #[arg(long)]
    s3_secret_key: Option<String>,

    /// File types to import from STAC assets.
    /// Supported values: cog, jp2.
    /// Defaults to COG only.
    #[arg(long, value_enum, num_args = 1.., value_delimiter = ' ', default_values_t = [ImportFileType::Cog])]
    file_types: Vec<ImportFileType>,

    /// if true, filter the stac item fields in the query to only fetch the fields required for the import, which may reduce the response size and speed up the import.
    #[arg(long, default_value_t = false)]
    filter_item_fields: bool,

    /// Override the no data value in the GDAL dataset parameters.
    /// If set, this value will be used as the no data value for all tiles.
    /// Otherwise, the default (None) is used.
    #[arg(long)]
    no_data_value: Option<f64>,

    /// Number of retries for accessing remote datasets via GDAL (vsicurl/vsis3).
    /// If set, configures GDAL retry options for the dataset parameters.
    #[arg(long)]
    gdal_retries: Option<usize>,
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
/// ```bash
/// cargo run --bin geoengine-cli stac-import \
/// --verbose \
/// --limit 267 \
/// --bbox "8.766 50.802 8.767 50.803" \
/// --time-start 2020-08-01T00:00:00Z \
/// --time-end 2020-08-31T23:59:59Z \
/// --stac-url https://earth-search.aws.element84.com/v1
/// ```
///
/// Example call for Sentinel 2 from CODE-DE:
/// ```bash
/// cargo run --bin geoengine-cli stac-import \
/// --verbose \
/// --limit 100 \
/// --bbox "8.766 50.802 8.767 50.803" \
/// --time-start 2020-08-01T00:00:00Z \
/// --time-end 2020-08-31T23:59:59Z \
/// --stac-url https://stac.nsiscloud.polsa.gov.pl/v1 \
/// --s3-endpoint eodata.nsiscloud.polsa.gov.pl \
/// --s3-access-key XXX \
/// --s3-secret-key YYY \
/// --file-types jp2
/// ```
pub async fn stac_import(params: StacImport) -> Result<(), anyhow::Error> {
    init_stac_import_logging(params.verbose);

    let mut importer = StacImporter::new(params).await?;
    importer.run().await
}

fn init_stac_import_logging(verbose: bool) {
    let level = if verbose {
        tracing::Level::DEBUG
    } else {
        tracing::Level::WARN
    };

    let filter_targets = tracing_subscriber::filter::Targets::new()
        .with_target("geoengine_services", level)
        .with_default(match level {
            tracing::Level::DEBUG => tracing::Level::INFO,
            _ => level,
        });

    let subscriber = tracing_subscriber::Registry::default()
        .with(filter_targets)
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(tracing_subscriber::filter::LevelFilter::from_level(level)),
        );

    let _ = tracing::subscriber::set_global_default(subscriber);
}

struct StacImporter {
    params: StacImport,
    client: reqwest::Client,
    session_id: String,
    bands: HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>>,
    known_datasets: HashSet<DatasetKey>,
    created_datasets: HashSet<DatasetKey>,
    items_processed_total: u64,
    import_start_time: Instant,
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
            params,
            client,
            session_id,
            bands,
            known_datasets: HashSet::new(),
            created_datasets: HashSet::new(),
            items_processed_total: 0,
            import_start_time: Instant::now(),
        })
    }

    async fn run(&mut self) -> Result<(), anyhow::Error> {
        if self.params.verbose {
            info!("Scanned collection, found bands for data type and resolution:");
            for (partial_dataset_key, bands) in &self.bands {
                info!(
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
            info!("Dataset tiles added successfully");
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

                debug!("Fetching page: {state:?}");

                let start = Instant::now();
                let result = query_item_collection(&client, &state).await;
                let elapsed = start.elapsed();

                debug!("Page fetched in {:.2}s", elapsed.as_secs_f64());

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

    async fn process_pages(
        &mut self,
        buffered_stream: impl futures::Stream<Item = Result<stac::ItemCollection, anyhow::Error>>,
    ) -> Result<(), anyhow::Error> {
        // Process pages as they arrive from the buffered stream
        futures::pin_mut!(buffered_stream);
        while let Some(result) = buffered_stream.next().await {
            let item_collection = result?;
            let number_returned = item_collection.items.len() as u64;

            let dataset_tiles = self.process_items(item_collection.clone()).await?;

            for (dataset_key, tiles) in &dataset_tiles {
                let dataset_name = dataset_key.dataset_name(&self.params.stac_collection);

                debug!("Adding {} tiles to dataset {}", tiles.len(), dataset_name,);

                let response = retry_with_backoff(
                    || async {
                        self.client
                            .post(format!(
                                "{}/dataset/{}/tiles",
                                self.params.geo_engine_url, dataset_name
                            ))
                            .header("Content-Type", "application/json")
                            .header("Authorization", format!("Bearer {}", self.session_id))
                            .json(&tiles)
                            .send()
                            .await
                    },
                    &format!("Add tiles to dataset {dataset_name}"),
                )
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

            self.items_processed_total += number_returned;
            self.print_progress(&item_collection);
        }

        Ok(())
    }

    async fn process_items(
        &mut self,
        item_collection: stac::ItemCollection,
    ) -> Result<HashMap<DatasetKey, Vec<AddDatasetTile>>, anyhow::Error> {
        let mut dataset_tiles = HashMap::new();

        for item in item_collection.items {
            let item_id = item.id.clone();

            if let Err(err) = self.process_item(item, &mut dataset_tiles).await
                && self.params.verbose
            {
                warn!("Skipping item {item_id}: {err:#}");
            }
        }

        Ok(dataset_tiles)
    }

    #[allow(clippy::too_many_lines)]
    async fn process_item(
        &mut self,
        item: stac::Item,
        dataset_tiles: &mut HashMap<DatasetKey, Vec<AddDatasetTile>>,
    ) -> Result<(), anyhow::Error> {
        let stac_extension_versions = StacExtensionVersions::try_from(&item)?;

        let datetime = item
            .properties
            .datetime
            .ok_or(anyhow::anyhow!("Missing datetime in item properties"))?;

        // TODO: make mapping of item datetime to tile time validity configurable
        let date_without_time = datetime
            .with_hour(0)
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_nanosecond(0))
            .context("Failed to set time to zero")?;
        let date_without_time: DateTime = date_without_time.into();
        let time: TimeInstance = date_without_time.into();

        // item-level epsg code (may be overwritten by asset)
        let item_epsg_code = epsg_code_from_item(&item, stac_extension_versions.projection)?;

        // Filter by EPSG code if epsgs parameter is provided
        // (asset-level EPSG is also filtered in the per-asset processing functions below)
        if let Some(epsg) = item_epsg_code
            && !self.params.epsgs.is_empty()
            && !self.params.epsgs.contains(&epsg)
        {
            anyhow::bail!("EPSG {epsg} not in filter list");
        }

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

        // note: we need special handling here because some fields are properly mappen by stac crate while others are only available in additional_fields
        // TODO: properly handle all values mapped by stac crate
        // TODO: support non-datetime properties for z-index
        let z_index = match self.params.z_index_property_name.as_deref() {
            Some("updated") => {
                // use `updated` datetime as z-index, so that newer updates are on top of older ones
                item.properties
                    .updated
                    .ok_or(anyhow::anyhow!(
                        "Missing updated datetime in item properties"
                    ))
                    .and_then(|updated| {
                        chrono::DateTime::parse_from_rfc3339(&updated)
                            .context("Failed to parse updated datetime")
                    })?
                    .timestamp_millis()
            }
            Some(property) => item
                .properties
                .additional_fields
                .get(property)
                .ok_or(anyhow::anyhow!(
                    "Missing z index property '{property}' in item additional fields"
                ))
                .and_then(|v| {
                    v.as_str().ok_or(anyhow::anyhow!(
                        "Z index property '{property}' is not a string"
                    ))
                })
                .and_then(|updated| {
                    chrono::DateTime::parse_from_rfc3339(updated)
                        .context(format!("Failed to parse '{property}' datetime"))
                })?
                .timestamp_millis(),
            _ => 0,
        };

        for (asset_key, asset) in &item.assets {
            if !matches_selected_file_types(asset.r#type.as_deref(), &self.params.file_types) {
                debug!(
                    "Skipping {asset_key}: unsupported asset type: {:?}",
                    asset.r#type
                );
                continue;
            }

            let tiles = match (&item.version, stac_extension_versions) {
                (
                    &stac::Version::v1_0_0,
                    StacExtensionVersions {
                        projection: StacExtensionMajorVersion::V1,
                        raster: StacExtensionMajorVersion::V1,
                        eo: StacExtensionMajorVersion::V1,
                    },
                ) => {
                    self.process_item_asset_v1_0_0(asset, item_epsg_code, time, z_index)
                        .await
                }
                (
                    &stac::Version::v1_1_0,
                    StacExtensionVersions {
                        projection: StacExtensionMajorVersion::V2,
                        raster: StacExtensionMajorVersion::V2,
                        eo: StacExtensionMajorVersion::V2,
                    },
                ) => {
                    self.process_item_asset_v1_1_0(asset, item_epsg_code, time, z_index)
                        .await
                }
                _ => Err(anyhow::anyhow!(
                    "Unsupported STAC version or extension versions: {:?}, {stac_extension_versions:?}",
                    item.version
                )),
            };

            match tiles {
                Ok(tiles) => {
                    for (dataset_key, tile) in tiles {
                        dataset_tiles.entry(dataset_key).or_default().push(tile);
                    }
                }
                Err(err) => {
                    if self.params.verbose {
                        warn!("Skipping asset {asset_key} of item {}: {err:#}", item.id);
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_item_asset_v1_0_0(
        &mut self,
        asset: &Asset,
        epsg: Option<u32>,
        time: TimeInstance,
        z_index: i64,
    ) -> Result<Vec<(DatasetKey, AddDatasetTile)>, anyhow::Error> {
        let geo_transform = geo_transform_from_fields(&asset.additional_fields)
            .ok_or(anyhow::anyhow!("missing proj:transform"))?;

        let data_type = data_type_from_asset(stac::Version::v1_0_0, asset)
            .context("Failed to determine data type from asset")?;

        let epsg = epsg_code_from_fields(StacExtensionMajorVersion::V1, &asset.additional_fields)
            .or(epsg)
            .ok_or(anyhow::anyhow!("Failed to determine EPSG code from asset"))?;

        // Filter by EPSG code if epsgs parameter is provided (catches asset-level EPSG)
        if !self.params.epsgs.is_empty() && !self.params.epsgs.contains(&epsg) {
            anyhow::bail!("EPSG {epsg} not in filter list");
        }

        let dataset_key = DatasetKey {
            epsg,
            data_type,
            resolution: geo_transform.x_pixel_size().into(),
        };

        let partial_key = PartialDatasetKey {
            data_type: dataset_key.data_type,
            resolution: dataset_key.resolution,
        };

        if !self.bands.contains_key(&partial_key) {
            if self.params.verbose {
                debug!(
                    "Skipping asset {}: no scanned dataset definition for {:?}",
                    asset.href, dataset_key
                );
            }
            return Ok(Vec::new());
        }

        if !self.known_datasets.contains(&dataset_key) {
            let dataset_name = dataset_key.dataset_name(&self.params.stac_collection);
            let dataset_exists = self
                .dataset_exists(&dataset_name)
                .await
                .with_context(|| format!("failed to check dataset existence for {dataset_name}"))?;

            if !dataset_exists {
                self.create_dataset(&dataset_key, geo_transform)
                    .await
                    .context(format!("failed to create dataset {dataset_key:?}"))?;
                self.created_datasets.insert(dataset_key.clone());
            }

            self.known_datasets.insert(dataset_key.clone());
        }

        let eo_bands = asset.additional_fields.get("eo:bands");
        let band_count = if let Some(eo_bands_value) = eo_bands {
            let parsed_eo_bands: Vec<EoBand> = serde_json::from_value(eo_bands_value.clone())
                .map_err(|e| anyhow::anyhow!("invalid eo:bands: {e}"))?;
            Some(parsed_eo_bands)
        } else {
            None
        };

        let dataset_bands = self
            .bands
            .get(&partial_key)
            .ok_or(anyhow::anyhow!("unknown dataset key: {dataset_key:?}"))?;

        let processor = AssetBandProcessor {
            asset,
            params: &self.params,
            time,
            geo_transform,
            dataset_bands,
            z_index,
            no_data_value: self.params.no_data_value,
            gdal_retries: self.params.gdal_retries,
        };

        let mut tiles = Vec::new();
        match band_count {
            Some(eo_bands) => {
                // Asset has both raster:bands and eo:bands
                for (band_idx, eo_band) in eo_bands.iter().enumerate() {
                    let band_name =
                        v1_0_0_band_name(asset.title.as_deref(), Some(eo_band), eo_bands.len());

                    let tile = processor
                        .process_band_v1_0_0(band_idx, &band_name)
                        .context(format!("Failed to process band {}", eo_band.name))?;
                    tiles.push((dataset_key.clone(), tile));
                }
            }
            None => {
                // Asset has only raster:bands (no eo:bands) - single-band asset
                let band_name = v1_0_0_band_name(asset.title.as_deref(), None, 1);
                let tile = processor
                    .process_band_v1_0_0(0, &band_name)
                    .context("Failed to process single-band asset")?;
                tiles.push((dataset_key.clone(), tile));
            }
        }

        Ok(tiles)
    }

    async fn process_item_asset_v1_1_0(
        &mut self,
        asset: &Asset,
        epsg: Option<u32>,
        time: TimeInstance,
        z_index: i64,
    ) -> Result<Vec<(DatasetKey, AddDatasetTile)>, anyhow::Error> {
        let geo_transform = geo_transform_from_fields(&asset.additional_fields)
            .ok_or(anyhow::anyhow!("missing proj:transform"))?;

        let data_type = data_type_from_asset(stac::Version::v1_1_0, asset)
            .context("Failed to determine data type from asset")?;

        let epsg = epsg_code_from_fields(StacExtensionMajorVersion::V2, &asset.additional_fields)
            .or(epsg)
            .ok_or(anyhow::anyhow!("Failed to determine EPSG code from asset"))?;

        // Filter by EPSG code if epsgs parameter is provided (catches asset-level EPSG)
        if !self.params.epsgs.is_empty() && !self.params.epsgs.contains(&epsg) {
            anyhow::bail!("EPSG {epsg} not in filter list");
        }

        let dataset_key = DatasetKey {
            epsg,
            data_type,
            resolution: geo_transform.x_pixel_size().into(),
        };

        let partial_key = PartialDatasetKey {
            data_type: dataset_key.data_type,
            resolution: dataset_key.resolution,
        };

        if !self.bands.contains_key(&partial_key) {
            if self.params.verbose {
                debug!(
                    "Skipping asset {}: no scanned dataset definition for {:?}",
                    asset.href, dataset_key
                );
            }
            return Ok(Vec::new());
        }

        if !self.known_datasets.contains(&dataset_key) {
            let dataset_name = dataset_key.dataset_name(&self.params.stac_collection);
            let dataset_exists = self
                .dataset_exists(&dataset_name)
                .await
                .with_context(|| format!("failed to check dataset existence for {dataset_name}"))?;

            if !dataset_exists {
                self.create_dataset(&dataset_key, geo_transform)
                    .await
                    .context(format!("failed to create dataset {dataset_key:?}"))?;
                self.created_datasets.insert(dataset_key.clone());
            }

            self.known_datasets.insert(dataset_key.clone());
        }

        let asset_bands = band_names_from_asset_v1_1_0(asset)?;

        let dataset_bands = self
            .bands
            .get(&partial_key)
            .ok_or(anyhow::anyhow!("unknown dataset key: {dataset_key:?}"))?;

        let processor = AssetBandProcessor {
            asset,
            params: &self.params,
            time,
            geo_transform,
            dataset_bands,
            z_index,
            no_data_value: self.params.no_data_value,
            gdal_retries: self.params.gdal_retries,
        };

        let mut tiles = Vec::new();
        for (band_idx, band_name) in asset_bands.iter().enumerate() {
            let tile = processor
                .process_band_v1_1_0(band_idx, band_name)
                .context(format!("Failed to process band {band_name}"))?;
            tiles.push((dataset_key.clone(), tile));
        }

        Ok(tiles)
    }

    async fn dataset_exists(&self, dataset_name: &str) -> Result<bool, anyhow::Error> {
        let response = retry_with_backoff(
            || async {
                self.client
                    .get(format!(
                        "{}/dataset/{}",
                        self.params.geo_engine_url, dataset_name
                    ))
                    .header("Authorization", format!("Bearer {}", self.session_id))
                    .send()
                    .await
            },
            &format!("Check dataset existence for {dataset_name}"),
        )
        .await
        .context("Failed to send dataset existence request")?;

        if response.status().is_success() {
            return Ok(true);
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::BAD_REQUEST
            && let Ok(error_response) = serde_json::from_str::<ErrorResponse>(&body)
        {
            // The dataset API may return a generic CannotLoadDataset message for unknown names.
            if error_response.error == "CannotLoadDataset" {
                return Ok(false);
            }
        }

        anyhow::bail!("Failed to check dataset '{dataset_name}': HTTP {status}: {body}");
    }

    #[allow(clippy::too_many_lines)]
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
            .context(format!("Failed to get bands for dataset: {dataset_key:?}"))?;

        let create_dataset = CreateDataset {
            data_path: DataPath::Volume(VolumeName(self.params.volume_name.clone())),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: Some(
                        DatasetName::from_str(dataset_name_str)
                            .context("Failed to create dataset name")?,
                    ),
                    display_name: dataset_name_str.clone(),
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
                                origin: TimeInstance::from_millis_unchecked(0).into(), // TODO: make configurable
                                step: TimeStep {
                                    granularity: TimeGranularity::Days, // TODO: make configurable
                                    step: 1,                            // TODO: make configurable
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
                            .context(format!("Failed to create band descriptors {bands:?}"))?,
                    },
                }),
            },
        };

        let response = retry_with_backoff(
            || async {
                self.client
                    .post(format!("{}/dataset", self.params.geo_engine_url))
                    .header("Content-Type", "application/json")
                    .header("Authorization", format!("Bearer {}", self.session_id))
                    .json(&create_dataset)
                    .send()
                    .await
            },
            &format!("Create dataset {dataset_name_str}"),
        )
        .await
        .context("Failed to send dataset creation request")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "Failed to create dataset {dataset_name_str}: HTTP {status}: {error_text}"
            );
        }

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
            let response = retry_with_backoff(
                || async {
                    self.client
                        .put(format!("{}/permissions", self.params.geo_engine_url))
                        .header("Content-Type", "application/json")
                        .header("Authorization", format!("Bearer {}", self.session_id))
                        .json(&permission)
                        .send()
                        .await
                },
                &format!("Add dataset permission for role {}", permission.role_id),
            )
            .await
            .context("Failed to add permission")?;

            if self.params.verbose {
                info!(
                    "Dataset '{}' shared with role {}: {}",
                    dataset_name,
                    permission.role_id,
                    response.text().await.unwrap_or_default()
                );
            }
        }

        Ok(())
    }

    fn print_progress(&self, item_collection: &stac::ItemCollection) {
        let elapsed_secs = self.import_start_time.elapsed().as_secs_f64();
        let items_per_sec = if elapsed_secs > 0.0 {
            self.items_processed_total as f64 / elapsed_secs
        } else {
            0.0
        };

        if let Some(number_matched) = item_collection
            .additional_fields
            .get("numberMatched")
            .and_then(serde_json::Value::as_u64)
        {
            let progress = (self.items_processed_total as f64 / number_matched as f64 * 100.0)
                .clamp(0.0, 100.0);

            let remaining = number_matched.saturating_sub(self.items_processed_total);
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
                progress, self.items_processed_total, number_matched, items_per_sec, eta_str
            );
        } else if !item_collection.items.is_empty() {
            // If number_matched is not available, just show the count and rate
            println!(
                "Processed {} items ({:.1} items/s)",
                self.items_processed_total, items_per_sec
            );
        }
    }

    async fn create_collections_and_layers(&self) -> anyhow::Result<()> {
        // Create root collection for STAC collection
        let root_collection_id = self
            .create_layer_collection(
                &LayerCollectionId(INTERNAL_LAYER_DB_ROOT_COLLECTION_ID.to_string()),
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

        // Under "By Data Type": DataType -> {Resolution, Epsg} -> layers
        self.create_hierarchy_with_branches(
            &by_datatype_id,
            &layer_ids,
            Attribute::DataType,
            &[Attribute::Resolution, Attribute::Epsg],
        )
        .await?;
        // Under "By Resolution": Resolution -> {DataType, Epsg} -> layers
        self.create_hierarchy_with_branches(
            &by_resolution_id,
            &layer_ids,
            Attribute::Resolution,
            &[Attribute::DataType, Attribute::Epsg],
        )
        .await?;
        // Under "By EPSG": Epsg -> {DataType, Resolution} -> layers
        self.create_hierarchy_with_branches(
            &by_epsg_id,
            &layer_ids,
            Attribute::Epsg,
            &[Attribute::DataType, Attribute::Resolution],
        )
        .await?;

        Ok(())
    }

    async fn create_layer_collection(
        &self,
        parent_id: &LayerCollectionId,
        name: &str,
        description: &str,
    ) -> anyhow::Result<LayerCollectionId> {
        // Reuse existing collection from previous runs if there is a child collection with the same name.
        if let Some(existing_id) = self
            .find_child_collection_id_by_name(parent_id, name)
            .await
            .context("Failed to query existing child collections")?
        {
            if self.params.verbose {
                info!("Found existing layer collection '{name}' with id {existing_id}");
            }

            return Ok(existing_id);
        }

        let add_collection = AddLayerCollection {
            name: name.to_string(),
            description: description.to_string(),
            properties: vec![],
        };

        let response: IdResponse<LayerCollectionId> = retry_with_backoff(
            || async {
                self.client
                    .post(format!(
                        "{}/layerDb/collections/{}/collections",
                        self.params.geo_engine_url, parent_id
                    ))
                    .header("Content-Type", "application/json")
                    .header("Authorization", format!("Bearer {}", self.session_id))
                    .json(&add_collection)
                    .send()
                    .await?
                    .json()
                    .await
            },
            &format!("Create layer collection '{name}'"),
        )
        .await
        .context("Failed to add layer collection")?;

        // Share with all users
        self.share_layer_collection(&response.id).await?;

        if self.params.verbose {
            debug!(
                "Created layer collection '{}' with id {}",
                name, response.id.0
            );
        }

        Ok(response.id)
    }

    async fn find_child_collection_id_by_name(
        &self,
        parent_id: &LayerCollectionId,
        child_name: &str,
    ) -> anyhow::Result<Option<LayerCollectionId>> {
        let mut offset: u32 = 0;
        let limit: u32 = 20;

        loop {
            let response = retry_with_backoff(
                || async {
                    self.client
                        .get(format!(
                            "{}/layers/collections/{}/{}",
                            self.params.geo_engine_url, INTERNAL_PROVIDER_ID, parent_id
                        ))
                        .query(&[("offset", offset), ("limit", limit)])
                        .header("Authorization", format!("Bearer {}", self.session_id))
                        .send()
                        .await?
                        .json::<LayerCollection>()
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

    async fn create_layer(
        &self,
        collection_id: &LayerCollectionId,
        dataset_key: &DatasetKey,
    ) -> anyhow::Result<LayerId> {
        let dataset_name = dataset_key.dataset_name(&self.params.stac_collection);
        let layer_name = format!(
            "EPSG:{} {:?} res:{}",
            dataset_key.epsg, dataset_key.data_type, dataset_key.resolution
        );

        let add_layer = AddLayer {
            name: layer_name.clone(),
            description: format!("Dataset: {dataset_name}"),
            workflow: Workflow::Legacy {
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

        let response: IdResponse<LayerId> = retry_with_backoff(
            || async {
                self.client
                    .post(format!(
                        "{}/layerDb/collections/{}/layers",
                        self.params.geo_engine_url, collection_id
                    ))
                    .header("Content-Type", "application/json")
                    .header("Authorization", format!("Bearer {}", self.session_id))
                    .json(&add_layer)
                    .send()
                    .await?
                    .json()
                    .await
            },
            &format!("Create layer '{layer_name}'"),
        )
        .await
        .context("Failed to add layer to collection")?;

        // Share with all users
        self.share_layer(&response.id).await?;

        if self.params.verbose {
            debug!("Created layer '{layer_name}' in collection {collection_id}");
        }

        Ok(response.id)
    }

    async fn add_existing_layer_to_collection(
        &self,
        collection_id: &LayerCollectionId,
        layer_id: &LayerId,
    ) -> anyhow::Result<()> {
        retry_with_backoff(
            || async {
                self.client
                    .post(format!(
                        "{}/layerDb/collections/{}/layers/{}",
                        self.params.geo_engine_url, collection_id, layer_id.0
                    ))
                    .header("Authorization", format!("Bearer {}", self.session_id))
                    .send()
                    .await
            },
            &format!("Add layer {} to collection {}", layer_id.0, collection_id),
        )
        .await
        .context("Failed to add existing layer to collection")?;

        if self.params.verbose {
            debug!("Added layer {} to collection {}", layer_id.0, collection_id);
        }

        Ok(())
    }

    async fn create_hierarchy_with_branches(
        &self,
        root_id: &LayerCollectionId,
        layer_ids: &HashMap<DatasetKey, LayerId>,
        first_attr: Attribute,
        second_attrs: &[Attribute; 2],
    ) -> anyhow::Result<()> {
        // Group by first attribute
        let grouped_level1 =
            Self::group_by_attribute(layer_ids.keys().cloned().collect(), first_attr);

        // Process each value of the first attribute
        for (value1, keys1) in grouped_level1 {
            // Create collection for this value of first attribute (e.g., "U16", "EPSG:32630")
            let name1 = first_attr.format_value(&value1);
            let desc1 = format!("{name1} datasets");
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
                let grouped_level2 = Self::group_by_attribute(keys1.clone(), second_attr);

                // Process each value of the second attribute
                for (value2, keys2) in grouped_level2 {
                    let name2 = second_attr.format_value(&value2);
                    let desc2 = format!("Layers: {name1} + {name2}");
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
            grouped.entry(value).or_default().push(key.clone());
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
            retry_with_backoff(
                || async {
                    self.client
                        .put(format!("{}/permissions", self.params.geo_engine_url))
                        .header("Content-Type", "application/json")
                        .header("Authorization", format!("Bearer {}", self.session_id))
                        .json(&permission)
                        .send()
                        .await
                },
                &format!("Share layer collection with role {}", permission.role_id),
            )
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
            retry_with_backoff(
                || async {
                    self.client
                        .put(format!("{}/permissions", self.params.geo_engine_url))
                        .header("Content-Type", "application/json")
                        .header("Authorization", format!("Bearer {}", self.session_id))
                        .json(&permission)
                        .send()
                        .await
                },
                &format!("Share layer with role {}", permission.role_id),
            )
            .await
            .context("Failed to add permission")?;
        }

        Ok(())
    }
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

fn epsg_code_from_fields(
    proj_extension_version: StacExtensionMajorVersion,
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Option<u32> {
    let proj_epsg = fields.get("proj:epsg").and_then(|value| {
        value
            .as_u64()
            .map(|code| code as u32)
            .or_else(|| value.as_str().and_then(|code| code.parse::<u32>().ok()))
    });

    let proj_code = fields
        .get("proj:code")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_epsg_from_proj_code);

    match proj_extension_version {
        StacExtensionMajorVersion::V1 => proj_epsg.or(proj_code),
        StacExtensionMajorVersion::V2 => proj_code.or(proj_epsg),
    }
}

#[allow(clippy::unnecessary_wraps)]
fn epsg_code_from_item(
    item: &stac::Item,
    proj_extension_version: StacExtensionMajorVersion,
) -> Result<Option<u32>, anyhow::Error> {
    let from_additional =
        epsg_code_from_fields(proj_extension_version, &item.properties.additional_fields);
    if from_additional.is_some() {
        return Ok(from_additional);
    }

    let Some(properties) = serde_json::to_value(item)
        .ok()
        .and_then(|value| value.get("properties").cloned())
        .and_then(|value| value.as_object().cloned())
    else {
        return Ok(None);
    };

    let from_properties = epsg_code_from_fields(proj_extension_version, &properties);
    if from_properties.is_some() {
        return Ok(from_properties);
    }

    let fallback_version = match proj_extension_version {
        StacExtensionMajorVersion::V1 => StacExtensionMajorVersion::V2,
        StacExtensionMajorVersion::V2 => StacExtensionMajorVersion::V1,
    };

    Ok(epsg_code_from_fields(fallback_version, &properties))
}

fn parse_epsg_from_proj_code(code: &str) -> Option<u32> {
    if let Some(code) = code.strip_prefix("EPSG:") {
        return code.parse::<u32>().ok();
    }

    // e.g. http://www.opengis.net/def/crs/EPSG/0/32632
    code.rsplit('/').next()?.parse::<u32>().ok()
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

    fn format_value(self, value: &AttributeValue) -> String {
        match (self, value) {
            (Attribute::DataType, AttributeValue::DataType(dt)) => format!("{dt:?}"),
            (Attribute::Resolution, AttributeValue::Resolution(res)) => format!("res:{res}"),
            (Attribute::Epsg, AttributeValue::Epsg(epsg)) => format!("EPSG:{epsg}"),
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

struct AssetBandProcessor<'a> {
    asset: &'a Asset,
    params: &'a StacImport,
    time: TimeInstance,
    geo_transform: GeoTransform,
    dataset_bands: &'a [RasterBandDescriptor],
    z_index: i64,
    no_data_value: Option<f64>,
    gdal_retries: Option<usize>,
}

impl AssetBandProcessor<'_> {
    fn process_band_v1_0_0(
        &self,
        band_idx: usize,
        band_name: &str,
    ) -> anyhow::Result<AddDatasetTile> {
        let band_index = self
            .dataset_bands
            .iter()
            .position(|b| b.name == band_name)
            .ok_or(anyhow::anyhow!("unknown band: {band_name}"))?;

        let proj_shape = self
            .asset
            .additional_fields
            .get("proj:shape")
            .ok_or(anyhow::anyhow!("missing proj:shape"))?;

        let proj_shape = proj_shape
            .as_array()
            .ok_or(anyhow::anyhow!("proj:shape is not an array"))?;

        let (height, width) = (
            proj_shape[0]
                .as_u64()
                .ok_or(anyhow::anyhow!("proj:shape[0] is not a u64"))? as usize,
            proj_shape[1]
                .as_u64()
                .ok_or(anyhow::anyhow!("proj:shape[1] is not a u64"))? as usize,
        );

        let grid_bounds = geoengine_datatypes::raster::GridBoundingBox2D::new(
            GridIdx2D { x_idx: 0, y_idx: 0 },
            GridIdx2D {
                x_idx: (width - 1) as isize,
                y_idx: (height - 1) as isize,
            },
        )
        .context("Failed to create grid bounds from proj:shape")?;

        let spatial_partition = self.geo_transform.grid_to_spatial_bounds(&grid_bounds);

        // println!(
        //     "Importing tile: date: {}, band: {}, href: {}",
        //     self.date, self.asset_key, self.asset.href
        // );

        let file_path = gdal_file_path(self.asset)?;

        let gdal_config_options = self.gdal_options_for_file_path(&file_path)?;

        let tile = AddDatasetTile {
            time: TimeInterval::new(self.time, self.time + i64::from(24 * 60 * 60 * 1000)) // TODO: make time validity configurable
                .context("Failed to create time interval")?
                .into(),
            spatial_partition: spatial_partition.into(),
            band: band_index as u32,
            z_index: self.z_index,
            params: GdalDatasetParameters {
                file_path: file_path.into(),
                rasterband_channel: band_idx + 1, // gdal channels are 1-based
                geo_transform: self.geo_transform.into(),
                width,
                height,
                file_not_found_handling: crate::api::model::operators::FileNotFoundHandling::Error,
                no_data_value: self.no_data_value,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options,
                allow_alphaband_as_mask: false,
                retry: self.gdal_retries.map(|max_retries| {
                    crate::api::model::operators::GdalRetryOptions { max_retries }
                }),
            },
        };

        Ok(tile)
    }

    fn gdal_options_for_file_path(
        &self,
        file_path: &GdalFilePath,
    ) -> Result<Option<Vec<GdalConfigOption>>, anyhow::Error> {
        let gdal_open_options = if let GdalFilePath::S3(_) = *file_path {
            // TODO: allow skipping s3 assets on missing credentials?
            let s3_endpoint = self.params.s3_endpoint.as_ref().ok_or(anyhow::anyhow!(
                "S3 endpoint must be provided for S3 assets"
            ))?;
            let s3_access_key = self.params.s3_access_key.as_ref().ok_or(anyhow::anyhow!(
                "S3 access key must be provided for S3 assets"
            ))?;
            let s3_secret_key = self.params.s3_secret_key.as_ref().ok_or(anyhow::anyhow!(
                "S3 secret key must be provided for S3 assets"
            ))?;

            // for old gdal version s3 endpoint may not include the protocol
            if s3_endpoint.starts_with("http://") || s3_endpoint.starts_with("https://") {
                anyhow::bail!(
                    "S3 endpoint should not include protocol (http/https), got: {s3_endpoint}"
                );
            }

            Some(vec![
                ("AWS_S3_ENDPOINT".to_string(), s3_endpoint.clone()).into(),
                ("AWS_ACCESS_KEY_ID".to_string(), s3_access_key.clone()).into(),
                ("AWS_SECRET_ACCESS_KEY".to_string(), s3_secret_key.clone()).into(),
                // StringPair(("AWS_HTTPS".to_string(), "YES".to_string())), // TODO: make configurable?
                ("AWS_VIRTUAL_HOSTING".to_string(), "FALSE".to_string()).into(), // TODO: make configurable?
            ])
        } else {
            None
        };
        Ok(gdal_open_options)
    }

    fn process_band_v1_1_0(
        &self,
        band_idx: usize,
        band_name: &str,
    ) -> anyhow::Result<AddDatasetTile> {
        let band_index = self
            .dataset_bands
            .iter()
            .position(|b| b.name == band_name)
            .ok_or(anyhow::anyhow!("unknown band: {band_name}"))?;

        let proj_shape = self
            .asset
            .additional_fields
            .get("proj:shape")
            .ok_or(anyhow::anyhow!("missing proj:shape"))?;

        let proj_shape = proj_shape
            .as_array()
            .ok_or(anyhow::anyhow!("proj:shape is not an array"))?;

        let (height, width) = (
            proj_shape[0]
                .as_u64()
                .ok_or(anyhow::anyhow!("proj:shape[0] is not a u64"))? as usize,
            proj_shape[1]
                .as_u64()
                .ok_or(anyhow::anyhow!("proj:shape[1] is not a u64"))? as usize,
        );

        let grid_bounds = geoengine_datatypes::raster::GridBoundingBox2D::new(
            GridIdx2D { x_idx: 0, y_idx: 0 },
            GridIdx2D {
                x_idx: (width - 1) as isize,
                y_idx: (height - 1) as isize,
            },
        )
        .context("Failed to create grid bounds from proj:shape")?;

        let spatial_partition = self.geo_transform.grid_to_spatial_bounds(&grid_bounds);

        // println!(
        //     "Importing tile: date: {}, band: {}, href: {}",
        //     self.date, self.asset_key, self.asset.href
        // );

        let file_path = gdal_file_path(self.asset)?;

        let gdal_config_options = self.gdal_options_for_file_path(&file_path)?;

        let tile = AddDatasetTile {
            time: TimeInterval::new(self.time, self.time + i64::from(24 * 60 * 60 * 1000)) // TODO: make time validity configurable
                .context("Failed to create time interval")?
                .into(),
            spatial_partition: spatial_partition.into(),
            band: band_index as u32,
            z_index: self.z_index,
            params: GdalDatasetParameters {
                file_path: file_path.into(),
                rasterband_channel: band_idx + 1, // gdal channels are 1-based
                geo_transform: self.geo_transform.into(),
                width,
                height,
                file_not_found_handling: crate::api::model::operators::FileNotFoundHandling::Error,
                no_data_value: self.no_data_value,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options,
                allow_alphaband_as_mask: false,
                retry: self.gdal_retries.map(|max_retries| {
                    crate::api::model::operators::GdalRetryOptions { max_retries }
                }),
            },
        };

        Ok(tile)
    }
}

fn gdal_file_path(asset: &Asset) -> anyhow::Result<GdalFilePath> {
    if asset.href.starts_with("http") {
        Ok(GdalFilePath::Http(format!("/vsicurl/{}", asset.href)))
    } else if let Some(s3_url) = asset.href.strip_prefix("s3://") {
        Ok(GdalFilePath::S3(format!("/vsis3/{s3_url}")))
    } else {
        anyhow::bail!("Unsupported asset href format for GDAL: {}", asset.href);
    }
}

enum GdalFilePath {
    Http(String),
    S3(String),
}

impl GdalFilePath {
    fn into(self) -> PathBuf {
        match self {
            GdalFilePath::Http(path) | GdalFilePath::S3(path) => PathBuf::from(path),
        }
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
            let item_collection: stac::ItemCollection = retry_with_backoff(
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
            let item_collection: stac::ItemCollection = retry_with_backoff(
                || async { client.get(next_url).send().await?.json().await },
                "Query STAC next page",
            )
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
        QueryState::Finished => anyhow::bail!("No more pages to query"),
    }
}

fn create_query_params(params: &StacImport) -> Vec<(String, String)> {
    let mut query_params: Vec<(String, String)> = Vec::new();

    // Add bbox if provided
    if let Some(bbox) = &params.bbox
        && bbox.len() == 4
    {
        query_params.push((
            "bbox".to_owned(),
            format!(
                "{},{},{},{}", // array-brackets are not used in standard but required here for unknkown reason
                bbox[0], bbox[1], bbox[2], bbox[3]
            ),
        )); // TODO: order coordinates depending on projection
    }

    if params.time_start.is_some() || params.time_end.is_some() {
        query_params.push((
            "datetime".to_owned(),
            format!(
                "{}/{}",
                params.time_start.as_deref().unwrap_or(""),
                params.time_end.as_deref().unwrap_or("")
            ),
        ));
    }

    if let Some(limit) = params.limit {
        query_params.push(("limit".to_owned(), limit.to_string()));
    }

    // TODO: only filter if server supports it? check via `conformance` field in API?
    if params.filter_item_fields {
        query_params.push(("fields".to_owned(), STAC_ITEMS_FIELDS_FILTER.join(",")));
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

/// STAC collection is split up by epsg, data type, resolution/grid to produce uniform Geo Engine datasets
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

        let resolution_str = format!("{}", self.resolution);
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
            cleaned_name, self.epsg, self.data_type, clean_resolution
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
    #[serde(default)]
    common_name: Option<String>,
    // description: String,
    // ...
}

fn normalize_label(value: &str) -> String {
    value
        .trim()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("_")
}

fn title_fallback_label(title: Option<&str>) -> String {
    if let Some(title) = title {
        // Prefer concise acronym-like labels in parentheses, e.g. "Scene classification map (SCL)".
        if let (Some(start), Some(end)) = (title.rfind('('), title.rfind(')'))
            && start < end
        {
            let short = title[start + 1..end].trim();
            if !short.is_empty()
                && short.len() <= 32
                && short
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            {
                return short.to_lowercase();
            }
        }

        let normalized = normalize_label(title);
        if !normalized.is_empty() {
            return normalized;
        }
    }

    "band".to_string()
}

fn rededge_variant_from_metadata(eo_name: &str, title: Option<&str>) -> Option<&'static str> {
    let eo = eo_name.to_lowercase();
    let title_lower = title.map(str::to_lowercase).unwrap_or_default();

    if eo.contains("b05")
        || eo.contains("band_5")
        || title_lower.contains("band 5")
        || title_lower.contains("b05")
    {
        return Some("rededge1");
    }
    if eo.contains("b06")
        || eo.contains("band_6")
        || title_lower.contains("band 6")
        || title_lower.contains("b06")
    {
        return Some("rededge2");
    }
    if eo.contains("b07")
        || eo.contains("band_7")
        || title_lower.contains("band 7")
        || title_lower.contains("b07")
    {
        return Some("rededge3");
    }

    None
}

fn v1_0_0_band_name(title: Option<&str>, eo_band: Option<&EoBand>, band_count: usize) -> String {
    let eo_name = eo_band.and_then(|band| {
        let eo_name = band.name.to_lowercase();
        let common_name = band.common_name.as_ref().map(|name| name.to_lowercase());

        match common_name.as_deref() {
            // `rededge` is used for multiple Sentinel-2 bands (B05/B06/B07).
            // Keep stable, unique names to avoid band collisions.
            Some("rededge") => rededge_variant_from_metadata(&eo_name, title)
                .map(std::string::ToString::to_string)
                .or_else(|| Some(format!("rededge[{eo_name}]"))),
            Some(common_name) => Some(common_name.to_string()),
            None => Some(eo_name),
        }
    });

    if band_count > 1 {
        let asset_label = title_fallback_label(title);
        let eo_name = eo_name.unwrap_or_else(|| "band".to_string());
        return format!("{asset_label}[{eo_name}]");
    }

    if let Some(eo_name) = eo_name {
        return eo_name;
    }

    title_fallback_label(title)
}

fn is_cog_media_type(media_type: Option<&str>) -> bool {
    media_type == Some("image/tiff; application=geotiff; profile=cloud-optimized")
}

fn is_jp2_media_type(media_type: Option<&str>) -> bool {
    media_type == Some("image/jp2")
}

fn matches_selected_file_types(media_type: Option<&str>, file_types: &[ImportFileType]) -> bool {
    file_types.iter().any(|file_type| match file_type {
        ImportFileType::Cog => is_cog_media_type(media_type),
        ImportFileType::Jp2 => is_jp2_media_type(media_type),
    })
}

async fn scan_collection(
    params: &StacImport,
    client: &reqwest::Client,
) -> anyhow::Result<HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>>> {
    let collection: stac::Collection = retry_with_backoff(
        || async {
            client
                .get(format!(
                    "{}/collections/{}",
                    params.stac_url, params.stac_collection
                ))
                .send()
                .await?
                .json()
                .await
        },
        "Scan STAC collection",
    )
    .await?;

    // create datasets by grouping items by (epsg, data_type, resolution/grid)
    // because datasets must have uniform epsg, data_type, resolution

    let stac_extension_versions = StacExtensionVersions::try_from(&collection)?;

    let mut dataset_bands: HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>> = HashMap::new();

    for (asset_key, asset) in &collection.item_assets {
        if !matches_selected_file_types(asset.r#type.as_deref(), &params.file_types) {
            println!(
                "[DEBUG] Skipping asset {asset_key} with unsupported media type: {:?}",
                asset.r#type
            );
            continue;
        }

        let asset_bands = match (&collection.version, stac_extension_versions) {
            (
                &stac::Version::v1_0_0,
                StacExtensionVersions {
                    projection: StacExtensionMajorVersion::V1,
                    raster: StacExtensionMajorVersion::V1,
                    eo: StacExtensionMajorVersion::V1,
                },
            ) => scan_item_asset_v1_0_0(asset),
            (
                &stac::Version::v1_1_0,
                StacExtensionVersions {
                    projection: StacExtensionMajorVersion::V2,
                    raster: StacExtensionMajorVersion::V2,
                    eo: StacExtensionMajorVersion::V2,
                },
            ) => scan_item_asset_v1_1_0(asset, &collection.summaries),
            _ => Err(anyhow::anyhow!(
                "Unsupported STAC version or extension versions: {:?}, {stac_extension_versions:?}",
                collection.version
            )),
        };

        match asset_bands {
            Ok(Some(asset_bands)) => {
                merge_dataset_bands(&mut dataset_bands, asset_bands);
            }
            Err(err) => {
                println!("[ERROR] Skipping asset {asset_key}: {err:#}");
            }
            _ => {}
        }
    }

    for bands in dataset_bands.values_mut() {
        bands.sort_by(|a: &RasterBandDescriptor, b| a.name.cmp(&b.name));
    }
    Ok(dataset_bands)
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

fn scan_item_asset_v1_0_0(
    asset: &stac::ItemAsset,
) -> anyhow::Result<Option<HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>>>> {
    let mut dataset_bands: HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>> = HashMap::new();

    let Some(raster_bands) = asset.additional_fields.get("raster:bands") else {
        return Ok(None);
    };
    let raster_bands: Vec<stac_extensions::raster::Band> =
        serde_json::from_value(raster_bands.clone())
            .map_err(|e| anyhow::anyhow!("invalid raster:bands: {e}",))?;

    let band_count = raster_bands.len();

    // Try to get eo:bands (optional)
    let eo_bands = asset
        .additional_fields
        .get("eo:bands")
        .and_then(|v| serde_json::from_value::<Vec<EoBand>>(v.clone()).ok());

    // If eo:bands is present, it must match raster:bands length
    if let Some(ref eo_bands_vec) = eo_bands {
        if band_count != eo_bands_vec.len() {
            return Ok(None);
        }
    } else if band_count != 1 {
        // If eo:bands is missing, only support single-band assets
        return Ok(None);
    }

    for (index, raster_band) in raster_bands.into_iter().enumerate() {
        let data_type = raster_band
            .data_type
            .ok_or(anyhow::anyhow!("Missing data_type in raster band"))?;
        let raster_data_type = raster_data_type_from_stac_data_type(&data_type)?;

        let geo_transform = geo_transform_from_fields(&asset.additional_fields)
            .ok_or(anyhow::anyhow!("missing proj:transform"))?;
        let resolution = geo_transform.x_pixel_size().into();

        let band_name = if let Some(ref eo_bands_vec) = eo_bands {
            // Use eo:bands metadata if available
            v1_0_0_band_name(
                asset.title.as_deref(),
                Some(&eo_bands_vec[index]),
                band_count,
            )
        } else {
            // For single-band assets without eo:bands, derive a stable name from title metadata.
            v1_0_0_band_name(asset.title.as_deref(), None, 1)
        };

        dataset_bands
                .entry(PartialDatasetKey {
                    data_type: raster_data_type,
                    resolution,
                })
                .or_default()
                .push(RasterBandDescriptor {
                    name: band_name,
                    // TODO: unit from raster_band.unit
                    measurement: Measurement::Unitless(UnitlessMeasurement { r#type: crate::api::model::datatypes::UnitlessMeasurementTypeTag::UnitlessMeasurementTypeTag }),
                });
    }

    Ok(Some(dataset_bands))
}

fn scan_item_asset_v1_1_0(
    asset: &stac::ItemAsset,
    collection_summaries: &Option<serde_json::Map<String, serde_json::Value>>,
) -> anyhow::Result<Option<HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>>>> {
    let mut dataset_bands: HashMap<PartialDatasetKey, Vec<RasterBandDescriptor>> = HashMap::new();

    // in STAC 1.1.0 the `data_type` is now common metadata
    let data_type = asset
        .additional_fields
        .get("data_type")
        .ok_or(anyhow::anyhow!(
            "Missing data_type in asset additional fields"
        ))?
        .as_str()
        .ok_or(anyhow::anyhow!("data_type is not a string"))?;

    let data_type = raster_data_type_from_stac_data_type_str(data_type)
        .context(format!("Unsupported data_type: {data_type}"))?;

    // in STAC 1.1.0 `raster:bands` and `eo:bands` are merged into common metadata `bands`
    let band_names = band_names_from_item_asset_v1_1_0(asset)?;

    let resolution = asset
        .additional_fields
        .get("gsd")
        .and_then(serde_json::Value::as_f64)
        .or_else(|| {
            // Fall back to deriving resolution from proj:transform if gsd is not available
            geo_transform_from_fields(&asset.additional_fields).map(|gt| gt.x_pixel_size().abs())
        })
        .or_else(|| {
            // Fall back to collection summaries for gsd (may list multiple resolutions)
            collection_summaries
                .as_ref()
                .and_then(|s| s.get("gsd"))
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_f64())
        })
        .ok_or(anyhow::anyhow!(
            "missing attribute `gsd` or `proj:transform`"
        ))?;

    for band_name in band_names {
        dataset_bands
            .entry(PartialDatasetKey {
                data_type,
                resolution: resolution.into(),
            })
            .or_default()
            .push(RasterBandDescriptor {
                name: band_name.clone(),
                // TODO: unit from raster_band.unit
                measurement: Measurement::Unitless(UnitlessMeasurement { r#type: crate::api::model::datatypes::UnitlessMeasurementTypeTag::UnitlessMeasurementTypeTag }),
            });
    }

    Ok(Some(dataset_bands))
}

fn band_names_from_asset_v1_1_0(asset: &stac::Asset) -> anyhow::Result<Vec<String>> {
    let asset_title = asset
        .title
        .as_deref()
        .ok_or(anyhow::anyhow!("Missing title in asset metadata"))?;

    let bands = &asset.bands;

    if bands.is_empty() {
        return Ok(vec![asset_title.to_string()]);
    }

    let mut names = Vec::new();
    if bands.len() == 1 {
        names.push(asset_title.to_string());
        return Ok(names);
    }

    for band in bands {
        let Some(band_name) = &band.name else {
            anyhow::bail!("Band is missing name for multi-band asset");
        };
        names.push(format!("{asset_title} [{band_name}]"));
    }

    Ok(names)
}

fn band_names_from_item_asset_v1_1_0(asset: &stac::ItemAsset) -> anyhow::Result<Vec<String>> {
    let asset_title = asset
        .title
        .as_deref()
        .ok_or(anyhow::anyhow!("Missing title in asset metadata"))?;

    let band_names = asset
        .additional_fields
        .get("bands")
        .and_then(serde_json::Value::as_array);

    let Some(bands) = band_names else {
        return Ok(vec![asset_title.to_string()]);
    };

    if bands.is_empty() {
        return Ok(vec![asset_title.to_string()]);
    }

    if bands.len() == 1 {
        return Ok(vec![asset_title.to_string()]);
    }

    let mut names = Vec::new();
    for band in bands {
        let band_name = band
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or(anyhow::anyhow!("Band is missing name for multi-band asset"))?;
        names.push(format!("{asset_title} [{band_name}]"));
    }

    Ok(names)
}

fn geo_transform_from_fields(
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Option<GeoTransform> {
    let proj_transform = fields.get("proj:transform")?;
    let proj_transform_array = proj_transform.as_array()?;
    let proj_transform_values: Vec<f64> = proj_transform_array
        .iter()
        .filter_map(serde_json::Value::as_f64)
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

#[allow(clippy::needless_pass_by_value)]
fn data_type_from_asset(
    stac_version: stac::Version,
    asset: &Asset,
) -> anyhow::Result<RasterDataType> {
    match stac_version {
        stac::Version::v1_0_0 => {
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
        stac::Version::v1_1_0 => {
            let data_type = asset
                .data_type
                .as_ref()
                .ok_or(anyhow::anyhow!("Missing data_type in asset"))?;

            raster_data_type_from_stac_data_type(data_type)
        }
        _ => {
            anyhow::bail!("Unsupported STAC version: {stac_version}");
        }
    }
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
                "Unsupported raster data type: {data_type_str}",
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
                "Unsupported raster data type: {data_type:?}",
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
    let response = retry_with_backoff(
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

/// Helper function to retry async operations with exponential backoff
async fn retry_with_backoff<F, Fut, T, E>(mut operation: F, operation_name: &str) -> Result<T, E>
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
                    println!("[ERROR] {operation_name} failed after {MAX_RETRIES} attempts: {err}",);
                    return Err(err);
                }
                let delay = Duration::from_millis(INITIAL_RETRY_DELAY_MS * 2_u64.pow(attempt - 1));
                println!(
                    "[WARN] {operation_name} failed (attempt {attempt}/{MAX_RETRIES}): {err}. Retrying in {delay:?}...",
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]

enum StacExtensionMajorVersion {
    V1,
    V2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ImportFileType {
    Cog,
    Jp2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StacExtensionVersions {
    projection: StacExtensionMajorVersion,
    raster: StacExtensionMajorVersion,
    eo: StacExtensionMajorVersion,
}

fn parse_stac_extension_versions(extensions: &[String]) -> anyhow::Result<StacExtensionVersions> {
    let mut projection = None;
    let mut raster = None;
    let mut eo = None;

    for extension in ["projection", "raster", "eo"] {
        if let Some(ext_str) = extensions
            .iter()
            .find(|ext| ext.starts_with(&format!("https://stac-extensions.github.io/{extension}")))
        {
            let version =
                stac_extension_version_from_str(ext_str, extension).with_context(|| {
                    format!("Failed to parse version for {extension} extension {ext_str}")
                })?;
            match extension {
                "projection" => projection = Some(version),
                "raster" => raster = Some(version),
                "eo" => eo = Some(version),
                _ => unreachable!(),
            }
        }
    }

    Ok(StacExtensionVersions {
        projection: projection.ok_or(anyhow::anyhow!("Missing projection extension"))?,
        raster: raster.ok_or(anyhow::anyhow!("Missing raster extension"))?,
        eo: eo.ok_or(anyhow::anyhow!("Missing eo extension"))?,
    })
}

fn infer_collection_extension_versions(
    collection: &stac::Collection,
) -> anyhow::Result<StacExtensionVersions> {
    match collection.version {
        stac::Version::v1_0_0 => {
            let has_projection = collection.item_assets.values().any(|asset| {
                asset.additional_fields.contains_key("proj:transform")
                    || asset.additional_fields.contains_key("proj:shape")
                    || asset.additional_fields.contains_key("proj:epsg")
            });
            let has_raster = collection
                .item_assets
                .values()
                .any(|asset| asset.additional_fields.contains_key("raster:bands"));
            let has_eo = collection
                .item_assets
                .values()
                .any(|asset| asset.additional_fields.contains_key("eo:bands"));

            Ok(StacExtensionVersions {
                projection: has_projection
                    .then_some(StacExtensionMajorVersion::V1)
                    .ok_or(anyhow::anyhow!("Missing projection extension"))?,
                raster: has_raster
                    .then_some(StacExtensionMajorVersion::V1)
                    .ok_or(anyhow::anyhow!("Missing raster extension"))?,
                eo: has_eo
                    .then_some(StacExtensionMajorVersion::V1)
                    .ok_or(anyhow::anyhow!("Missing eo extension"))?,
            })
        }
        stac::Version::v1_1_0 => {
            // In STAC 1.1.0, projection extension is v2.0.0,
            // raster:bands/eo:bands are merged into common metadata `bands`,
            // and `data_type`/`nodata`/`gsd` are common metadata fields.
            //
            // Projection/raster/eo info is often only present at the item
            // level, not in collection-level item_assets. The collection
            // scan functions don't use the extension versions for data
            // extraction — they only need to know which scan function to
            // call. So we default all to V2 for any STAC 1.1.0 collection.
            Ok(StacExtensionVersions {
                projection: StacExtensionMajorVersion::V2,
                raster: StacExtensionMajorVersion::V2,
                eo: StacExtensionMajorVersion::V2,
            })
        }
        _ => {
            anyhow::bail!(
                "Cannot infer extension versions for STAC {}",
                collection.version
            );
        }
    }
}

impl TryFrom<&stac::Collection> for StacExtensionVersions {
    type Error = anyhow::Error;

    fn try_from(collection: &stac::Collection) -> Result<Self, Self::Error> {
        parse_stac_extension_versions(&collection.extensions)
            .or_else(|_| infer_collection_extension_versions(collection))
    }
}

impl TryFrom<&stac::Item> for StacExtensionVersions {
    type Error = anyhow::Error;

    fn try_from(item: &stac::Item) -> Result<Self, Self::Error> {
        parse_stac_extension_versions(&item.extensions).or_else(|_| {
            // For STAC 1.1.0 items (or newer), the raster/eo extensions may not
            // be listed because they're common metadata. Default to V2 for all
            // extensions since the scan functions handle the actual field checks.
            if matches!(
                item.version,
                stac::Version::v1_1_0 | stac::Version::Unknown(_)
            ) && !item.assets.is_empty()
            {
                return Ok(StacExtensionVersions {
                    projection: StacExtensionMajorVersion::V2,
                    raster: StacExtensionMajorVersion::V2,
                    eo: StacExtensionMajorVersion::V2,
                });
            }
            Err(anyhow::anyhow!("Missing raster extension"))
        })
    }
}

fn stac_extension_version_from_str(
    extension_str: &str,
    extension_name: &str,
) -> anyhow::Result<StacExtensionMajorVersion> {
    let version_str = extension_str
        .strip_prefix(&format!(
            "https://stac-extensions.github.io/{extension_name}/v"
        ))
        .and_then(|rem| rem.strip_suffix("/schema.json"))
        .ok_or_else(|| anyhow::anyhow!("Unknown version for extension {extension_name}"))?;

    match version_str.split('.').next() {
        Some("1") => Ok(StacExtensionMajorVersion::V1),
        Some("2") => Ok(StacExtensionMajorVersion::V2),
        _ => Err(anyhow::anyhow!(
            "Unknown version '{version_str}' for extension {extension_name}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httptest::{
        Expectation, Server, all_of,
        matchers::{self, request},
        responders,
    };
    use std::path::PathBuf;

    fn stac_params(stac_url: String, geo_engine_url: String) -> StacImport {
        StacImport {
            stac_url,
            stac_collection: "sentinel-2-l2a".to_string(),
            limit: Some(10),
            time_start: Some("2026-01-01T00:00:00Z".to_string()),
            time_end: Some("2026-01-31T23:59:59Z".to_string()),
            bbox: Some(vec![8.766, 50.802, 8.767, 50.803]),
            geo_engine_url,
            geo_engine_email: "admin@localhost".to_string(),
            geo_engine_password: "adminadmin".to_string(),
            volume_name: "geodata-test".to_string(),
            dataset_name_prefix: "Sentinel2".to_string(),
            verbose: false,
            prefetch_pages: 1,
            epsgs: vec![],
            z_index_property_name: Some("updated".to_string()),
            s3_endpoint: None,
            s3_access_key: None,
            s3_secret_key: None,
            file_types: vec![ImportFileType::Cog],
            filter_item_fields: false,
            no_data_value: None,
            gdal_retries: None,
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_element84_stac_v1_0_0_creates_expected_dataset_and_tiles() {
        let stac_server = Server::run();
        let geo_server = Server::run();

        stac_server.expect(
            Expectation::matching(request::method_path("GET", "/collections/sentinel-2-l2a"))
                .respond_with(responders::json_encoded(
                    serde_json::from_str::<serde_json::Value>(include_str!(
                        "../../../test_data/stac_responses/collections/element84.json"
                    ))
                    .expect("valid element84 collection fixture"),
                )),
        );

        stac_server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path("/collections/sentinel-2-l2a/items"),
                request::query(matchers::url_decoded(matchers::contains((
                    "bbox",
                    "8.766,50.802,8.767,50.803"
                )))),
                request::query(matchers::url_decoded(matchers::contains((
                    "datetime",
                    "2026-01-01T00:00:00Z/2026-01-31T23:59:59Z"
                )))),
                request::query(matchers::url_decoded(matchers::contains(("limit", "10")))),
            ])
            .respond_with(responders::json_encoded(
                serde_json::from_str::<serde_json::Value>(include_str!(
                    "../../../test_data/stac_responses/items/element84-marburg-minimal.json"
                ))
                .expect("valid element84 items fixture"),
            )),
        );

        geo_server.expect(
            Expectation::matching(request::method_path("POST", "/api/login")).respond_with(
                responders::json_encoded(serde_json::json!({ "id": "test-session" })),
            ),
        );

        geo_server.expect(
            Expectation::matching(request::method_path(
                "GET",
                "/api/dataset/sentinel-2-l2a_EPSG32632_U16_10",
            ))
            .respond_with(
                responders::status_code(400)
                    .append_header("Content-Type", "application/json")
                    .body(
                        serde_json::json!({
                            "error": "CannotLoadDataset",
                            "message": "Dataset not found"
                        })
                        .to_string(),
                    ),
            ),
        );

        geo_server.expect(
            Expectation::matching(request::method_path("POST", "/api/dataset")).respond_with(
                responders::json_encoded(serde_json::json!({
                    "datasetName": "sentinel-2-l2a_EPSG32632_U16_10"
                })),
            ),
        );

        geo_server.expect(
            Expectation::matching(request::method_path("PUT", "/api/permissions"))
                .times(2)
                .respond_with(responders::status_code(200)),
        );

        let mut importer = StacImporter::new(stac_params(
            stac_server.url_str("").trim_end_matches('/').to_string(),
            geo_server.url_str("/api").trim_end_matches('/').to_string(),
        ))
        .await
        .expect("importer should initialize");

        let query_state = QueryState::FirstPage {
            query_url: format!(
                "{}/collections/{}/items",
                importer.params.stac_url, importer.params.stac_collection
            ),
            query_params: create_query_params(&importer.params),
        };

        let (item_collection, query_state) = query_item_collection(&importer.client, &query_state)
            .await
            .expect("items should be fetched");
        assert!(matches!(query_state, QueryState::Finished));

        let dataset_tiles = importer
            .process_items(item_collection)
            .await
            .expect("item processing should succeed");

        assert_eq!(dataset_tiles.len(), 1);

        let partial_key = PartialDatasetKey {
            data_type: RasterDataType::U16,
            resolution: OrderedFloat(10.),
        };

        let expected_band_idx = importer
            .bands
            .get(&partial_key)
            .expect("10m U16 dataset should be present")
            .iter()
            .position(|band| band.name == "blue")
            .expect("blue band should exist") as u32;

        let (dataset_key, tiles) = dataset_tiles
            .iter()
            .next()
            .expect("exactly one dataset key should be present");

        assert_eq!(dataset_key.epsg, 32632);
        assert_eq!(dataset_key.data_type, RasterDataType::U16);
        assert_eq!(dataset_key.resolution, OrderedFloat(10.));
        assert_eq!(tiles.len(), 1);

        let tile = &tiles[0];
        assert_eq!(tile.band, expected_band_idx);
        assert_eq!(tile.params.rasterband_channel, 1);
        assert_eq!(tile.params.width, 10_980);
        assert_eq!(tile.params.height, 10_980);
        assert_eq!(
            tile.params.file_path,
            PathBuf::from(
                "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/32/U/MB/2026/1/S2B_32UMB_20260128_0_L2A/B02.tif"
            )
        );

        let expected_z_index = chrono::DateTime::parse_from_rfc3339("2026-01-29T21:55:06.896Z")
            .expect("valid updated datetime")
            .timestamp_millis();
        assert_eq!(tile.z_index, expected_z_index);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_polsa_stac_v1_1_0_creates_expected_dataset_and_tiles() {
        let stac_server = Server::run();
        let geo_server = Server::run();

        stac_server.expect(
            Expectation::matching(request::method_path("GET", "/collections/sentinel-2-l2a"))
                .respond_with(responders::json_encoded(
                    serde_json::from_str::<serde_json::Value>(include_str!(
                        "../../../test_data/stac_responses/collections/code-de.json"
                    ))
                    .expect("valid code-de collection fixture"),
                )),
        );

        stac_server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path("/collections/sentinel-2-l2a/items"),
                request::query(matchers::url_decoded(matchers::contains((
                    "bbox",
                    "8.766,50.802,8.767,50.803"
                )))),
                request::query(matchers::url_decoded(matchers::contains((
                    "datetime",
                    "2026-01-01T00:00:00Z/2026-01-31T23:59:59Z"
                )))),
                request::query(matchers::url_decoded(matchers::contains(("limit", "10")))),
            ])
            .respond_with(responders::json_encoded(
                serde_json::from_str::<serde_json::Value>(include_str!(
                    "../../../test_data/stac_responses/items/polsa-marburg-minimal.json"
                ))
                .expect("valid polsa items fixture"),
            )),
        );

        geo_server.expect(
            Expectation::matching(request::method_path("POST", "/api/login")).respond_with(
                responders::json_encoded(serde_json::json!({ "id": "test-session" })),
            ),
        );

        geo_server.expect(
            Expectation::matching(request::method_path(
                "GET",
                "/api/dataset/sentinel-2-l2a_EPSG32632_U16_10",
            ))
            .respond_with(
                responders::status_code(400)
                    .append_header("Content-Type", "application/json")
                    .body(
                        serde_json::json!({
                            "error": "CannotLoadDataset",
                            "message": "Dataset not found"
                        })
                        .to_string(),
                    ),
            ),
        );

        geo_server.expect(
            Expectation::matching(request::method_path("POST", "/api/dataset")).respond_with(
                responders::json_encoded(serde_json::json!({
                    "datasetName": "sentinel-2-l2a_EPSG32632_U16_10"
                })),
            ),
        );

        geo_server.expect(
            Expectation::matching(request::method_path("PUT", "/api/permissions"))
                .times(2)
                .respond_with(responders::status_code(200)),
        );

        let mut params = stac_params(
            stac_server.url_str("").trim_end_matches('/').to_string(),
            geo_server.url_str("/api").trim_end_matches('/').to_string(),
        );
        params.s3_endpoint = Some("localhost:9000".to_string());
        params.s3_access_key = Some("mock-access-key".to_string());
        params.s3_secret_key = Some("mock-secret-key".to_string());
        params.file_types = vec![ImportFileType::Jp2];

        let mut importer = StacImporter::new(params)
            .await
            .expect("importer should initialize");

        let query_state = QueryState::FirstPage {
            query_url: format!(
                "{}/collections/{}/items",
                importer.params.stac_url, importer.params.stac_collection
            ),
            query_params: create_query_params(&importer.params),
        };

        let (item_collection, query_state) = query_item_collection(&importer.client, &query_state)
            .await
            .expect("items should be fetched");
        assert!(matches!(query_state, QueryState::Finished));

        let dataset_tiles = importer
            .process_items(item_collection)
            .await
            .expect("item processing should succeed");

        assert_eq!(dataset_tiles.len(), 1);

        let partial_key = PartialDatasetKey {
            data_type: RasterDataType::U16,
            resolution: OrderedFloat(10.),
        };

        let expected_band_idx = importer
            .bands
            .get(&partial_key)
            .expect("10m U16 dataset should be present")
            .iter()
            .position(|band| band.name == "Blue (band 2) - 10m")
            .expect("blue band should exist") as u32;

        let (dataset_key, tiles) = dataset_tiles
            .iter()
            .next()
            .expect("exactly one dataset key should be present");

        assert_eq!(dataset_key.epsg, 32632);
        assert_eq!(dataset_key.data_type, RasterDataType::U16);
        assert_eq!(dataset_key.resolution, OrderedFloat(10.));
        assert_eq!(tiles.len(), 1);

        let tile = &tiles[0];
        assert_eq!(tile.band, expected_band_idx);
        assert_eq!(tile.params.rasterband_channel, 1);
        assert_eq!(tile.params.width, 10_980);
        assert_eq!(tile.params.height, 10_980);
        assert_eq!(
            tile.params.file_path,
            PathBuf::from("/vsis3/eodata/Sentinel-2/MSI/L2A/2026/01/28/example/B02_10m.jp2")
        );

        let expected_z_index = chrono::DateTime::parse_from_rfc3339("2026-01-29T14:19:30Z")
            .expect("valid updated datetime")
            .timestamp_millis();
        assert_eq!(tile.z_index, expected_z_index);

        let gdal_options = tile
            .params
            .gdal_config_options
            .as_ref()
            .expect("s3 tiles should include gdal config options");

        assert!(gdal_options.iter().any(|opt| {
            let (key, value): (String, String) = opt.clone().into();
            key == "AWS_S3_ENDPOINT" && value == "localhost:9000"
        }));
        assert!(gdal_options.iter().any(|opt| {
            let (key, value): (String, String) = opt.clone().into();
            key == "AWS_ACCESS_KEY_ID" && value == "mock-access-key"
        }));
        assert!(gdal_options.iter().any(|opt| {
            let (key, value): (String, String) = opt.clone().into();
            key == "AWS_SECRET_ACCESS_KEY" && value == "mock-secret-key"
        }));
    }
}
