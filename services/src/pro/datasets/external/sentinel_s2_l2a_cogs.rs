use crate::contexts::GeoEngineDb;
use crate::datasets::listing::ProvenanceOutput;
use crate::error::{self, Error, Result};
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerListing,
    ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{
    LayerCollectionId, LayerCollectionProvider, ProviderCapabilities, SearchCapabilities,
};
use crate::projects::{RasterSymbology, Symbology};
use crate::stac::{Feature as StacFeature, FeatureCollection as StacCollection, StacAsset};
use crate::util::operators::source_operator_from_dataset;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataId, DataProviderId, LayerId, NamedData};
use geoengine_datatypes::operations::image::{RasterColorizer, RgbaColor};
use geoengine_datatypes::operations::reproject::{
    CoordinateProjection, CoordinateProjector, ReprojectClipped,
};
use geoengine_datatypes::primitives::CacheTtlSeconds;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, DateTime, Duration, RasterQueryRectangle,
    SpatialPartitioned, TimeInstance, TimeInterval, VectorQueryRectangle,
};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceAuthority};
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, OperatorName, RasterBandDescriptors, RasterOperator,
    RasterResultDescriptor, TypedOperator, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    GdalDatasetGeoTransform, GdalDatasetParameters, GdalLoadingInfo, GdalLoadingInfoTemporalSlice,
    GdalLoadingInfoTemporalSliceIterator, GdalRetryOptions, GdalSource, GdalSourceParameters,
    OgrSourceDataset,
};
use geoengine_operators::util::retry::retry;
use log::debug;
use postgres_types::{FromSql, ToSql};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::Debug;
use std::path::PathBuf;

static STAC_RETRY_MAX_BACKOFF_MS: u64 = 60 * 60 * 1000;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct SentinelS2L2ACogsProviderDefinition {
    pub name: String,
    pub id: DataProviderId,
    pub description: String,
    pub priority: Option<i16>,
    pub api_url: String,
    pub bands: Vec<StacBand>,
    pub zones: Vec<StacZone>,
    #[serde(default)]
    pub stac_api_retries: StacApiRetries,
    #[serde(default)]
    pub gdal_retries: GdalRetries,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
    #[serde(default)]
    pub query_buffer: StacQueryBuffer,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
/// A struct that represents buffers to apply to stac requests
pub struct StacQueryBuffer {
    pub start_seconds: i64,
    pub end_seconds: i64,
    // TODO: add also spatial buffers?
}

impl Default for StacQueryBuffer {
    fn default() -> Self {
        Self {
            start_seconds: 60,
            end_seconds: 60,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StacApiRetries {
    pub number_of_retries: usize,
    pub initial_delay_ms: u64,
    pub exponential_backoff_factor: f64,
}

impl Default for StacApiRetries {
    // TODO: find good defaults
    fn default() -> Self {
        Self {
            number_of_retries: 3,
            initial_delay_ms: 125,
            exponential_backoff_factor: 2.,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GdalRetries {
    /// retry at most `number_of_retries` times with exponential backoff
    pub number_of_retries: usize,
}

impl Default for GdalRetries {
    fn default() -> Self {
        Self {
            number_of_retries: 10,
        }
    }
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for SentinelS2L2ACogsProviderDefinition {
    async fn initialize(self: Box<Self>, _db: D) -> crate::error::Result<Box<dyn DataProvider>> {
        Ok(Box::new(SentinelS2L2aCogsDataProvider::new(
            self.id,
            self.name,
            self.description,
            self.api_url,
            &self.bands,
            &self.zones,
            self.stac_api_retries,
            self.gdal_retries,
            self.cache_ttl,
            self.query_buffer,
        )))
    }

    fn type_name(&self) -> &'static str {
        "SentinelS2L2ACogs"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> geoengine_datatypes::dataset::DataProviderId {
        self.id
    }

    fn priority(&self) -> i16 {
        self.priority.unwrap_or(0)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct StacBand {
    pub name: String,
    pub no_data_value: Option<f64>,
    pub data_type: RasterDataType,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, FromSql, ToSql)]
pub struct StacZone {
    pub name: String,
    pub epsg: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SentinelDataset {
    band: StacBand,
    zone: StacZone,
    listing: Layer,
}

#[derive(Debug)]
pub struct SentinelS2L2aCogsDataProvider {
    id: DataProviderId,

    name: String,
    description: String,

    api_url: String,

    datasets: HashMap<LayerId, SentinelDataset>,

    stac_api_retries: StacApiRetries,
    gdal_retries: GdalRetries,

    cache_ttl: CacheTtlSeconds,

    query_buffer: StacQueryBuffer,
}

impl SentinelS2L2aCogsDataProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: DataProviderId,
        name: String,
        description: String,
        api_url: String,
        bands: &[StacBand],
        zones: &[StacZone],
        stac_api_retries: StacApiRetries,
        gdal_retries: GdalRetries,
        cache_ttl: CacheTtlSeconds,
        query_buffer: StacQueryBuffer,
    ) -> Self {
        Self {
            id,
            name,
            description,
            api_url,
            datasets: Self::create_datasets(&id, bands, zones),
            stac_api_retries,
            gdal_retries,
            cache_ttl,
            query_buffer,
        }
    }

    fn create_datasets(
        id: &DataProviderId,
        bands: &[StacBand],
        zones: &[StacZone],
    ) -> HashMap<LayerId, SentinelDataset> {
        zones
            .iter()
            .flat_map(|zone| {
                bands.iter().map(move |band| {
                    let layer_id = LayerId(format!("{}:{}", zone.name, band.name));
                    let listing = Layer {
                        id: ProviderLayerId {
                            provider_id: *id,
                            layer_id: layer_id.clone(),
                        },
                        name: format!("Sentinel S2 L2A COGS {}:{}", zone.name, band.name),
                        description: String::new(),
                        workflow: Workflow {
                            operator: source_operator_from_dataset(
                                GdalSource::TYPE_NAME,
                                &NamedData {
                                    namespace: None,
                                    provider: Some(id.to_string()),
                                    name: layer_id.to_string(),
                                },
                            )
                            .expect("Gdal source is a valid operator."),
                        },
                        symbology: Some(Symbology::Raster(RasterSymbology {
                            opacity: 1.0,
                            raster_colorizer: RasterColorizer::SingleBand {
                                band: 0, band_colorizer:
                                geoengine_datatypes::operations::image::Colorizer::linear_gradient(
                                    vec![
                                        (0.0, RgbaColor::white())
                                            .try_into()
                                            .expect("valid breakpoint"),
                                        (10_000.0, RgbaColor::black())
                                            .try_into()
                                            .expect("valid breakpoint"),
                                    ],
                                    RgbaColor::transparent(),
                                    RgbaColor::white(),
                                    RgbaColor::black(),
                                )
                                .expect("valid colorizer"),
                        }})), // TODO: individual colorizer per band
                        properties: vec![],
                        metadata: HashMap::new(),
                    };

                    let dataset = SentinelDataset {
                        zone: zone.clone(),
                        band: band.clone(),
                        listing,
                    };

                    (layer_id, dataset)
                })
            })
            .collect()
    }
}

#[async_trait]
impl DataProvider for SentinelS2L2aCogsDataProvider {
    async fn provenance(&self, id: &DataId) -> Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            data: id.clone(),
            provenance: None, // TODO
        })
    }
}

#[async_trait]
impl LayerCollectionProvider for SentinelS2L2aCogsDataProvider {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            listing: true,
            search: SearchCapabilities::none(),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    async fn load_layer_collection(
        &self,
        collection: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        ensure!(
            *collection == self.get_root_layer_collection_id().await?,
            error::UnknownLayerCollectionId {
                id: collection.clone()
            }
        );

        let mut items = self
            .datasets
            .values()
            .map(|d| {
                Ok(CollectionItem::Layer(LayerListing {
                    id: d.listing.id.clone(),
                    name: d.listing.name.clone(),
                    description: d.listing.description.clone(),
                    properties: vec![],
                }))
            })
            .collect::<Result<Vec<CollectionItem>>>()?;
        items.sort_by_key(|e| e.name().to_string());

        let items = items
            .into_iter()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .collect();

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: self.id,
                collection_id: collection.clone(),
            },
            name: "Element 84 AWS STAC".to_owned(),
            description: "SentinelS2L2ACogs".to_owned(),
            items,
            entry_label: None,
            properties: vec![],
        })
    }

    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId("SentinelS2L2ACogs".to_owned()))
    }

    async fn load_layer(&self, id: &LayerId) -> Result<Layer> {
        let dataset = self.datasets.get(id).ok_or(Error::UnknownDataId)?;

        Ok(Layer {
            id: ProviderLayerId {
                provider_id: self.id,
                layer_id: id.clone(),
            },
            name: dataset.listing.name.clone(),
            description: dataset.listing.description.clone(),
            workflow: Workflow {
                operator: TypedOperator::Raster(
                    GdalSource {
                        params: GdalSourceParameters {
                            data: NamedData {
                                namespace: None,
                                provider: Some(self.id.to_string()),
                                name: id.to_string(),
                            },
                        },
                    }
                    .boxed(),
                ),
            },
            symbology: dataset.listing.symbology.clone(),
            properties: vec![],
            metadata: HashMap::new(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct SentinelS2L2aCogsMetaData {
    api_url: String,
    zone: StacZone,
    band: StacBand,
    stac_api_retries: StacApiRetries,
    gdal_retries: GdalRetries,
    cache_ttl: CacheTtlSeconds,
    stac_query_buffer: StacQueryBuffer,
}

impl SentinelS2L2aCogsMetaData {
    #[allow(clippy::too_many_lines)]
    async fn create_loading_info(&self, query: RasterQueryRectangle) -> Result<GdalLoadingInfo> {
        // for reference: https://stacspec.org/STAC-ext-api.html#operation/getSearchSTAC
        debug!("create_loading_info with: {:?}", &query);
        let request_params = self.request_params(&query)?;

        let query_start_buffer = Duration::seconds(self.stac_query_buffer.start_seconds);
        let query_end_buffer = Duration::seconds(self.stac_query_buffer.end_seconds);

        if request_params.is_none() {
            log::debug!("Request params are empty -> returning empty loading info");
            return Ok(GdalLoadingInfo::new(
                // we do not know anything about the data. Can only use query -/+ buffer to determine time bounds.
                GdalLoadingInfoTemporalSliceIterator::Static {
                    parts: vec![].into_iter(),
                },
                query.time_interval.start() - query_start_buffer,
                query.time_interval.end() + query_end_buffer,
            ));
        }

        let request_params = request_params.expect("The none case was checked above");

        debug!("queried with: {:?}", &request_params);
        let features = self.load_all_features(&request_params).await?;
        debug!("number of features returned by STAC: {}", features.len());
        let mut features: Vec<StacFeature> = features
            .into_iter()
            .filter(|f| {
                f.properties
                    .proj_epsg
                    .map_or(false, |epsg| epsg == self.zone.epsg)
            })
            .collect();

        features.sort_by_key(|a| a.properties.datetime);

        let start_times_pre: Vec<TimeInstance> = features
            .iter()
            .map(|f| TimeInstance::from(f.properties.datetime))
            .collect();
        let start_times = Self::make_unique_start_times_from_sorted_features(&start_times_pre);

        let mut parts: Vec<GdalLoadingInfoTemporalSlice> = vec![];
        let num_features = features.len();
        let mut known_time_start: Option<TimeInstance> = None;
        let mut known_time_end: Option<TimeInstance> = None;
        debug!("number of features in current zone: {}", num_features);
        for i in 0..num_features {
            let feature = &features[i];

            let start = start_times[i];

            // feature is valid until next feature starts
            let end = if i < num_features - 1 {
                start_times[i + 1]
            } else {
                // (or end of query?)
                query.time_interval.end() + query_end_buffer
            };

            let time_interval = TimeInterval::new(start, end)?;

            if time_interval.start() <= query.time_interval.start() {
                let t = if time_interval.end() > query.time_interval.start() {
                    time_interval.start()
                } else {
                    time_interval.end()
                };
                known_time_start = known_time_start.map(|old| old.max(t)).or(Some(t));
            }

            if time_interval.end() >= query.time_interval.end() {
                let t = if time_interval.start() < query.time_interval.end() {
                    time_interval.end()
                } else {
                    time_interval.start()
                };
                known_time_end = known_time_end.map(|old| old.min(t)).or(Some(t));
            }

            if time_interval.intersects(&query.time_interval) {
                debug!(
                    "STAC asset time: {}, url: {}",
                    time_interval,
                    feature
                        .assets
                        .get(&self.band.name)
                        .map_or(&"n/a".to_string(), |a| &a.href)
                );

                let asset =
                    feature
                        .assets
                        .get(&self.band.name)
                        .ok_or(error::Error::StacNoSuchBand {
                            band_name: self.band.name.clone(),
                        })?;

                parts.push(self.create_loading_info_part(time_interval, asset, self.cache_ttl)?);
            }
        }
        debug!("number of generated loading infos: {}", parts.len());

        // if there is no information of time outside the query, we fallback to the only information we know: query -/+ buffer.
        let known_time_before =
            known_time_start.unwrap_or(query.time_interval.start() - query_start_buffer);

        let known_time_after =
            known_time_end.unwrap_or(query.time_interval.end() + query_end_buffer);

        Ok(GdalLoadingInfo::new(
            GdalLoadingInfoTemporalSliceIterator::Static {
                parts: parts.into_iter(),
            },
            known_time_before,
            known_time_after,
        ))
    }

    fn make_unique_start_times_from_sorted_features(
        start_times: &[TimeInstance],
    ) -> Vec<TimeInstance> {
        let mut unique_start_times: Vec<TimeInstance> = Vec::with_capacity(start_times.len());
        for (i, &t_start) in start_times.iter().enumerate() {
            let real_start = if i == 0 {
                t_start
            } else {
                let prev_start = start_times[i - 1];
                if t_start == prev_start {
                    let prev_u_start = unique_start_times[i - 1];
                    let new_u_start = prev_u_start + 1;
                    log::debug!(
                        "duplicate start time: {} insert as {} following {}",
                        t_start.as_datetime_string(),
                        new_u_start.as_datetime_string(),
                        prev_u_start.as_datetime_string()
                    );
                    new_u_start
                } else {
                    t_start
                }
            };

            unique_start_times.push(real_start);
        }
        unique_start_times
    }

    fn create_loading_info_part(
        &self,
        time_interval: TimeInterval,
        asset: &StacAsset,
        cache_ttl: CacheTtlSeconds,
    ) -> Result<GdalLoadingInfoTemporalSlice> {
        let [stac_shape_y, stac_shape_x] = asset.proj_shape.ok_or(error::Error::StacInvalidBbox)?;

        Ok(GdalLoadingInfoTemporalSlice {
            time: time_interval,
            params: Some(GdalDatasetParameters {
                file_path: PathBuf::from(format!("/vsicurl/{}", asset.href)),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform::from(
                    asset
                        .gdal_geotransform()
                        .ok_or(error::Error::StacInvalidGeoTransform)?,
                ),
                width: stac_shape_x as usize,
                height: stac_shape_y as usize,
                file_not_found_handling: geoengine_operators::source::FileNotFoundHandling::NoData,
                no_data_value: self.band.no_data_value,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: Some(vec![
                    // only read the tif file and no aux files, etc.
                    (
                        "CPL_VSIL_CURL_ALLOWED_EXTENSIONS".to_string(),
                        ".tif".to_string(),
                    ),
                    // do not perform a directory scan on the AWS bucket
                    (
                        "GDAL_DISABLE_READDIR_ON_OPEN".to_string(),
                        "EMPTY_DIR".to_string(),
                    ),
                    // do not try to read credentials from home directory
                    ("GDAL_HTTP_NETRC".to_string(), "NO".to_string()),
                    // disable Gdal's retry because geo engine does its own retry
                    ("GDAL_HTTP_MAX_RETRY".to_string(), "0".to_string()),
                ]),
                allow_alphaband_as_mask: true,
                retry: Some(GdalRetryOptions {
                    max_retries: self.gdal_retries.number_of_retries,
                }),
            }),
            cache_ttl,
        })
    }

    fn request_params(
        &self,
        query: &RasterQueryRectangle,
    ) -> Result<Option<Vec<(String, String)>>> {
        let (t_start, t_end) = Self::time_range_request(&query.time_interval)?;

        let t_start = t_start - Duration::seconds(self.stac_query_buffer.start_seconds);
        let t_end = t_end + Duration::seconds(self.stac_query_buffer.end_seconds);

        // request all features in zone in order to be able to determine the temporal validity of individual tile
        let projector = CoordinateProjector::from_known_srs(
            SpatialReference::new(SpatialReferenceAuthority::Epsg, self.zone.epsg),
            SpatialReference::epsg_4326(),
        )?;

        let spatial_partition = query.spatial_partition(); // TODO: use SpatialPartition2D directly
        let bbox = BoundingBox2D::new_upper_left_lower_right_unchecked(
            spatial_partition.upper_left(),
            spatial_partition.lower_right(),
        );
        let bbox = bbox.reproject_clipped(&projector)?; // TODO: use reproject_clipped on SpatialPartition2D

        Ok(bbox.map(|bbox| {
            vec![
                (
                    "collections[]".to_owned(),
                    "sentinel-s2-l2a-cogs".to_owned(),
                ),
                (
                    "bbox".to_owned(),
                    format!(
                        "[{},{},{},{}]", // array-brackets are not used in standard but required here for unknkown reason
                        bbox.lower_left().x,
                        bbox.lower_left().y,
                        bbox.upper_right().x,
                        bbox.upper_right().y
                    ),
                ), // TODO: order coordinates depending on projection
                (
                    "datetime".to_owned(),
                    format!(
                        "{}/{}",
                        t_start.to_datetime_string(),
                        t_end.to_datetime_string()
                    ),
                ),
                ("limit".to_owned(), "500".to_owned()),
            ]
        }))
    }

    async fn load_all_features<T: Serialize + ?Sized + Debug>(
        &self,
        params: &T,
    ) -> Result<Vec<StacFeature>> {
        let mut features = vec![];

        let mut collection = self.load_collection(params, 1).await?;
        features.append(&mut collection.features);

        let num_pages =
            (collection.context.matched as f64 / collection.context.limit as f64).ceil() as u32;

        for page in 2..=num_pages {
            let mut collection = self.load_collection(params, page).await?;
            features.append(&mut collection.features);
        }

        Ok(features)
    }

    async fn load_collection<T: Serialize + ?Sized + Debug>(
        &self,
        params: &T,
        page: u32,
    ) -> Result<StacCollection> {
        let client = Client::builder().build()?;

        retry(
            self.stac_api_retries.number_of_retries,
            self.stac_api_retries.initial_delay_ms,
            self.stac_api_retries.exponential_backoff_factor,
            Some(STAC_RETRY_MAX_BACKOFF_MS),
            || async {
                let text = client
                    .get(&self.api_url)
                    .query(&params)
                    .query(&[("page", &page.to_string())])
                    .send()
                    .await
                    .context(error::Reqwest)?
                    .text()
                    .await
                    .context(error::Reqwest)?;

                serde_json::from_str::<StacCollection>(&text).map_err(|error| {
                    error::Error::StacJsonResponse {
                        url: self.api_url.clone(),
                        response: text,
                        error,
                    }
                })
            },
        )
        .await
    }

    fn time_range_request(time: &TimeInterval) -> Result<(DateTime, DateTime)> {
        let t_start =
            time.start()
                .as_date_time()
                .ok_or(geoengine_operators::error::Error::DataType {
                    source: geoengine_datatypes::error::Error::NoDateTimeValid {
                        time_instance: time.start(),
                    },
                })?;

        let t_end =
            time.end()
                .as_date_time()
                .ok_or(geoengine_operators::error::Error::DataType {
                    source: geoengine_datatypes::error::Error::NoDateTimeValid {
                        time_instance: time.end(),
                    },
                })?;

        Ok((t_start, t_end))
    }
}

#[async_trait]
impl MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for SentinelS2L2aCogsMetaData
{
    async fn loading_info(
        &self,
        query: RasterQueryRectangle,
    ) -> geoengine_operators::util::Result<GdalLoadingInfo> {
        // TODO: propagate error properly
        debug!("loading_info for: {:?}", &query);
        self.create_loading_info(query).await.map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })
    }

    async fn result_descriptor(&self) -> geoengine_operators::util::Result<RasterResultDescriptor> {
        Ok(RasterResultDescriptor {
            data_type: self.band.data_type,
            spatial_reference: SpatialReference::new(
                SpatialReferenceAuthority::Epsg,
                self.zone.epsg,
            )
            .into(),
            time: None,
            bbox: None,
            resolution: None, // TODO: determine from STAC or data or hardcode it
            bands: RasterBandDescriptors::new_single_band(),
        })
    }

    fn box_clone(
        &self,
    ) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for SentinelS2L2aCogsDataProvider
{
    async fn meta_data(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let id: DataId = id.clone();

        let dataset = self
            .datasets
            .get(
                &id.external()
                    .ok_or(geoengine_operators::error::Error::LoadingInfo {
                        source: Box::new(error::Error::DataIdTypeMissMatch),
                    })?
                    .layer_id,
            )
            .ok_or(geoengine_operators::error::Error::UnknownDataId)?;

        Ok(Box::new(SentinelS2L2aCogsMetaData {
            api_url: self.api_url.clone(),
            zone: dataset.zone.clone(),
            band: dataset.band.clone(),
            stac_api_retries: self.stac_api_retries,
            gdal_retries: self.gdal_retries,
            cache_ttl: self.cache_ttl,
            stac_query_buffer: self.query_buffer,
        }))
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for SentinelS2L2aCogsDataProvider
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for SentinelS2L2aCogsDataProvider
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        contexts::{ApplicationContext, SessionContext},
        layers::storage::{LayerProviderDb, LayerProviderListing, LayerProviderListingOptions},
        pro::{
            contexts::{ProPostgresContext, ProPostgresDb},
            ge_context,
            layers::ProLayerProviderDb,
            users::UserAuth,
            util::tests::admin_login,
        },
        test_data,
    };
    use futures::StreamExt;
    use geoengine_datatypes::{
        dataset::{DatasetId, ExternalDataId},
        primitives::{BandSelection, SpatialPartition2D, SpatialResolution},
        util::{gdal::hide_gdal_errors, test::TestDefault, Identifier},
    };
    use geoengine_operators::{
        engine::{
            ChunkByteSize, MockExecutionContext, MockQueryContext, RasterOperator,
            WorkflowOperatorPath,
        },
        source::{FileNotFoundHandling, GdalMetaDataStatic, GdalSource, GdalSourceParameters},
    };
    use httptest::{
        all_of,
        matchers::{contains, request, url_decoded},
        responders::{self},
        Expectation, Server,
    };
    use std::{fs::File, io::BufReader, str::FromStr};
    use tokio_postgres::NoTls;

    #[ge_context::test]
    async fn loading_info(app_ctx: ProPostgresContext<NoTls>) -> Result<()> {
        // TODO: mock STAC endpoint

        let def: SentinelS2L2ACogsProviderDefinition = serde_json::from_reader(BufReader::new(
            File::open(test_data!("provider_defs/pro/sentinel_s2_l2a_cogs.json"))?,
        ))?;

        let provider = Box::new(def)
            .initialize(
                app_ctx
                    .session_context(app_ctx.create_anonymous_session().await?)
                    .db(),
            )
            .await?;

        let meta: Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> =
            provider
                .meta_data(
                    &ExternalDataId {
                        provider_id: DataProviderId::from_str(
                            "5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5",
                        )?,
                        layer_id: LayerId("UTM32N:B01".to_owned()),
                    }
                    .into(),
                )
                .await
                .unwrap();

        let loading_info = meta
            .loading_info(RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new(
                    (166_021.44, 9_329_005.18).into(),
                    (534_994.66, 0.00).into(),
                )
                .unwrap(),
                time_interval: TimeInterval::new_instant(DateTime::new_utc(2021, 1, 2, 10, 2, 26))?,
                spatial_resolution: SpatialResolution::one(),
                attributes: BandSelection::first(),
            })
            .await
            .unwrap();

        let expected = vec![GdalLoadingInfoTemporalSlice {
            time: TimeInterval::new_unchecked(1_609_581_746_000, 1_609_581_758_000),
            params: Some(GdalDatasetParameters {
                file_path: "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/32/R/PU/2021/1/S2B_32RPU_20210102_0_L2A/B01.tif".into(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: (600_000.0, 3_400_020.0).into(),
                    x_pixel_size: 60.,
                    y_pixel_size: -60.,
                },
                width: 1830,
                height: 1830,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: Some(0.),
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: Some(vec![
                    ("CPL_VSIL_CURL_ALLOWED_EXTENSIONS".to_owned(), ".tif".to_owned()),
                    ("GDAL_DISABLE_READDIR_ON_OPEN".to_owned(), "EMPTY_DIR".to_owned()),
                    ("GDAL_HTTP_NETRC".to_owned(), "NO".to_owned()),
                    ("GDAL_HTTP_MAX_RETRY".to_owned(), "0".to_owned())
                    ]),
                allow_alphaband_as_mask: true,
                retry: Some(GdalRetryOptions { max_retries: 10 }),
            }),
            cache_ttl: CacheTtlSeconds::new(86_400),
        }];

        if let GdalLoadingInfoTemporalSliceIterator::Static { parts } = loading_info.info {
            let result: Vec<_> = parts.collect();

            assert_eq!(result.len(), 1);

            assert_eq!(result, expected);
        } else {
            unreachable!();
        }

        Ok(())
    }

    #[ge_context::test]
    async fn query_data(app_ctx: ProPostgresContext<NoTls>) -> Result<()> {
        // TODO: mock STAC endpoint

        let mut exe = MockExecutionContext::test_default();

        let def: SentinelS2L2ACogsProviderDefinition = serde_json::from_reader(BufReader::new(
            File::open(test_data!("provider_defs/pro/sentinel_s2_l2a_cogs.json"))?,
        ))?;

        let provider = Box::new(def)
            .initialize(
                app_ctx
                    .session_context(app_ctx.create_anonymous_session().await?)
                    .db(),
            )
            .await?;

        let meta: Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> =
            provider
                .meta_data(
                    &ExternalDataId {
                        provider_id: DataProviderId::from_str(
                            "5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5",
                        )?,
                        layer_id: LayerId("UTM32N:B01".to_owned()),
                    }
                    .into(),
                )
                .await?;

        let name = NamedData {
            namespace: None,
            provider: Some("5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5".into()),
            name: "UTM32N-B01".into(),
        };

        exe.add_meta_data(
            ExternalDataId {
                provider_id: DataProviderId::from_str("5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5")?,
                layer_id: LayerId("UTM32N:B01".to_owned()),
            }
            .into(),
            name.clone(),
            meta,
        );

        let op = GdalSource {
            params: GdalSourceParameters { data: name },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe)
        .await
        .unwrap();

        let processor = op.query_processor()?.get_u16().unwrap();

        let spatial_bounds =
            SpatialPartition2D::new((166_021.44, 9_329_005.18).into(), (534_994.66, 0.00).into())
                .unwrap();

        let spatial_resolution = SpatialResolution::new_unchecked(
            spatial_bounds.size_x() / 256.,
            spatial_bounds.size_y() / 256.,
        );
        let query = RasterQueryRectangle {
            spatial_bounds,
            time_interval: TimeInterval::new_instant(DateTime::new_utc(2021, 1, 2, 10, 2, 26))?,
            spatial_resolution,
            attributes: BandSelection::first(),
        };

        let ctx = MockQueryContext::new(ChunkByteSize::MAX);

        let result = processor
            .raster_query(query, &ctx)
            .await?
            .collect::<Vec<_>>()
            .await;

        // TODO: check actual data
        assert_eq!(result.len(), 1);

        Ok(())
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn query_data_with_failing_requests(app_ctx: ProPostgresContext<NoTls>) {
        // crate::util::tests::initialize_debugging_in_test(); // use for debugging
        hide_gdal_errors();

        let stac_response =
            std::fs::read_to_string(test_data!("pro/stac_responses/items_page_1_limit_500.json"))
                .unwrap();
        let server = Server::run();

        // STAC response
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/v0/collections/sentinel-s2-l2a-cogs/items",),
                request::query(url_decoded(contains((
                    "collections[]",
                    "sentinel-s2-l2a-cogs"
                )))),
                request::query(url_decoded(contains(("page", "1")))),
                request::query(url_decoded(contains(("limit", "500")))),
                request::query(url_decoded(contains((
                    "bbox",
                    "[33.899332958586406,-2.261536424319933,33.900232774450984,-2.2606312588790414]"
                )))),
                request::query(url_decoded(contains((
                    "datetime",
                    // default case adds one minute to the start/end of the query to catch elements before/after 
                    "2021-09-23T08:09:44+00:00/2021-09-23T08:11:44+00:00"
                )))),
            ])
            .times(2)
            .respond_with(responders::cycle![
                // first fail
                responders::status_code(404),
                // then succeed
                responders::status_code(200)
                    .append_header("Content-Type", "application/json")
                    .body(stac_response),
            ]),
        );

        // HEAD request
        let head_success_response = || {
            responders::status_code(200)
                .append_header(
                    "x-amz-id-2",
                    "avRd0/ks4ATH99UNXBCfqZAEQ3BckuLJTj7iG1jQrGoxOtwswqHrok10u+VMHO3twVIhUmQKLwg=",
                )
                .append_header("x-amz-request-id", "VVHWX1P45NP7KNWV")
                .append_header("Date", "Tue, 11 Oct 2022 16:06:03 GMT")
                .append_header("Last-Modified", "Fri, 09 Sep 2022 00:32:25 GMT")
                .append_header("ETag", "\"09a4c36021930e67dd1c71ed303cdf4e-24\"")
                .append_header("Cache-Control", "public, max-age=31536000, immutable")
                .append_header("Accept-Ranges", "bytes")
                .append_header(
                    "Content-Type",
                    "image/tiff; application=geotiff; profile=cloud-optimized",
                )
                .append_header("Server", "AmazonS3")
                .append_header("Content-Length", "197770048")
        };

        server.expect(
            Expectation::matching(request::method_path(
                "HEAD",
                "/sentinel-s2-l2a-cogs/36/M/WC/2021/9/S2B_36MWC_20210923_0_L2A/B04.tif",
            ))
            .times(7)
            .respond_with(responders::cycle![
                // first fail
                responders::status_code(500),
                // then time out
                responders::delay_and_then(
                    std::time::Duration::from_secs(10),
                    responders::status_code(500)
                ),
                // then succeed
                head_success_response(), // -> GET COG header fails
                head_success_response(), // -> GET COG header times out
                head_success_response(), // -> GET COG header succeeds, GET tile fails
                head_success_response(), // -> GET COG header succeeds, GET tile times out
                head_success_response(), // -> GET COG header succeeds, GET tile IReadBlock failed
            ]),
        );

        // GET request to read contents of COG header
        let get_success_response = || {
            responders::status_code(206)
                .append_header("Content-Type", "application/json")
                .body(
                    include_bytes!("../../../../../test_data/pro/stac_responses/cog-header.bin")
                        .to_vec(),
                )
                .append_header(
                    "x-amz-id-2",
                    "avRd0/ks4ATH99UNXBCfqZAEQ3BckuLJTj7iG1jQrGoxOtwswqHrok10u+VMHO3twVIhUmQKLwg=",
                )
                .append_header("x-amz-request-id", "VVHWX1P45NP7KNWV")
                .append_header("Date", "Tue, 11 Oct 2022 16:06:03 GMT")
                .append_header("Last-Modified", "Fri, 09 Sep 2022 00:32:25 GMT")
                .append_header("ETag", "\"09a4c36021930e67dd1c71ed303cdf4e-24\"")
                .append_header("Cache-Control", "public, max-age=31536000, immutable")
                .append_header("Accept-Ranges", "bytes")
                .append_header("Content-Range", "bytes 0-16383/197770048")
                .append_header(
                    "Content-Type",
                    "image/tiff; application=geotiff; profile=cloud-optimized",
                )
                .append_header("Server", "AmazonS3")
                .append_header("Content-Length", "16384")
        };

        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "GET",
                    "/sentinel-s2-l2a-cogs/36/M/WC/2021/9/S2B_36MWC_20210923_0_L2A/B04.tif",
                ),
                request::headers(contains(("range", "bytes=0-16383"))),
            ])
            .times(5)
            .respond_with(responders::cycle![
                // first fail
                responders::status_code(500),
                // then time out
                responders::delay_and_then(
                    std::time::Duration::from_secs(10),
                    responders::status_code(500)
                ),
                // then succeed
                get_success_response(), // -> GET tile fails
                get_success_response(), // -> GET tile times out
                get_success_response(), // -> GET tile times IReadBlock failed
            ]),
        );

        // GET request of the COG tile
        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "GET",
                    "/sentinel-s2-l2a-cogs/36/M/WC/2021/9/S2B_36MWC_20210923_0_L2A/B04.tif",
                ),
                request::headers(contains(("range", "bytes=46170112-46186495"))),
            ])
            .times(4)
            .respond_with(responders::cycle![
                // first fail
                responders::status_code(500),
                // then time out
                responders::delay_and_then(
                    std::time::Duration::from_secs(10),
                    responders::status_code(500)
                ),
                // then return incomplete tile (to force error "band 1: IReadBlock failed at X offset 0, Y offset 0: TIFFReadEncodedTile() failed.")
                responders::status_code(206)
                    .append_header("Content-Type", "application/json")
                    .body(
                        include_bytes!(
                            "../../../../../test_data/pro/stac_responses/cog-tile.bin"
                        )[0..2]
                        .to_vec()
                    ).append_header(
                        "x-amz-id-2",
                        "avRd0/ks4ATH99UNXBCfqZAEQ3BckuLJTj7iG1jQrGoxOtwswqHrok10u+VMHO3twVIhUmQKLwg=",
                    )
                    .append_header("x-amz-request-id", "VVHWX1P45NP7KNWV")
                    .append_header("Date", "Tue, 11 Oct 2022 16:06:03 GMT")
                    .append_header("Last-Modified", "Fri, 09 Sep 2022 00:32:25 GMT")
                    .append_header("ETag", "\"09a4c36021930e67dd1c71ed303cdf4e-24\"")
                    .append_header("Cache-Control", "public, max-age=31536000, immutable")
                    .append_header("Accept-Ranges", "bytes")
                    .append_header("Content-Range", "bytes 46170112-46170113/173560205")
                    .append_header(
                        "Content-Type",
                        "image/tiff; application=geotiff; profile=cloud-optimized",
                    )
                    .append_header("Server", "AmazonS3")
                    .append_header("Content-Length", "2"),
                 // then succeed
                responders::status_code(206)
                    .append_header("Content-Type", "application/json")
                    .body(
                        include_bytes!(
                            "../../../../../test_data/pro/stac_responses/cog-tile.bin"
                        )
                        .to_vec()
                    ).append_header(
                        "x-amz-id-2",
                        "avRd0/ks4ATH99UNXBCfqZAEQ3BckuLJTj7iG1jQrGoxOtwswqHrok10u+VMHO3twVIhUmQKLwg=",
                    )
                    .append_header("x-amz-request-id", "VVHWX1P45NP7KNWV")
                    .append_header("Date", "Tue, 11 Oct 2022 16:06:03 GMT")
                    .append_header("Last-Modified", "Fri, 09 Sep 2022 00:32:25 GMT")
                    .append_header("ETag", "\"09a4c36021930e67dd1c71ed303cdf4e-24\"")
                    .append_header("Cache-Control", "public, max-age=31536000, immutable")
                    .append_header("Accept-Ranges", "bytes")
                    .append_header("Content-Range", "bytes 46170112-46186495/173560205")
                    .append_header(
                        "Content-Type",
                        "image/tiff; application=geotiff; profile=cloud-optimized",
                    )
                    .append_header("Server", "AmazonS3")
                    .append_header("Content-Length", "16384"),
            ]),
        );

        let provider_id: DataProviderId = "5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5".parse().unwrap();

        let provider_def: Box<dyn DataProviderDefinition<ProPostgresDb<NoTls>>> =
            Box::new(SentinelS2L2ACogsProviderDefinition {
                name: "Element 84 AWS STAC".into(),
                id: provider_id,
                description: "Access to Sentinel 2 L2A COGs on AWS".into(),
                priority: Some(22),
                api_url: server.url_str("/v0/collections/sentinel-s2-l2a-cogs/items"),
                bands: vec![StacBand {
                    name: "B04".into(),
                    no_data_value: Some(0.),
                    data_type: RasterDataType::U16,
                }],
                zones: vec![StacZone {
                    name: "UTM36S".into(),
                    epsg: 32736,
                }],
                stac_api_retries: Default::default(),
                gdal_retries: GdalRetries {
                    number_of_retries: 999,
                },
                cache_ttl: Default::default(),
                query_buffer: Default::default(),
            });

        let provider = provider_def
            .initialize(
                app_ctx
                    .session_context(app_ctx.create_anonymous_session().await.unwrap())
                    .db(),
            )
            .await
            .unwrap();

        let meta: Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> =
            provider
                .meta_data(
                    &ExternalDataId {
                        provider_id,
                        layer_id: LayerId("UTM36S:B04".to_owned()),
                    }
                    .into(),
                )
                .await
                .unwrap();

        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked(
                (600_000.00, 9_750_100.).into(),
                (600_100.0, 9_750_000.).into(),
            ),
            time_interval: TimeInterval::new_instant(DateTime::new_utc(2021, 9, 23, 8, 10, 44))
                .unwrap(),
            spatial_resolution: SpatialResolution::new_unchecked(10., 10.),
            attributes: BandSelection::first(),
        };

        let loading_info = meta.loading_info(query).await.unwrap();
        let parts =
            if let GdalLoadingInfoTemporalSliceIterator::Static { parts } = loading_info.info {
                parts.collect::<Vec<_>>()
            } else {
                panic!("expected static parts");
            };

        assert_eq!(
            parts,
            vec![GdalLoadingInfoTemporalSlice {
                time: TimeInterval::new_unchecked(1_632_384_644_000,1_632_384_704_000),
                params: Some(GdalDatasetParameters {
                    file_path: "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/36/M/WC/2021/9/S2B_36MWC_20210923_0_L2A/B04.tif".into(),
                    rasterband_channel: 1,
                    geo_transform: GdalDatasetGeoTransform {
                        origin_coordinate: (499_980.0,9_800_020.00).into(),
                        x_pixel_size: 10.,
                        y_pixel_size: -10.,
                    },
                    width: 10980,
                    height: 10980,
                    file_not_found_handling: FileNotFoundHandling::NoData,
                    no_data_value: Some(0.),
                    properties_mapping: None,
                    gdal_open_options: None,
                    gdal_config_options: Some(vec![
                        ("CPL_VSIL_CURL_ALLOWED_EXTENSIONS".to_owned(), ".tif".to_owned()),
                        ("GDAL_DISABLE_READDIR_ON_OPEN".to_owned(), "EMPTY_DIR".to_owned()),
                        ("GDAL_HTTP_NETRC".to_owned(), "NO".to_owned()),
                        ("GDAL_HTTP_MAX_RETRY".to_owned(), "0".to_string()),
                        ]),
                    allow_alphaband_as_mask: true,
                    retry: Some(GdalRetryOptions { max_retries: 999 }),
                }),
                cache_ttl: CacheTtlSeconds::default(),
            }]
        );

        let mut params = parts[0].clone().params.unwrap();
        params.file_path = params
            .file_path
            .to_str()
            .unwrap()
            .replace(
                "https://sentinel-cogs.s3.us-west-2.amazonaws.com/",
                &server.url_str(""),
            )
            .into();
        // add a low `GDAL_HTTP_TIMEOUT` value to test timeouts
        params.gdal_config_options = params.gdal_config_options.map(|mut options| {
            options.push(("GDAL_HTTP_TIMEOUT".to_owned(), "1".to_owned()));
            options
        });

        let mut execution_context = MockExecutionContext::test_default();
        let id: geoengine_datatypes::dataset::DataId = DatasetId::new().into();
        let name = NamedData {
            namespace: None,
            provider: None,
            name: "UTM36S-B04".into(),
        };
        execution_context.add_meta_data(
            id.clone(),
            name.clone(),
            Box::new(GdalMetaDataStatic {
                time: None,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U16,
                    spatial_reference: SpatialReference::from_str("EPSG:32736").unwrap().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
                params,
                cache_ttl: CacheTtlSeconds::default(),
            }),
        );

        let gdal_source = GdalSource {
            params: GdalSourceParameters { data: name },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u16()
        .unwrap();

        let query_context = MockQueryContext::test_default();

        let stream = gdal_source
            .raster_query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (499_980., 9_804_800.).into(),
                        (499_990., 9_804_810.).into(),
                    ),
                    time_interval: TimeInterval::new_instant(DateTime::new_utc(
                        2014, 3, 1, 0, 0, 0,
                    ))
                    .unwrap(),
                    spatial_resolution: SpatialResolution::new(10., 10.).unwrap(),
                    attributes: BandSelection::first(),
                },
                &query_context,
            )
            .await
            .unwrap();

        let result = stream.collect::<Vec<_>>().await;

        assert_eq!(result.len(), 1);
        assert!(result[0].is_ok());
    }

    #[test]
    fn make_unique_timestamps_no_dups() {
        let timestamps = vec![
            TimeInstance::from_millis(1_632_384_644_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_000).unwrap(),
            TimeInstance::from_millis(1_632_384_646_000).unwrap(),
            TimeInstance::from_millis(1_632_384_647_000).unwrap(),
            TimeInstance::from_millis(1_632_384_648_000).unwrap(),
            TimeInstance::from_millis(1_632_384_649_000).unwrap(),
        ];

        let uts =
            SentinelS2L2aCogsMetaData::make_unique_start_times_from_sorted_features(&timestamps);

        assert_eq!(uts, timestamps);
    }

    #[test]
    fn make_unique_timestamps_two_dups() {
        let timestamps = vec![
            TimeInstance::from_millis(1_632_384_644_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_000).unwrap(),
            TimeInstance::from_millis(1_632_384_646_000).unwrap(),
            TimeInstance::from_millis(1_632_384_647_000).unwrap(),
            TimeInstance::from_millis(1_632_384_648_000).unwrap(),
        ];

        let uts =
            SentinelS2L2aCogsMetaData::make_unique_start_times_from_sorted_features(&timestamps);

        let expected_timestamps = vec![
            TimeInstance::from_millis(1_632_384_644_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_001).unwrap(),
            TimeInstance::from_millis(1_632_384_646_000).unwrap(),
            TimeInstance::from_millis(1_632_384_647_000).unwrap(),
            TimeInstance::from_millis(1_632_384_648_000).unwrap(),
        ];

        assert_eq!(uts, expected_timestamps);
    }

    #[test]
    fn make_unique_timestamps_three_dups() {
        let timestamps = vec![
            TimeInstance::from_millis(1_632_384_644_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_000).unwrap(),
            TimeInstance::from_millis(1_632_384_646_000).unwrap(),
            TimeInstance::from_millis(1_632_384_647_000).unwrap(),
        ];

        let uts =
            SentinelS2L2aCogsMetaData::make_unique_start_times_from_sorted_features(&timestamps);

        let expected_timestamps = vec![
            TimeInstance::from_millis(1_632_384_644_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_001).unwrap(),
            TimeInstance::from_millis(1_632_384_645_002).unwrap(),
            TimeInstance::from_millis(1_632_384_646_000).unwrap(),
            TimeInstance::from_millis(1_632_384_647_000).unwrap(),
        ];

        assert_eq!(uts, expected_timestamps);
    }

    #[test]
    fn make_unique_timestamps_four_dups() {
        let timestamps = vec![
            TimeInstance::from_millis(1_632_384_644_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_000).unwrap(),
            TimeInstance::from_millis(1_632_384_646_000).unwrap(),
        ];

        let uts =
            SentinelS2L2aCogsMetaData::make_unique_start_times_from_sorted_features(&timestamps);

        let expected_timestamps = vec![
            TimeInstance::from_millis(1_632_384_644_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_000).unwrap(),
            TimeInstance::from_millis(1_632_384_645_001).unwrap(),
            TimeInstance::from_millis(1_632_384_645_002).unwrap(),
            TimeInstance::from_millis(1_632_384_645_003).unwrap(),
            TimeInstance::from_millis(1_632_384_646_000).unwrap(),
        ];

        assert_eq!(uts, expected_timestamps);
    }

    #[ge_context::test]
    async fn it_adds_the_data_provider_to_the_db(app_ctx: ProPostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let def: SentinelS2L2ACogsProviderDefinition = serde_json::from_reader(BufReader::new(
            File::open(test_data!("provider_defs/pro/sentinel_s2_l2a_cogs.json")).unwrap(),
        ))
        .unwrap();

        ctx.db().add_pro_layer_provider(def.into()).await.unwrap();

        ctx.db()
            .load_layer_provider(DataProviderId::from_u128(
                0x5779494c_f3a2_48b3_8a2d_5fbba8c5b6c5,
            ))
            .await
            .unwrap();

        let providers = ctx
            .db()
            .list_layer_providers(LayerProviderListingOptions {
                offset: 0,
                limit: 2,
            })
            .await
            .unwrap();

        assert_eq!(providers.len(), 1);

        assert_eq!(
            providers[0],
            LayerProviderListing {
                id: DataProviderId::from_u128(0x5779494c_f3a2_48b3_8a2d_5fbba8c5b6c5),
                name: "Element 84 AWS STAC".to_owned(),
                priority: 0,
            }
        );
    }
}
