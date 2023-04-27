use crate::api::model::datatypes::{DataId, DataProviderId, ExternalDataId, LayerId};
use crate::datasets::listing::ProvenanceOutput;
use crate::error::{self, Error, Result};
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerListing,
    ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::projects::{RasterSymbology, Symbology};
use crate::stac::{Feature as StacFeature, FeatureCollection as StacCollection, StacAsset};
use crate::util::operators::source_operator_from_dataset;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use geoengine_datatypes::operations::image::{DefaultColors, RgbaColor};
use geoengine_datatypes::operations::reproject::{
    CoordinateProjection, CoordinateProjector, ReprojectClipped,
};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, DateTime, Duration, Measurement, RasterQueryRectangle,
    SpatialPartitioned, SpatialQuery, TimeInstance, TimeInterval, VectorQueryRectangle,
};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceAuthority};
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, OperatorName, RasterOperator, RasterResultDescriptor,
    TypedOperator, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    GdalDatasetGeoTransform, GdalDatasetParameters, GdalLoadingInfo, GdalLoadingInfoTemporalSlice,
    GdalLoadingInfoTemporalSliceIterator, GdalRetryOptions, GdalSource, GdalSourceParameters,
    OgrSourceDataset,
};
use geoengine_operators::util::retry::retry;
use log::debug;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::Debug;
use std::path::PathBuf;

static STAC_RETRY_MAX_BACKOFF_MS: u64 = 60 * 60 * 1000;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SentinelS2L2ACogsProviderDefinition {
    name: String,
    id: DataProviderId,
    api_url: String,
    bands: Vec<Band>,
    zones: Vec<Zone>,
    #[serde(default)]
    stac_api_retries: StacApiRetries,
    #[serde(default)]
    gdal_retries: GdalRetries,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StacApiRetries {
    number_of_retries: usize,
    initial_delay_ms: u64,
    exponential_backoff_factor: f64,
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GdalRetries {
    /// retry at most `number_of_retries` times with exponential backoff
    number_of_retries: usize,
}

impl Default for GdalRetries {
    fn default() -> Self {
        Self {
            number_of_retries: 10,
        }
    }
}

#[typetag::serde]
#[async_trait]
impl DataProviderDefinition for SentinelS2L2ACogsProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn DataProvider>> {
        Ok(Box::new(SentinelS2L2aCogsDataProvider::new(
            self.id,
            self.api_url,
            &self.bands,
            &self.zones,
            self.stac_api_retries,
            self.gdal_retries,
        )))
    }

    fn type_name(&self) -> &'static str {
        "SentinelS2L2ACogs"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        self.id
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Band {
    pub name: String,
    pub no_data_value: Option<f64>,
    pub data_type: RasterDataType,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Zone {
    pub name: String,
    pub epsg: u32,
}

#[derive(Debug, Clone)]
pub struct SentinelDataset {
    band: Band,
    zone: Zone,
    listing: Layer,
}

#[derive(Debug)]
pub struct SentinelS2L2aCogsDataProvider {
    id: DataProviderId,

    api_url: String,

    datasets: HashMap<LayerId, SentinelDataset>,

    stac_api_retries: StacApiRetries,
    gdal_retries: GdalRetries,
}

impl SentinelS2L2aCogsDataProvider {
    pub fn new(
        id: DataProviderId,
        api_url: String,
        bands: &[Band],
        zones: &[Zone],
        stac_api_retries: StacApiRetries,
        gdal_retries: GdalRetries,
    ) -> Self {
        Self {
            id,
            api_url,
            datasets: Self::create_datasets(&id, bands, zones),
            stac_api_retries,
            gdal_retries,
        }
    }

    fn create_datasets(
        id: &DataProviderId,
        bands: &[Band],
        zones: &[Zone],
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
                                &DataId::External(ExternalDataId {
                                    provider_id: *id,
                                    layer_id: layer_id.clone(),
                                })
                                .into(),
                            )
                            .expect("Gdal source is a valid operator."),
                        },
                        symbology: Some(Symbology::Raster(RasterSymbology {
                            opacity: 1.0,
                            colorizer:
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
                                    DefaultColors::OverUnder {
                                        over_color: RgbaColor::white(),
                                        under_color: RgbaColor::black(),
                                    },
                                )
                                .expect("valid colorizer")
                                .into(),
                        })), // TODO: individual colorizer per band
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
                            data: DataId::External(ExternalDataId {
                                provider_id: self.id,
                                layer_id: id.clone(),
                            })
                            .into(),
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
    zone: Zone,
    band: Band,
    stac_api_retries: StacApiRetries,
    gdal_retries: GdalRetries,
}

impl SentinelS2L2aCogsMetaData {
    async fn create_loading_info(&self, query: RasterQueryRectangle) -> Result<GdalLoadingInfo> {
        // for reference: https://stacspec.org/STAC-ext-api.html#operation/getSearchSTAC
        debug!("create_loading_info with: {:?}", &query);
        let request_params = self.request_params(query)?;

        if request_params.is_none() {
            log::debug!("Request params are empty -> returning empty loading info");
            return Ok(GdalLoadingInfo {
                info: GdalLoadingInfoTemporalSliceIterator::Static {
                    parts: vec![].into_iter(),
                },
            });
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

        let mut parts = vec![];
        let num_features = features.len();
        debug!("number of features in current zone: {}", num_features);
        for i in 0..num_features {
            let feature = &features[i];

            let start = start_times[i];

            // feature is valid until next feature starts
            let end = if i < num_features - 1 {
                start_times[i + 1]
            } else {
                start + 1000 // TODO: determine correct validity for last tile
            };

            let time_interval = TimeInterval::new(start, end)?;

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

                parts.push(self.create_loading_info_part(time_interval, asset)?);
            }
        }
        debug!("number of generated loading infos: {}", parts.len());

        Ok(GdalLoadingInfo {
            info: GdalLoadingInfoTemporalSliceIterator::Static {
                parts: parts.into_iter(),
            },
        })
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
        })
    }

    fn request_params(&self, query: RasterQueryRectangle) -> Result<Option<Vec<(String, String)>>> {
        let (t_start, t_end) = Self::time_range_request(&query.time_interval)?;

        // request all features in zone in order to be able to determine the temporal validity of individual tile
        let projector = CoordinateProjector::from_known_srs(
            SpatialReference::new(SpatialReferenceAuthority::Epsg, self.zone.epsg),
            SpatialReference::epsg_4326(),
        )?;

        let bbox = query.spatial_query().spatial_partition().as_bbox(); // TODO: use SpatialPartition2D directly
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

        // shift start by 1 minute to ensure getting the most recent data for start time
        let t_start = t_start - Duration::minutes(1);

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
            measurement: Measurement::Unitless,
            time: None,
            bbox: None,
            resolution: None, // TODO: determine from STAC or data or hardcode it
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
        let id: DataId = id.clone().into();

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
    use std::{fs::File, io::BufReader, str::FromStr};

    use crate::test_data;
    use futures::StreamExt;
    use geoengine_datatypes::{
        dataset::DatasetId,
        primitives::{Coordinate2D, SpatialPartition2D, SpatialResolution},
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

    use super::*;

    #[tokio::test]
    async fn loading_info() -> Result<()> {
        // TODO: mock STAC endpoint

        let def: Box<dyn DataProviderDefinition> = serde_json::from_reader(BufReader::new(
            File::open(test_data!("provider_defs/pro/sentinel_s2_l2a_cogs.json"))?,
        ))?;

        let provider = def.initialize().await?;

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
            .loading_info(RasterQueryRectangle::with_partition_and_resolution(
                SpatialPartition2D::new(
                    (166_021.44, 9_329_005.18).into(),
                    (534_994.66, 0.00).into(),
                )
                .unwrap(),
                SpatialResolution::one(),
                TimeInterval::new_instant(DateTime::new_utc(2021, 1, 2, 10, 2, 26))?,
            ))
            .await
            .unwrap();

        let expected = vec![GdalLoadingInfoTemporalSlice {
            time: TimeInterval::new_unchecked(1_609_581_746_000, 1_609_581_747_000),
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

    #[tokio::test]
    async fn query_data() -> Result<()> {
        // TODO: mock STAC endpoint

        let mut exe = MockExecutionContext::test_default();

        let def: Box<dyn DataProviderDefinition> = serde_json::from_reader(BufReader::new(
            File::open(test_data!("provider_defs/pro/sentinel_s2_l2a_cogs.json"))?,
        ))?;

        let provider = def.initialize().await?;

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

        exe.add_meta_data(
            ExternalDataId {
                provider_id: DataProviderId::from_str("5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5")?,
                layer_id: LayerId("UTM32N:B01".to_owned()),
            }
            .into(),
            meta,
        );

        let op = GdalSource {
            params: GdalSourceParameters {
                data: ExternalDataId {
                    provider_id: DataProviderId::from_str("5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5")?,
                    layer_id: LayerId("UTM32N:B01".to_owned()),
                }
                .into(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe)
        .await
        .unwrap();

        let processor = op.query_processor()?.get_u16().unwrap();

        let sp =
            SpatialPartition2D::new((166_021.44, 9_329_005.18).into(), (534_994.66, 0.00).into())
                .unwrap();
        let sr = SpatialResolution::new_unchecked(sp.size_x() / 256., sp.size_y() / 256.);
        let query = RasterQueryRectangle::with_partition_and_resolution_and_origin(
            sp,
            sr,
            exe.tiling_specification.origin_coordinate,
            TimeInterval::new_instant(DateTime::new_utc(2021, 1, 2, 10, 2, 26))?,
        );

        let ctx = MockQueryContext::new(ChunkByteSize::MAX);

        let result = processor
            .raster_query(query, &ctx)
            .await?
            .collect::<Vec<_>>()
            .await;

        // TODO: check actual data
        // Note this is 1 IF the tile size larger then 256x25
        assert_eq!(result.len(), 1);

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn query_data_with_failing_requests() {
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
                    // TODO: why do we request with one minute earlier?
                    "2021-09-23T08:09:44+00:00/2021-09-23T08:10:44+00:00"
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

        let provider_def: Box<dyn DataProviderDefinition> =
            Box::new(SentinelS2L2ACogsProviderDefinition {
                name: "Element 84 AWS STAC".into(),
                id: provider_id,
                api_url: server.url_str("/v0/collections/sentinel-s2-l2a-cogs/items"),
                bands: vec![Band {
                    name: "B04".into(),
                    no_data_value: Some(0.),
                    data_type: RasterDataType::U16,
                }],
                zones: vec![Zone {
                    name: "UTM36S".into(),
                    epsg: 32736,
                }],
                stac_api_retries: Default::default(),
                gdal_retries: GdalRetries {
                    number_of_retries: 999,
                },
            });

        let provider = provider_def.initialize().await.unwrap();

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

        let query = RasterQueryRectangle::with_partition_and_resolution_and_origin(
            SpatialPartition2D::new_unchecked(
                (600_000.00, 9_750_100.).into(),
                (600_100.0, 9_750_000.).into(),
            ),
            SpatialResolution::new_unchecked(10., 10.),
            Coordinate2D::new(0., 0.), // FIXME: this is the default tiling strategy origin
            TimeInterval::new_instant(DateTime::new_utc(2021, 9, 23, 8, 10, 44)).unwrap(),
        );

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
                time: TimeInterval::new_unchecked(1_632_384_644_000, 1_632_384_645_000),
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
        execution_context.add_meta_data(
            id.clone(),
            Box::new(GdalMetaDataStatic {
                time: None,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U16,
                    spatial_reference: SpatialReference::from_str("EPSG:32736").unwrap().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
                params,
            }),
        );

        let gdal_source = GdalSource {
            params: GdalSourceParameters { data: id },
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
                RasterQueryRectangle::with_partition_and_resolution_and_origin(
                    SpatialPartition2D::new_unchecked(
                        (499_980., 9_804_800.).into(),
                        (499_990., 9_804_810.).into(),
                    ),
                    SpatialResolution::new(10., 10.).unwrap(),
                    execution_context.tiling_specification.origin_coordinate,
                    TimeInterval::new_instant(DateTime::new_utc(2014, 3, 1, 0, 0, 0)).unwrap(),
                ),
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
}
