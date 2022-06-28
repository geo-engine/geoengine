use crate::datasets::listing::{DatasetListing, ProvenanceOutput};
use crate::error::{self, Error, Result};
use crate::layers::external::{ExternalLayerProvider, ExternalLayerProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollectionListOptions, LayerListing, ProviderLayerId,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider, LayerId};
use crate::projects::{RasterSymbology, Symbology};
use crate::stac::{Feature as StacFeature, FeatureCollection as StacCollection, StacAsset};
use crate::util::retry::retry;
use crate::util::user_input::Validated;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DatasetId, ExternalDatasetId, LayerProviderId};
use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
use geoengine_datatypes::operations::reproject::{
    CoordinateProjection, CoordinateProjector, ReprojectClipped,
};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, DateTime, Duration, Measurement, RasterQueryRectangle,
    SpatialPartitioned, TimeInstance, TimeInterval, VectorQueryRectangle,
};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceAuthority};
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterOperator, RasterResultDescriptor, TypedOperator,
    VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    GdalDatasetGeoTransform, GdalDatasetParameters, GdalLoadingInfo, GdalLoadingInfoTemporalSlice,
    GdalLoadingInfoTemporalSliceIterator, GdalSource, GdalSourceParameters, OgrSourceDataset,
};
use log::debug;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::Debug;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SentinelS2L2ACogsProviderDefinition {
    name: String,
    id: LayerProviderId,
    api_url: String,
    bands: Vec<Band>,
    zones: Vec<Zone>,
    #[serde(default)]
    stac_api_retries: StacApiRetries,
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

#[typetag::serde]
#[async_trait]
impl ExternalLayerProviderDefinition for SentinelS2L2ACogsProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn ExternalLayerProvider>> {
        Ok(Box::new(SentinelS2L2aCogsDataProvider::new(
            self.id,
            self.api_url,
            &self.bands,
            &self.zones,
            self.stac_api_retries,
        )))
    }

    fn type_name(&self) -> String {
        "SentinelS2L2ACogs".to_owned()
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> LayerProviderId {
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
    listing: DatasetListing,
}

#[derive(Debug)]
pub struct SentinelS2L2aCogsDataProvider {
    id: LayerProviderId,

    api_url: String,

    datasets: HashMap<DatasetId, SentinelDataset>,

    stac_api_retries: StacApiRetries,
}

impl SentinelS2L2aCogsDataProvider {
    pub fn new(
        id: LayerProviderId,
        api_url: String,
        bands: &[Band],
        zones: &[Zone],
        stac_api_retries: StacApiRetries,
    ) -> Self {
        Self {
            id,
            api_url,
            datasets: Self::create_datasets(&id, bands, zones),
            stac_api_retries,
        }
    }

    fn create_datasets(
        id: &LayerProviderId,
        bands: &[Band],
        zones: &[Zone],
    ) -> HashMap<DatasetId, SentinelDataset> {
        zones
            .iter()
            .flat_map(|zone| {
                bands.iter().map(move |band| {
                    let dataset_id: DatasetId = ExternalDatasetId {
                        provider_id: *id,
                        dataset_id: format!("{}:{}", zone.name, band.name),
                    }
                    .into();
                    let listing = DatasetListing {
                        id: dataset_id.clone(),
                        name: format!("Sentinel S2 L2A COGS {}:{}", zone.name, band.name),
                        description: "".to_owned(),
                        tags: vec![],
                        source_operator: "GdalSource".to_owned(),
                        result_descriptor: RasterResultDescriptor {
                            data_type: band.data_type,
                            spatial_reference: SpatialReference::new(
                                SpatialReferenceAuthority::Epsg,
                                zone.epsg,
                            )
                            .into(),
                            measurement: Measurement::Unitless, // TODO: add measurement
                            no_data_value: band.no_data_value,
                            time: None, // TODO: determine time
                            bbox: None, // TODO: determine bbox
                        }
                        .into(),
                        symbology: Some(Symbology::Raster(RasterSymbology {
                            opacity: 1.0,
                            colorizer: Colorizer::linear_gradient(
                                vec![
                                    (0.0, RgbaColor::white())
                                        .try_into()
                                        .expect("valid breakpoint"),
                                    (10_000.0, RgbaColor::black())
                                        .try_into()
                                        .expect("valid breakpoint"),
                                ],
                                RgbaColor::transparent(),
                                RgbaColor::transparent(),
                            )
                            .expect("valid colorizer"),
                        })), // TODO: individual colorizer per band
                    };

                    let dataset = SentinelDataset {
                        zone: zone.clone(),
                        band: band.clone(),
                        listing,
                    };

                    (dataset_id, dataset)
                })
            })
            .collect()
    }
}

#[async_trait]
impl ExternalLayerProvider for SentinelS2L2aCogsDataProvider {
    async fn provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            dataset: dataset.clone(),
            provenance: None, // TODO
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl LayerCollectionProvider for SentinelS2L2aCogsDataProvider {
    async fn collection_items(
        &self,
        _collection: &LayerCollectionId,
        _options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        // TODO: check collection id

        // TODO: options
        let mut x = self
            .datasets
            .values()
            .map(|d| {
                let id = d.listing.id.external().ok_or(Error::InvalidDatasetId)?;
                Ok(CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider: id.provider_id,
                        item: LayerId(id.dataset_id),
                    },
                    name: d.listing.name.clone(),
                    description: d.listing.description.clone(),
                }))
            })
            .collect::<Result<Vec<CollectionItem>>>()?;
        x.sort_by_key(|e| e.name().to_string());
        Ok(x)
    }

    async fn root_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId("SentinelS2L2ACogs".to_owned()))
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        let dataset_id = DatasetId::External(ExternalDatasetId {
            provider_id: self.id,
            dataset_id: id.0.clone(),
        });

        let dataset = self
            .datasets
            .get(&dataset_id)
            .ok_or(Error::UnknownDatasetId)?;

        Ok(Layer {
            id: ProviderLayerId {
                provider: self.id,
                item: id.clone(),
            },
            name: dataset.listing.name.clone(),
            description: dataset.listing.description.clone(),
            workflow: Workflow {
                operator: TypedOperator::Raster(
                    GdalSource {
                        params: GdalSourceParameters {
                            dataset: dataset_id,
                        },
                    }
                    .boxed(),
                ),
            },
            symbology: dataset.listing.symbology.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct SentinelS2L2aCogsMetaData {
    api_url: String,
    zone: Zone,
    band: Band,
    stac_api_retires: StacApiRetries,
}

impl SentinelS2L2aCogsMetaData {
    async fn create_loading_info(&self, query: RasterQueryRectangle) -> Result<GdalLoadingInfo> {
        // for reference: https://stacspec.org/STAC-ext-api.html#operation/getSearchSTAC
        debug!("create_loading_info with: {:?}", &query);
        let request_params = self.request_params(query)?;
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
                        t_start.as_rfc3339(),
                        new_u_start.as_rfc3339(),
                        prev_u_start.as_rfc3339()
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
                gdal_config_options: None,
            }),
        })
    }

    fn request_params(&self, query: RasterQueryRectangle) -> Result<Vec<(String, String)>> {
        let (t_start, t_end) = Self::time_range_request(&query.time_interval)?;

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

        Ok(vec![
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
                format!("{}/{}", t_start.to_rfc3339(), t_end.to_rfc3339()),
            ),
            ("limit".to_owned(), "500".to_owned()),
        ])
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
            self.stac_api_retires.number_of_retries,
            self.stac_api_retires.initial_delay_ms,
            self.stac_api_retires.exponential_backoff_factor,
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
            no_data_value: self.band.no_data_value,
            time: None,
            bbox: None,
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
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let dataset = self
            .datasets
            .get(dataset)
            .ok_or(geoengine_operators::error::Error::UnknownDatasetId)?;

        Ok(Box::new(SentinelS2L2aCogsMetaData {
            api_url: self.api_url.clone(),
            zone: dataset.zone.clone(),
            band: dataset.band.clone(),
            stac_api_retires: self.stac_api_retries,
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
        _dataset: &DatasetId,
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
        _dataset: &DatasetId,
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
        primitives::{SpatialPartition2D, SpatialResolution},
        util::test::TestDefault,
    };
    use geoengine_operators::{
        engine::{ChunkByteSize, MockExecutionContext, MockQueryContext, RasterOperator},
        source::{FileNotFoundHandling, GdalSource, GdalSourceParameters},
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

        let def: Box<dyn ExternalLayerProviderDefinition> =
            serde_json::from_reader(BufReader::new(File::open(test_data!(
                "provider_defs/pro/sentinel_s2_l2a_cogs.json"
            ))?))?;

        let provider = def.initialize().await?;

        let meta: Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> =
            provider
                .meta_data(
                    &ExternalDatasetId {
                        provider_id: LayerProviderId::from_str(
                            "5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5",
                        )?,
                        dataset_id: "UTM32N:B01".to_owned(),
                    }
                    .into(),
                )
                .await
                .unwrap();

        let loading_info = meta
            .loading_info(RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new_unchecked(
                    (166_021.44, 0.00).into(),
                    (534_994.66, 9_329_005.18).into(),
                ),
                time_interval: TimeInterval::new_instant(DateTime::new_utc(2021, 1, 2, 10, 2, 26))?,
                spatial_resolution: SpatialResolution::one(),
            })
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
                gdal_config_options: None,
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

        let def: Box<dyn ExternalLayerProviderDefinition> =
            serde_json::from_reader(BufReader::new(File::open(test_data!(
                "provider_defs/pro/sentinel_s2_l2a_cogs.json"
            ))?))?;

        let provider = def.initialize().await?;

        let meta: Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> =
            provider
                .meta_data(
                    &ExternalDatasetId {
                        provider_id: LayerProviderId::from_str(
                            "5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5",
                        )?,
                        dataset_id: "UTM32N:B01".to_owned(),
                    }
                    .into(),
                )
                .await?;

        exe.add_meta_data(
            ExternalDatasetId {
                provider_id: LayerProviderId::from_str("5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5")?,
                dataset_id: "UTM32N:B01".to_owned(),
            }
            .into(),
            meta,
        );

        let op = GdalSource {
            params: GdalSourceParameters {
                dataset: ExternalDatasetId {
                    provider_id: LayerProviderId::from_str("5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5")?,
                    dataset_id: "UTM32N:B01".to_owned(),
                }
                .into(),
            },
        }
        .boxed()
        .initialize(&exe)
        .await
        .unwrap();

        let processor = op.query_processor()?.get_u16().unwrap();

        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked(
                (166_021.44, 9_329_005.18).into(),
                (534_994.66, 0.00).into(),
            ),
            time_interval: TimeInterval::new_instant(DateTime::new_utc(2021, 1, 2, 10, 2, 26))?,
            spatial_resolution: SpatialResolution::new_unchecked(
                166_021.44 / 256.,
                (9_329_005.18 - 534_994.66) / 256.,
            ),
        };

        let ctx = MockQueryContext::new(ChunkByteSize::MAX);

        let result = processor
            .raster_query(query, &ctx)
            .await?
            .collect::<Vec<_>>()
            .await;

        // TODO: check actual data
        assert_eq!(result.len(), 2);

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn query_data_with_failing_requests() {
        // util::tests::initialize_debugging_in_test; // use for debugging

        let stac_response =
            std::fs::read_to_string(test_data!("pro/stac_responses/items_page_1_limit_500.json"))
                .unwrap();
        let server = Server::run();
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

        let provider_id: LayerProviderId = "5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5".parse().unwrap();

        let provider_def: Box<dyn ExternalLayerProviderDefinition> =
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
            });

        let provider = provider_def.initialize().await.unwrap();

        let meta: Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> =
            provider
                .meta_data(
                    &ExternalDatasetId {
                        provider_id,
                        dataset_id: "UTM36S:B04".to_owned(),
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
                    gdal_config_options: None,
                }),
            }]
        );
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
