use std::collections::HashMap;
use std::ops::Bound;

use crate::{datasets::listing::DatasetListOptions, error::Result};
use crate::{
    datasets::{
        listing::{DatasetListing, DatasetProvider},
        storage::DatasetProviderDefinition,
    },
    error,
    util::user_input::Validated,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
use geoengine_datatypes::operations::reproject::{CoordinateProjection, CoordinateProjector};
use geoengine_datatypes::primitives::{BoundingBox2D, Measurement, TimeInterval};
use geoengine_datatypes::raster::{Raster, RasterDataType};
use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceAuthority};
use geoengine_operators::engine::QueryRectangle;
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use log::{debug, info};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SentinelS2L2ACogsProviderDefinition {
    name: String,
    id: DatasetProviderId,
    api_url: String,
}

#[typetag::serde]
#[async_trait]
impl DatasetProviderDefinition for SentinelS2L2ACogsProviderDefinition {
    async fn initialize(
        self: Box<Self>,
    ) -> crate::error::Result<Box<dyn crate::datasets::listing::DatasetProvider>> {
        Ok(Box::new(SentinelS2L2aCogsDataProvider::new(
            self.id,
            self.api_url,
        )))
    }

    fn type_name(&self) -> String {
        "SentinelS2L2ACogs".to_owned()
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DatasetProviderId {
        self.id
    }
}

#[derive(Debug, Clone)]
pub struct Band {
    pub name: String,
    pub no_data_value: Option<f64>,
    pub data_type: RasterDataType,
}

impl Band {
    pub fn new(name: String, no_data_value: Option<f64>, data_type: RasterDataType) -> Self {
        Self {
            name,
            no_data_value,
            data_type,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Zone {
    pub name: String,
    pub epsg: u32,
}

impl Zone {
    pub fn new(name: String, epsg: u32) -> Self {
        Self { name, epsg }
    }
}

#[derive(Debug, Clone)]
pub struct SentinelMetaData {
    bands: Vec<Band>,
    zones: Vec<Zone>,
}

#[derive(Debug, Clone)]
pub struct SentinelDataset {
    band: Band,
    zone: Zone,
    listing: DatasetListing,
}

pub struct SentinelS2L2aCogsDataProvider {
    id: DatasetProviderId,
    api_url: String,
    meta_data: SentinelMetaData,
    datasets: HashMap<DatasetId, SentinelDataset>,
}

impl SentinelS2L2aCogsDataProvider {
    pub fn new(id: DatasetProviderId, api_url: String) -> Self {
        let meta_data = Self::load_metadata();
        Self {
            id,
            api_url,
            datasets: Self::create_datasets(&id, &meta_data),
            meta_data,
        }
    }

    fn load_metadata() -> SentinelMetaData {
        // TODO: fetch dataset metadata from config or remote
        // TODO: is there a no data value?
        SentinelMetaData {
            bands: vec![
                Band::new("B01".to_owned(), None, RasterDataType::U16),
                Band::new("B02".to_owned(), None, RasterDataType::U16),
            ],
            zones: vec![
                Zone::new("UTM32N".to_owned(), 32632),
                Zone::new("UTM36S".to_owned(), 32736),
            ],
        }
    }

    fn create_datasets(
        id: &DatasetProviderId,
        meta_data: &SentinelMetaData,
    ) -> HashMap<DatasetId, SentinelDataset> {
        meta_data
            .zones
            .iter()
            .flat_map(|zone| {
                meta_data.bands.iter().map(move |band| {
                    let dataset_id: DatasetId = ExternalDatasetId {
                        provider: *id,
                        id: format!("{}:{}", zone.name, band.name),
                    }
                    .into();
                    let listing = DatasetListing {
                        id: dataset_id.clone(),
                        name: format!("Sentinal S2 L2A COGS {}:{}", zone.name, band.name),
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
                            measurement: Measurement::Unitless,
                            no_data_value: band.no_data_value,
                        }
                        .into(),
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
impl DatasetProvider for SentinelS2L2aCogsDataProvider {
    async fn list(&self, _options: Validated<DatasetListOptions>) -> Result<Vec<DatasetListing>> {
        // TODO: options
        Ok(self.datasets.values().map(|d| d.listing.clone()).collect())
    }

    async fn load(
        &self,
        _dataset: &geoengine_datatypes::dataset::DatasetId,
    ) -> crate::error::Result<crate::datasets::storage::Dataset> {
        Err(error::Error::NotYetImplemented)
    }
}

#[derive(Debug, Clone)]
pub struct SentinelS2L2aCogsMetaData {
    api_url: String,
    zone: Zone,
    band: Band,
}

impl SentinelS2L2aCogsMetaData {
    async fn web_request<T: Serialize + ?Sized>(
        &self,
        params: &T,
    ) -> geoengine_operators::util::Result<String> {
        let client = reqwest::Client::new();
        client
            .get(&self.api_url)
            .query(&params)
            .send()
            .await
            .map_err(|_| geoengine_operators::error::Error::LoadingInfo)?
            .text()
            .await
            .map_err(|_| geoengine_operators::error::Error::LoadingInfo)
    }

    fn time_range_request(time: &TimeInterval) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
        let t_start =
            time.start()
                .as_utc_date_time()
                .ok_or(geoengine_operators::error::Error::DataType {
                    source: geoengine_datatypes::error::Error::NoDateTimeValid {
                        time_instance: time.start(),
                    },
                })?;

        // shift start by 1 day to ensure getting the most recent data for start time
        let t_start = t_start - Duration::days(1);

        let t_end =
            time.end()
                .as_utc_date_time()
                .ok_or(geoengine_operators::error::Error::DataType {
                    source: geoengine_datatypes::error::Error::NoDateTimeValid {
                        time_instance: time.end(),
                    },
                })?;

        Ok((t_start, t_end))
    }

    fn bbox_wgs84(&self, bbox: BoundingBox2D) -> Result<BoundingBox2D> {
        let projector = CoordinateProjector::from_known_srs(
            SpatialReference::new(SpatialReferenceAuthority::Epsg, self.zone.epsg),
            SpatialReference::epsg_4326(),
        )?;
        let coords = projector.project_coordinates(&[bbox.lower_left(), bbox.upper_right()])?;
        Ok(BoundingBox2D::new(coords[0], coords[1])?)
    }
}

#[async_trait]
impl MetaData<GdalLoadingInfo, RasterResultDescriptor> for SentinelS2L2aCogsMetaData {
    async fn loading_info(
        &self,
        query: QueryRectangle,
    ) -> geoengine_operators::util::Result<GdalLoadingInfo> {
        // for reference: https://stacspec.org/STAC-ext-api.html#operation/getSearchSTAC

        let (t_start, t_end) = Self::time_range_request(&query.time_interval)
            .map_err(|_| geoengine_operators::error::Error::LoadingInfo)?;

        let bbox = self
            .bbox_wgs84(query.bbox)
            .map_err(|_| geoengine_operators::error::Error::LoadingInfo)?;

        let params = [
            ("collections[]", "sentinel-s2-l2a-cogs"),
            (
                "bbox",
                &format!(
                    "[{},{},{},{}]", // array-brackets are not used in standard but required here for unknkown reason
                    bbox.lower_left().x,
                    bbox.lower_left().y,
                    bbox.upper_right().x,
                    bbox.upper_right().y
                ),
            ), // TODO: order coordinates depending on projection
            (
                "datetime",
                &format!("{}/{}", t_start.to_rfc3339(), t_end.to_rfc3339()),
            ),
        ];

        debug!("params {:?}", params);

        let result = self.web_request(&params).await?;

        debug!("result {:?}", result);

        Err(geoengine_operators::error::Error::NotImplemented)
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
        })
    }

    fn box_clone(&self) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor> for SentinelS2L2aCogsDataProvider {
    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        let dataset = self
            .datasets
            .get(&dataset)
            .ok_or(geoengine_operators::error::Error::UnknownDatasetId)?;

        Ok(Box::new(SentinelS2L2aCogsMetaData {
            api_url: self.api_url.clone(),
            zone: dataset.zone.clone(),
            band: dataset.band.clone(),
        }))
    }
}

#[async_trait]
impl MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>
    for SentinelS2L2aCogsDataProvider
{
    async fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor> for SentinelS2L2aCogsDataProvider {
    async fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}
