use crate::api::model::datatypes::{DataId, DataProviderId, ExternalDataId, LayerId};
use crate::datasets::listing::ProvenanceOutput;
use crate::error::{self, Error, Result};
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerListing,
    ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::util::parsing::deserialize_base_url;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::primitives::{
    BoundingBox2D, ContinuousMeasurement, Coordinate2D, FeatureDataType, Measurement,
    RasterQueryRectangle, TimeInstance, TimeInterval, VectorQueryRectangle,
};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterOperator, RasterResultDescriptor, StaticMetaData,
    TypedOperator, VectorColumnInfo, VectorOperator, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalLoadingInfo,
    GdalLoadingInfoTemporalSlice, GdalLoadingInfoTemporalSliceIterator, GdalSource,
    GdalSourceParameters, OgrSource, OgrSourceColumnSpec, OgrSourceDataset,
    OgrSourceDatasetTimeType, OgrSourceDurationSpec, OgrSourceErrorSpec, OgrSourceParameters,
    OgrSourceTimeFormat,
};
use lazy_static::lazy_static;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EdrDataProviderDefinition {
    name: String,
    id: DataProviderId,
    #[serde(deserialize_with = "deserialize_base_url")]
    base_url: Url,
    column_spec: Option<EdrColumnSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdrColumnSpec {
    x: String,
    y: Option<String>,
    t: String,
}

#[typetag::serde]
#[async_trait]
impl DataProviderDefinition for EdrDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> Result<Box<dyn DataProvider>> {
        Ok(Box::new(EdrDataProvider {
            id: self.id,
            base_url: self.base_url,
            column_spec: self.column_spec,
            client: Client::new(),
        }))
    }

    fn type_name(&self) -> &'static str {
        "Environmental Data Retrieval"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        self.id
    }
}

#[derive(Debug)]
pub struct EdrDataProvider {
    id: DataProviderId,
    base_url: Url,
    column_spec: Option<EdrColumnSpec>,
    client: Client,
}

#[async_trait]
impl DataProvider for EdrDataProvider {
    async fn provenance(&self, id: &DataId) -> Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            data: id.clone(),
            provenance: None,
        })
    }
}

impl EdrDataProvider {
    async fn load_metadata(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<EdrCollectionMetaData, geoengine_operators::error::Error> {
        let layer_id = id
            .external()
            .ok_or(Error::InvalidDataId)
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?
            .layer_id;

        self.client
            .get(
                self.base_url
                    .join(&format!("collections/{layer_id}?f=json"))
                    .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                        source: Box::new(e),
                    })?
            )
            .send()
            .await
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?
            .json()
            .await
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })
    }
}

#[derive(Deserialize)]
struct EdrCollectionsMetaData {
    collections: Vec<EdrCollectionMetaData>,
}

#[derive(Deserialize)]
struct EdrCollectionMetaData {
    id: String,
    title: String,
    description: Option<String>,
    extent: EdrExtents,
    parameter_names: HashMap<String, EdrParameter>,
    output_formats: Vec<String>,
    data_queries: EdrDataQueries,
}

#[derive(Deserialize)]
struct EdrDataQueries {
    cube: Option<serde_json::Value>,
}

lazy_static! {
    static ref GEO_FILETYPES: HashMap<String, bool> = {
        let mut m = HashMap::new();
        m.insert("GeoTIFF".to_string(), true);
        m.insert("GeoJSON".to_string(), false);
        m
    };
}

impl EdrCollectionMetaData {
    fn get_time_interval(&self) -> Result<TimeInterval, geoengine_operators::error::Error> {
        let temporal_extent = self.extent.temporal.as_ref().ok_or_else(|| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(EdrProviderError::MissingTemporalExtent),
            }
        })?;

        Ok(TimeInterval::new_unchecked(
            TimeInstance::from_str(temporal_extent.interval[0][0].as_str()).unwrap(),
            TimeInstance::from_str(temporal_extent.interval[0][1].as_str()).unwrap(),
        ))
    }

    fn get_bounding_box(&self) -> Result<BoundingBox2D, geoengine_operators::error::Error> {
        let spatial_extent = self.extent.spatial.as_ref().ok_or_else(|| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(EdrProviderError::MissingSpatialExtent),
            }
        })?;

        Ok(BoundingBox2D::new_unchecked(
            Coordinate2D::new(spatial_extent.bbox[0][0], spatial_extent.bbox[0][1]),
            Coordinate2D::new(spatial_extent.bbox[0][2], spatial_extent.bbox[0][3]),
        ))
    }

    fn select_output_format(&self) -> Result<String, geoengine_operators::error::Error> {
        for format in &self.output_formats {
            if GEO_FILETYPES.contains_key(format) {
                return Ok(format.to_string());
            }
        }
        return Err(geoengine_operators::error::Error::DatasetMetaData {
            source: Box::new(EdrProviderError::NoSupportedOutputFormat),
        });
    }

    fn is_raster_file(&self) -> Result<bool, geoengine_operators::error::Error> {
        Ok(*GEO_FILETYPES
            .get(&self.select_output_format()?)
            .expect("can only return values in map"))
    }

    fn get_download_url(
        &self,
        base_url: &Url,
    ) -> Result<PathBuf, geoengine_operators::error::Error> {
        let vertical_extent = &self.extent.vertical;
        let spatial_extent = self.extent.spatial.as_ref().ok_or_else(|| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(EdrProviderError::MissingSpatialExtent),
            }
        })?;
        let temporal_extent = self.extent.temporal.as_ref().ok_or_else(|| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(EdrProviderError::MissingTemporalExtent),
            }
        })?;

        Ok(format!(
            "/vsicurl/{}collections/{}/cube?bbox={},{},{},{}&z={}%2F{}&datetime={}%2F{}&f={}",
            base_url,
            self.id,
            spatial_extent.bbox[0][0],
            spatial_extent.bbox[0][1],
            spatial_extent.bbox[0][2],
            spatial_extent.bbox[0][3],
            vertical_extent.interval[0][1],
            vertical_extent.interval[0][0],
            temporal_extent.interval[0][0],
            temporal_extent.interval[0][1],
            self.select_output_format()?
        )
        .into())
    }
}

#[derive(Deserialize)]
struct EdrExtents {
    spatial: Option<EdrSpatialExtent>,
    #[serde(default)]
    vertical: EdrVerticalExtent,
    temporal: Option<EdrTemporalExtent>,
}

#[derive(Deserialize)]
struct EdrSpatialExtent {
    bbox: Vec<Vec<f64>>,
}

#[derive(Deserialize)]
struct EdrVerticalExtent {
    interval: Vec<Vec<String>>,
}

impl Default for EdrVerticalExtent {
    fn default() -> Self {
        EdrVerticalExtent {
            //default from OpenWeather
            interval: vec![vec!["300".to_string(), "1000".to_string()]],
        }
    }
}

#[derive(Deserialize)]
struct EdrTemporalExtent {
    interval: Vec<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EdrParameter {
    unit: Option<EdrUnit>,
    observed_property: ObservedProperty,
}

#[derive(Deserialize)]
struct EdrUnit {
    symbol: String,
}

#[derive(Deserialize)]
struct ObservedProperty {
    label: String,
}

#[async_trait]
impl LayerCollectionProvider for EdrDataProvider {
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

        let collections: EdrCollectionsMetaData = self
            .client
            .get(self.base_url.join("collections?f=json")?)
            .send()
            .await?
            .json()
            .await?;

        let items: Vec<_> = collections
            .collections
            .into_iter()
            .filter(|item| item.data_queries.cube.is_some())
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(|item| {
                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: self.id,
                        layer_id: LayerId(item.id),
                    },
                    name: item.title,
                    description: item.description.unwrap_or(String::new()),
                    properties: vec![],
                })
            })
            .collect();

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: self.id,
                collection_id: collection.clone(),
            },
            name: "EDR".to_owned(),
            description: "Environmental Data Retrieval".to_owned(),
            items,
            entry_label: None,
            properties: vec![],
        })
    }

    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId("edr".to_owned()))
    }

    async fn load_layer(&self, id: &LayerId) -> Result<Layer> {
        let collection: EdrCollectionMetaData = self
            .client
            .get(self.base_url.join(&format!("collections/{id}?f=json"))?)
            .send()
            .await?
            .json()
            .await?;

        let operator = if collection.is_raster_file()? {
            TypedOperator::Raster(
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
            )
        } else {
            TypedOperator::Vector(
                OgrSource {
                    params: OgrSourceParameters {
                        data: DataId::External(ExternalDataId {
                            provider_id: self.id,
                            layer_id: id.clone(),
                        })
                        .into(),
                        attribute_projection: None,
                        attribute_filters: None,
                    },
                }
                .boxed(),
            )
        };

        Ok(Layer {
            id: ProviderLayerId {
                provider_id: self.id,
                layer_id: id.clone(),
            },
            name: collection.title,
            description: String::new(),
            workflow: Workflow { operator },
            symbology: None, // TODO
            properties: vec![],
            metadata: HashMap::new(),
        })
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for EdrDataProvider
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
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for EdrDataProvider
{
    async fn meta_data(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let column_spec =
            self.column_spec
                .clone()
                .ok_or_else(|| geoengine_operators::error::Error::DatasetMetaData {
                    source: Box::new(EdrProviderError::NoColumnSpecConfigured),
                })?;
        let collection = self.load_metadata(id).await?;

        // Map column definition
        let int = vec![];
        let mut float = vec![];
        let text = vec![];
        let bool = vec![];
        let datetime = vec![];
        let mut column_map: HashMap<String, VectorColumnInfo> = HashMap::new();

        for (parameter_name, parameter_metadata) in &collection.parameter_names {
            float.push(parameter_name.clone());
            column_map.insert(
                parameter_name.to_string(),
                VectorColumnInfo {
                    data_type: FeatureDataType::Float,
                    measurement: Measurement::Continuous(ContinuousMeasurement {
                        measurement: parameter_metadata.observed_property.label.clone(),
                        unit: parameter_metadata.unit.as_ref().map(|x| x.symbol.clone()),
                    }),
                },
            );
        }

        Ok(Box::new(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: collection.get_download_url(&self.base_url)?,
                layer_name: "EDR".to_string(),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::Start {
                    start_field: column_spec.t,
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                },
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: column_spec.x,
                    y: column_spec.y,
                    int,
                    float,
                    text,
                    bool,
                    datetime,
                    rename: None,
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Abort,
                sql_query: None,
                attribute_query: None,
            },
            result_descriptor: VectorResultDescriptor {
                spatial_reference: SpatialReference::epsg_4326().into(),
                data_type: VectorDataType::MultiPoint,
                columns: column_map,
                time: Some(collection.get_time_interval()?),
                bbox: Some(collection.get_bounding_box()?),
            },
            phantom: Default::default(),
        }))
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for EdrDataProvider
{
    async fn meta_data(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let collection = self.load_metadata(id).await?;

        Ok(Box::new(StaticMetaData {
            loading_info: GdalLoadingInfo {
                info: GdalLoadingInfoTemporalSliceIterator::Static {
                    parts: vec![GdalLoadingInfoTemporalSlice {
                        time: collection.get_time_interval()?,
                        params: Some(GdalDatasetParameters {
                            file_path: collection.get_download_url(&self.base_url)?,
                            rasterband_channel: 0,
                            geo_transform: GdalDatasetGeoTransform {
                                origin_coordinate: Coordinate2D::new(0.0, 0.0),
                                x_pixel_size: 1.0,
                                y_pixel_size: 1.0,
                            },
                            width: 0,
                            height: 0,
                            file_not_found_handling: FileNotFoundHandling::NoData,
                            no_data_value: None,
                            properties_mapping: None,
                            gdal_open_options: None,
                            gdal_config_options: None,
                            allow_alphaband_as_mask: false,
                            retry: None,
                        }),
                    }]
                    .into_iter(),
                },
            },
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::F32,
                spatial_reference: SpatialReferenceOption::SpatialReference(
                    SpatialReference::epsg_4326(),
                ),
                measurement: Default::default(),
                time: Some(collection.get_time_interval()?),
                bbox: None,
                resolution: None,
            },
            phantom: Default::default(),
        }))
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum EdrProviderError {
    MissingVerticalExtent,
    MissingSpatialExtent,
    MissingTemporalExtent,
    NoSupportedOutputFormat,
    NoColumnSpecConfigured,
}
