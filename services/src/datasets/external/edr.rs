use crate::api::model::datatypes::{DataId, DataProviderId, ExternalDataId, LayerId};
use crate::datasets::listing::ProvenanceOutput;
use crate::error::{Error, Result};
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerCollectionListing,
    LayerListing, ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::util::parsing::deserialize_base_url;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, ContinuousMeasurement, Coordinate2D, FeatureDataType,
    Measurement, RasterQueryRectangle, SpatialPartition2D, TimeInstance, TimeInterval,
    VectorQueryRectangle,
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
use std::str::FromStr;
use url::Url;

lazy_static! {
    static ref GEO_FILETYPES: HashMap<String, bool> = {
        let mut m = HashMap::new();
        //name:is_raster
        m.insert("GeoTIFF".to_string(), true);
        m.insert("GeoJSON".to_string(), false);
        m
    };
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EdrDataProviderDefinition {
    name: String,
    id: DataProviderId,
    #[serde(deserialize_with = "deserialize_base_url")]
    base_url: Url,
    vector_spec: Option<EdrVectorSpec>,
    raster_spec: Option<EdrRasterSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdrVectorSpec {
    x: String,
    y: Option<String>,
    t: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EdrRasterSpec {
    x_pixel_size: f64,
    y_pixel_size: f64,
    width: usize,
    height: usize,
}

#[typetag::serde]
#[async_trait]
impl DataProviderDefinition for EdrDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> Result<Box<dyn DataProvider>> {
        Ok(Box::new(EdrDataProvider {
            id: self.id,
            base_url: self.base_url,
            vector_spec: self.vector_spec,
            raster_spec: self.raster_spec,
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
    vector_spec: Option<EdrVectorSpec>,
    raster_spec: Option<EdrRasterSpec>,
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
    ) -> Result<(EdrCollectionId, EdrCollectionMetaData), geoengine_operators::error::Error> {
        let layer_id = id
            .external()
            .ok_or(Error::InvalidDataId)
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?
            .layer_id;
        let edr_id: EdrCollectionId = EdrCollectionId::from_str(&layer_id.0).map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })?;
        let collection_id = edr_id.get_collection_id().map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })?;

        self.client
            .get(
                self.base_url
                    .join(&format!("collections/{collection_id}?f=json"))
                    .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                        source: Box::new(e),
                    })?,
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
            .map(|item| (edr_id, item))
    }
}

#[derive(Deserialize)]
struct EdrCollectionsMetaData {
    collections: Vec<EdrCollectionMetaData>,
}

#[derive(Deserialize)]
struct EdrCollectionMetaData {
    id: String,
    title: Option<String>,
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

impl EdrCollectionMetaData {
    fn get_time_interval(&self) -> Result<TimeInterval, geoengine_operators::error::Error> {
        let temporal_extent = self.extent.temporal.as_ref().ok_or_else(|| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(EdrProviderError::MissingTemporalExtent),
            }
        })?;

        Ok(TimeInterval::new_unchecked(
            TimeInstance::from_str(&temporal_extent.interval[0][0]).unwrap(),
            TimeInstance::from_str(&temporal_extent.interval[0][1]).unwrap(),
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
        Err(geoengine_operators::error::Error::DatasetMetaData {
            source: Box::new(EdrProviderError::NoSupportedOutputFormat),
        })
    }

    fn is_raster_file(&self) -> Result<bool, geoengine_operators::error::Error> {
        Ok(*GEO_FILETYPES
            .get(&self.select_output_format()?)
            .expect("can only return values in map"))
    }

    fn get_vector_download_url(
        &self,
        base_url: &Url,
        height: &str,
    ) -> Result<(String, String), geoengine_operators::error::Error> {
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
        let download_url = format!(
            "/vsicurl_streaming/{}collections/{}/cube?bbox={},{},{},{}&z={}%2F{}&datetime={}%2F{}&f={}",
            base_url,
            self.id,
            spatial_extent.bbox[0][0],
            spatial_extent.bbox[0][1],
            spatial_extent.bbox[0][2],
            spatial_extent.bbox[0][3],
            height,
            height,
            temporal_extent.interval[0][0],
            temporal_extent.interval[0][1],
            self.select_output_format()?
        );
        let mut layer_name = format!(
            "cube?bbox={},{},{},{}&z={}%2F{}&datetime={}%2F{}",
            spatial_extent.bbox[0][0],
            spatial_extent.bbox[0][1],
            spatial_extent.bbox[0][2],
            spatial_extent.bbox[0][3],
            height,
            height,
            temporal_extent.interval[0][0],
            temporal_extent.interval[0][1]
        );
        if let Some(last_dot_pos) = layer_name.rfind('.') {
            layer_name = layer_name[0..last_dot_pos].to_string();
        }
        Ok((download_url, layer_name))
    }

    fn get_raster_download_url(
        &self,
        base_url: &Url,
        parameter_name: &str,
        height: &str,
        time: &str,
    ) -> Result<String, geoengine_operators::error::Error> {
        let spatial_extent = self.extent.spatial.as_ref().ok_or_else(|| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(EdrProviderError::MissingSpatialExtent),
            }
        })?;
        Ok(format!(
            "/vsicurl_streaming/{}collections/{}/cube?bbox={},{},{},{}&z={}%2F{}&datetime={}%2F{}&f={}&parameter-name={}",
            base_url,
            self.id,
            spatial_extent.bbox[0][0],
            spatial_extent.bbox[0][1],
            spatial_extent.bbox[0][2],
            spatial_extent.bbox[0][3],
            height,
            height,
            time,
            time,
            self.select_output_format()?,
            parameter_name
        ))
    }
}

#[derive(Deserialize)]
struct EdrExtents {
    spatial: Option<EdrSpatialExtent>,
    vertical: Option<EdrVerticalExtent>,
    temporal: Option<EdrTemporalExtent>,
}

#[derive(Deserialize)]
struct EdrSpatialExtent {
    bbox: Vec<Vec<f64>>,
}

#[derive(Deserialize)]
struct EdrVerticalExtent {
    values: Vec<String>,
}

#[derive(Deserialize, Clone)]
struct EdrTemporalExtent {
    interval: Vec<Vec<String>>,
    values: Vec<String>,
}

#[derive(Deserialize)]
struct EdrParameter {
    #[serde(rename = "data-type")]
    data_type: Option<String>,
    unit: Option<EdrUnit>,
    #[serde(rename = "observedProperty")]
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

enum EdrCollectionId {
    Collections,
    Collection {
        collection: String,
    },
    ParameterOrHeight {
        collection: String,
        parameter: String,
    },
    ParameterAndHeight {
        collection: String,
        parameter: String,
        height: String,
    },
}

impl EdrCollectionId {
    fn get_collection_id(&self) -> Result<&String> {
        match self {
            EdrCollectionId::Collections => Err(Error::InvalidLayerId),
            EdrCollectionId::Collection { collection }
            | EdrCollectionId::ParameterOrHeight { collection, .. }
            | EdrCollectionId::ParameterAndHeight { collection, .. } => Ok(collection),
        }
    }
}

impl FromStr for EdrCollectionId {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let split = s.split('/').collect::<Vec<_>>();

        Ok(match *split.as_slice() {
            ["collections"] => EdrCollectionId::Collections,
            ["collections", collection] => EdrCollectionId::Collection {
                collection: collection.to_string(),
            },
            ["collections", collection, parameter] => EdrCollectionId::ParameterOrHeight {
                collection: collection.to_string(),
                parameter: parameter.to_string(),
            },
            ["collections", collection, parameter, height] => EdrCollectionId::ParameterAndHeight {
                collection: collection.to_string(),
                parameter: parameter.to_string(),
                height: height.to_string(),
            },
            _ => return Err(Error::InvalidLayerCollectionId),
        })
    }
}

impl TryFrom<EdrCollectionId> for LayerCollectionId {
    type Error = Error;

    fn try_from(value: EdrCollectionId) -> std::result::Result<Self, Self::Error> {
        let s = match value {
            EdrCollectionId::Collections => "collections".to_string(),
            EdrCollectionId::Collection { collection } => format!("collections/{collection}"),
            EdrCollectionId::ParameterOrHeight {
                collection,
                parameter,
            } => format!("collections/{collection}/{parameter}"),
            EdrCollectionId::ParameterAndHeight { .. } => {
                return Err(Error::InvalidLayerCollectionId)
            }
        };

        Ok(LayerCollectionId(s))
    }
}

impl TryFrom<EdrCollectionId> for LayerId {
    type Error = Error;

    fn try_from(value: EdrCollectionId) -> std::result::Result<Self, Self::Error> {
        let s = match value {
            EdrCollectionId::Collections => return Err(Error::InvalidLayerId),
            EdrCollectionId::Collection { collection } => format!("collections/{collection}"),
            EdrCollectionId::ParameterOrHeight {
                collection,
                parameter,
            } => format!("collections/{collection}/{parameter}"),
            EdrCollectionId::ParameterAndHeight {
                collection,
                parameter,
                height,
            } => format!("collections/{collection}/{parameter}/{height}"),
        };

        Ok(LayerId(s))
    }
}

#[async_trait]
impl LayerCollectionProvider for EdrDataProvider {
    async fn load_layer_collection(
        &self,
        collection_id: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        let edr_id: EdrCollectionId = EdrCollectionId::from_str(&collection_id.0)
            .map_err(|_e| Error::InvalidLayerCollectionId)?;

        match edr_id {
            EdrCollectionId::Collections => {
                let collections: EdrCollectionsMetaData = self
                    .client
                    .get(self.base_url.join("collections?f=json")?)
                    .send()
                    .await?
                    .json()
                    .await?;

                let items = collections
                    .collections
                    .into_iter()
                    .filter(|collection| collection.data_queries.cube.is_some())
                    .skip(options.offset as usize)
                    .take(options.limit as usize)
                    .map(|collection| {
                        if collection.is_raster_file()? {
                            Ok(CollectionItem::Collection(LayerCollectionListing {
                                id: ProviderLayerCollectionId {
                                    provider_id: self.id,
                                    collection_id: EdrCollectionId::Collection {
                                        collection: collection.id.clone(),
                                    }
                                    .try_into()?,
                                },
                                name: collection.title.unwrap_or(collection.id),
                                description: collection.description.unwrap_or(String::new()),
                                properties: vec![],
                            }))
                        } else {
                            Ok(CollectionItem::Layer(LayerListing {
                                id: ProviderLayerId {
                                    provider_id: self.id,
                                    layer_id: EdrCollectionId::Collection {
                                        collection: collection.id.clone(),
                                    }
                                    .try_into()?,
                                },
                                name: collection.title.unwrap_or(collection.id),
                                description: collection.description.unwrap_or(String::new()),
                                properties: vec![],
                            }))
                        }
                    })
                    .collect::<Result<Vec<CollectionItem>>>()?;

                Ok(LayerCollection {
                    id: ProviderLayerCollectionId {
                        provider_id: self.id,
                        collection_id: collection_id.clone(),
                    },
                    name: "EDR".to_owned(),
                    description: "Environmental Data Retrieval".to_owned(),
                    items,
                    entry_label: None,
                    properties: vec![],
                })
            }
            EdrCollectionId::Collection { collection } => {
                let collection_meta: EdrCollectionMetaData = self
                    .client
                    .get(
                        self.base_url
                            .join(&format!("collections/{collection}?f=json"))?,
                    )
                    .send()
                    .await?
                    .json()
                    .await?;

                if collection_meta.is_raster_file()? {
                    let items = collection_meta
                        .parameter_names
                        .into_keys()
                        .skip(options.offset as usize)
                        .take(options.limit as usize)
                        .map(|parameter_name| {
                            Ok(CollectionItem::Collection(LayerCollectionListing {
                                id: ProviderLayerCollectionId {
                                    provider_id: self.id,
                                    collection_id: EdrCollectionId::ParameterOrHeight {
                                        collection: collection.clone(),
                                        parameter: parameter_name.clone(),
                                    }
                                    .try_into()?,
                                },
                                name: parameter_name,
                                description: String::new(),
                                properties: vec![],
                            }))
                        })
                        .collect::<Result<Vec<CollectionItem>>>()?;

                    Ok(LayerCollection {
                        id: ProviderLayerCollectionId {
                            provider_id: self.id,
                            collection_id: collection_id.clone(),
                        },
                        name: collection.clone(),
                        description: format!("Parameters of {collection}"),
                        items,
                        entry_label: None,
                        properties: vec![],
                    })
                } else if collection_meta.extent.vertical.is_some() {
                    let items = collection_meta
                        .extent
                        .vertical
                        .expect("checked before")
                        .values
                        .into_iter()
                        .skip(options.offset as usize)
                        .take(options.limit as usize)
                        .map(|height| {
                            Ok(CollectionItem::Layer(LayerListing {
                                id: ProviderLayerId {
                                    provider_id: self.id,
                                    layer_id: EdrCollectionId::ParameterOrHeight {
                                        collection: collection.clone(),
                                        parameter: height.clone(),
                                    }
                                    .try_into()?,
                                },
                                name: height,
                                description: String::new(),
                                properties: vec![],
                            }))
                        })
                        .collect::<Result<Vec<CollectionItem>>>()?;

                    Ok(LayerCollection {
                        id: ProviderLayerCollectionId {
                            provider_id: self.id,
                            collection_id: collection_id.clone(),
                        },
                        name: collection.clone(),
                        description: format!("Height selection of {collection}"),
                        items,
                        entry_label: None,
                        properties: vec![],
                    })
                } else {
                    Err(Error::InvalidLayerCollectionId)
                }
            }
            EdrCollectionId::ParameterOrHeight {
                collection,
                parameter,
            } => {
                let collection_meta: EdrCollectionMetaData = self
                    .client
                    .get(
                        self.base_url
                            .join(&format!("collections/{collection}?f=json"))?,
                    )
                    .send()
                    .await?
                    .json()
                    .await?;

                if !collection_meta.is_raster_file()? || collection_meta.extent.vertical.is_none() {
                    return Err(Error::InvalidLayerCollectionId);
                }

                let items = collection_meta
                    .extent
                    .vertical
                    .expect("checked before")
                    .values
                    .into_iter()
                    .skip(options.offset as usize)
                    .take(options.limit as usize)
                    .map(|height| {
                        Ok(CollectionItem::Layer(LayerListing {
                            id: ProviderLayerId {
                                provider_id: self.id,
                                layer_id: EdrCollectionId::ParameterAndHeight {
                                    collection: collection.clone(),
                                    parameter: parameter.clone(),
                                    height: height.clone(),
                                }
                                .try_into()?,
                            },
                            name: height,
                            description: String::new(),
                            properties: vec![],
                        }))
                    })
                    .collect::<Result<Vec<CollectionItem>>>()?;

                Ok(LayerCollection {
                    id: ProviderLayerCollectionId {
                        provider_id: self.id,
                        collection_id: collection_id.clone(),
                    },
                    name: collection.clone(),
                    description: format!("Height selection of {collection}"),
                    items,
                    entry_label: None,
                    properties: vec![],
                })
            }
            EdrCollectionId::ParameterAndHeight { .. } => Err(Error::InvalidLayerCollectionId),
        }
    }

    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        EdrCollectionId::Collections.try_into()
    }

    async fn load_layer(&self, id: &LayerId) -> Result<Layer> {
        let edr_id: EdrCollectionId = EdrCollectionId::from_str(&id.0)?;
        let collection_id = edr_id.get_collection_id()?;

        let collection: EdrCollectionMetaData = self
            .client
            .get(
                self.base_url
                    .join(&format!("collections/{collection_id}?f=json"))?,
            )
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
            name: collection.title.unwrap_or(collection.id),
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
        let vector_spec = self.vector_spec.clone().ok_or_else(|| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(EdrProviderError::NoVectorSpecConfigured),
            }
        })?;
        let (edr_id, collection) = self.load_metadata(id).await?;

        // Map column definition
        let mut int = vec![];
        let mut float = vec![];
        let mut text = vec![];
        let bool = vec![];
        let datetime = vec![];
        let mut column_map: HashMap<String, VectorColumnInfo> = HashMap::new();

        for (parameter_name, parameter_metadata) in &collection.parameter_names {
            let data_type = if let Some(data_type) = parameter_metadata.data_type.as_ref() {
                data_type.as_str().to_uppercase()
            } else {
                "FLOAT".to_string()
            };
            match data_type.as_str() {
                "STRING" => {
                    text.push(parameter_name.clone());
                    column_map.insert(
                        parameter_name.to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Default::default(),
                        },
                    );
                }
                "INTEGER" => {
                    int.push(parameter_name.clone());
                    column_map.insert(
                        parameter_name.to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Continuous(ContinuousMeasurement {
                                measurement: parameter_metadata.observed_property.label.clone(),
                                unit: parameter_metadata.unit.as_ref().map(|x| x.symbol.clone()),
                            }),
                        },
                    );
                }
                _ => {
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
            }
        }
        let height = match edr_id {
            EdrCollectionId::Collection { .. } => "0".to_string(),
            EdrCollectionId::ParameterOrHeight { parameter, .. } => parameter,
            _ => unreachable!(),
        };
        let (download_url, layer_name) =
            collection.get_vector_download_url(&self.base_url, &height)?;

        Ok(Box::new(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: download_url.into(),
                layer_name,
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::Start {
                    start_field: vector_spec.t,
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                },
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: vector_spec.x,
                    y: vector_spec.y,
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
        let raster_spec = self.raster_spec.clone().ok_or_else(|| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(EdrProviderError::NoRasterSpecConfigured),
            }
        })?;
        let (edr_id, collection) = self.load_metadata(id).await?;
        let bbox = collection.get_bounding_box()?;
        let bbox = SpatialPartition2D::new_unchecked(bbox.upper_left(), bbox.lower_right());

        let (parameter, height) = match edr_id {
            EdrCollectionId::ParameterOrHeight { parameter, .. } => (parameter, "0".to_string()),
            EdrCollectionId::ParameterAndHeight {
                parameter, height, ..
            } => (parameter, height),
            _ => unreachable!(),
        };

        let mut parts: Vec<GdalLoadingInfoTemporalSlice> = Vec::new();

        if let Some(temporal_extent) = collection.extent.temporal.clone() {
            let mut previous_start: Option<String> = None;

            for elem in &temporal_extent.values {
                if let Some(previous_start_val) = previous_start.clone() {
                    parts.push(GdalLoadingInfoTemporalSlice {
                        time: TimeInterval::new_unchecked(
                            TimeInstance::from_str(&previous_start_val).unwrap(),
                            TimeInstance::from_str(&elem).unwrap(),
                        ),
                        params: Some(GdalDatasetParameters {
                            file_path: collection
                                .get_raster_download_url(
                                    &self.base_url,
                                    &parameter,
                                    &height,
                                    &elem,
                                )?
                                .into(),
                            rasterband_channel: 1,
                            geo_transform: GdalDatasetGeoTransform {
                                origin_coordinate: bbox.upper_left(),
                                x_pixel_size: raster_spec.x_pixel_size,
                                y_pixel_size: raster_spec.y_pixel_size,
                            },
                            width: raster_spec.width,
                            height: raster_spec.height,
                            file_not_found_handling: FileNotFoundHandling::NoData,
                            no_data_value: None,
                            properties_mapping: None,
                            gdal_open_options: None,
                            gdal_config_options: None,
                            allow_alphaband_as_mask: false,
                            retry: None,
                        }),
                    });
                }
                previous_start = Some(elem.clone());
            }
            let previous_start_val =
                previous_start.expect("will be set because at least one time value");

            parts.push(GdalLoadingInfoTemporalSlice {
                time: TimeInterval::new_unchecked(
                    TimeInstance::from_str(&previous_start_val).unwrap(),
                    TimeInstance::from_str(&temporal_extent.interval[0][1]).unwrap(),
                ),
                params: Some(GdalDatasetParameters {
                    file_path: collection
                        .get_raster_download_url(
                            &self.base_url,
                            &parameter,
                            &height,
                            &previous_start_val,
                        )?
                        .into(),
                    rasterband_channel: 1,
                    geo_transform: GdalDatasetGeoTransform {
                        origin_coordinate: bbox.upper_left(),
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
            });
        } else {
            parts.push(GdalLoadingInfoTemporalSlice {
                time: TimeInterval::default(),
                params: Some(GdalDatasetParameters {
                    file_path: collection
                        .get_raster_download_url(
                            &self.base_url,
                            &parameter,
                            &height,
                            "2023-06-06T00:00:00Z", //dummy
                        )?
                        .into(),
                    rasterband_channel: 1,
                    geo_transform: GdalDatasetGeoTransform {
                        origin_coordinate: bbox.upper_left(),
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
            });
        }

        Ok(Box::new(StaticMetaData {
            loading_info: GdalLoadingInfo {
                info: GdalLoadingInfoTemporalSliceIterator::Static {
                    parts: parts.into_iter(),
                },
            },
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReferenceOption::SpatialReference(
                    SpatialReference::epsg_4326(),
                ),
                measurement: Default::default(),
                time: Some(collection.get_time_interval()?),
                bbox: Some(bbox),
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
    NoVectorSpecConfigured,
    NoRasterSpecConfigured,
}
