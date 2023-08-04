use crate::api::model::datatypes::{DataId, DataProviderId, LayerId};
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
use gdal::Dataset;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::hashmap;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, CacheTtlSeconds, ContinuousMeasurement, Coordinate2D,
    FeatureDataType, Measurement, RasterQueryRectangle, SpatialPartition2D, TimeInstance,
    TimeInterval, VectorQueryRectangle,
};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterOperator, RasterResultDescriptor, StaticMetaData,
    TypedOperator, VectorColumnInfo, VectorOperator, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetParameters, GdalLoadingInfo, GdalLoadingInfoTemporalSlice,
    GdalMetaDataList, GdalSource, GdalSourceParameters, OgrSource, OgrSourceColumnSpec,
    OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceDurationSpec, OgrSourceErrorSpec,
    OgrSourceParameters, OgrSourceTimeFormat,
};
use geoengine_operators::util::gdal::gdal_open_dataset;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::OnceLock;
use url::Url;

static GEO_FILETYPES: OnceLock<HashMap<String, bool>> = OnceLock::new();

// TODO: change to `LazyLock' once stable
fn init_geo_filetypes() -> HashMap<String, bool> {
    //name:is_raster
    hashmap! {
        "GeoTIFF".to_string() => true,
        "GeoJSON".to_string() => false
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EdrDataProviderDefinition {
    name: String,
    id: DataProviderId,
    #[serde(deserialize_with = "deserialize_base_url")]
    base_url: Url,
    vector_spec: Option<EdrVectorSpec>,
    #[serde(default)]
    cache_ttl: CacheTtlSeconds,
    #[serde(default)]
    discrete_vrs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdrVectorSpec {
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
            vector_spec: self.vector_spec,
            client: Client::new(),
            cache_ttl: self.cache_ttl,
            discrete_vrs: self.discrete_vrs,
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
    client: Client,
    cache_ttl: CacheTtlSeconds,
    discrete_vrs: Vec<String>,
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
    async fn load_collection_by_name(
        &self,
        collection_name: &str,
    ) -> Result<EdrCollectionMetaData> {
        Ok(self
            .client
            .get(
                self.base_url
                    .join(&format!("collections/{collection_name}?f=json"))?,
            )
            .send()
            .await?
            .json()
            .await?)
    }

    async fn load_collection_by_dataid(
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
        let collection_name = edr_id.get_collection_id().map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })?;
        let collection_meta: EdrCollectionMetaData = self
            .load_collection_by_name(collection_name)
            .await
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?;
        Ok((edr_id, collection_meta))
    }

    async fn get_root_collection(
        &self,
        collection_id: &LayerCollectionId,
        options: &LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
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
            .filter(|collection| {
                collection.data_queries.cube.is_some() && collection.extent.spatial.is_some()
            })
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(|collection| {
                if collection.is_raster_file()? || collection.extent.vertical.is_some() {
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

    fn get_raster_parameter_collection(
        &self,
        collection_id: &LayerCollectionId,
        collection_meta: EdrCollectionMetaData,
        options: &LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        let items = collection_meta
            .parameter_names
            .into_keys()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(|parameter_name| {
                if collection_meta.extent.vertical.is_some() {
                    Ok(CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: self.id,
                            collection_id: EdrCollectionId::ParameterOrHeight {
                                collection: collection_meta.id.clone(),
                                parameter: parameter_name.clone(),
                            }
                            .try_into()?,
                        },
                        name: parameter_name,
                        description: String::new(),
                        properties: vec![],
                    }))
                } else {
                    Ok(CollectionItem::Layer(LayerListing {
                        id: ProviderLayerId {
                            provider_id: self.id,
                            layer_id: EdrCollectionId::ParameterOrHeight {
                                collection: collection_meta.id.clone(),
                                parameter: parameter_name.clone(),
                            }
                            .try_into()?,
                        },
                        name: parameter_name,
                        description: String::new(),
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
            name: collection_meta.id.clone(),
            description: format!("Parameters of {}", collection_meta.id),
            items,
            entry_label: None,
            properties: vec![],
        })
    }

    fn get_vector_height_collection(
        &self,
        collection_id: &LayerCollectionId,
        collection_meta: EdrCollectionMetaData,
        options: &LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
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
                            collection: collection_meta.id.clone(),
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
            name: collection_meta.id.clone(),
            description: format!("Height selection of {}", collection_meta.id),
            items,
            entry_label: None,
            properties: vec![],
        })
    }

    fn get_raster_height_collection(
        &self,
        collection_id: &LayerCollectionId,
        collection_meta: EdrCollectionMetaData,
        parameter: &str,
        options: &LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
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
                            collection: collection_meta.id.clone(),
                            parameter: parameter.to_string(),
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
            name: collection_meta.id.clone(),
            description: format!("Height selection of {}", collection_meta.id),
            items,
            entry_label: None,
            properties: vec![],
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
    title: Option<String>,
    description: Option<String>,
    extent: EdrExtents,
    //for paging keys need to be returned in same order every time
    parameter_names: BTreeMap<String, EdrParameter>,
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
            if GEO_FILETYPES
                .get_or_init(init_geo_filetypes)
                .contains_key(format)
            {
                return Ok(format.to_string());
            }
        }
        Err(geoengine_operators::error::Error::DatasetMetaData {
            source: Box::new(EdrProviderError::NoSupportedOutputFormat),
        })
    }

    fn is_raster_file(&self) -> Result<bool, geoengine_operators::error::Error> {
        Ok(*GEO_FILETYPES
            .get_or_init(init_geo_filetypes)
            .get(&self.select_output_format()?)
            .expect("can only return values in map"))
    }

    fn get_vector_download_url(
        &self,
        base_url: &Url,
        height: &str,
        discrete_vrs: &[String],
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
        let z = if height == "default" {
            String::new()
        } else if self.extent.has_discrete_vertical_axis(discrete_vrs) {
            format!("&z={height}")
        } else {
            format!("&z={height}%2F{height}")
        };
        let download_url = format!(
            "/vsicurl_streaming/{}collections/{}/cube?bbox={},{},{},{}{}&datetime={}%2F{}&f={}",
            base_url,
            self.id,
            spatial_extent.bbox[0][0],
            spatial_extent.bbox[0][1],
            spatial_extent.bbox[0][2],
            spatial_extent.bbox[0][3],
            z,
            temporal_extent.interval[0][0],
            temporal_extent.interval[0][1],
            self.select_output_format()?
        );
        let mut layer_name = format!(
            "cube?bbox={},{},{},{}{}&datetime={}%2F{}",
            spatial_extent.bbox[0][0],
            spatial_extent.bbox[0][1],
            spatial_extent.bbox[0][2],
            spatial_extent.bbox[0][3],
            z,
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
        discrete_vrs: &[String],
    ) -> Result<String, geoengine_operators::error::Error> {
        let spatial_extent = self.extent.spatial.as_ref().ok_or_else(|| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(EdrProviderError::MissingSpatialExtent),
            }
        })?;
        let z = if height == "default" {
            String::new()
        } else if self.extent.has_discrete_vertical_axis(discrete_vrs) {
            format!("&z={height}")
        } else {
            format!("&z={height}%2F{height}")
        };
        Ok(format!(
            "/vsicurl_streaming/{}collections/{}/cube?bbox={},{},{},{}{}&datetime={}%2F{}&f={}&parameter-name={}",
            base_url,
            self.id,
            spatial_extent.bbox[0][0],
            spatial_extent.bbox[0][1],
            spatial_extent.bbox[0][2],
            spatial_extent.bbox[0][3],
            z,
            time,
            time,
            self.select_output_format()?,
            parameter_name
        ))
    }

    fn get_vector_result_descriptor(
        &self,
    ) -> Result<VectorResultDescriptor, geoengine_operators::error::Error> {
        let column_map: HashMap<String, VectorColumnInfo> = self
            .parameter_names
            .iter()
            .map(|(parameter_name, parameter_metadata)| {
                let data_type = if let Some(data_type) = parameter_metadata.data_type.as_ref() {
                    data_type.as_str().to_uppercase()
                } else {
                    "FLOAT".to_string()
                };
                match data_type.as_str() {
                    "STRING" => (
                        parameter_name.to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    "INTEGER" => (
                        parameter_name.to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Continuous(ContinuousMeasurement {
                                measurement: parameter_metadata.observed_property.label.clone(),
                                unit: parameter_metadata.unit.as_ref().map(|x| x.symbol.clone()),
                            }),
                        },
                    ),
                    _ => (
                        parameter_name.to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Float,
                            measurement: Measurement::Continuous(ContinuousMeasurement {
                                measurement: parameter_metadata.observed_property.label.clone(),
                                unit: parameter_metadata.unit.as_ref().map(|x| x.symbol.clone()),
                            }),
                        },
                    ),
                }
            })
            .collect();

        Ok(VectorResultDescriptor {
            spatial_reference: SpatialReference::epsg_4326().into(),
            data_type: VectorDataType::MultiPoint,
            columns: column_map,
            time: Some(self.get_time_interval()?),
            bbox: Some(self.get_bounding_box()?),
        })
    }

    fn get_column_spec(&self, vector_spec: EdrVectorSpec) -> OgrSourceColumnSpec {
        let mut int = vec![];
        let mut float = vec![];
        let mut text = vec![];
        let bool = vec![];
        let datetime = vec![];

        for (parameter_name, parameter_metadata) in &self.parameter_names {
            let data_type = if let Some(data_type) = parameter_metadata.data_type.as_ref() {
                data_type.as_str().to_uppercase()
            } else {
                "FLOAT".to_string()
            };
            match data_type.as_str() {
                "STRING" => {
                    text.push(parameter_name.clone());
                }
                "INTEGER" => {
                    int.push(parameter_name.clone());
                }
                _ => {
                    float.push(parameter_name.clone());
                }
            }
        }
        OgrSourceColumnSpec {
            format_specifics: None,
            x: vector_spec.x,
            y: vector_spec.y,
            int,
            float,
            text,
            bool,
            datetime,
            rename: None,
        }
    }

    fn get_ogr_source_ds(
        &self,
        download_url: String,
        layer_name: String,
        vector_spec: EdrVectorSpec,
        cache_ttl: CacheTtlSeconds,
    ) -> OgrSourceDataset {
        OgrSourceDataset {
            file_name: download_url.into(),
            layer_name,
            data_type: Some(VectorDataType::MultiPoint),
            time: OgrSourceDatasetTimeType::Start {
                start_field: vector_spec.t.clone(),
                start_format: OgrSourceTimeFormat::Auto,
                duration: OgrSourceDurationSpec::Zero,
            },
            default_geometry: None,
            columns: Some(self.get_column_spec(vector_spec)),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Abort,
            sql_query: None,
            attribute_query: None,
            cache_ttl,
        }
    }

    fn get_ogr_metadata(
        &self,
        base_url: &Url,
        height: &str,
        vector_spec: EdrVectorSpec,
        cache_ttl: CacheTtlSeconds,
        discrete_vrs: &[String],
    ) -> Result<StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>
    {
        let (download_url, layer_name) =
            self.get_vector_download_url(base_url, height, discrete_vrs)?;
        let omd = self.get_ogr_source_ds(download_url, layer_name, vector_spec, cache_ttl);

        Ok(StaticMetaData {
            loading_info: omd,
            result_descriptor: self.get_vector_result_descriptor()?,
            phantom: Default::default(),
        })
    }

    fn get_raster_result_descriptor(
        &self,
    ) -> Result<RasterResultDescriptor, geoengine_operators::error::Error> {
        let bbox = self.get_bounding_box()?;
        let bbox = SpatialPartition2D::new_unchecked(bbox.upper_left(), bbox.lower_right());

        Ok(RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReferenceOption::SpatialReference(
                SpatialReference::epsg_4326(),
            ),
            measurement: Measurement::Unitless,
            time: Some(self.get_time_interval()?),
            bbox: Some(bbox),
            resolution: None,
        })
    }

    fn get_gdal_loading_info_temporal_slice(
        &self,
        provider: &EdrDataProvider,
        parameter: &str,
        height: &str,
        data_time: TimeInterval,
        current_time: &str,
        dataset: &Dataset,
    ) -> Result<GdalLoadingInfoTemporalSlice, geoengine_operators::error::Error> {
        let rasterband = &dataset.rasterband(1)?;

        Ok(GdalLoadingInfoTemporalSlice {
            time: data_time,
            params: Some(GdalDatasetParameters {
                file_path: self
                    .get_raster_download_url(
                        &provider.base_url,
                        parameter,
                        height,
                        current_time,
                        &provider.discrete_vrs,
                    )?
                    .into(),
                rasterband_channel: 1,
                geo_transform: dataset
                    .geo_transform()
                    .context(crate::error::Gdal)
                    .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                        source: Box::new(e),
                    })?
                    .into(),
                width: rasterband.x_size(),
                height: rasterband.y_size(),
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: None,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: Some(vec![(
                    "GTIFF_HONOUR_NEGATIVE_SCALEY".to_string(),
                    "YES".to_string(),
                )]),
                allow_alphaband_as_mask: false,
                retry: None,
            }),
            cache_ttl: provider.cache_ttl,
        })
    }
}

#[derive(Deserialize)]
struct EdrExtents {
    spatial: Option<EdrSpatialExtent>,
    vertical: Option<EdrVerticalExtent>,
    temporal: Option<EdrTemporalExtent>,
}

impl EdrExtents {
    fn has_discrete_vertical_axis(&self, discrete_vrs: &[String]) -> bool {
        self.vertical
            .as_ref()
            .map_or(false, |val| discrete_vrs.contains(&val.vrs))
    }
}

#[derive(Deserialize)]
struct EdrSpatialExtent {
    bbox: Vec<Vec<f64>>,
}

#[derive(Deserialize)]
struct EdrVerticalExtent {
    values: Vec<String>,
    vrs: String,
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
        let split = s.split('!').collect::<Vec<_>>();

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
            EdrCollectionId::Collection { collection } => format!("collections!{collection}"),
            EdrCollectionId::ParameterOrHeight {
                collection,
                parameter,
            } => format!("collections!{collection}!{parameter}"),
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
            EdrCollectionId::Collection { collection } => format!("collections!{collection}"),
            EdrCollectionId::ParameterOrHeight {
                collection,
                parameter,
            } => format!("collections!{collection}!{parameter}"),
            EdrCollectionId::ParameterAndHeight {
                collection,
                parameter,
                height,
            } => format!("collections!{collection}!{parameter}!{height}"),
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
            EdrCollectionId::Collections => self.get_root_collection(collection_id, &options).await,
            EdrCollectionId::Collection { collection } => {
                let collection_meta = self.load_collection_by_name(&collection).await?;

                if collection_meta.is_raster_file()? {
                    self.get_raster_parameter_collection(collection_id, collection_meta, &options)
                } else if collection_meta.extent.vertical.is_some() {
                    self.get_vector_height_collection(collection_id, collection_meta, &options)
                } else {
                    Err(Error::InvalidLayerCollectionId)
                }
            }
            EdrCollectionId::ParameterOrHeight {
                collection,
                parameter,
            } => {
                let collection_meta = self.load_collection_by_name(&collection).await?;

                if !collection_meta.is_raster_file()? || collection_meta.extent.vertical.is_none() {
                    return Err(Error::InvalidLayerCollectionId);
                }
                self.get_raster_height_collection(
                    collection_id,
                    collection_meta,
                    &parameter,
                    &options,
                )
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

        let collection = self.load_collection_by_name(collection_id).await?;

        let operator = if collection.is_raster_file()? {
            TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters {
                        data: geoengine_datatypes::dataset::NamedData::with_system_provider(
                            self.id.to_string(),
                            id.to_string(),
                        ),
                    },
                }
                .boxed(),
            )
        } else {
            TypedOperator::Vector(
                OgrSource {
                    params: OgrSourceParameters {
                        data: geoengine_datatypes::dataset::NamedData::with_system_provider(
                            self.id.to_string(),
                            id.to_string(),
                        ),
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
        let (edr_id, collection) = self.load_collection_by_dataid(id).await?;

        let height = match edr_id {
            EdrCollectionId::Collection { .. } => "default".to_string(),
            EdrCollectionId::ParameterOrHeight { parameter, .. } => parameter,
            _ => unreachable!(),
        };

        let smd = collection
            .get_ogr_metadata(
                &self.base_url,
                &height,
                vector_spec,
                self.cache_ttl,
                &self.discrete_vrs,
            )
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?;

        Ok(Box::new(smd))
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
        let (edr_id, collection) = self.load_collection_by_dataid(id).await?;

        let (parameter, height) = match edr_id {
            EdrCollectionId::ParameterOrHeight { parameter, .. } => {
                (parameter, "default".to_string())
            }
            EdrCollectionId::ParameterAndHeight {
                parameter, height, ..
            } => (parameter, height),
            _ => unreachable!(),
        };

        let mut params: Vec<GdalLoadingInfoTemporalSlice> = Vec::new();

        if let Some(temporal_extent) = collection.extent.temporal.clone() {
            let mut temporal_values_iter = temporal_extent.values.iter();
            let mut previous_start = temporal_values_iter.next().unwrap();
            let dataset = gdal_open_dataset(
                collection
                    .get_raster_download_url(
                        &self.base_url,
                        &parameter,
                        &height,
                        previous_start,
                        &self.discrete_vrs,
                    )?
                    .as_ref(),
            )?;

            for current_time in temporal_values_iter {
                params.push(collection.get_gdal_loading_info_temporal_slice(
                    self,
                    &parameter,
                    &height,
                    TimeInterval::new_unchecked(
                        TimeInstance::from_str(previous_start).unwrap(),
                        TimeInstance::from_str(current_time).unwrap(),
                    ),
                    previous_start,
                    &dataset,
                )?);
                previous_start = current_time;
            }
            params.push(collection.get_gdal_loading_info_temporal_slice(
                self,
                &parameter,
                &height,
                TimeInterval::new_unchecked(
                    TimeInstance::from_str(previous_start).unwrap(),
                    TimeInstance::from_str(&temporal_extent.interval[0][1]).unwrap(),
                ),
                previous_start,
                &dataset,
            )?);
        } else {
            let dummy_time = "2023-06-06T00:00:00Z";
            let dataset = gdal_open_dataset(
                collection
                    .get_raster_download_url(
                        &self.base_url,
                        &parameter,
                        &height,
                        dummy_time,
                        &self.discrete_vrs,
                    )?
                    .as_ref(),
            )?;
            params.push(collection.get_gdal_loading_info_temporal_slice(
                self,
                &parameter,
                &height,
                TimeInterval::default(),
                dummy_time,
                &dataset,
            )?);
        }

        Ok(Box::new(GdalMetaDataList {
            result_descriptor: collection.get_raster_result_descriptor()?,
            params,
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
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::BufReader};

    use crate::api::model::datatypes::ExternalDataId;
    use futures_util::StreamExt;
    use geoengine_datatypes::{
        primitives::SpatialResolution,
        test_data,
        util::{gdal::hide_gdal_errors, test::TestDefault},
    };
    use geoengine_operators::engine::{
        ChunkByteSize, MockExecutionContext, MockQueryContext, QueryProcessor, WorkflowOperatorPath,
    };

    use super::*;

    #[tokio::test]
    async fn query_data() -> Result<()> {
        hide_gdal_errors();

        let layer_id = "collections!GFS_between-depth!liquid-volumetric-soil-moisture-non-frozen";
        let provider_id = "dc2ddc34-b0d9-4ee0-bf3e-414f01a805ad";

        let mut exe: MockExecutionContext = MockExecutionContext::test_default();

        let def: Box<dyn DataProviderDefinition> = serde_json::from_reader(BufReader::new(
            File::open(test_data!("provider_defs/open_weather.json"))?,
        ))?;

        let provider = def.initialize().await.unwrap();

        let meta: Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> =
            provider
                .meta_data(
                    &DataId::External(ExternalDataId {
                        provider_id: DataProviderId::from_str(provider_id)?,
                        layer_id: LayerId(layer_id.to_owned()),
                    })
                    .into(),
                )
                .await?;

        exe.add_meta_data(
            DataId::External(ExternalDataId {
                provider_id: DataProviderId::from_str(provider_id)?,
                layer_id: LayerId(layer_id.to_owned()),
            })
            .into(),
            geoengine_datatypes::dataset::NamedData::with_system_provider(
                provider_id.to_string(),
                layer_id.to_string(),
            ),
            meta,
        );

        let op = GdalSource {
            params: GdalSourceParameters {
                data: geoengine_datatypes::dataset::NamedData::with_system_provider(
                    provider_id.to_string(),
                    layer_id.to_owned(),
                ),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe)
        .await
        .unwrap();

        let processor = op.query_processor()?.get_u8().unwrap();

        let spatial_bounds = SpatialPartition2D::new((0., 90.).into(), (180., 0.).into()).unwrap();

        let spatial_resolution = SpatialResolution::new_unchecked(1., 1.);
        let query = RasterQueryRectangle {
            spatial_bounds,
            time_interval: TimeInterval::new_unchecked(1_687_867_200_000, 1_688_806_800_000),
            spatial_resolution,
        };

        let ctx = MockQueryContext::new(ChunkByteSize::MAX);

        let tile_stream = processor.query(query, &ctx).await?;
        assert_eq!(tile_stream.count().await, 2);

        Ok(())
    }
}
