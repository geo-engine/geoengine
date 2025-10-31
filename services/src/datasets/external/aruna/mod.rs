use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::str::FromStr;

use crate::api::model::services::SECRET_REPLACEMENT;
use crate::contexts::GeoEngineDb;
use crate::datasets::external::aruna::metadata::{DataType, GEMetadata, RasterInfo, VectorInfo};
use crate::datasets::listing::ProvenanceOutput;
use crate::layers::external::{DataProvider, DataProviderDefinition, TypedDataProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerListing,
    ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{
    LayerCollectionId, LayerCollectionProvider, ProviderCapabilities, SearchCapabilities,
};
use crate::workflows::workflow::Workflow;
use aruna_rust_api::api::storage::models::v2::relation::Relation as ArunaRelationEnum;
use aruna_rust_api::api::storage::models::v2::{
    Dataset, InternalRelationVariant, KeyValue, KeyValueVariant, Object, ResourceVariant,
};
use aruna_rust_api::api::storage::models::v2::{
    Relation as ArunaRelationStruct, RelationDirection,
};
use aruna_rust_api::api::storage::services::v2::dataset_service_client::DatasetServiceClient;
use aruna_rust_api::api::storage::services::v2::object_service_client::ObjectServiceClient;
use aruna_rust_api::api::storage::services::v2::project_service_client::ProjectServiceClient;
use aruna_rust_api::api::storage::services::v2::{
    GetDatasetRequest, GetDatasetsRequest, GetDownloadUrlRequest, GetObjectsRequest,
    GetProjectRequest,
};
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::dataset::{DataId, DataProviderId, LayerId};
use geoengine_datatypes::primitives::CacheTtlSeconds;
use geoengine_datatypes::primitives::{
    FeatureDataType, Measurement, RasterQueryRectangle, VectorQueryRectangle,
};
use geoengine_datatypes::raster::{BoundedGrid, GeoTransform, GridShape2D};
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterBandDescriptor, RasterBandDescriptors, RasterOperator,
    RasterResultDescriptor, ResultDescriptor, SpatialGridDescriptor, TimeDescriptor, TypedOperator,
    VectorColumnInfo, VectorOperator, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetParameters, GdalLoadingInfo, GdalLoadingInfoTemporalSlice,
    GdalLoadingInfoTemporalSliceIterator, GdalSource, GdalSourceParameters, OgrSource,
    OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceDurationSpec,
    OgrSourceErrorSpec, OgrSourceParameters, OgrSourceTimeFormat,
};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use tonic::codegen::InterceptedService;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

pub use self::error::ArunaProviderError;

pub mod error;
pub mod metadata;

#[cfg(test)]
#[macro_use]
mod mock_grpc_server;

type Result<T, E = ArunaProviderError> = std::result::Result<T, E>;

const URL_REPLACEMENT: &str = "%URL%";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct ArunaDataProviderDefinition {
    pub id: DataProviderId,
    pub name: String,
    pub description: String,
    pub priority: Option<i16>,
    pub api_url: String,
    pub project_id: String,
    pub api_token: String,
    pub filter_label: String,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
}

#[async_trait::async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for ArunaDataProviderDefinition {
    async fn initialize(self: Box<Self>, _db: D) -> crate::error::Result<Box<dyn DataProvider>> {
        Ok(Box::new(ArunaDataProvider::new(self).await?))
    }

    fn type_name(&self) -> &'static str {
        "Aruna"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        self.id
    }

    fn priority(&self) -> i16 {
        self.priority.unwrap_or(0)
    }

    fn update(&self, new: TypedDataProviderDefinition) -> TypedDataProviderDefinition
    where
        Self: Sized,
    {
        match new {
            TypedDataProviderDefinition::ArunaDataProviderDefinition(mut new) => {
                if new.api_token == SECRET_REPLACEMENT {
                    new.api_token.clone_from(&self.api_token);
                }
                TypedDataProviderDefinition::ArunaDataProviderDefinition(new)
            }
            _ => new,
        }
    }
}

/// Intercepts `gRPC` calls to the core-storage and attaches the authorization token
#[derive(Clone, Debug)]
struct APITokenInterceptor {
    key: AsciiMetadataKey,
    token: AsciiMetadataValue,
}

impl APITokenInterceptor {
    fn new(token: &str) -> Result<APITokenInterceptor> {
        let key = AsciiMetadataKey::from_bytes("Authorization".as_bytes())
            .expect("Key should be convertable to bytes");
        let value = AsciiMetadataValue::try_from(format!("Bearer {token}"))
            .map_err(|source| ArunaProviderError::InvalidAPIToken { source })?;

        Ok(APITokenInterceptor { key, token: value })
    }
}

impl Interceptor for APITokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        request
            .metadata_mut()
            .append(self.key.clone(), self.token.clone());
        Ok(request)
    }
}

/// In the Aruna Object Storage, a geoengine dataset is mapped to a single dataset.
/// The dataset has exactly two objects with one id each.
/// One object is a json that maps directly to `GEMetadata`.
///
#[derive(Debug, PartialEq)]
struct ArunaDatasetObjectIds {
    meta_object: String,
    data_object: String,
}

/// In the Aruna Object Storage, a geoengine dataset is mapped to a single dataset.
/// The dataset has a dataset id and exactly two objects.
/// The meta object is a json that maps directly to `GEMetadata`.
/// The data object contains the actual data.
///
#[derive(Debug, PartialEq)]
struct ArunaDatasetIds {
    dataset: String,
    meta_object: String,
    data_object: String,
}

/// The actual provider implementation. It holds `gRPC` stubs to all relevant
/// API endpoints. Those stubs need to be cloned, because all calls require
/// a mutable self reference. However, according to the docs, cloning
/// is cheap.
#[derive(Debug)]
pub struct ArunaDataProvider {
    name: String,
    description: String,
    id: DataProviderId,
    project_id: String,
    project_stub: ProjectServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    dataset_stub: DatasetServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    object_stub: ObjectServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    label_filter: Option<String>,
    cache_ttl: CacheTtlSeconds,
}

impl ArunaDataProvider {
    /// Creates a new provider from the given definition.
    async fn new(def: Box<ArunaDataProviderDefinition>) -> Result<ArunaDataProvider> {
        let url = def.api_url;
        let timeout = 10;

        let channel = Endpoint::from_str(url.as_str())
            .map_err(|_| ArunaProviderError::InvalidUri { uri_string: url })?
            .connect_timeout(core::time::Duration::from_secs(timeout as u64))
            .connect()
            .await?;

        let interceptor = APITokenInterceptor::new(&def.api_token[..])?;

        let project_stub =
            ProjectServiceClient::with_interceptor(channel.clone(), interceptor.clone());
        let dataset_stub =
            DatasetServiceClient::with_interceptor(channel.clone(), interceptor.clone());

        let object_stub = ObjectServiceClient::with_interceptor(channel, interceptor);

        let label_filter = Some(def.filter_label.to_string());

        Ok(ArunaDataProvider {
            name: def.name,
            description: def.description,
            id: def.id,
            project_id: def.project_id,
            project_stub,
            dataset_stub,
            object_stub,
            label_filter,
            cache_ttl: def.cache_ttl,
        })
    }

    /// Extracts the aruna store id from the given dataset id
    fn dataset_aruna_id(id: &DataId) -> Result<String> {
        match id {
            DataId::External(id) => Ok(id.layer_id.0.clone()),
            DataId::Internal { .. } => Err(ArunaProviderError::InvalidDataId),
        }
    }

    /// Retrieves information for the dataset with the given id.
    async fn get_dataset_info(&self, id: &DataId) -> Result<ArunaDatasetIds> {
        self.get_aruna_dataset_ids(Self::dataset_aruna_id(id)?)
            .await
    }

    /// Retrieves information for the dataset with the given id.
    async fn get_dataset_info_from_layer(&self, id: &LayerId) -> Result<ArunaDatasetIds> {
        self.get_aruna_dataset_ids(id.0.clone()).await
    }

    fn get_outgoing_internal_relation_ids(
        relations: &Vec<ArunaRelationStruct>,
        target_resource_variant: ResourceVariant,
    ) -> Result<Vec<String>> {
        let mut ids = vec![];
        for relation in relations {
            match relation
                .relation
                .as_ref()
                .ok_or(ArunaProviderError::MissingRelation)?
            {
                ArunaRelationEnum::External(_) => {}
                ArunaRelationEnum::Internal(x) => {
                    if x.direction() == RelationDirection::Outbound
                        && x.resource_variant() == target_resource_variant
                    {
                        ids.push(x.resource_id.clone());
                    }
                }
            }
        }

        Ok(ids)
    }

    fn has_filter_label(&self, key_values: &Vec<KeyValue>) -> bool {
        let label = &self.label_filter;

        if let Some(filter) = label {
            for key_value in key_values {
                if key_value.variant() == KeyValueVariant::Label
                    && key_value.key == *filter
                    && key_value.value == *filter
                {
                    return true;
                }
            }
            return false;
        }
        true
    }

    async fn get_verified_datasets(&self, dataset_ids: Vec<String>) -> Result<Vec<Dataset>> {
        let mut dataset_stub = self.dataset_stub.clone();

        let requested_datasets = dataset_stub
            .get_datasets(GetDatasetsRequest { dataset_ids })
            .await?
            .into_inner()
            .datasets;

        let mut verified_datasets = vec![];

        for dataset in requested_datasets {
            if self
                .verify_resource(&dataset.id, &dataset.key_values, dataset.status())
                .is_ok()
            {
                let outgoing_objects = Self::get_outgoing_internal_relation_ids(
                    &dataset.relations,
                    ResourceVariant::Object,
                )
                .ok();
                if let Some(object_ids) = outgoing_objects
                    && self.get_verified_dataset_objects(object_ids).await.is_ok()
                {
                    verified_datasets.push(dataset);
                }
            }
        }

        Ok(verified_datasets)
    }

    async fn get_verified_dataset_objects(
        &self,
        object_ids: Vec<String>,
    ) -> Result<ArunaDatasetObjectIds> {
        let mut object_stub = self.object_stub.clone();

        let requested_objects = object_stub
            .get_objects(GetObjectsRequest { object_ids })
            .await?
            .into_inner()
            .objects;

        let mut verified_objects = vec![];

        for object in requested_objects {
            if self
                .verify_resource(&object.id, &object.key_values, object.status())
                .is_ok()
            {
                verified_objects.push(object);
            }
        }

        let aruna_objects = self.verify_object_hierarchy_and_extract_ids(verified_objects)?;

        Ok(aruna_objects)
    }

    fn verify_resource(
        &self,
        resource_id: &str,
        key_values: &Vec<KeyValue>,
        status: aruna_rust_api::api::storage::models::v2::Status,
    ) -> Result<()> {
        if !self.has_filter_label(key_values) {
            return Err(ArunaProviderError::MissingLabel {
                resource_id: resource_id.to_owned(),
            });
        }
        if status != aruna_rust_api::api::storage::models::v2::Status::Available {
            return Err(ArunaProviderError::ResourceNotAvailable {
                resource_id: resource_id.to_owned(),
            });
        }
        Ok(())
    }

    fn verify_object_hierarchy_and_extract_ids(
        &self,
        mut aruna_objects: Vec<Object>,
    ) -> Result<ArunaDatasetObjectIds> {
        if aruna_objects.len() != 2 {
            return Err(ArunaProviderError::UnexpectedObjectHierarchy);
        }

        for object in &aruna_objects {
            self.verify_resource(&object.id, &object.key_values, object.status())?;
        }

        let object_1 = aruna_objects
            .pop()
            .expect("There should be two Objects in the Aruna Dataset");
        let object_2 = aruna_objects
            .pop()
            .expect("There should be two Objects in the Aruna Dataset");
        let meta_object_id;
        let data_object_id;

        if Self::is_metadata(&object_1, &object_2.id)? {
            meta_object_id = object_1.id;
            data_object_id = object_2.id;
        } else if Self::is_metadata(&object_2, &object_1.id)? {
            meta_object_id = object_2.id;
            data_object_id = object_1.id;
        } else {
            return Err(ArunaProviderError::MissingMetaObject);
        }

        Ok(ArunaDatasetObjectIds {
            meta_object: meta_object_id,
            data_object: data_object_id,
        })
    }

    fn is_metadata(meta_candidate: &Object, data_candidate_id: &String) -> Result<bool> {
        for relation in &meta_candidate.relations {
            match relation
                .relation
                .as_ref()
                .ok_or(ArunaProviderError::MissingRelation)?
            {
                ArunaRelationEnum::External(_) => {}
                ArunaRelationEnum::Internal(x) => {
                    if x.defined_variant() == InternalRelationVariant::Metadata
                        && x.direction() == RelationDirection::Outbound
                        && x.resource_variant() == ResourceVariant::Object
                        && x.resource_id == *data_candidate_id
                    {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    /// Retrieves all ids in the aruna object storage for the given `dataset_id`.
    async fn get_aruna_dataset_ids(&self, dataset_id: String) -> Result<ArunaDatasetIds> {
        let mut dataset_stub = self.dataset_stub.clone();

        let dataset = dataset_stub
            .get_dataset(GetDatasetRequest {
                dataset_id: dataset_id.clone(),
            })
            .await?
            .into_inner()
            .dataset
            .ok_or(ArunaProviderError::MissingDataset)?;

        self.verify_resource(&dataset.id, &dataset.key_values, dataset.status())?;

        let dataset_relations = dataset.relations;

        let object_ids =
            Self::get_outgoing_internal_relation_ids(&dataset_relations, ResourceVariant::Object)?;

        let aruna_objects = self.get_verified_dataset_objects(object_ids).await?;

        Ok(ArunaDatasetIds {
            dataset: dataset_id,
            meta_object: aruna_objects.meta_object,
            data_object: aruna_objects.data_object,
        })
    }

    async fn get_dataset_overview(&self, aruna_dataset_ids: &ArunaDatasetIds) -> Result<Dataset> {
        let mut dataset_stub = self.dataset_stub.clone();

        let dataset = dataset_stub
            .get_dataset(GetDatasetRequest {
                dataset_id: aruna_dataset_ids.dataset.clone(),
            })
            .await?
            .into_inner()
            .dataset
            .ok_or(ArunaProviderError::MissingDataset)?;

        self.verify_resource(&dataset.id, &dataset.key_values, dataset.status())?;

        Ok(dataset)
    }

    /// Extracts the geoengine metadata from a dataset in the Aruna Object Storage
    async fn get_metadata(&self, aruna_dataset_ids: &ArunaDatasetIds) -> Result<GEMetadata> {
        let mut object_stub = self.object_stub.clone();

        let download_url = object_stub
            .get_download_url(GetDownloadUrlRequest {
                object_id: aruna_dataset_ids.meta_object.clone(),
            })
            .await?
            .into_inner()
            .url;

        let data_get_response = reqwest::Client::new().get(download_url).send().await?;

        if let reqwest::StatusCode::OK = data_get_response.status() {
            let json = data_get_response.json::<GEMetadata>().await?;
            Ok(json)
        } else {
            Err(ArunaProviderError::MissingArunaMetaData)
        }
    }

    /// Creates a result descriptor for vector data
    fn create_single_vector_file_result_descriptor(
        crs: SpatialReferenceOption,
        info: &VectorInfo,
    ) -> VectorResultDescriptor {
        let columns = info
            .attributes
            .iter()
            .map(|a| {
                (
                    a.name.clone(),
                    VectorColumnInfo {
                        data_type: a.r#type,
                        measurement: Measurement::Unitless, // TODO: get measurement
                    },
                )
            })
            .collect::<HashMap<String, VectorColumnInfo>>();

        VectorResultDescriptor {
            data_type: info.vector_type,
            spatial_reference: crs,
            columns,
            time: info.time,
            bbox: info.bbox,
        }
    }

    /// Creates a result descriptor for raster data
    fn create_single_raster_file_result_descriptor(
        crs: SpatialReferenceOption,
        info: &RasterInfo,
    ) -> geoengine_operators::util::Result<RasterResultDescriptor> {
        let shape = GridShape2D::new_2d(info.width, info.height).bounding_box();
        let geo_transform = GeoTransform::try_from(info.geo_transform)?;

        Ok(RasterResultDescriptor {
            data_type: info.data_type,
            spatial_reference: crs,
            time: TimeDescriptor::new_irregular(Some(info.time_interval)),
            spatial_grid: SpatialGridDescriptor::source_from_parts(geo_transform, shape),
            bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                "band".into(),
                info.measurement
                    .as_ref()
                    .map_or(Measurement::Unitless, Clone::clone),
            )])?,
        })
    }

    /// Creates the loading template for vector files. This is basically a loading
    /// info with a placeholder for the download-url. It will be replaced with
    /// a concrete url on every call to `MetaData.loading_info()`.
    /// This is required, since download links from the core-storage are only valid
    /// for 15 minutes.
    fn vector_loading_template(
        vi: &VectorInfo,
        rd: &VectorResultDescriptor,
        cache_ttl: CacheTtlSeconds,
    ) -> OgrSourceDataset {
        let data_type = match rd.data_type {
            VectorDataType::Data => None,
            x => Some(x),
        };

        // Map column definition
        let mut int = vec![];
        let mut float = vec![];
        let mut text = vec![];
        let mut bool = vec![];
        let mut datetime = vec![];

        for (k, v) in rd.columns.iter().map(|(name, info)| (name, info.data_type)) {
            match v {
                FeatureDataType::Category | FeatureDataType::Int => int.push(k.to_string()),
                FeatureDataType::Float => float.push(k.to_string()),
                FeatureDataType::Text => text.push(k.to_string()),
                FeatureDataType::Bool => bool.push(k.to_string()),
                FeatureDataType::DateTime => datetime.push(k.to_string()),
            }
        }

        let link = format!("/vsicurl_streaming/{URL_REPLACEMENT}");

        let column_spec = OgrSourceColumnSpec {
            format_specifics: None,
            x: String::new(),
            y: None,
            int,
            float,
            text,
            bool,
            datetime,
            rename: None,
        };

        let time = match &vi.temporal_extend {
            Some(metadata::TemporalExtend::Instant { attribute }) => {
                OgrSourceDatasetTimeType::Start {
                    start_field: attribute.clone(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                }
            }
            Some(metadata::TemporalExtend::Interval {
                attribute_start,
                attribute_end,
            }) => OgrSourceDatasetTimeType::StartEnd {
                start_field: attribute_start.clone(),
                start_format: OgrSourceTimeFormat::Auto,
                end_field: attribute_end.clone(),
                end_format: OgrSourceTimeFormat::Auto,
            },
            Some(metadata::TemporalExtend::Duration {
                attribute_start,
                attribute_duration,
                unit: _,
            }) => OgrSourceDatasetTimeType::StartDuration {
                start_field: attribute_start.clone(),
                start_format: OgrSourceTimeFormat::Auto,
                duration_field: attribute_duration.clone(),
            },
            None => OgrSourceDatasetTimeType::None,
        };

        OgrSourceDataset {
            file_name: PathBuf::from(link),
            layer_name: vi.layer_name.clone(),
            data_type,
            time,
            default_geometry: None,
            columns: Some(column_spec),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Abort,
            sql_query: None,
            attribute_query: None,
            cache_ttl,
        }
    }

    /// Creates the loading template for raster files. This is basically a loading
    /// info with a placeholder for the download-url. It will be replaced with
    /// a concrete url on every call to `MetaData.loading_info()`.
    /// This is required, since download links from the core-storage are only valid
    /// for 15 minutes.
    fn raster_loading_template(info: &RasterInfo, cache_ttl: CacheTtlSeconds) -> GdalLoadingInfo {
        let part = GdalLoadingInfoTemporalSlice {
            time: info.time_interval,
            params: Some(GdalDatasetParameters {
                file_path: PathBuf::from(format!("/vsicurl_streaming/{URL_REPLACEMENT}")),
                rasterband_channel: info.rasterband_channel,
                geo_transform: info.geo_transform,
                width: info.width,
                height: info.height,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: None,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: true,
                retry: None,
            }),
            cache_ttl,
        };

        // FIXME: We need more information about the datasets. One way would be to store the time information as metadata in Aruna itself.
        GdalLoadingInfo::new_no_known_time_bounds(GdalLoadingInfoTemporalSliceIterator::Static {
            parts: vec![part].into_iter(),
        })
    }
}

#[async_trait::async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for ArunaDataProvider
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> geoengine_operators::util::Result<
        Box<
            dyn MetaData<
                    MockDatasetDataSourceLoadingInfo,
                    VectorResultDescriptor,
                    VectorQueryRectangle,
                >,
        >,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[async_trait::async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for ArunaDataProvider
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
    > {
        let id: DataId = id.clone();

        let aruna_dataset_ids = self.get_dataset_info(&id).await.map_err(|e| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            }
        })?;

        let meta_data = self.get_metadata(&aruna_dataset_ids).await.map_err(|e| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            }
        })?;

        match meta_data.data_type {
            DataType::SingleVectorFile(info) => {
                let result_descriptor =
                    Self::create_single_vector_file_result_descriptor(meta_data.crs.into(), &info);
                let template =
                    Self::vector_loading_template(&info, &result_descriptor, self.cache_ttl);

                let res = ArunaMetaData {
                    object_id: aruna_dataset_ids.data_object,
                    template,
                    result_descriptor,
                    _phantom: Default::default(),
                    object_stub: self.object_stub.clone(),
                };
                Ok(Box::new(res))
            }
            DataType::SingleRasterFile(_) => Err(geoengine_operators::error::Error::InvalidType {
                found: meta_data.data_type.to_string(),
                expected: "SingleVectorFile".to_string(),
            }),
        }
    }
}

#[async_trait::async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for ArunaDataProvider
{
    async fn meta_data(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
    > {
        let id: DataId = id.clone();

        let aruna_dataset_ids = self.get_dataset_info(&id).await.map_err(|e| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            }
        })?;

        let meta_data = self.get_metadata(&aruna_dataset_ids).await.map_err(|e| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            }
        })?;

        match &meta_data.data_type {
            DataType::SingleRasterFile(info) => {
                let result_descriptor =
                    Self::create_single_raster_file_result_descriptor(meta_data.crs.into(), info)?;
                let template = Self::raster_loading_template(info, self.cache_ttl);

                let res = ArunaMetaData {
                    object_id: aruna_dataset_ids.data_object,
                    template,
                    result_descriptor,
                    _phantom: Default::default(),
                    object_stub: self.object_stub.clone(),
                };
                Ok(Box::new(res))
            }
            DataType::SingleVectorFile(_) => Err(geoengine_operators::error::Error::InvalidType {
                found: meta_data.data_type.to_string(),
                expected: "SingleRasterFile".to_string(),
            }),
        }
    }
}

#[async_trait::async_trait]
impl DataProvider for ArunaDataProvider {
    async fn provenance(&self, id: &DataId) -> crate::error::Result<ProvenanceOutput> {
        let aruna_dataset_ids = self.get_dataset_info(id).await?;
        let metadata = self.get_metadata(&aruna_dataset_ids).await?;

        Ok(ProvenanceOutput {
            data: id.clone(),
            provenance: metadata.provenance,
        })
    }
}

#[async_trait::async_trait]
impl LayerCollectionProvider for ArunaDataProvider {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            listing: true,
            search: SearchCapabilities::none(),
        }
    }

    fn name(&self) -> &str {
        self.name.as_ref()
    }

    fn description(&self) -> &str {
        self.description.as_ref()
    }

    async fn load_layer_collection(
        &self,
        collection: &LayerCollectionId,
        _options: LayerCollectionListOptions,
    ) -> crate::error::Result<LayerCollection> {
        ensure!(
            *collection == self.get_root_layer_collection_id().await?,
            crate::error::UnknownLayerCollectionId {
                id: collection.clone()
            }
        );

        let mut project_stub = self.project_stub.clone();

        let project = project_stub
            .get_project(GetProjectRequest {
                project_id: self.project_id.clone(),
            })
            .await
            .map_err(|source| ArunaProviderError::TonicStatus { source })?
            .into_inner()
            .project
            .ok_or(ArunaProviderError::MissingProject)?;

        self.verify_resource(&project.id, &project.key_values, project.status())?;

        let dataset_ids =
            Self::get_outgoing_internal_relation_ids(&project.relations, ResourceVariant::Dataset)?;

        let items = self
            .get_verified_datasets(dataset_ids)
            .await?
            .into_iter()
            .map(|col| {
                CollectionItem::Layer(LayerListing {
                    r#type: Default::default(),
                    id: ProviderLayerId {
                        provider_id: self.id,
                        layer_id: LayerId(col.id),
                    },
                    name: col.name,
                    description: col.description,
                    properties: vec![],
                })
            })
            .collect();

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: self.id,
                collection_id: collection.clone(),
            },
            name: self.name.clone(),
            description: "NFDI".to_string(),
            items,
            entry_label: None,
            properties: vec![],
        })
    }

    async fn get_root_layer_collection_id(&self) -> crate::error::Result<LayerCollectionId> {
        Ok(LayerCollectionId("root".to_string()))
    }

    async fn load_layer(&self, id: &LayerId) -> crate::error::Result<Layer> {
        let aruna_dataset_ids = self.get_dataset_info_from_layer(id).await?;

        let dataset = self.get_dataset_overview(&aruna_dataset_ids).await?;
        let meta_data = self.get_metadata(&aruna_dataset_ids).await?;

        let operator = match meta_data.data_type {
            DataType::SingleVectorFile(_) => TypedOperator::Vector(
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
            ),
            DataType::SingleRasterFile(_) => TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters::new(
                        geoengine_datatypes::dataset::NamedData::with_system_provider(
                            self.id.to_string(),
                            id.to_string(),
                        ),
                    ),
                }
                .boxed(),
            ),
        };

        Ok(Layer {
            id: ProviderLayerId {
                provider_id: self.id,
                layer_id: id.clone(),
            },
            name: dataset.name,
            description: dataset.description,
            workflow: Workflow { operator },
            symbology: None,
            properties: vec![],
            metadata: HashMap::new(),
        })
    }
}

/*
 * Internal structures
 */

/// This trait models expiring download links as used in the core storage.
trait ExpiringDownloadLink {
    /// This function instantiates the implementor with the given fresh download link.
    fn new_link(&self, url: String) -> std::result::Result<Self, geoengine_operators::error::Error>
    where
        Self: Sized;
}

impl ExpiringDownloadLink for OgrSourceDataset {
    fn new_link(
        &self,
        url: String,
    ) -> std::result::Result<Self, geoengine_operators::error::Error> {
        let path = self.file_name.to_str().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Could not parse original path as string {}",
                    self.file_name.display()
                ),
            )
        })?;
        let new_path = PathBuf::from(path.replace(URL_REPLACEMENT, url.as_str()));

        Ok(Self {
            file_name: new_path,
            layer_name: self.layer_name.clone(),
            data_type: self.data_type,
            time: self.time.clone(),
            default_geometry: self.default_geometry.clone(),
            columns: self.columns.clone(),
            force_ogr_time_filter: self.force_ogr_time_filter,
            force_ogr_spatial_filter: self.force_ogr_spatial_filter,
            on_error: self.on_error,
            sql_query: self.sql_query.clone(),
            attribute_query: self.attribute_query.clone(),
            cache_ttl: self.cache_ttl,
        })
    }
}

impl ExpiringDownloadLink for GdalLoadingInfo {
    #[allow(clippy::needless_collect)]
    fn new_link(
        &self,
        url: String,
    ) -> std::prelude::rust_2015::Result<Self, geoengine_operators::error::Error>
    where
        Self: Sized,
    {
        match &self.info {
            GdalLoadingInfoTemporalSliceIterator::Static { parts }
                if parts.as_slice().len() == 1 =>
            {
                let new_parts = parts
                    .as_slice()
                    .iter()
                    .map(|part| {
                        let mut new_part = part.clone();
                        if let Some(params) = new_part.params.as_mut() {
                            params.file_path = PathBuf::from(
                                params
                                    .file_path
                                    .to_string_lossy()
                                    .as_ref()
                                    .replace(URL_REPLACEMENT, url.as_str()),
                            );
                        }
                        new_part
                    })
                    .collect::<std::vec::Vec<_>>();

                Ok(Self::new_no_known_time_bounds(
                    // FIXME: someone with more detailed insights into the provider needs to add the time bounds
                    GdalLoadingInfoTemporalSliceIterator::Static {
                        parts: new_parts.into_iter(),
                    },
                ))
            }
            _ => Err(geoengine_operators::error::Error::InvalidType {
                found: "GdalLoadingInfoPartIterator::Dynamic".to_string(),
                expected: "GdalLoadingInfoPartIterator::Static".to_string(),
            }),
        }
    }
}

/// This is the metadata for datasets retrieved from the aruna object storage.
/// It stores an object-id, for which to generate new download links each
/// time the `load_info()` function is called.
#[derive(Clone, Debug)]
struct ArunaMetaData<L, R, Q>
where
    L: Debug + Clone + Send + Sync + ExpiringDownloadLink + 'static,
    R: Debug + Send + Sync + 'static + ResultDescriptor,
    Q: Debug + Clone + Send + Sync + 'static,
{
    result_descriptor: R,
    object_id: String,
    template: L,
    object_stub: ObjectServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    _phantom: PhantomData<Q>,
}

#[async_trait::async_trait]
impl<L, R, Q> MetaData<L, R, Q> for ArunaMetaData<L, R, Q>
where
    L: Debug + Clone + Send + Sync + ExpiringDownloadLink + 'static,
    R: Debug + Send + Sync + 'static + ResultDescriptor,
    Q: Debug + Clone + Send + Sync + 'static,
{
    async fn loading_info(&self, _query: Q) -> geoengine_operators::util::Result<L> {
        let mut object_stub = self.object_stub.clone();

        let url = object_stub
            .get_download_url(GetDownloadUrlRequest {
                object_id: self.object_id.clone(),
            })
            .await
            .map_err(|source| ArunaProviderError::TonicStatus { source })
            .map_err(|source| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(source),
            })?
            .into_inner()
            .url;
        self.template.new_link(url)
    }

    async fn result_descriptor(&self) -> geoengine_operators::util::Result<R> {
        Ok(self.result_descriptor.clone())
    }

    fn box_clone(&self) -> Box<dyn MetaData<L, R, Q>> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::str::FromStr;
    use std::task::Poll;

    use aruna_rust_api::api::storage::models::v2::Relation as ArunaRelationStruct;
    use aruna_rust_api::api::storage::models::v2::relation::Relation as ArunaRelationDirection;
    use aruna_rust_api::api::storage::models::v2::{
        DataClass, Dataset, InternalRelation, InternalRelationVariant, KeyValue, KeyValueVariant,
        Object, Project, RelationDirection, ResourceVariant, Status,
    };
    use aruna_rust_api::api::storage::services::v2::{
        GetDatasetRequest, GetDatasetResponse, GetDatasetsRequest, GetDatasetsResponse,
        GetDownloadUrlRequest, GetDownloadUrlResponse, GetObjectsRequest, GetObjectsResponse,
        GetProjectRequest, GetProjectResponse,
    };
    use futures::StreamExt;
    use httptest::responders::status_code;
    use httptest::{Expectation, Server, responders};
    use serde_json::{Value, json};
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;
    use tonic::Code;
    use tonic::codegen::http::Request;
    use tonic::codegen::{Body, Service, http};
    use tonic::transport::server::Router;

    use geoengine_datatypes::collections::{FeatureCollectionInfos, MultiPointCollection};
    use geoengine_datatypes::dataset::{DataId, DataProviderId, ExternalDataId, LayerId};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, CacheTtlSeconds, ColumnSelection, TimeInterval, VectorQueryRectangle,
    };
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{
        MetaData, MetaDataProvider, MockExecutionContext, QueryProcessor,
        TypedVectorQueryProcessor, VectorOperator, VectorResultDescriptor, WorkflowOperatorPath,
    };
    use geoengine_operators::source::{OgrSource, OgrSourceDataset, OgrSourceParameters};

    use crate::datasets::external::aruna::metadata::GEMetadata;
    use crate::datasets::external::aruna::mock_grpc_server::MockGRPCServer;
    use crate::datasets::external::aruna::mock_grpc_server::{
        InfallibleHttpResponseFuture, MapResponseService,
    };
    use crate::datasets::external::aruna::{
        ArunaDataProvider, ArunaDataProviderDefinition, ArunaDatasetIds, ArunaProviderError,
        ExpiringDownloadLink,
    };
    use crate::layers::external::DataProvider;
    use crate::layers::layer::LayerCollectionListOptions;
    use crate::layers::listing::LayerCollectionProvider;

    generate_mapping_grpc_service!(
        "aruna.api.storage.services.v2.ObjectService",
        MockMapObjectService,
        "/aruna.api.storage.services.v2.ObjectService/GetDownloadURL",
        GetDownloadUrlRequest,
        GetDownloadUrlResponse,
        download_map,
        F,
        String,
        id_extractor_1,
        "/aruna.api.storage.services.v2.ObjectService/GetObjects",
        GetObjectsRequest,
        GetObjectsResponse,
        id_map,
        G,
        Vec<String>,
        id_extractor_2,
    );

    generate_mapping_grpc_service!(
        "aruna.api.storage.services.v2.DatasetService",
        MockDatasetMapService,
        "/aruna.api.storage.services.v2.DatasetService/GetDataset",
        GetDatasetRequest,
        GetDatasetResponse,
        dataset,
        F,
        String,
        dataset_extractor,
        "/aruna.api.storage.services.v2.DatasetService/GetDatasets",
        GetDatasetsRequest,
        GetDatasetsResponse,
        datasets_overview,
        G,
        Vec<String>,
        datasets_extractor,
    );

    generate_mapping_grpc_service!(
        "aruna.api.storage.services.v2.ProjectService",
        MockProjectMapService,
        "/aruna.api.storage.services.v2.ProjectService/GetProject",
        GetProjectRequest,
        GetProjectResponse,
        project,
        F,
        String,
        project_extractor,
    );

    const PROVIDER_ID: &str = "86a7f7ce-1bab-4ce9-a32b-172c0f958ee0";
    const TOKEN: &str = "DUMMY_TOKEN";

    const PROJECT_ID: &str = "PROJECT_ID";
    const PROJECT_NAME: &str = "PROJECT_NAME";
    const PROJECT_DESCRIPTION: &str = "PROJECT_DESCRIPTION";

    const DATASET_ID: &str = "DATASET_ID";
    const DATASET_NAME: &str = "DATASET_NAME";
    const DATASET_DESCRIPTION: &str = "DATASET_DESCRIPTION";

    const META_OBJECT_ID: &str = "META_ID";
    const META_OBJECT_NAME: &str = "META.json";

    const DATA_OBJECT_ID: &str = "DATA_ID";
    const DATA_OBJECT_NAME: &str = "DATA.fgb";

    const FILTER_LABEL: &str = "geoengine";

    async fn new_provider_with_url(url: String) -> ArunaDataProvider {
        let def = ArunaDataProviderDefinition {
            id: DataProviderId::from_str(PROVIDER_ID).unwrap(),
            api_token: TOKEN.to_string(),
            api_url: url,
            project_id: PROJECT_ID.to_string(),
            name: "NFDI".to_string(),
            description: "Access to NFDI data stored in Aruna".to_string(),
            priority: Some(123),
            filter_label: FILTER_LABEL.to_string(),
            cache_ttl: Default::default(),
        };
        ArunaDataProvider::new(Box::new(def)).await.unwrap()
    }

    pub(crate) fn vector_meta_data() -> Value {
        serde_json::json!({
            "crs":"EPSG:4326",
            "dataType":{
                "singleVectorFile":{
                    "vectorType":"MultiPoint",
                    "layerName":"points",
                        "attributes":[{
                            "name":"num",
                            "type":"int"
                        },{
                            "name":"txt",
                            "type":"text"
                        }],
                    "temporalExtend":null
                }
            },
            "provenance": [
                {
                    "citation":"Test",
                    "license":"MIT",
                    "uri":"http://geoengine.io"
                }
            ]
        })
    }

    pub(crate) fn raster_meta_data() -> Value {
        serde_json::json!({
            "crs":"EPSG:4326",
            "dataType":{
                "singleRasterFile":{
                    "dataType":"U8",
                    "noDataValue":0.0,
                    "rasterbandChannel":1,
                    "width":3600,
                    "height":1800,
                    "geoTransform":{
                        "originCoordinate":{
                            "x":-180.0,
                            "y":90.0
                        },
                        "xPixelSize":0.1,
                        "yPixelSize":-0.1
                    }
                }
            },
            "provenance": [
                {
                    "citation":"Test",
                    "license":"MIT",
                    "uri":"http://geoengine.io"
                }
            ]
        })
    }

    struct ArunaMockServer {
        _download_server: Option<Server>,
        _grpc_server: MockGRPCServer,
        provider: ArunaDataProvider,
        dataset_ids: ArunaDatasetIds,
    }

    fn default_ge_filter_label() -> KeyValue {
        KeyValue {
            key: FILTER_LABEL.to_string(),
            value: FILTER_LABEL.to_string(),
            variant: i32::from(KeyValueVariant::Label),
        }
    }

    #[derive(Clone)]
    enum ObjectType {
        Json,
        Csv,
    }

    #[derive(Clone)]
    #[allow(clippy::struct_excessive_bools)]
    struct DownloadableObject {
        id: String,
        filename: String,
        object_type: ObjectType,
        meta: bool,
        link_meta: bool,
        labeled: bool,
        available: bool,
        content: Vec<u8>,
        content_length: i64,
        expected_downloads: usize,
        url: Option<String>,
    }

    impl DownloadableObject {
        fn set_url(&mut self, server_path: &str) {
            self.url = Some(format!("{server_path}/{}", self.filename));
        }
    }

    impl From<DownloadableObject> for Object {
        fn from(value: DownloadableObject) -> Self {
            let relations = if value.link_meta {
                vec![ArunaRelationStruct {
                    relation: Some(ArunaRelationDirection::Internal(default_meta_relation(
                        value.meta,
                    ))),
                }]
            } else {
                vec![]
            };
            let key_values = if value.labeled {
                vec![default_ge_filter_label()]
            } else {
                vec![]
            };
            let status = if value.available {
                i32::from(Status::Available)
            } else {
                i32::from(Status::Initializing)
            };

            Object {
                id: value.id.clone(),
                name: value.filename.clone(),
                title: value.filename.clone(),
                description: String::new(),
                key_values,
                relations,
                content_len: value.content_length,
                data_class: i32::from(DataClass::Private),
                created_at: None,
                created_by: "Someone".to_string(),
                authors: vec![],
                status,
                dynamic: false,
                endpoints: vec![],
                hashes: vec![],
                metadata_license_tag: "Some Meta License".to_string(),
                data_license_tag: "Some Data License".to_string(),
                rule_bindings: vec![],
            }
        }
    }

    fn json_meta_object(value: &Value, expected_downloads: usize) -> DownloadableObject {
        let vector_data = value.to_string().into_bytes();
        let vector_length = vector_data.len();
        DownloadableObject {
            id: META_OBJECT_ID.to_string(),
            filename: META_OBJECT_NAME.to_string(),
            object_type: ObjectType::Json,
            meta: true,
            link_meta: true,
            labeled: true,
            available: true,
            content: vector_data,
            content_length: vector_length as i64,
            expected_downloads,
            url: None,
        }
    }

    async fn vector_data_object(expected_downloads: usize) -> DownloadableObject {
        let mut data = vec![];
        let mut file = File::open(geoengine_datatypes::test_data!("vector/data/points.fgb"))
            .await
            .unwrap();
        let file_size = file.read_to_end(&mut data).await.unwrap();
        DownloadableObject {
            id: DATA_OBJECT_ID.to_string(),
            filename: DATA_OBJECT_NAME.to_string(),
            object_type: ObjectType::Csv,
            meta: false,
            link_meta: true,
            labeled: true,
            available: true,
            content: data,
            content_length: file_size as i64,
            expected_downloads,
            url: None,
        }
    }

    fn empty_valid_object(meta: bool) -> DownloadableObject {
        empty_object(meta, true, true, true)
    }

    #[allow(clippy::fn_params_excessive_bools)]
    fn empty_object(
        meta: bool,
        link_meta: bool,
        labeled: bool,
        available: bool,
    ) -> DownloadableObject {
        let id;
        let filename;
        let object_type;

        if meta {
            id = META_OBJECT_ID.to_string();
            filename = META_OBJECT_NAME.to_string();
            object_type = ObjectType::Json;
        } else {
            id = DATA_OBJECT_ID.to_string();
            filename = DATA_OBJECT_NAME.to_string();
            object_type = ObjectType::Csv;
        }
        DownloadableObject {
            id,
            filename,
            object_type,
            meta,
            link_meta,
            labeled,
            available,
            content: vec![],
            content_length: 0,
            expected_downloads: 0,
            url: None,
        }
    }

    fn create_belonging_resource_relation(
        object_id: &str,
        variant: ResourceVariant,
    ) -> InternalRelation {
        InternalRelation {
            resource_id: object_id.to_string(),
            resource_variant: i32::from(variant),
            defined_variant: i32::from(InternalRelationVariant::BelongsTo),
            custom_variant: None,
            direction: i32::from(RelationDirection::Outbound),
        }
    }

    fn default_project_relations() -> Vec<InternalRelation> {
        let dataset = create_belonging_resource_relation(DATASET_ID, ResourceVariant::Dataset);

        vec![dataset]
    }

    fn default_dataset_relations() -> Vec<InternalRelation> {
        let meta = create_belonging_resource_relation(META_OBJECT_ID, ResourceVariant::Object);
        let data = create_belonging_resource_relation(DATA_OBJECT_ID, ResourceVariant::Object);

        vec![meta, data]
    }

    fn default_meta_relation(meta: bool) -> InternalRelation {
        if meta {
            InternalRelation {
                resource_id: DATA_OBJECT_ID.to_string(),
                resource_variant: i32::from(ResourceVariant::Object),
                defined_variant: i32::from(InternalRelationVariant::Metadata),
                custom_variant: None,
                direction: i32::from(RelationDirection::Outbound),
            }
        } else {
            InternalRelation {
                resource_id: META_OBJECT_ID.to_string(),
                resource_variant: i32::from(ResourceVariant::Object),
                defined_variant: i32::from(InternalRelationVariant::Metadata),
                custom_variant: None,
                direction: i32::from(RelationDirection::Inbound),
            }
        }
    }

    fn default_dataset(
        labeled: bool,
        available: bool,
        dataset_relations: Vec<InternalRelation>,
    ) -> Dataset {
        let mut relations = vec![];
        for relation in dataset_relations {
            relations.push(ArunaRelationStruct {
                relation: Some(ArunaRelationDirection::Internal(relation)),
            });
        }
        let key_values = if labeled {
            vec![default_ge_filter_label()]
        } else {
            vec![]
        };
        let status = if available {
            Status::Available
        } else {
            Status::Initializing
        };

        Dataset {
            id: DATASET_ID.to_string(),
            name: DATASET_NAME.to_string(),
            title: DATASET_NAME.to_string(),
            description: DATASET_DESCRIPTION.to_string(),
            key_values,
            relations,
            stats: None,
            data_class: i32::from(DataClass::Private),
            created_at: None,
            created_by: String::new(),
            authors: vec![],
            status: i32::from(status),
            dynamic: false,
            endpoints: vec![],
            metadata_license_tag: String::new(),
            default_data_license_tag: String::new(),
            rule_bindings: vec![],
        }
    }

    fn default_project(
        labeled: bool,
        available: bool,
        project_relations: Vec<InternalRelation>,
    ) -> Project {
        let mut relations = vec![];
        for relation in project_relations {
            relations.push(ArunaRelationStruct {
                relation: Some(ArunaRelationDirection::Internal(relation)),
            });
        }
        let key_values = if labeled {
            vec![default_ge_filter_label()]
        } else {
            vec![]
        };
        let status = if available {
            Status::Available
        } else {
            Status::Initializing
        };

        Project {
            id: PROJECT_ID.to_string(),
            name: PROJECT_NAME.to_string(),
            title: PROJECT_NAME.to_string(),
            description: PROJECT_DESCRIPTION.to_string(),
            key_values,
            relations,
            stats: None,
            data_class: i32::from(DataClass::Private),
            created_at: None,
            created_by: String::new(),
            authors: vec![],
            status: i32::from(status),
            dynamic: false,
            endpoints: vec![],
            metadata_license_tag: String::new(),
            default_data_license_tag: String::new(),
            rule_bindings: vec![],
        }
    }

    fn start_download_server_with(download_objects: &mut Vec<DownloadableObject>) -> Server {
        let download_server = Server::run();
        for i in download_objects {
            if i.expected_downloads > 0 {
                let responder = match i.object_type {
                    ObjectType::Json => status_code(200)
                        .append_header("content-Type", "application/json")
                        .body(i.content.clone()),
                    ObjectType::Csv => status_code(200)
                        .append_header("content-type", "text/csv")
                        .append_header("content-length", i.content_length)
                        .body(i.content.clone()),
                };
                let object_path = format!("/{}", i.filename);
                download_server.expect(
                    Expectation::matching(httptest::matchers::request::path(object_path))
                        .times(i.expected_downloads)
                        .respond_with(responder),
                );
                i.set_url(format!("http://{}", download_server.addr()).as_str());
            }
        }
        download_server
    }

    async fn mock_server(
        download_server: Option<Server>,
        download_objects: Vec<DownloadableObject>,
        dataset: Option<Dataset>,
        project: Option<Project>,
    ) -> ArunaMockServer {
        let mut download_map = HashMap::new();

        let mut object_ids = vec![];
        let mut aruna_objects = vec![];
        let mut id_map = HashMap::new();

        for i in download_objects {
            let aruna_object = i.clone().into();
            if let Some(url) = i.url {
                download_map.insert(i.id.clone(), GetDownloadUrlResponse { url: url.clone() });
            }
            object_ids.push(i.id.clone());
            aruna_objects.push(aruna_object);
        }
        id_map.insert(
            object_ids,
            GetObjectsResponse {
                objects: aruna_objects,
            },
        );

        let object_service = MockMapObjectService {
            download_map: MapResponseService::new(download_map, |req: GetDownloadUrlRequest| {
                req.object_id
            }),
            id_map: MapResponseService::new(id_map, |req: GetObjectsRequest| req.object_ids),
        };

        let mut dataset_id_map = HashMap::new();
        let mut datasets_id_map = HashMap::new();

        if let Some(dataset) = dataset {
            dataset_id_map.insert(
                dataset.id.clone(),
                GetDatasetResponse {
                    dataset: Some(dataset.clone()),
                },
            );
            datasets_id_map.insert(
                vec![DATASET_ID.to_string()],
                GetDatasetsResponse {
                    datasets: vec![dataset],
                },
            );
        }

        let dataset_service = MockDatasetMapService {
            dataset: MapResponseService::new(dataset_id_map, |req: GetDatasetRequest| {
                req.dataset_id
            }),
            datasets_overview: MapResponseService::new(
                datasets_id_map,
                |req: GetDatasetsRequest| req.dataset_ids,
            ),
        };

        let mut project_map = HashMap::new();
        if let Some(project) = project {
            project_map.insert(
                PROJECT_ID.to_string(),
                GetProjectResponse {
                    project: Some(project),
                },
            );
        }

        let project_service = MockProjectMapService {
            project: MapResponseService::new(project_map, |req: GetProjectRequest| req.project_id),
        };

        let builder: Router = tonic::transport::Server::builder()
            .add_service(project_service)
            .add_service(dataset_service)
            .add_service(object_service);
        let grpc_server = MockGRPCServer::start_with_router(builder).await.unwrap();
        let grpc_server_address = format!("http://{}", grpc_server.address());

        let dataset_ids = ArunaDatasetIds {
            dataset: DATASET_ID.to_string(),
            meta_object: META_OBJECT_ID.to_string(),
            data_object: DATA_OBJECT_ID.to_string(),
        };

        let provider = new_provider_with_url(grpc_server_address).await;

        ArunaMockServer {
            _download_server: download_server,
            _grpc_server: grpc_server,
            provider,
            dataset_ids,
        }
    }

    #[tokio::test]
    async fn extract_aruna_ids_success() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let aruna_mock_server = mock_server(
            None,
            vec![empty_valid_object(true), empty_valid_object(false)],
            dataset,
            None,
        )
        .await;

        let result = aruna_mock_server
            .provider
            .get_aruna_dataset_ids(DATASET_ID.to_string())
            .await
            .unwrap();

        let expected = ArunaDatasetIds {
            dataset: DATASET_ID.to_string(),
            meta_object: META_OBJECT_ID.to_string(),
            data_object: DATA_OBJECT_ID.to_string(),
        };

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn extract_aruna_ids_no_meta_object_relationship() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let aruna_mock_server = mock_server(
            None,
            vec![
                empty_object(true, false, true, true),
                empty_object(false, false, true, true),
            ],
            dataset,
            None,
        )
        .await;

        let result = aruna_mock_server
            .provider
            .get_aruna_dataset_ids(DATASET_ID.to_string())
            .await;

        assert!(matches!(result, Err(ArunaProviderError::MissingMetaObject)));
    }

    #[tokio::test]
    async fn extract_aruna_ids_no_data_object() {
        let relation = create_belonging_resource_relation(META_OBJECT_ID, ResourceVariant::Object);
        let dataset = Some(default_dataset(true, true, vec![relation]));
        let aruna_mock_server = mock_server(
            None,
            vec![empty_object(true, false, true, true)],
            dataset,
            None,
        )
        .await;

        let result = aruna_mock_server
            .provider
            .get_aruna_dataset_ids(DATASET_ID.to_string())
            .await;

        assert!(matches!(
            result,
            Err(ArunaProviderError::UnexpectedObjectHierarchy)
        ));
    }

    #[tokio::test]
    async fn extract_aruna_ids_meta_object_not_labeled() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let aruna_mock_server = mock_server(
            None,
            vec![
                empty_object(true, true, false, true),
                empty_valid_object(false),
            ],
            dataset,
            None,
        )
        .await;

        let result = aruna_mock_server
            .provider
            .get_aruna_dataset_ids(DATASET_ID.to_string())
            .await;

        assert!(matches!(
            result,
            Err(ArunaProviderError::UnexpectedObjectHierarchy)
        ));
    }

    #[tokio::test]
    async fn extract_aruna_ids_data_object_not_labeled() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let aruna_mock_server = mock_server(
            None,
            vec![
                empty_valid_object(true),
                empty_object(false, true, false, true),
            ],
            dataset,
            None,
        )
        .await;

        let result = aruna_mock_server
            .provider
            .get_aruna_dataset_ids(DATASET_ID.to_string())
            .await;

        assert!(matches!(
            result,
            Err(ArunaProviderError::UnexpectedObjectHierarchy)
        ));
    }

    #[tokio::test]
    async fn extract_aruna_ids_dataset_not_labeled() {
        let dataset = Some(default_dataset(false, true, default_dataset_relations()));
        let aruna_mock_server = mock_server(
            None,
            vec![empty_valid_object(true), empty_valid_object(false)],
            dataset,
            None,
        )
        .await;

        let result = aruna_mock_server
            .provider
            .get_aruna_dataset_ids(DATASET_ID.to_string())
            .await;

        assert!(
            matches!(result, Err(ArunaProviderError::MissingLabel { resource_id }) if resource_id == *DATASET_ID)
        );
    }

    #[tokio::test]
    async fn extract_aruna_ids_meta_object_not_available() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let aruna_mock_server = mock_server(
            None,
            vec![
                empty_object(true, true, true, false),
                empty_valid_object(false),
            ],
            dataset,
            None,
        )
        .await;

        let result = aruna_mock_server
            .provider
            .get_aruna_dataset_ids(DATASET_ID.to_string())
            .await;

        assert!(matches!(
            result,
            Err(ArunaProviderError::UnexpectedObjectHierarchy)
        ));
    }

    #[tokio::test]
    async fn extract_aruna_ids_data_object_not_available() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let aruna_mock_server = mock_server(
            None,
            vec![
                empty_valid_object(true),
                empty_object(false, true, true, false),
            ],
            dataset,
            None,
        )
        .await;

        let result = aruna_mock_server
            .provider
            .get_aruna_dataset_ids(DATASET_ID.to_string())
            .await;

        assert!(matches!(
            result,
            Err(ArunaProviderError::UnexpectedObjectHierarchy)
        ));
    }

    #[tokio::test]
    async fn extract_aruna_ids_dataset_not_available() {
        let dataset = Some(default_dataset(true, false, default_dataset_relations()));
        let aruna_mock_server = mock_server(
            None,
            vec![empty_valid_object(true), empty_valid_object(false)],
            dataset,
            None,
        )
        .await;

        let result = aruna_mock_server
            .provider
            .get_aruna_dataset_ids(DATASET_ID.to_string())
            .await;

        assert!(
            matches!(result, Err(ArunaProviderError::ResourceNotAvailable { resource_id }) if resource_id == *DATASET_ID)
        );
    }

    #[tokio::test]
    async fn extract_meta_data_ok() {
        let mut download_objects = vec![json_meta_object(&vector_meta_data(), 1)];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            None,
            None,
        )
        .await;

        let md = aruna_mock_server
            .provider
            .get_metadata(&aruna_mock_server.dataset_ids)
            .await;
        let des = serde_json::from_value::<GEMetadata>(vector_meta_data()).unwrap();
        assert_eq!(des, md.unwrap());
    }

    #[tokio::test]
    async fn extract_meta_data_not_present() {
        let mut object = json_meta_object(&vector_meta_data(), 1);

        let download_server = Server::run();
        let object_path = format!("/{}", object.filename);
        download_server.expect(
            Expectation::matching(httptest::matchers::request::path(object_path))
                .times(1)
                .respond_with(responders::cycle![responders::status_code(404)]),
        );

        object.set_url(&format!("http://{}", download_server.addr()));

        let aruna_mock_server = mock_server(Some(download_server), vec![object], None, None).await;

        let result = aruna_mock_server
            .provider
            .get_metadata(&aruna_mock_server.dataset_ids)
            .await;

        assert!(matches!(
            result,
            Err(ArunaProviderError::MissingArunaMetaData)
        ));
    }

    #[tokio::test]
    async fn extract_meta_data_parse_error() {
        let mut object = json_meta_object(&vector_meta_data(), 1);

        let faulty_meta_data = b"{\"foo\": \"bar\"}".to_vec();
        let download_server = Server::run();
        let responder = status_code(200)
            .append_header("Content-Type", "application/json")
            .body(faulty_meta_data);
        let object_path = format!("/{}", object.filename);
        download_server.expect(
            Expectation::matching(httptest::matchers::request::path(object_path))
                .times(1)
                .respond_with(responder),
        );

        object.set_url(format!("http://{}", download_server.addr()).as_str());
        let aruna_mock_server = mock_server(Some(download_server), vec![object], None, None).await;

        let result = aruna_mock_server
            .provider
            .get_metadata(&aruna_mock_server.dataset_ids)
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn map_vector_dataset() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let mut download_objects = vec![
            json_meta_object(&vector_meta_data(), 1),
            empty_valid_object(false),
        ];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            dataset,
            None,
        )
        .await;

        let layer_id = LayerId(DATASET_ID.to_string());
        let result = aruna_mock_server
            .provider
            .load_layer(&layer_id)
            .await
            .unwrap();

        assert_eq!(
            json!({
                "type": "Vector",
                "operator": {
                    "type": "OgrSource",
                    "params": {
                        "data": "_:86a7f7ce-1bab-4ce9-a32b-172c0f958ee0:DATASET_ID",
                        "attributeProjection": null,
                        "attributeFilters": null
                    }
                }
            }),
            serde_json::to_value(&result.workflow.operator).unwrap()
        );
    }

    #[tokio::test]
    async fn map_raster_dataset() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let mut download_objects = vec![
            json_meta_object(&raster_meta_data(), 1),
            empty_valid_object(false),
        ];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            dataset,
            None,
        )
        .await;

        let layer_id = LayerId(DATASET_ID.to_string());
        let result = aruna_mock_server
            .provider
            .load_layer(&layer_id)
            .await
            .unwrap();

        assert_eq!(
            json!({
                "type": "Raster",
                "operator": {
                    "type": "GdalSource",
                    "params": {
                        "data": "_:86a7f7ce-1bab-4ce9-a32b-172c0f958ee0:DATASET_ID",
                        "overviewLevel": null
                    }
                }
            }),
            serde_json::to_value(&result.workflow.operator).unwrap()
        );
    }

    #[tokio::test]
    async fn vector_loading_template() {
        let mut download_objects = vec![json_meta_object(&vector_meta_data(), 1)];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            None,
            None,
        )
        .await;

        let md = aruna_mock_server
            .provider
            .get_metadata(&aruna_mock_server.dataset_ids)
            .await
            .unwrap();
        let vi = match md.data_type {
            super::metadata::DataType::SingleVectorFile(vi) => vi,
            super::metadata::DataType::SingleRasterFile(_) => panic!("Expected vector description"),
        };

        let rd = ArunaDataProvider::create_single_vector_file_result_descriptor(md.crs.into(), &vi);

        let template =
            ArunaDataProvider::vector_loading_template(&vi, &rd, CacheTtlSeconds::default());

        let url = template
            .new_link("test".to_string())
            .unwrap()
            .file_name
            .to_string_lossy()
            .to_string();

        assert_eq!("/vsicurl_streaming/test", url.as_str());
    }

    #[tokio::test]
    async fn raster_loading_template() {
        let mut download_objects = vec![json_meta_object(&raster_meta_data(), 1)];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            None,
            None,
        )
        .await;

        let md = aruna_mock_server
            .provider
            .get_metadata(&aruna_mock_server.dataset_ids)
            .await
            .unwrap();
        let ri = match md.data_type {
            super::metadata::DataType::SingleRasterFile(ri) => ri,
            super::metadata::DataType::SingleVectorFile(_) => panic!("Expected raster description"),
        };

        let template = ArunaDataProvider::raster_loading_template(&ri, CacheTtlSeconds::default());

        let url = template
            .new_link("test".to_string())
            .unwrap()
            .info
            .next()
            .unwrap()
            .unwrap()
            .params
            .unwrap()
            .file_path
            .to_string_lossy()
            .to_string();

        assert_eq!("/vsicurl_streaming/test", url.as_str());
    }

    #[tokio::test]
    async fn it_lists() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let project = Some(default_project(true, true, default_project_relations()));
        let aruna_mock_server = mock_server(
            None,
            vec![empty_valid_object(true), empty_valid_object(false)],
            dataset,
            project,
        )
        .await;

        let root = aruna_mock_server
            .provider
            .get_root_layer_collection_id()
            .await
            .unwrap();

        let opts = LayerCollectionListOptions {
            limit: 100,
            offset: 0,
        };

        let res = aruna_mock_server
            .provider
            .load_layer_collection(&root, opts)
            .await;

        assert!(res.is_ok());
        let res = res.unwrap();
        assert_eq!(1, res.items.len());
    }

    #[tokio::test]
    async fn it_lists_only_available_dataset() {
        let dataset = Some(default_dataset(true, false, default_dataset_relations()));
        let project = Some(default_project(true, true, default_project_relations()));
        let aruna_mock_server = mock_server(
            None,
            vec![empty_valid_object(true), empty_valid_object(false)],
            dataset,
            project,
        )
        .await;

        let root = aruna_mock_server
            .provider
            .get_root_layer_collection_id()
            .await
            .unwrap();

        let opts = LayerCollectionListOptions {
            limit: 100,
            offset: 0,
        };

        let res = aruna_mock_server
            .provider
            .load_layer_collection(&root, opts)
            .await;

        assert!(res.is_ok());
        let res = res.unwrap();
        assert_eq!(0, res.items.len());
    }

    #[tokio::test]
    async fn it_lists_only_labeled_dataset() {
        let dataset = Some(default_dataset(false, true, default_dataset_relations()));
        let project = Some(default_project(true, true, default_project_relations()));
        let aruna_mock_server = mock_server(
            None,
            vec![empty_valid_object(true), empty_valid_object(false)],
            dataset,
            project,
        )
        .await;

        let root = aruna_mock_server
            .provider
            .get_root_layer_collection_id()
            .await
            .unwrap();

        let opts = LayerCollectionListOptions {
            limit: 100,
            offset: 0,
        };

        let res = aruna_mock_server
            .provider
            .load_layer_collection(&root, opts)
            .await;

        assert!(res.is_ok());
        let res = res.unwrap();
        assert_eq!(0, res.items.len());
    }

    #[tokio::test]
    async fn it_lists_only_dataset_with_verified_objects() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let project = Some(default_project(true, true, default_project_relations()));
        let aruna_mock_server = mock_server(
            None,
            vec![
                empty_object(true, true, true, false),
                empty_valid_object(false),
            ],
            dataset,
            project,
        )
        .await;

        let root = aruna_mock_server
            .provider
            .get_root_layer_collection_id()
            .await
            .unwrap();

        let opts = LayerCollectionListOptions {
            limit: 100,
            offset: 0,
        };

        let res = aruna_mock_server
            .provider
            .load_layer_collection(&root, opts)
            .await;

        assert!(res.is_ok());
        let res = res.unwrap();
        assert_eq!(0, res.items.len());
    }

    #[tokio::test]
    async fn unlabeled_project() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let project = Some(default_project(false, true, default_project_relations()));
        let aruna_mock_server = mock_server(
            None,
            vec![empty_valid_object(true), empty_valid_object(false)],
            dataset,
            project,
        )
        .await;

        let root = aruna_mock_server
            .provider
            .get_root_layer_collection_id()
            .await
            .unwrap();

        let opts = LayerCollectionListOptions {
            limit: 100,
            offset: 0,
        };

        let res = aruna_mock_server
            .provider
            .load_layer_collection(&root, opts)
            .await;

        assert!(res.is_err());

        if let Err(source) = res {
            match source {
                crate::error::Error::ArunaProvider { source } => {
                    assert!(
                        matches!(source, ArunaProviderError::MissingLabel { resource_id } if resource_id == *PROJECT_ID)
                    );
                }
                _ => panic!("Expected Error::ArunaProvider"),
            }
        }
    }

    #[tokio::test]
    async fn unavailable_project() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let project = Some(default_project(true, false, default_project_relations()));
        let aruna_mock_server = mock_server(
            None,
            vec![empty_valid_object(true), empty_valid_object(false)],
            dataset,
            project,
        )
        .await;

        let root = aruna_mock_server
            .provider
            .get_root_layer_collection_id()
            .await
            .unwrap();

        let opts = LayerCollectionListOptions {
            limit: 100,
            offset: 0,
        };

        let res = aruna_mock_server
            .provider
            .load_layer_collection(&root, opts)
            .await;

        assert!(res.is_err());

        if let Err(source) = res {
            match source {
                crate::error::Error::ArunaProvider { source } => {
                    assert!(
                        matches!(source, ArunaProviderError::ResourceNotAvailable { resource_id } if resource_id == *PROJECT_ID)
                    );
                }
                _ => panic!("Expected Error::ArunaProvider"),
            }
        }
    }

    #[tokio::test]
    async fn it_loads_provenance() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let mut download_objects = vec![
            json_meta_object(&raster_meta_data(), 1),
            empty_valid_object(false),
        ];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            dataset,
            None,
        )
        .await;

        let id = DataId::External(ExternalDataId {
            provider_id: DataProviderId::from_str(PROVIDER_ID).unwrap(),
            layer_id: LayerId(DATASET_ID.to_string()),
        });

        let res = aruna_mock_server.provider.provenance(&id).await;

        assert!(res.is_ok());

        let res = res.unwrap();

        assert!(res.provenance.is_some());
    }

    #[tokio::test]
    async fn it_loads_meta_data() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let mut download_objects = vec![json_meta_object(&vector_meta_data(), 1)];
        let server = start_download_server_with(&mut download_objects);

        let mut raster_object = vector_data_object(1).await;
        raster_object.set_url(format!("http://{}", server.addr()).as_str());
        download_objects.push(raster_object);

        let aruna_mock_server = mock_server(Some(server), download_objects, dataset, None).await;

        let id = DataId::External(ExternalDataId {
            provider_id: DataProviderId::from_str(PROVIDER_ID).unwrap(),
            layer_id: LayerId(DATASET_ID.to_string()),
        });

        let res: geoengine_operators::util::Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = aruna_mock_server.provider.meta_data(&id).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_executes_loads() {
        let dataset = Some(default_dataset(true, true, default_dataset_relations()));
        let mut download_objects = vec![
            json_meta_object(&vector_meta_data(), 1),
            vector_data_object(1).await,
        ];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            dataset,
            None,
        )
        .await;

        let id = DataId::External(ExternalDataId {
            provider_id: DataProviderId::from_str(PROVIDER_ID).unwrap(),
            layer_id: LayerId(DATASET_ID.to_string()),
        });
        let name = geoengine_datatypes::dataset::NamedData::with_system_provider(
            PROVIDER_ID.to_string(),
            DATASET_ID.to_string(),
        );

        let meta: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        > = aruna_mock_server
            .provider
            .meta_data(&id.clone())
            .await
            .unwrap();

        let mut context = MockExecutionContext::test_default();
        context.add_meta_data(id.clone(), name.clone(), meta);

        let src = OgrSource {
            params: OgrSourceParameters {
                data: name,
                attribute_projection: None,
                attribute_filters: None,
            },
        }
        .boxed();

        let initialized_op = src
            .initialize(WorkflowOperatorPath::initialize_root(), &context)
            .await
            .unwrap();

        let proc = initialized_op.query_processor().unwrap();
        let TypedVectorQueryProcessor::MultiPoint(proc) = proc else {
            panic!("Expected MultiPoint QueryProcessor");
        };

        let ctx = context.mock_query_context_test_default();

        let qr = VectorQueryRectangle::new(
            BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
            TimeInterval::default(),
            ColumnSelection::all(),
        );

        let result: Vec<MultiPointCollection> = proc
            .query(qr, &ctx)
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect()
            .await;

        let element_count: usize = result.iter().map(MultiPointCollection::len).sum();
        assert_eq!(2, element_count);
    }
}
