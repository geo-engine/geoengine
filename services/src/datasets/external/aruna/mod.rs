pub use self::error::ArunaProviderError;
use crate::contexts::GeoEngineDb;
use crate::datasets::external::aruna::metadata::{DataType, GEMetadata, RasterInfo, VectorInfo};
use crate::datasets::listing::ProvenanceOutput;
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerListing,
    ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{
    LayerCollectionId, LayerCollectionProvider, ProviderCapabilities, SearchCapabilities,
};
use crate::workflows::workflow::Workflow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::str::FromStr;

use aruna_rust_api::api::storage::models::v2::{
    Dataset, InternalRelationVariant, KeyValue, KeyValueVariant, Object, ResourceVariant,
};
use aruna_rust_api::api::storage::models::v2::{
    Relation as ArunaRelationStruct, RelationDirection,
};
use aruna_rust_api::api::storage::models::v2::relation::Relation as ArunaRelationEnum;
use aruna_rust_api::api::storage::services::v2::{GetDatasetRequest, GetDatasetsRequest, GetDownloadUrlRequest, GetObjectsRequest, GetProjectRequest};
use aruna_rust_api::api::storage::services::v2::dataset_service_client::DatasetServiceClient;
use aruna_rust_api::api::storage::services::v2::object_service_client::ObjectServiceClient;
use aruna_rust_api::api::storage::services::v2::project_service_client::ProjectServiceClient;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use tonic::{Request, Status};
use tonic::codegen::InterceptedService;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};

use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::dataset::{DataId, DataProviderId, LayerId};
use geoengine_datatypes::primitives::{
    FeatureDataType, Measurement, RasterQueryRectangle, SpatialResolution, VectorQueryRectangle,
};
use geoengine_datatypes::primitives::CacheTtlSeconds;
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterBandDescriptor, RasterBandDescriptors, RasterOperator,
    RasterResultDescriptor, ResultDescriptor, TypedOperator, VectorColumnInfo, VectorOperator,
    VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetParameters, GdalLoadingInfo, GdalLoadingInfoTemporalSlice,
    GdalLoadingInfoTemporalSliceIterator, GdalSource, GdalSourceParameters, OgrSource,
    OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceDurationSpec,
    OgrSourceErrorSpec, OgrSourceParameters, OgrSourceTimeFormat,
};

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

/// In the Aruna Object Storage, a geoengine dataset is mapped to a single collection.
/// The collection consists of exactly one object group with exactly two objects.
/// The meta object is a json that maps directly to `GEMetadata`.
/// The data object contains the actual data.
///
#[derive(Debug, PartialEq)]
struct ArunaDatasetIds {
    dataset_id: String,
    meta_object_id: String,
    data_object_id: String,
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
        let channel = Endpoint::from_str(url.as_str())
            .map_err(|_| ArunaProviderError::InvalidUri { uri_string: url })?
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
        mut relations: Vec<ArunaRelationStruct>,
        target_resource_variant: ResourceVariant,
    ) -> Result<Vec<String>> {
        let mut ids = vec![];
        for relation in relations {
            match relation
                .relation
                .ok_or(ArunaProviderError::MissingRelation)?
            {
                ArunaRelationEnum::External(_) => {}
                ArunaRelationEnum::Internal(x) => {
                    if x.direction() == RelationDirection::Outbound
                        && x.resource_variant() == target_resource_variant
                    {
                        ids.push(x.resource_id);
                    }
                }
            }
        }

        Ok(ids)
    }

    async fn has_filter_label(&self, key_values: &Vec<KeyValue>) -> bool {
        let label = &self.label_filter;

        if let Some(filter) = label {
            for key_value in key_values {
                if key_value.variant() == KeyValueVariant::Label
                    && key_value.key == filter.to_string()
                    && key_value.value == filter.to_string()
                {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    async fn get_available_labeled_datasets(
        &self,
        dataset_ids: Vec<String>,
    ) -> Result<Vec<Dataset>> {
        let mut dataset_stub = self.dataset_stub.clone();

        let datasets = dataset_stub
            .get_datasets(GetDatasetsRequest { dataset_ids })
            .await?
            .into_inner()
            .datasets;
        // //TODO: Workaround until deleted access permissions are fixed.
        // let mut datasets = vec![];
        //
        // for dataset_id in dataset_ids {
        //     let dataset_response = dataset_stub
        //         .get_dataset(GetDatasetRequest {
        //             dataset_id: dataset_id.clone(),
        //         })
        //         .await?;
        //
        //     if dataset_response.is_err_and(|x| x.code() == Code::Unauthenticated) {
        //         log::debug!(
        //             "Ignoring Code::Unauthenticated Error in Aruna Dataset Request For id={} (Check if Dataset is deleted)",
        //             dataset_id,
        //         )
        //     } else {
        //         let dataset = dataset_response
        //             .into_inner()
        //             .dataset
        //             .ok_or(ArunaProviderError::MissingDataset)?;
        //         if dataset.status() == aruna_rust_api::api::storage::models::v2::Status::Available
        //             && self.has_filter_label(&dataset.key_values)
        //         {
        //             datasets.push(dataset);
        //         }
        //     }
        // }

        Ok(datasets)
    }

    async fn get_available_objects(&self, object_ids: Vec<String>) -> Result<Vec<Object>> {
        let mut object_stub = self.object_stub.clone();

        let objects = object_stub
            .get_objects(GetObjectsRequest { object_ids })
            .await?
            .into_inner()
            .objects;
        // //TODO: Workaround until deleted access permissions are fixed.
        // let mut objects = vec![];
        //
        // for object_id in object_ids {
        //     let object_response = object_stub.get_object(GetObjectRequest { object_id }).await;
        //
        //     if object_response.is_err_and(|x| x.code() == Code::Unauthenticated) {
        //         log::debug!(
        //             "Ignoring Code::Unauthenticated Error in Aruna Object Request For id={} (Check if Object is deleted)",
        //             object_id,
        //         )
        //     } else {
        //         let object = object_response?
        //             .into_inner()
        //             .object
        //             .ok_or(ArunaProviderError::MissingObject)?;
        //         if object.status() == aruna_rust_api::api::storage::models::v2::Status::Available {
        //             objects.push(object);
        //         }
        //     }
        // }

        Ok(objects)
    }

    fn is_metadata(meta_candidate: &Object, data_candidate_id: &String) -> Result<bool> {
        for relation in &meta_candidate.relations {
            match relation
                .relation
                .clone()
                .ok_or(ArunaProviderError::MissingRelation)?
            {
                ArunaRelationEnum::External(_) => {}
                ArunaRelationEnum::Internal(x) => {
                    if x.defined_variant() == InternalRelationVariant::Metadata
                        && x.direction() == RelationDirection::Outbound
                        && x.resource_variant() == ResourceVariant::Object
                        && x.resource_id == data_candidate_id.to_string()
                    {
                        return Ok(true);
                    }
                }
            }
        }
        return Ok(false);
    }

    /// Retrieves all ids in the aruna object storage for the given `collection_id`.
    async fn get_aruna_dataset_ids(&self, dataset_id: String) -> Result<ArunaDatasetIds> {
        let mut dataset_stub = self.dataset_stub.clone();

        let dataset_relations = dataset_stub
            .get_dataset(GetDatasetRequest {
                dataset_id: dataset_id.clone(),
            })
            .await?
            .into_inner()
            .dataset
            .ok_or(ArunaProviderError::MissingDataset)?
            .relations;

        let object_ids =
            Self::get_outgoing_internal_relation_ids(dataset_relations, ResourceVariant::Object)?;

        let mut aruna_objects = self.get_available_objects(object_ids).await?;

        if aruna_objects.len() != 2 {
            return Err(ArunaProviderError::UnexpectedObjectHierarchy);
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

        Ok(ArunaDatasetIds {
            dataset_id,
            meta_object_id,
            data_object_id,
        })
    }

    async fn get_collection_overview(
        &self,
        aruna_dataset_ids: &ArunaDatasetIds,
    ) -> Result<Dataset> {
        let mut dataset_stub = self.dataset_stub.clone();

        let dataset = dataset_stub
            .get_dataset(GetDatasetRequest {
                dataset_id: aruna_dataset_ids.dataset_id.clone(),
            })
            .await?
            .into_inner()
            .dataset
            .ok_or(ArunaProviderError::MissingDataset)?;

        Ok(dataset)
    }

    /// Extracts the geoengine metadata from a collection in the Aruna Object Storage
    async fn get_metadata(&self, aruna_dataset_ids: &ArunaDatasetIds) -> Result<GEMetadata> {
        let mut object_stub = self.object_stub.clone();

        let download_url = object_stub
            .get_download_url(GetDownloadUrlRequest {
                object_id: aruna_dataset_ids.meta_object_id.clone(),
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
        Ok(RasterResultDescriptor {
            data_type: info.data_type,
            spatial_reference: crs,

            time: Some(info.time_interval),
            bbox: Some(
                info.geo_transform
                    .spatial_partition(info.width, info.height),
            ),
            resolution: Some(SpatialResolution::try_from((
                info.geo_transform.x_pixel_size,
                info.geo_transform.y_pixel_size,
            ))?),
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

        GdalLoadingInfo {
            info: GdalLoadingInfoTemporalSliceIterator::Static {
                parts: vec![part].into_iter(),
            },
        }
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
        id: &geoengine_datatypes::dataset::DataId,
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
                    collection_id: aruna_dataset_ids.dataset_id,
                    object_id: aruna_dataset_ids.data_object_id,
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
                    collection_id: aruna_dataset_ids.dataset_id,
                    object_id: aruna_dataset_ids.data_object_id,
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

        let project_relations = project_stub
            .get_project(GetProjectRequest {
                project_id: self.project_id.clone(),
            })
            .await
            .map_err(|source| ArunaProviderError::TonicStatus { source })?
            .into_inner()
            .project
            .ok_or(ArunaProviderError::MissingProject)?
            .relations;

        let dataset_ids =
            Self::get_outgoing_internal_relation_ids(project_relations, ResourceVariant::Dataset)?;

        let items = self
            .get_available_labeled_datasets(dataset_ids)
            .await?
            .into_iter()
            .map(|col| {
                CollectionItem::Layer(LayerListing {
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

        let dataset = self.get_collection_overview(&aruna_dataset_ids).await?;
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
                    params: GdalSourceParameters {
                        data: geoengine_datatypes::dataset::NamedData::with_system_provider(
                            self.id.to_string(),
                            id.to_string(),
                        ),
                    },
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
                    "Could not parse original path as string {:?}",
                    &self.file_name
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
                        };
                        new_part
                    })
                    .collect::<std::vec::Vec<_>>();

                Ok(Self {
                    info: GdalLoadingInfoTemporalSliceIterator::Static {
                        parts: new_parts.into_iter(),
                    },
                })
            }
            _ => Err(geoengine_operators::error::Error::InvalidType {
                found: "GdalLoadingInfoPartIterator::Dynamic".to_string(),
                expected: "GdalLoadingInfoPartIterator::Static".to_string(),
            }),
        }
    }
}

/// This is the metadata for datasets retrieved from the core-storage.
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
    collection_id: String,
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
    use std::path::Path;
    use std::str::FromStr;
    use std::task::Poll;

    use aruna_rust_api::api::storage::models::v2::{DataClass, Dataset, InternalRelation, InternalRelationVariant, Object, Project, RelationDirection, ResourceVariant, Status};
    use aruna_rust_api::api::storage::models::v2::Relation as ArunaRelationStruct;
    use aruna_rust_api::api::storage::models::v2::relation::Relation as ArunaRelationDirection;
    use aruna_rust_api::api::storage::services::v2::{GetDatasetRequest, GetDatasetResponse, GetDatasetsRequest, GetDatasetsResponse, GetDownloadUrlRequest, GetDownloadUrlResponse, GetObjectsRequest, GetObjectsResponse, GetProjectRequest, GetProjectResponse};
    use futures::StreamExt;
    use httptest::{Expectation, responders, Server};
    use httptest::responders::status_code;
    use serde_json::{json, Value};
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;
    use tonic::Code;
    use tonic::codegen::{Body, http, Service};
    use tonic::codegen::http::Request;
    use tonic::transport::server::Router;

    use geoengine_datatypes::collections::{FeatureCollectionInfos, MultiPointCollection};
    use geoengine_datatypes::dataset::{DataId, DataProviderId, ExternalDataId, LayerId};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, CacheTtlSeconds, ColumnSelection, SpatialResolution, TimeInterval,
        VectorQueryRectangle,
    };
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{
        MetaData, MetaDataProvider, MockExecutionContext, MockQueryContext, QueryProcessor,
        TypedVectorQueryProcessor, VectorOperator, VectorResultDescriptor, WorkflowOperatorPath,
    };
    use geoengine_operators::source::{OgrSource, OgrSourceDataset, OgrSourceParameters};

    use crate::datasets::external::aruna::{
        ArunaDataProvider, ArunaDataProviderDefinition, ArunaDatasetIds, ArunaProviderError,
        ExpiringDownloadLink,
    };
    use crate::datasets::external::aruna::metadata::GEMetadata;
    use crate::datasets::external::aruna::mock_grpc_server::{
        InfallibleHttpResponseFuture, MapResponseService,
    };
    use crate::datasets::external::aruna::mock_grpc_server::MockGRPCServer;
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

    const DATASET_ID: &str = "COLLECTION_ID";
    const DATASET_NAME: &str = "COLLECTION_NAME";
    const DATASET_DESCRIPTION: &str = "COLLECTION_DESCRIPTION";

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

    #[derive(Clone)]
    enum ObjectType {
        Json,
        Csv,
    }

    #[derive(Clone)]
    struct DownloadableObject {
        id: String,
        filename: String,
        object_type: ObjectType,
        meta: bool,
        link_meta: bool,
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

    impl Into<Object> for DownloadableObject {
        fn into(self) -> Object {
            let relations;
            if self.link_meta {
                relations = vec![ArunaRelationStruct{relation: Some(ArunaRelationDirection::Internal(default_meta_relation(self.meta)))}];
            } else {
                relations = vec![];
            }
            Object{
                 id: self.id.clone(),
                 name:  self.filename.clone(),
                 title: self.filename.clone(),
                 description: "".to_string(),
                 key_values: vec![],
                 relations,
                 content_len: self.content_length,
                 data_class: i32::from(DataClass::Private),
                 created_at: None,
                 created_by: "Someone".to_string(),
                 authors: vec![],
                 status:  i32::from(Status::Available),
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
            content: data,
            content_length: file_size as i64,
            expected_downloads,
            url: None,
        }
    }

    fn empty_object(meta: bool, link_meta: bool) -> DownloadableObject {
        if meta {
            DownloadableObject {
                id: META_OBJECT_ID.to_string(),
                filename: META_OBJECT_NAME.to_string(),
                object_type: ObjectType::Json,
                meta,
                link_meta,
                content: vec![],
                content_length: 0,
                expected_downloads: 0,
                url: None,
            }
        } else {
            DownloadableObject {
                id: DATA_OBJECT_ID.to_string(),
                filename: DATA_OBJECT_NAME.to_string(),
                object_type: ObjectType::Csv,
                meta,
                link_meta,
                content: vec![],
                content_length: 0,
                expected_downloads: 0,
                url: None,
            }
        }
    }

    fn create_object(id: String, filename: String) -> Object {
        Object{
            id,
            name: filename.to_string(),
            title: filename.to_string(),
            description: "".to_string(),
            key_values: vec![],
            relations: vec![],
            content_len: 0,
            data_class: i32::from(DataClass::Private),
            created_at: None,
            created_by: "".to_string(),
            authors: vec![],
            status: 0,
            dynamic: false,
            endpoints: vec![],
            hashes: vec![],
            metadata_license_tag: "".to_string(),
            data_license_tag: "".to_string(),
            rule_bindings: vec![],
        }
    }

    fn create_belonging_resource_relation(object_id: String, variant: ResourceVariant) -> InternalRelation {
        InternalRelation {
            resource_id: object_id.clone(),
            resource_variant: i32::from(variant),
            defined_variant: i32::from(InternalRelationVariant::BelongsTo),
            custom_variant: None,
            direction: i32::from(RelationDirection::Outbound),
        }
    }

    fn default_project_relations() -> Vec<InternalRelation> {
        let dataset = create_belonging_resource_relation(DATASET_ID.to_string(), ResourceVariant::Dataset);

        vec![dataset]
    }

    fn default_dataset_relations() -> Vec<InternalRelation> {
        let meta = create_belonging_resource_relation(META_OBJECT_ID.to_string(), ResourceVariant::Object);
        let data = create_belonging_resource_relation(DATA_OBJECT_ID.to_string(), ResourceVariant::Object);

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
        dataset_relations: Vec<InternalRelation>,
        project_relations: Vec<InternalRelation>,
    ) -> ArunaMockServer {
        let mut download_map = HashMap::new();

        let mut object_ids = vec![];
        let mut aruna_objects = vec![];
        let mut id_map = HashMap::new();

        for i in download_objects {
            let aruna_object = i.clone().into();
            if let Some(url) = i.url {
                download_map.insert(
                    i.id.clone(),
                    GetDownloadUrlResponse {
                        url: url.clone(),
                    },
                );
            }
            object_ids.push(i.id.clone());
            aruna_objects.push(aruna_object);
        }
        id_map.insert(object_ids, GetObjectsResponse{ objects: aruna_objects });

        let object_service = MockMapObjectService {
            download_map: MapResponseService::new(download_map, |req: GetDownloadUrlRequest| {
                req.object_id
            }),
            id_map: MapResponseService::new(id_map, |req: GetObjectsRequest| req.object_ids),
        };

        let mut relations = vec![];
        for relation in dataset_relations {
            relations.push(ArunaRelationStruct { relation: Some(ArunaRelationDirection::Internal(relation)) });
        }

        let dataset = Dataset {
            id: DATASET_ID.to_string(),
            name: DATASET_NAME.to_string(),
            title: DATASET_NAME.to_string(),
            description: DATASET_DESCRIPTION.to_string(),
            key_values: vec![],
            relations,
            stats: None,
            data_class: i32::from(DataClass::Private),
            created_at: None,
            created_by: "".to_string(),
            authors: vec![],
            status: 0,
            dynamic: false,
            endpoints: vec![],
            metadata_license_tag: "".to_string(),
            default_data_license_tag: "".to_string(),
            rule_bindings: vec![],
        };

        let mut dataset_id_map = HashMap::new();
        dataset_id_map.insert(
            DATASET_ID.to_string(),
            GetDatasetResponse {
                dataset: Some(dataset.clone()),
            },
        );

        let mut collection_id_map = HashMap::new();
        collection_id_map.insert(
            vec![DATASET_ID.to_string()],
            GetDatasetsResponse {
                datasets: vec![dataset],
            },
        );

        let collection_service = MockDatasetMapService {
            dataset: MapResponseService::new(
                dataset_id_map,
                |req: GetDatasetRequest| req.dataset_id,
            ),
            datasets_overview: MapResponseService::new(
                collection_id_map,
                |req: GetDatasetsRequest| req.dataset_ids,
            )
        };


        let mut relations = vec![];
        for relation in project_relations {
            relations.push(ArunaRelationStruct { relation: Some(ArunaRelationDirection::Internal(relation)) });
        }
        let mut project_map = HashMap::new();
        project_map.insert(PROJECT_ID.to_string(), GetProjectResponse { project: Some(Project{
            id: PROJECT_ID.to_string(),
            name: PROJECT_NAME.to_string(),
            title: PROJECT_NAME.to_string(),
            description: PROJECT_DESCRIPTION.to_string(),
            key_values: vec![],
            relations,
            stats: None,
            data_class: i32::from(DataClass::Private),
            created_at: None,
            created_by: "".to_string(),
            authors: vec![],
            status: i32::from(Status::Available),
            dynamic: false,
            endpoints: vec![],
            metadata_license_tag: "".to_string(),
            default_data_license_tag: "".to_string(),
            rule_bindings: vec![],
        })});

        let project_service = MockProjectMapService {
            project: MapResponseService::new(project_map, |req: GetProjectRequest| req.project_id),
        };

        let builder: Router = tonic::transport::Server::builder()
            .add_service(project_service)
            .add_service(collection_service)
            .add_service(object_service);
        let grpc_server = MockGRPCServer::start_with_router(builder).await.unwrap();
        let grpc_server_address = format!("http://{}", grpc_server.address());

        let dataset_ids = ArunaDatasetIds {
            dataset_id: DATASET_ID.to_string(),
            meta_object_id: META_OBJECT_ID.to_string(),
            data_object_id: DATA_OBJECT_ID.to_string(),
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
        let aruna_mock_server = mock_server(
            None,
            vec![empty_object(true, true), empty_object(false, true)],
            default_dataset_relations(),
            vec![],
        )
        .await;

        let result = aruna_mock_server
            .provider
            .get_aruna_dataset_ids(DATASET_ID.to_string())
            .await
            .unwrap();

        let expected = ArunaDatasetIds {
            dataset_id: DATASET_ID.to_string(),
            meta_object_id: META_OBJECT_ID.to_string(),
            data_object_id: DATA_OBJECT_ID.to_string(),
        };

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn extract_aruna_ids_no_meta_object_relationship() {
        let aruna_mock_server =
            mock_server(None, vec![empty_object(true, false), empty_object(false, false)], default_dataset_relations(), vec![], ).await;

        let result = aruna_mock_server
            .provider
            .get_aruna_dataset_ids(DATASET_ID.to_string())
            .await;

        assert!(matches!(result, Err(ArunaProviderError::MissingMetaObject)));
    }

    #[tokio::test]
    async fn extract_aruna_ids_no_data_object() {
        let relation = create_belonging_resource_relation(META_OBJECT_ID.to_string(), ResourceVariant::Object);
        let aruna_mock_server =
            mock_server(None, vec![empty_object(true, false)], vec![relation], vec![], ).await;

        let result = aruna_mock_server
            .provider
            .get_aruna_dataset_ids(DATASET_ID.to_string())
            .await;

        assert!(matches!(result, Err(ArunaProviderError::UnexpectedObjectHierarchy)));
    }

    #[tokio::test]
    async fn test_extract_meta_data_ok() {
        let mut download_objects = vec![json_meta_object(&vector_meta_data(), 1)];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            vec![],
            vec![],
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
    async fn test_extract_meta_data_not_present() {
        let mut object = json_meta_object(&vector_meta_data(), 1);

        let download_server = Server::run();
        let object_path = format!("/{}", object.filename);
        download_server.expect(
            Expectation::matching(httptest::matchers::request::path(object_path))
                .times(1)
                .respond_with(responders::cycle![responders::status_code(404)]),
        );

        object.set_url(&format!("http://{}", download_server.addr()));

        let aruna_mock_server = mock_server(
            Some(download_server),
            vec![object],
            vec![],
            vec![],
        )
        .await;

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
    async fn test_extract_meta_data_parse_error() {
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
        let aruna_mock_server = mock_server(
            Some(download_server),
            vec![object],
            vec![],
            vec![],
        )
        .await;

        let result = aruna_mock_server
            .provider
            .get_metadata(&aruna_mock_server.dataset_ids)
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_map_vector_dataset() {
        let mut download_objects = vec![json_meta_object(&vector_meta_data(), 1), empty_object(false, true)];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            default_dataset_relations(),
            vec![],
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
                        "data": "_:86a7f7ce-1bab-4ce9-a32b-172c0f958ee0:COLLECTION_ID",
                        "attributeProjection": null,
                        "attributeFilters": null
                    }
                }
            }),
            serde_json::to_value(&result.workflow.operator).unwrap()
        );
    }

    #[tokio::test]
    async fn test_map_raster_dataset() {
        let mut download_objects = vec![json_meta_object(&raster_meta_data(), 1), empty_object(false, true)];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            default_dataset_relations(),
            vec![],
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
                        "data": "_:86a7f7ce-1bab-4ce9-a32b-172c0f958ee0:COLLECTION_ID"
                    }
                }
            }),
            serde_json::to_value(&result.workflow.operator).unwrap()
        );
    }

    #[tokio::test]
    async fn test_vector_loading_template() {
        let mut download_objects = vec![json_meta_object(&vector_meta_data(), 1)];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            vec![],
            vec![],
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
    async fn test_raster_loading_template() {
        let mut download_objects = vec![json_meta_object(&raster_meta_data(), 1)];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            vec![],
            vec![],
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
        let aruna_mock_server = mock_server(None, vec![], vec![],
                                            default_project_relations()).await;
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
    async fn it_loads_provenance() {
        let mut download_objects = vec![json_meta_object(&raster_meta_data(), 1), empty_object(false, true)];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            default_dataset_relations(),
            vec![],
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
        let mut download_objects = vec![json_meta_object(&vector_meta_data(), 1)];
        let server = start_download_server_with(&mut download_objects);

        let mut raster_object = vector_data_object(1).await;
        raster_object.set_url(format!("http://{}", server.addr()).as_str());
        download_objects.push(raster_object);

        let aruna_mock_server = mock_server(
            Some(server),
            download_objects,
            default_dataset_relations(),
            vec![],
        )
        .await;

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
        let mut download_objects = vec![
            json_meta_object(&vector_meta_data(), 1),
            vector_data_object(1).await,
        ];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            default_dataset_relations(),
            vec![],
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

        let ctx = MockQueryContext::test_default();

        let qr = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
            attributes: ColumnSelection::all(),
        };

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
