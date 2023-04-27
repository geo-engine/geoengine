pub use self::error::ArunaProviderError;
use crate::api::model::datatypes::{DataId, DataProviderId, ExternalDataId, LayerId};
use crate::datasets::external::aruna::metadata::{DataType, GEMetadata, RasterInfo, VectorInfo};
use crate::datasets::listing::ProvenanceOutput;
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerListing,
    ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::workflows::workflow::Workflow;
use aruna_rust_api::api::storage::models::v1::{
    CollectionOverview, KeyValue, LabelFilter, LabelOrIdQuery, Object,
};
use aruna_rust_api::api::storage::services::v1::collection_service_client::CollectionServiceClient;
use aruna_rust_api::api::storage::services::v1::object_group_service_client::ObjectGroupServiceClient;
use aruna_rust_api::api::storage::services::v1::object_service_client::ObjectServiceClient;
use aruna_rust_api::api::storage::services::v1::{
    GetCollectionByIdRequest, GetCollectionsRequest, GetDownloadUrlRequest, GetObjectByIdRequest,
    GetObjectGroupObjectsRequest, GetObjectGroupsRequest,
};
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::primitives::{
    FeatureDataType, Measurement, RasterQueryRectangle, SpatialResolution, VectorQueryRectangle,
};
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterOperator, RasterResultDescriptor, ResultDescriptor,
    TypedOperator, VectorColumnInfo, VectorOperator, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetParameters, GdalLoadingInfo, GdalLoadingInfoTemporalSlice,
    GdalLoadingInfoTemporalSliceIterator, GdalSource, GdalSourceParameters, OgrSource,
    OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceDurationSpec,
    OgrSourceErrorSpec, OgrSourceParameters, OgrSourceTimeFormat,
};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::str::FromStr;
use tonic::codegen::InterceptedService;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

pub mod error;
pub mod metadata;
#[cfg(test)]
#[macro_use]
mod mock_grpc_server;

type Result<T, E = ArunaProviderError> = std::result::Result<T, E>;

const URL_REPLACEMENT: &str = "%URL%";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArunaDataProviderDefinition {
    id: DataProviderId,
    name: String,
    api_url: String,
    project_id: String,
    api_token: String,
    filter_label: String,
}

#[typetag::serde]
#[async_trait::async_trait]
impl DataProviderDefinition for ArunaDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn DataProvider>> {
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
    collection_id: String,
    _object_group_id: String,
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
    id: DataProviderId,
    project_id: String,
    collection_stub: CollectionServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    object_group_stub: ObjectGroupServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    object_stub: ObjectServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    label_filter: Option<LabelOrIdQuery>,
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

        let collection_stub =
            CollectionServiceClient::with_interceptor(channel.clone(), interceptor.clone());
        let object_group_stub =
            ObjectGroupServiceClient::with_interceptor(channel.clone(), interceptor.clone());

        let object_stub = ObjectServiceClient::with_interceptor(channel, interceptor);

        let label_filter = Some(LabelOrIdQuery {
            labels: Some(LabelFilter {
                labels: vec![KeyValue {
                    key: def.filter_label.to_string(),
                    value: String::new(),
                }],
                and_or_or: true,
                keys_only: true,
            }),
            ids: vec![],
        });

        Ok(ArunaDataProvider {
            name: def.name,
            id: def.id,
            project_id: def.project_id,
            collection_stub,
            object_group_stub,
            object_stub,
            label_filter,
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
        self.get_collection_info(Self::dataset_aruna_id(id)?).await
    }

    /// Retrieves information for the dataset with the given id.
    async fn get_dataset_info_from_layer(&self, id: &LayerId) -> Result<ArunaDatasetIds> {
        self.get_collection_info(id.0.clone()).await
    }

    /// Retrieves all ids in the aruna object storage for the given `collection_id`.
    async fn get_collection_info(&self, collection_id: String) -> Result<ArunaDatasetIds> {
        let mut object_group_stub = self.object_group_stub.clone();

        let mut aruna_object_group_overview = object_group_stub
            .get_object_groups(GetObjectGroupsRequest {
                collection_id: collection_id.clone(),
                page_request: None,
                label_id_filter: self.label_filter.clone(),
            })
            .await?
            .into_inner()
            .object_groups
            .ok_or(ArunaProviderError::MissingObjectGroup)?
            .object_group_overviews;

        if aruna_object_group_overview.is_empty() {
            return Err(ArunaProviderError::MissingObjectGroup);
        }

        if aruna_object_group_overview.len() != 1 {
            return Err(ArunaProviderError::UnexpectedObjectHierarchy);
        }
        let object_group_id = aruna_object_group_overview
            .pop()
            .expect("Object groups should have size one")
            .id;

        let aruna_objects = object_group_stub
            .get_object_group_objects(GetObjectGroupObjectsRequest {
                collection_id: collection_id.clone(),
                group_id: object_group_id.clone(),
                page_request: None,
                meta_only: false,
            })
            .await?
            .into_inner()
            .object_group_objects;

        if aruna_objects.len() > 2 {
            return Err(ArunaProviderError::UnexpectedObjectHierarchy);
        }

        let mut meta_object_id = None;
        let mut data_object_id = None;
        for i in aruna_objects {
            if i.is_metadata {
                meta_object_id = Some(i.object.ok_or(ArunaProviderError::MissingMetaObject)?.id);
            } else {
                data_object_id = Some(i.object.ok_or(ArunaProviderError::MissingDataObject)?.id);
            }
        }

        Ok(ArunaDatasetIds {
            collection_id,
            _object_group_id: object_group_id,
            meta_object_id: meta_object_id.ok_or(ArunaProviderError::MissingMetaObject)?,
            data_object_id: data_object_id.ok_or(ArunaProviderError::MissingDataObject)?,
        })
    }

    async fn get_collection_overview(
        &self,
        aruna_dataset_ids: &ArunaDatasetIds,
    ) -> Result<CollectionOverview> {
        let mut collection_stub = self.collection_stub.clone();

        let collection_overview = collection_stub
            .get_collection_by_id(GetCollectionByIdRequest {
                collection_id: aruna_dataset_ids.collection_id.clone(),
            })
            .await?
            .into_inner()
            .collection
            .ok_or(ArunaProviderError::InvalidDataId)?;

        Ok(collection_overview)
    }

    /// Extracts the geoengine metadata from a collection in the Aruna Object Storage
    async fn get_metadata(&self, aruna_dataset_ids: &ArunaDatasetIds) -> Result<GEMetadata> {
        let mut object_stub = self.object_stub.clone();

        let download_url = object_stub
            .get_download_url(GetDownloadUrlRequest {
                collection_id: aruna_dataset_ids.collection_id.clone(),
                object_id: aruna_dataset_ids.meta_object_id.clone(),
            })
            .await?
            .into_inner()
            .url
            .ok_or(ArunaProviderError::MissingURL)?
            .url;

        let data_get_response = reqwest::Client::new().get(download_url).send().await?;

        return if let reqwest::StatusCode::OK = data_get_response.status() {
            let json = data_get_response.json::<GEMetadata>().await?;
            Ok(json)
        } else {
            Err(ArunaProviderError::MissingArunaMetaData)
        };
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
            measurement: info
                .measurement
                .as_ref()
                .map_or(Measurement::Unitless, Clone::clone),
            time: Some(info.time_interval),
            bbox: Some(
                info.geo_transform
                    .spatial_partition(info.width, info.height),
            ),
            resolution: Some(SpatialResolution::try_from((
                info.geo_transform.x_pixel_size,
                info.geo_transform.y_pixel_size,
            ))?),
        })
    }

    /// Retrieves a file-object from the aruna object storage. It assumes, that the dataset consists
    /// only of a single object (the file).
    async fn get_single_file_object(&self, aruna_dataset_ids: &ArunaDatasetIds) -> Result<Object> {
        let mut object_stub = self.object_stub.clone();

        object_stub
            .get_object_by_id(GetObjectByIdRequest {
                collection_id: aruna_dataset_ids.collection_id.clone(),
                object_id: aruna_dataset_ids.data_object_id.clone(),
                with_url: false,
            })
            .await?
            .into_inner()
            .object
            .ok_or(ArunaProviderError::MissingDataObject)
            .map(|x| x.object)?
            .ok_or(ArunaProviderError::MissingDataObject)
    }

    /// Creates the loading template for vector files. This is basically a loading
    /// info with a placeholder for the download-url. It will be replaced with
    /// a concrete url on every call to `MetaData.loading_info()`.
    /// This is required, since download links from the core-storage are only valid
    /// for 15 minutes.
    fn vector_loading_template(vi: &VectorInfo, rd: &VectorResultDescriptor) -> OgrSourceDataset {
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
        }
    }

    /// Creates the loading template for raster files. This is basically a loading
    /// info with a placeholder for the download-url. It will be replaced with
    /// a concrete url on every call to `MetaData.loading_info()`.
    /// This is required, since download links from the core-storage are only valid
    /// for 15 minutes.
    fn raster_loading_template(info: &RasterInfo) -> GdalLoadingInfo {
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
        let id: DataId = id.clone().into();

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

        let aruna_data_object = self
            .get_single_file_object(&aruna_dataset_ids)
            .await
            .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            })?;

        match meta_data.data_type {
            DataType::SingleVectorFile(info) => {
                let result_descriptor =
                    Self::create_single_vector_file_result_descriptor(meta_data.crs.into(), &info);
                let template = Self::vector_loading_template(&info, &result_descriptor);

                let res = ArunaMetaData {
                    collection_id: aruna_dataset_ids.collection_id,
                    object_id: aruna_data_object.id,
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
        let id: DataId = id.clone().into();

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

        let aruna_data_object = self
            .get_single_file_object(&aruna_dataset_ids)
            .await
            .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            })?;

        match &meta_data.data_type {
            DataType::SingleRasterFile(info) => {
                let result_descriptor =
                    Self::create_single_raster_file_result_descriptor(meta_data.crs.into(), info)?;
                let template = Self::raster_loading_template(info);

                let res = ArunaMetaData {
                    collection_id: aruna_dataset_ids.collection_id,
                    object_id: aruna_data_object.id,
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

        let mut collection_stub = self.collection_stub.clone();

        let items = collection_stub
            .get_collections(GetCollectionsRequest {
                project_id: self.project_id.to_string(),
                label_or_id_filter: self.label_filter.clone(),
                page_request: None,
            })
            .await
            .map_err(|source| ArunaProviderError::TonicStatus { source })?
            .into_inner()
            .collections
            .ok_or(ArunaProviderError::MissingCollection)?
            .collection_overviews
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
            ),
            DataType::SingleRasterFile(_) => TypedOperator::Raster(
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
                        if let Some(mut params) = new_part.params.as_mut() {
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
                collection_id: self.collection_id.clone(),
                object_id: self.object_id.clone(),
            })
            .await
            .map_err(|source| ArunaProviderError::TonicStatus { source })
            .map_err(|source| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(source),
            })?
            .into_inner()
            .url
            .ok_or(ArunaProviderError::MissingURL)
            .map_err(|source| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(source),
            })?;
        self.template.new_link(url.url)
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
    use crate::api::model::datatypes::{DataId, DataProviderId, ExternalDataId, LayerId};
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
    use aruna_rust_api::api::storage::models::v1::{
        CollectionOverview, CollectionOverviews, KeyValue, Object, ObjectGroupOverview,
        ObjectGroupOverviews,
    };
    use aruna_rust_api::api::storage::services::v1::{
        GetCollectionByIdRequest, GetCollectionByIdResponse, GetCollectionsRequest,
        GetCollectionsResponse, GetDownloadUrlRequest, GetDownloadUrlResponse,
        GetObjectByIdRequest, GetObjectByIdResponse, GetObjectGroupObjectsRequest,
        GetObjectGroupObjectsResponse, GetObjectGroupsRequest, GetObjectGroupsResponse,
        ObjectGroupObject, ObjectWithUrl, Url,
    };
    use futures::StreamExt;
    use geoengine_datatypes::collections::{FeatureCollectionInfos, MultiPointCollection};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, SpatialResolution, TimeInterval, VectorQueryRectangle,
    };
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{
        MetaData, MetaDataProvider, MockExecutionContext, MockQueryContext, QueryProcessor,
        TypedVectorQueryProcessor, VectorOperator, VectorResultDescriptor, WorkflowOperatorPath,
    };
    use geoengine_operators::source::{OgrSource, OgrSourceDataset, OgrSourceParameters};
    use httptest::responders::status_code;
    use httptest::{responders, Expectation, Server};
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::str::FromStr;
    use std::task::Poll;
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;
    use tonic::codegen::http::Request;
    use tonic::codegen::{http, Body, Service};
    use tonic::transport::server::Router;
    use tonic::Code;

    generate_mapping_grpc_service!(
        "aruna.api.storage.services.v1.ObjectService",
        MockMapObjectService,
        "/aruna.api.storage.services.v1.ObjectService/GetDownloadURL",
        GetDownloadUrlRequest,
        GetDownloadUrlResponse,
        download_map,
        F,
        String,
        id_extractor_1,
        "/aruna.api.storage.services.v1.ObjectService/GetObjectByID",
        GetObjectByIdRequest,
        GetObjectByIdResponse,
        id_map,
        G,
        String,
        id_extractor_2,
    );

    generate_mapping_grpc_service!(
        "aruna.api.storage.services.v1.ObjectGroupService",
        MockObjectGroupMapService,
        "/aruna.api.storage.services.v1.ObjectGroupService/GetObjectGroups",
        GetObjectGroupsRequest,
        GetObjectGroupsResponse,
        object_groups,
        F,
        String,
        collection_id_extractor,
        "/aruna.api.storage.services.v1.ObjectGroupService/GetObjectGroupObjects",
        GetObjectGroupObjectsRequest,
        GetObjectGroupObjectsResponse,
        object_group_objects,
        G,
        String,
        group_id_extractor,
    );

    generate_mapping_grpc_service!(
        "aruna.api.storage.services.v1.CollectionService",
        MockCollectionMapService,
        "/aruna.api.storage.services.v1.CollectionService/GetCollections",
        GetCollectionsRequest,
        GetCollectionsResponse,
        collection_overview,
        F,
        String,
        project_extractor,
        "/aruna.api.storage.services.v1.CollectionService/GetCollectionByID",
        GetCollectionByIdRequest,
        GetCollectionByIdResponse,
        id_collection_overview,
        G,
        String,
        id_extractor,
    );

    const PROVIDER_ID: &str = "86a7f7ce-1bab-4ce9-a32b-172c0f958ee0";
    const TOKEN: &str = "DUMMY_TOKEN";

    const PROJECT_ID: &str = "PROJECT_ID";

    const COLLECTION_ID: &str = "COLLECTION_ID";
    const COLLECTION_NAME: &str = "COLLECTION_NAME";
    const COLLECTION_DESCRIPTION: &str = "COLLECTION_DESCRIPTION";

    const OBJECT_GROUP_ID: &str = "OBJECT_GROUP_ID";
    const OBJECT_GROUP_NAME: &str = "OBJECT_GROUP_NAME";

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
            filter_label: FILTER_LABEL.to_string(),
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
    struct DownloadObject {
        id: String,
        filename: String,
        object_type: ObjectType,
        content: Vec<u8>,
        content_length: usize,
        url: Option<String>,
    }

    impl DownloadObject {
        fn set_url(&mut self, server_path: &str) {
            self.url = Some(format!("{server_path}/{}", self.filename));
        }
    }

    fn json_meta_object(value: &Value) -> DownloadObject {
        let vector_data = value.to_string().into_bytes();
        let vector_length = vector_data.len();
        DownloadObject {
            id: META_OBJECT_ID.to_string(),
            filename: META_OBJECT_NAME.to_string(),
            object_type: ObjectType::Json,
            content: vector_data,
            content_length: vector_length,
            url: None,
        }
    }

    async fn raster_data_object() -> DownloadObject {
        let mut data = vec![];
        let mut file = File::open(geoengine_datatypes::test_data!("vector/data/points.fgb"))
            .await
            .unwrap();
        let file_size = file.read_to_end(&mut data).await.unwrap();
        DownloadObject {
            id: DATA_OBJECT_ID.to_string(),
            filename: DATA_OBJECT_NAME.to_string(),
            object_type: ObjectType::Csv,
            content: data,
            content_length: file_size,
            url: None,
        }
    }

    fn create_object(id: String, filename: String) -> Object {
        Object {
            id,
            filename,
            labels: vec![],
            hooks: vec![],
            created: None,
            content_len: 0,
            status: 0,
            origin: None,
            data_class: 0,
            rev_number: 0,
            source: None,
            latest: true,
            auto_update: false,
            hashes: vec![],
        }
    }

    fn default_object_groups() -> HashMap<String, GetObjectGroupsResponse> {
        let mut groups = HashMap::new();
        groups.insert(
            COLLECTION_ID.to_string(),
            GetObjectGroupsResponse {
                object_groups: Some(ObjectGroupOverviews {
                    object_group_overviews: vec![ObjectGroupOverview {
                        id: OBJECT_GROUP_ID.to_string(),
                        name: OBJECT_GROUP_NAME.to_string(),
                        description: String::new(),
                        labels: vec![KeyValue {
                            key: FILTER_LABEL.to_string(),
                            value: String::new(),
                        }],
                        hooks: vec![],
                        stats: None,
                        rev_number: 0,
                    }],
                }),
            },
        );
        groups
    }

    fn default_object_group_objects() -> HashMap<String, GetObjectGroupObjectsResponse> {
        let mut group_objects = HashMap::new();
        group_objects.insert(
            OBJECT_GROUP_ID.to_string(),
            GetObjectGroupObjectsResponse {
                object_group_objects: vec![
                    ObjectGroupObject {
                        object: Some(create_object(
                            META_OBJECT_ID.to_string(),
                            META_OBJECT_NAME.to_string(),
                        )),
                        is_metadata: true,
                    },
                    ObjectGroupObject {
                        object: Some(create_object(
                            DATA_OBJECT_ID.to_string(),
                            DATA_OBJECT_NAME.to_string(),
                        )),
                        is_metadata: false,
                    },
                ],
            },
        );
        group_objects
    }

    fn start_download_server_with(download_objects: &mut Vec<DownloadObject>) -> Server {
        let download_server = Server::run();
        for i in download_objects {
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
                    .times(1)
                    .respond_with(responder),
            );
            i.set_url(format!("http://{}", download_server.addr()).as_str());
        }
        download_server
    }

    async fn mock_server(
        download_server: Option<Server>,
        download_objects: Vec<DownloadObject>,
        object_groups: HashMap<String, GetObjectGroupsResponse>,
        object_group_objects: HashMap<String, GetObjectGroupObjectsResponse>,
    ) -> ArunaMockServer {
        let mut download_map = HashMap::new();
        let mut id_map = HashMap::new();
        for i in download_objects {
            let url = i.url.unwrap();
            download_map.insert(
                i.id.clone(),
                GetDownloadUrlResponse {
                    url: Some(Url { url: url.clone() }),
                },
            );
            id_map.insert(
                i.id.clone(),
                GetObjectByIdResponse {
                    object: Some(ObjectWithUrl {
                        object: Some(create_object(i.id, i.filename)),
                        url,
                        paths: vec![],
                    }),
                },
            );
        }
        let object_service = MockMapObjectService {
            download_map: MapResponseService::new(download_map, |req: GetDownloadUrlRequest| {
                req.object_id
            }),
            id_map: MapResponseService::new(id_map, |req: GetObjectByIdRequest| req.object_id),
        };

        let collection_overview = CollectionOverview {
            id: COLLECTION_ID.to_string(),
            name: COLLECTION_NAME.to_string(),
            description: COLLECTION_DESCRIPTION.to_string(),
            labels: vec![],
            hooks: vec![],
            label_ontology: None,
            created: None,
            stats: None,
            is_public: false,
            version: None,
        };

        let mut overview_map = HashMap::new();
        overview_map.insert(
            PROJECT_ID.to_string(),
            GetCollectionsResponse {
                collections: Some(CollectionOverviews {
                    collection_overviews: vec![collection_overview.clone()],
                }),
            },
        );
        let mut collection_id_map = HashMap::new();
        collection_id_map.insert(
            COLLECTION_ID.to_string(),
            GetCollectionByIdResponse {
                collection: Some(collection_overview),
            },
        );

        let collection_service = MockCollectionMapService {
            collection_overview: MapResponseService::new(
                overview_map,
                |req: GetCollectionsRequest| req.project_id,
            ),
            id_collection_overview: MapResponseService::new(
                collection_id_map,
                |req: GetCollectionByIdRequest| req.collection_id,
            ),
        };

        let object_group_service = MockObjectGroupMapService {
            object_groups: MapResponseService::new(object_groups, |req: GetObjectGroupsRequest| {
                req.collection_id
            }),
            object_group_objects: MapResponseService::new(
                object_group_objects,
                |req: GetObjectGroupObjectsRequest| req.group_id,
            ),
        };

        let builder: Router = tonic::transport::Server::builder()
            .add_service(collection_service)
            .add_service(object_group_service)
            .add_service(object_service);
        let grpc_server = MockGRPCServer::start_with_router(builder).await.unwrap();
        let grpc_server_address = format!("http://{}", grpc_server.address());

        let dataset_ids = ArunaDatasetIds {
            collection_id: COLLECTION_ID.to_string(),
            _object_group_id: OBJECT_GROUP_ID.to_string(),
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
            vec![],
            default_object_groups(),
            default_object_group_objects(),
        )
        .await;

        let result = aruna_mock_server
            .provider
            .get_collection_info(COLLECTION_ID.to_string())
            .await
            .unwrap();

        let expected = ArunaDatasetIds {
            collection_id: COLLECTION_ID.to_string(),
            _object_group_id: OBJECT_GROUP_ID.to_string(),
            meta_object_id: META_OBJECT_ID.to_string(),
            data_object_id: DATA_OBJECT_ID.to_string(),
        };

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn extract_aruna_ids_no_group() {
        let aruna_mock_server = mock_server(None, vec![], HashMap::new(), HashMap::new()).await;

        let result = aruna_mock_server
            .provider
            .get_collection_info(COLLECTION_ID.to_string())
            .await;

        assert!(matches!(
            result,
            Err(ArunaProviderError::TonicStatus { source: _ })
        ));
    }

    #[tokio::test]
    async fn extract_aruna_ids_no_meta_object() {
        let mut group_objects = HashMap::new();
        group_objects.insert(
            OBJECT_GROUP_ID.to_string(),
            GetObjectGroupObjectsResponse {
                object_group_objects: vec![ObjectGroupObject {
                    object: Some(create_object(
                        DATA_OBJECT_ID.to_string(),
                        DATA_OBJECT_NAME.to_string(),
                    )),
                    is_metadata: false,
                }],
            },
        );
        let aruna_mock_server =
            mock_server(None, vec![], default_object_groups(), group_objects).await;

        let result = aruna_mock_server
            .provider
            .get_collection_info(COLLECTION_ID.to_string())
            .await;

        assert!(matches!(result, Err(ArunaProviderError::MissingMetaObject)));
    }

    #[tokio::test]
    async fn extract_aruna_ids_no_data_object() {
        let mut group_objects = HashMap::new();
        group_objects.insert(
            OBJECT_GROUP_ID.to_string(),
            GetObjectGroupObjectsResponse {
                object_group_objects: vec![ObjectGroupObject {
                    object: Some(create_object(
                        META_OBJECT_ID.to_string(),
                        META_OBJECT_NAME.to_string(),
                    )),
                    is_metadata: true,
                }],
            },
        );
        let aruna_mock_server =
            mock_server(None, vec![], default_object_groups(), group_objects).await;

        let result = aruna_mock_server
            .provider
            .get_collection_info(COLLECTION_ID.to_string())
            .await;

        assert!(matches!(result, Err(ArunaProviderError::MissingDataObject)));
    }

    #[tokio::test]
    async fn test_extract_meta_data_ok() {
        let mut download_objects = vec![json_meta_object(&vector_meta_data())];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            HashMap::new(),
            HashMap::new(),
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
        let mut object = json_meta_object(&vector_meta_data());

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
            HashMap::new(),
            HashMap::new(),
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
        let mut object = json_meta_object(&vector_meta_data());

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
            HashMap::new(),
            HashMap::new(),
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
        let mut download_objects = vec![json_meta_object(&vector_meta_data())];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            default_object_groups(),
            default_object_group_objects(),
        )
        .await;

        let layer_id = LayerId(COLLECTION_ID.to_string());
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
                        "data": {
                            "type": "external",
                            "providerId": "86a7f7ce-1bab-4ce9-a32b-172c0f958ee0",
                            "layerId": "COLLECTION_ID"
                        },
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
        let mut download_objects = vec![json_meta_object(&raster_meta_data())];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            default_object_groups(),
            default_object_group_objects(),
        )
        .await;

        let layer_id = LayerId(COLLECTION_ID.to_string());
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
                        "data": {
                            "type": "external",
                            "providerId": "86a7f7ce-1bab-4ce9-a32b-172c0f958ee0",
                            "layerId": "COLLECTION_ID"
                        }
                    }
                }
            }),
            serde_json::to_value(&result.workflow.operator).unwrap()
        );
    }

    #[tokio::test]
    async fn test_vector_loading_template() {
        let mut download_objects = vec![json_meta_object(&vector_meta_data())];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            HashMap::new(),
            HashMap::new(),
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

        let template = ArunaDataProvider::vector_loading_template(&vi, &rd);

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
        let mut download_objects = vec![json_meta_object(&raster_meta_data())];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            HashMap::new(),
            HashMap::new(),
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

        let template = ArunaDataProvider::raster_loading_template(&ri);

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
        let aruna_mock_server = mock_server(None, vec![], HashMap::new(), HashMap::new()).await;
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
        let mut download_objects = vec![json_meta_object(&raster_meta_data())];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            default_object_groups(),
            default_object_group_objects(),
        )
        .await;

        let id = DataId::External(ExternalDataId {
            provider_id: DataProviderId::from_str(PROVIDER_ID).unwrap(),
            layer_id: LayerId(COLLECTION_ID.to_string()),
        });

        let res = aruna_mock_server.provider.provenance(&id).await;

        assert!(res.is_ok());

        let res = res.unwrap();

        assert!(res.provenance.is_some());
    }

    #[tokio::test]
    async fn it_loads_meta_data() {
        let mut download_objects = vec![json_meta_object(&vector_meta_data())];
        let server = start_download_server_with(&mut download_objects);

        let mut raster_object = raster_data_object().await;
        raster_object.set_url(format!("http://{}", server.addr()).as_str());
        download_objects.push(raster_object);

        let aruna_mock_server = mock_server(
            Some(server),
            download_objects,
            default_object_groups(),
            default_object_group_objects(),
        )
        .await;

        let id = DataId::External(ExternalDataId {
            provider_id: DataProviderId::from_str(PROVIDER_ID).unwrap(),
            layer_id: LayerId(COLLECTION_ID.to_string()),
        });

        let res: geoengine_operators::util::Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = aruna_mock_server.provider.meta_data(&id.into()).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_executes_loads() {
        let mut download_objects = vec![
            json_meta_object(&vector_meta_data()),
            raster_data_object().await,
        ];
        let aruna_mock_server = mock_server(
            Some(start_download_server_with(&mut download_objects)),
            download_objects,
            default_object_groups(),
            default_object_group_objects(),
        )
        .await;

        let id = DataId::External(ExternalDataId {
            provider_id: DataProviderId::from_str(PROVIDER_ID).unwrap(),
            layer_id: LayerId(COLLECTION_ID.to_string()),
        });

        let meta: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        > = aruna_mock_server
            .provider
            .meta_data(&id.clone().into())
            .await
            .unwrap();

        let mut context = MockExecutionContext::test_default();
        context.add_meta_data(id.clone().into(), meta);

        let src = OgrSource {
            params: OgrSourceParameters {
                data: id.into(),
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
        let TypedVectorQueryProcessor::MultiPoint(proc) = proc else { panic!("Expected MultiPoint QueryProcessor"); };

        let ctx = MockQueryContext::test_default();

        let qr = VectorQueryRectangle::with_bounds_and_resolution(
            BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
            TimeInterval::default(),
            SpatialResolution::zero_point_one(),
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
