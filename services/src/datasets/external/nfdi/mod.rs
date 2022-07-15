use crate::datasets::external::nfdi::metadata::{DataType, GEMetadata, RasterInfo, VectorInfo};
use crate::datasets::listing::{
     ProvenanceOutput,
};
use crate::error::{Error, Result, self};
use crate::layers::external::{DataProviderDefinition, DataProvider};
use crate::layers::layer::{LayerCollectionListOptions, CollectionItem, Layer, LayerListing, ProviderLayerId};
use crate::layers::listing::{LayerCollectionProvider, LayerCollectionId};
use crate::util::operators::source_operator_from_dataset;
use crate::util::user_input::Validated;
use crate::workflows::workflow::Workflow;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::dataset::{DataId, ExternalDataId, DataProviderId, LayerId};
use geoengine_datatypes::primitives::{
    FeatureDataType, Measurement, RasterQueryRectangle, VectorQueryRectangle,
};
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterResultDescriptor, ResultDescriptor, TypedResultDescriptor,
    VectorResultDescriptor, VectorOperator, TypedOperator, RasterOperator,
    VectorColumnInfo, OperatorName,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetParameters, GdalLoadingInfo, GdalLoadingInfoTemporalSlice,
    GdalLoadingInfoTemporalSliceIterator, OgrSourceColumnSpec, OgrSourceDataset,
    OgrSourceDatasetTimeType, OgrSourceDurationSpec, OgrSourceErrorSpec, OgrSourceTimeFormat, OgrSourceParameters, OgrSource, GdalSource, GdalSourceParameters,
};
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::models::v1::Object;
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::services::v1::dataset_service_client::DatasetServiceClient;
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::services::v1::object_load_service_client::ObjectLoadServiceClient;
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::services::v1::project_service_client::ProjectServiceClient;
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::services::v1::{
    CreateDownloadLinkRequest, GetDatasetObjectGroupsRequest, GetDatasetRequest,
    GetProjectDatasetsRequest,
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

pub mod metadata;

const URL_REPLACEMENT: &str = "%URL%";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NFDIDataProviderDefinition {
    id: DataProviderId,
    name: String,
    api_url: String,
    project_id: String,
    api_token: String,
}

#[typetag::serde]
#[async_trait::async_trait]
impl DataProviderDefinition for NFDIDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> Result<Box<dyn DataProvider>> {
        Ok(Box::new(NFDIDataProvider::new(self).await?))
    }

    fn type_name(&self) -> String {
        "NFDI".to_owned()
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
        let key = AsciiMetadataKey::from_static("api_token");
        let value = AsciiMetadataValue::from_str(token).map_err(|_| Error::InvalidAPIToken {
            message: "Could not encode configured token as ASCII.".to_owned(),
        })?;

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

/// The actual provider implementation. It holds `gRPC` stubs to all relevant
/// API endpoints. Those stubs need to be cloned, because all calls require
/// a mutable self reference. However, according to the docs, cloning
/// is cheap.
#[derive(Debug)]
pub struct NFDIDataProvider {
    id: DataProviderId,
    project_id: String,
    project_stub: ProjectServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    dataset_stub: DatasetServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    object_stub: ObjectLoadServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
}

impl NFDIDataProvider {
    /// Creates a new provider from the given definition.
    async fn new(def: Box<NFDIDataProviderDefinition>) -> Result<NFDIDataProvider> {
        let url = def.api_url;
        let channel = Endpoint::from_str(url.as_str())
            .map_err(|_| Error::InvalidUri { uri_string: url })?
            .connect()
            .await?;

        let interceptor = APITokenInterceptor::new(&def.api_token[..])?;

        let project_stub =
            ProjectServiceClient::with_interceptor(channel.clone(), interceptor.clone());
        let dataset_stub =
            DatasetServiceClient::with_interceptor(channel.clone(), interceptor.clone());

        let object_stub = ObjectLoadServiceClient::with_interceptor(channel, interceptor);

        Ok(NFDIDataProvider {
            id: def.id,
            project_id: def.project_id,
            project_stub,
            dataset_stub,
            object_stub,
        })
    }

    /// Extracts the core store id from the given dataset id
    fn dataset_nfdi_id(id: &DataId) -> Result<String> {
        match id {
            DataId::External(id) => Ok(id.layer_id.0.clone()),
            DataId::Internal { .. } => Err(Error::InvalidDataId),
        }
    }

    /// Extracts the geoengine metadata from a Dataset returnd from the core store
    fn extract_metadata(
        ds: &scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::models::v1::Dataset,
    ) -> Result<GEMetadata> {
        Ok(serde_json::from_slice::<GEMetadata>(
            ds.metadata
                .iter()
                .find(|ds| ds.key.eq_ignore_ascii_case(metadata::METADATA_KEY))
                .ok_or(Error::MissingNFDIMetaData)?
                .metadata
                .as_slice(),
        )?)
    }

    /// Retrieves information for the datasat with the given id.
    async fn dataset_info(&self, id: &DataId) -> Result<(Layer, GEMetadata)> {
        let id = Self::dataset_nfdi_id(id)?;
        let mut stub = self.dataset_stub.clone();

        let resp = stub
            .get_dataset(GetDatasetRequest { id })
            .await?
            .into_inner();

        resp.dataset.ok_or(Error::InvalidDataId).and_then(|ds| {
            // Extract and parse geoengine metadata
            let md = Self::extract_metadata(&ds)?;
            Ok((self.map_layer(&ds, &md)?, md))
        })
    }

    /// Maps the `gRPC` dataset representation to geoengine's internal representation.
    fn map_layer(
        &self,
        ds: &scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::models::v1::Dataset,
        md: &GEMetadata,
    ) -> Result<Layer> {
        let id = ProviderLayerId {
            provider_id: self.id,
            layer_id: LayerId(ds.id.clone()),
        };

        // Create type specific infos
        let (_result_descriptor, source_operator) = match &md.data_type {
            DataType::SingleVectorFile(info) => (
                TypedResultDescriptor::Vector(Self::create_vector_result_descriptor(
                    md.crs.into(),
                    info,
                )),
                OgrSource::TYPE_NAME,
            ),
            DataType::SingleRasterFile(info) => (
                TypedResultDescriptor::Raster(Self::create_raster_result_descriptor(
                    md.crs.into(),
                    info,
                )),
                GdalSource::TYPE_NAME,
            ),
        };

        Ok(Layer {
            id,
            name: ds.name.clone(),
            description: ds.description.clone(),
            workflow: Workflow {
                operator: source_operator_from_dataset(
                    source_operator,
                    &DataId::External(ExternalDataId {
                        provider_id: self.id,
                        layer_id: LayerId(ds.id.clone()),
                    }),
                )?,
            },
            symbology: None,
        })
    }

    /// Creates a result descriptor for vector data
    fn create_vector_result_descriptor(
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
            time: None,
            bbox: None,
        }
    }

    /// Creates a result descriptor for raster data
    fn create_raster_result_descriptor(
        crs: SpatialReferenceOption,
        info: &RasterInfo,
    ) -> RasterResultDescriptor {
        RasterResultDescriptor {
            data_type: info.data_type,
            spatial_reference: crs,
            measurement: info
                .measurement
                .as_ref()
                .map_or(Measurement::Unitless, Clone::clone),
            time: None,
            bbox: None,
        }
    }

    /// Retrieves a file-object from the core-storage. It assumes, that the dataset consists
    /// only of a single object group with a single object (the file).
    async fn get_single_file_object(&self, id: &DataId) -> Result<Object> {
        let mut ds_stub = self.dataset_stub.clone();

        let group = ds_stub
            .get_dataset_object_groups(GetDatasetObjectGroupsRequest {
                id: Self::dataset_nfdi_id(id)?,
                page_request: None,
            })
            .await?
            .into_inner()
            .object_groups
            .into_iter()
            .next()
            .ok_or(Error::NoMainFileCandidateFound)?;

        group
            .objects
            .into_iter()
            .next()
            .ok_or(Error::NoMainFileCandidateFound)
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

        let link = format!("/vsicurl/{}", URL_REPLACEMENT);

        let column_spec = OgrSourceColumnSpec {
            format_specifics: None,
            x: "".to_string(),
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
                file_path: PathBuf::from(format!("/vsicurl/{}", URL_REPLACEMENT)),
                rasterband_channel: info.rasterband_channel,
                geo_transform: info.geo_transform,
                width: info.width,
                height: info.height,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: None,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
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
    for NFDIDataProvider
{
    async fn meta_data(
        &self,
        _id: &DataId,
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
    for NFDIDataProvider
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
    > {
        let (_, md) = self.dataset_info(id).await.map_err(|e| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            }
        })?;

        let object = self.get_single_file_object(id).await.map_err(|e| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            }
        })?;

        match md.data_type {
            DataType::SingleVectorFile(info) => {
                let result_descriptor = Self::create_vector_result_descriptor(md.crs.into(), &info);
                let template = Self::vector_loading_template(&info, &result_descriptor);

                let res = NFDIMetaData {
                    object_id: object.id,
                    template,
                    result_descriptor,
                    _phantom: Default::default(),
                    object_stub: self.object_stub.clone(),
                };
                Ok(Box::new(res))
            }
            DataType::SingleRasterFile(_) => Err(geoengine_operators::error::Error::InvalidType {
                found: md.data_type.to_string(),
                expected: "SingleVectorFile".to_string(),
            }),
        }
    }
}

#[async_trait::async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for NFDIDataProvider
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
    > {
        let (_, md) = self.dataset_info(id).await.map_err(|e| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            }
        })?;

        let object = self.get_single_file_object(id).await.map_err(|e| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            }
        })?;

        match &md.data_type {
            DataType::SingleRasterFile(info) => {
                let result_descriptor = Self::create_raster_result_descriptor(md.crs.into(), info);
                let template = Self::raster_loading_template(info);

                let res = NFDIMetaData {
                    object_id: object.id,
                    template,
                    result_descriptor,
                    _phantom: Default::default(),
                    object_stub: self.object_stub.clone(),
                };
                Ok(Box::new(res))
            }
            DataType::SingleVectorFile(_) => Err(geoengine_operators::error::Error::InvalidType {
                found: md.data_type.to_string(),
                expected: "SingleRasterFile".to_string(),
            }),
        }
    }
}

#[async_trait::async_trait]
impl DataProvider for NFDIDataProvider {
    async fn provenance(&self, id: &DataId) -> Result<ProvenanceOutput> {
        let (_, metadata) = self.dataset_info(id).await?;

        Ok(ProvenanceOutput {
            data: id.clone(),
            provenance: metadata.provenance,
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait::async_trait]
impl LayerCollectionProvider for NFDIDataProvider {
    async fn collection_items(
        &self,
        collection: &LayerCollectionId,
        _options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        ensure!(
            *collection == self.root_collection_id().await?,
            error::UnknownLayerCollectionId {
                id: collection.clone()
            }
        );

        let mut project_stub = self.project_stub.clone();

        let resp = project_stub
            .get_project_datasets(GetProjectDatasetsRequest {
                id: self.project_id.clone(),
            })
            .await?
            .into_inner();

        Ok(resp
            .datasets
            .into_iter()
            .map(|ds| {
                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: self.id,
                        layer_id: LayerId(ds.id),
                    },
                    name: ds.name,
                    description: ds.description,
                })
            })
            .collect())
    }

    async fn root_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId("root".to_string()))
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        let mut project_stub = self.project_stub.clone();

        let resp = project_stub
            .get_project_datasets(GetProjectDatasetsRequest {
                id: self.project_id.clone(),
            })
            .await?
            .into_inner();

        let dataset = resp
            .datasets
            .into_iter()
            .find(|ds| ds.id == id.0)
            .ok_or(Error::UnknownDataId)?;

        let meta_data = Self::extract_metadata(&dataset)?;

        let operator = match meta_data.data_type {
            DataType::SingleVectorFile(_) => TypedOperator::Vector(
                OgrSource {
                    params: OgrSourceParameters {
                        data: DataId::External(ExternalDataId {
                            provider_id: self.id,
                            layer_id: id.clone(),
                        }),
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
                        }),
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
struct NFDIMetaData<L, R, Q>
where
    L: Debug + Clone + Send + Sync + ExpiringDownloadLink + 'static,
    R: Debug + Send + Sync + 'static + ResultDescriptor,
    Q: Debug + Clone + Send + Sync + 'static,
{
    result_descriptor: R,
    object_id: String,
    template: L,
    object_stub: ObjectLoadServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    _phantom: PhantomData<Q>,
}

#[async_trait::async_trait]
impl<L, R, Q> MetaData<L, R, Q> for NFDIMetaData<L, R, Q>
where
    L: Debug + Clone + Send + Sync + ExpiringDownloadLink + 'static,
    R: Debug + Send + Sync + 'static + ResultDescriptor,
    Q: Debug + Clone + Send + Sync + 'static,
{
    async fn loading_info(&self, _query: Q) -> geoengine_operators::util::Result<L> {
        let mut stub = self.object_stub.clone();
        let url = stub
            .create_download_link(CreateDownloadLinkRequest {
                id: self.object_id.clone(),
                range: None,
            })
            .await
            .map_err(|source| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(source),
            })?
            .into_inner();
        self.template.new_link(url.download_link)
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
    use crate::datasets::external::nfdi::metadata::{GEMetadata, METADATA_KEY};
    use crate::datasets::external::nfdi::{
        ExpiringDownloadLink, NFDIDataProvider, NFDIDataProviderDefinition,
    };
    use crate::layers::external::DataProvider;
    use crate::layers::layer::LayerCollectionListOptions;
    use crate::layers::listing::LayerCollectionProvider;
    use futures::StreamExt;
    use geoengine_datatypes::dataset::{DataId, DataProviderId, ExternalDataId, LayerId};
    use httptest::responders::status_code;
    use httptest::{Expectation, Server};
    use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::models::v1::{
        Dataset, Metadata, Object, ObjectGroup,
    };
    use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::services::v1::{
        CreateDownloadLinkResponse, GetDatasetObjectGroupsResponse, GetDatasetResponse,
        GetProjectDatasetsResponse,
    };
    use serde_json::{json, Value};
    use std::str::FromStr;
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    use crate::util::user_input::UserInput;

    use geoengine_datatypes::collections::{FeatureCollectionInfos, MultiPointCollection};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, SpatialResolution, TimeInterval, VectorQueryRectangle,
    };
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{
        MetaData, MetaDataProvider, MockExecutionContext, MockQueryContext, QueryProcessor,
        TypedVectorQueryProcessor, VectorOperator, VectorResultDescriptor,
    };
    use geoengine_operators::source::{OgrSource, OgrSourceDataset, OgrSourceParameters};

    mod wiremock_gen {
        wiremock_grpc::generate!("sciobjsdb.api.storage.services.v1", TestProjectServer);
    }
    use wiremock_gen::*;
    use wiremock_grpc::*;

    const PROVIDER_ID: &str = "86a7f7ce-1bab-4ce9-a32b-172c0f958ee0";
    const PROJECT_ID: &str = "B";
    const DATASET_ID: &str = "C";
    const TOKEN: &str = "DUMMY";

    async fn new_provider_with_url(url: String) -> NFDIDataProvider {
        let def = NFDIDataProviderDefinition {
            id: DataProviderId::from_str(PROVIDER_ID).unwrap(),
            api_token: TOKEN.to_string(),
            api_url: url,
            project_id: PROJECT_ID.to_string(),
            name: "NFDI".to_string(),
        };
        NFDIDataProvider::new(Box::new(def)).await.unwrap()
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
            "provenance":{
            "citation":"Test",
            "license":"MIT",
            "uri":"http://geoengine.io"
            }
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
            "provenance":{
                "citation":"Test",
                "license":"MIT",
                "uri":"http://geoengine.io"
            }
        })
    }

    #[tokio::test]
    async fn test_extract_meta_data_ok() {
        let ds = Dataset {
            id: DATASET_ID.to_string(),
            name: "Test".to_string(),
            description: "Test".to_string(),
            created: None,
            labels: vec![],
            metadata: vec![Metadata {
                key: METADATA_KEY.to_string(),
                labels: vec![],
                metadata: vector_meta_data().to_string().into_bytes(),
                schema: None,
            }],
            project_id: PROJECT_ID.to_string(),
            is_public: true,
            status: 0,
            bucket: "".to_string(),
        };
        let md = NFDIDataProvider::extract_metadata(&ds).unwrap();
        let des = serde_json::from_value::<GEMetadata>(vector_meta_data()).unwrap();
        assert_eq!(des, md);
    }

    #[tokio::test]
    async fn test_extract_meta_data_not_present() {
        let ds = Dataset {
            id: DATASET_ID.to_string(),
            name: "Test".to_string(),
            description: "Test".to_string(),
            created: None,
            labels: vec![],
            metadata: vec![],
            project_id: PROJECT_ID.to_string(),
            is_public: true,
            status: 0,
            bucket: "".to_string(),
        };
        assert!(NFDIDataProvider::extract_metadata(&ds).is_err());
    }

    #[tokio::test]
    async fn test_extract_meta_data_parse_error() {
        let ds = Dataset {
            id: DATASET_ID.to_string(),
            name: "Test".to_string(),
            description: "Test".to_string(),
            created: None,
            labels: vec![],
            metadata: vec![Metadata {
                key: METADATA_KEY.to_string(),
                labels: vec![],
                metadata: b"{\"foo\": \"bar\"}".to_vec(),
                schema: None,
            }],
            project_id: PROJECT_ID.to_string(),
            is_public: true,
            status: 0,
            bucket: "".to_string(),
        };
        assert!(NFDIDataProvider::extract_metadata(&ds).is_err());
    }

    #[tokio::test]
    async fn test_map_vector_dataset() {
        let ds = Dataset {
            id: DATASET_ID.to_string(),
            name: "Test".to_string(),
            description: "Test".to_string(),
            created: None,
            labels: vec![],
            metadata: vec![Metadata {
                key: METADATA_KEY.to_string(),
                labels: vec![],
                metadata: vector_meta_data().to_string().into_bytes(),
                schema: None,
            }],
            project_id: PROJECT_ID.to_string(),
            is_public: true,
            status: 0,
            bucket: "".to_string(),
        };

        let server = TestProjectServer::start_default().await;
        let addr = format!("http://{}", server.address());
        let provider = new_provider_with_url(addr).await;

        let md = NFDIDataProvider::extract_metadata(&ds).unwrap();
        let layer = provider.map_layer(&ds, &md).unwrap();
        assert!(matches!(
            md.data_type,
            super::metadata::DataType::SingleVectorFile(_)
        ));
        assert_eq!(
            json!({
                "type": "Vector",
                "operator": {
                    "type": "OgrSource",
                    "params": {
                        "data": {
                            "type": "external",
                            "providerId": "86a7f7ce-1bab-4ce9-a32b-172c0f958ee0",
                            "layerId": "C"
                        },
                        "attributeProjection": null,
                        "attributeFilters": null
                    }
                }
            }),
            serde_json::to_value(&layer.workflow.operator).unwrap()
        );
    }

    #[tokio::test]
    async fn test_map_raster_dataset() {
        let ds = Dataset {
            id: DATASET_ID.to_string(),
            name: "Test".to_string(),
            description: "Test".to_string(),
            created: None,
            labels: vec![],
            metadata: vec![Metadata {
                key: METADATA_KEY.to_string(),
                labels: vec![],
                metadata: raster_meta_data().to_string().into_bytes(),
                schema: None,
            }],
            project_id: PROJECT_ID.to_string(),
            is_public: true,
            status: 0,
            bucket: "".to_string(),
        };

        let md = NFDIDataProvider::extract_metadata(&ds).unwrap();
        assert!(matches!(
            md.data_type,
            super::metadata::DataType::SingleRasterFile(_)
        ));

        let server = TestProjectServer::start_default().await;
        let addr = format!("http://{}", server.address());
        let provider = new_provider_with_url(addr).await;

        let layer = provider.map_layer(&ds, &md).unwrap();

        assert_eq!(
            json!({
                "type": "Raster",
                "operator": {
                    "type": "GdalSource",
                    "params": {
                        "data": {
                            "type": "external",
                            "providerId": "86a7f7ce-1bab-4ce9-a32b-172c0f958ee0",
                            "layerId": "C"
                        }
                    }
                }
            }),
            serde_json::to_value(&layer.workflow.operator).unwrap()
        );
    }

    #[tokio::test]
    async fn test_vector_loading_template() {
        let ds = Dataset {
            id: DATASET_ID.to_string(),
            name: "Test".to_string(),
            description: "Test".to_string(),
            created: None,
            labels: vec![],
            metadata: vec![Metadata {
                key: METADATA_KEY.to_string(),
                labels: vec![],
                metadata: vector_meta_data().to_string().into_bytes(),
                schema: None,
            }],
            project_id: PROJECT_ID.to_string(),
            is_public: true,
            status: 0,
            bucket: "".to_string(),
        };

        let md = NFDIDataProvider::extract_metadata(&ds).unwrap();
        let vi = match md.data_type {
            super::metadata::DataType::SingleVectorFile(vi) => vi,
            super::metadata::DataType::SingleRasterFile(_) => panic!("Expected vector description"),
        };

        let rd = NFDIDataProvider::create_vector_result_descriptor(md.crs.into(), &vi);

        let template = NFDIDataProvider::vector_loading_template(&vi, &rd);

        let url = template
            .new_link("test".to_string())
            .unwrap()
            .file_name
            .to_string_lossy()
            .to_string();

        assert_eq!("/vsicurl/test", url.as_str());
    }

    #[tokio::test]
    async fn test_raster_loading_template() {
        let ds = Dataset {
            id: DATASET_ID.to_string(),
            name: "Test".to_string(),
            description: "Test".to_string(),
            created: None,
            labels: vec![],
            metadata: vec![Metadata {
                key: METADATA_KEY.to_string(),
                labels: vec![],
                metadata: raster_meta_data().to_string().into_bytes(),
                schema: None,
            }],
            project_id: PROJECT_ID.to_string(),
            is_public: true,
            status: 0,
            bucket: "".to_string(),
        };

        let md = NFDIDataProvider::extract_metadata(&ds).unwrap();
        let ri = match md.data_type {
            super::metadata::DataType::SingleRasterFile(ri) => ri,
            super::metadata::DataType::SingleVectorFile(_) => panic!("Expected raster description"),
        };

        let template = NFDIDataProvider::raster_loading_template(&ri);

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

        assert_eq!("/vsicurl/test", url.as_str());
    }

    #[tokio::test]
    async fn it_lists() {
        let mut server = TestProjectServer::start_default().await;

        server.setup(
            MockBuilder::when()
                .path("/sciobjsdb.api.storage.services.v1.ProjectService/GetProjectDatasets")
                .then()
                .return_status(tonic::Code::Ok)
                .return_body(|| GetProjectDatasetsResponse {
                    datasets: vec![Dataset {
                        id: DATASET_ID.to_string(),
                        name: "Test".to_string(),
                        description: "Test".to_string(),
                        created: None,
                        labels: vec![],
                        metadata: vec![Metadata {
                            key: METADATA_KEY.to_string(),
                            labels: vec![],
                            metadata: raster_meta_data().to_string().into_bytes(),
                            schema: None,
                        }],
                        project_id: PROJECT_ID.to_string(),
                        is_public: true,
                        status: 0,
                        bucket: "".to_string(),
                    }],
                }),
        );

        let addr = format!("http://{}", server.address());
        let provider = new_provider_with_url(addr).await;

        let root = provider.root_collection_id().await.unwrap();

        let opts = LayerCollectionListOptions {
            limit: 100,
            offset: 0,
        }
        .validated()
        .unwrap();

        let res = provider.collection_items(&root, opts).await;
        assert!(res.is_ok());
        let res = res.unwrap();
        assert_eq!(1, res.len());
    }

    #[tokio::test]
    async fn it_loads_provenance() {
        let mut server = TestProjectServer::start_default().await;

        server.setup(
            MockBuilder::when()
                .path("/sciobjsdb.api.storage.services.v1.DatasetService/GetDataset")
                .then()
                .return_status(tonic::Code::Ok)
                .return_body(|| GetDatasetResponse {
                    dataset: Some(Dataset {
                        id: DATASET_ID.to_string(),
                        name: "Test".to_string(),
                        description: "Test".to_string(),
                        created: None,
                        labels: vec![],
                        metadata: vec![Metadata {
                            key: METADATA_KEY.to_string(),
                            labels: vec![],
                            metadata: raster_meta_data().to_string().into_bytes(),
                            schema: None,
                        }],
                        project_id: PROJECT_ID.to_string(),
                        is_public: true,
                        status: 0,
                        bucket: "".to_string(),
                    }),
                }),
        );

        let id = DataId::External(ExternalDataId {
            provider_id: DataProviderId::from_str(PROVIDER_ID).unwrap(),
            layer_id: LayerId(DATASET_ID.to_string()),
        });

        let addr = format!("http://{}", server.address());
        let provider = new_provider_with_url(addr).await;

        let res = provider.provenance(&id).await;

        assert!(res.is_ok());

        let res = res.unwrap();

        assert!(res.provenance.is_some());
    }

    #[tokio::test]
    async fn it_loads_meta_data() {
        let mut server = TestProjectServer::start_default().await;

        server.setup(
            MockBuilder::when()
                .path("/sciobjsdb.api.storage.services.v1.DatasetService/GetDataset")
                .then()
                .return_status(tonic::Code::Ok)
                .return_body(|| GetDatasetResponse {
                    dataset: Some(Dataset {
                        id: DATASET_ID.to_string(),
                        name: "Test".to_string(),
                        description: "Test".to_string(),
                        created: None,
                        labels: vec![],
                        metadata: vec![Metadata {
                            key: METADATA_KEY.to_string(),
                            labels: vec![],
                            metadata: vector_meta_data().to_string().into_bytes(),
                            schema: None,
                        }],
                        project_id: PROJECT_ID.to_string(),
                        is_public: true,
                        status: 0,
                        bucket: "".to_string(),
                    }),
                }),
        );

        server.setup(
            MockBuilder::when()
                .path("/sciobjsdb.api.storage.services.v1.DatasetService/GetDatasetObjectGroups")
                .then()
                .return_status(tonic::Code::Ok)
                .return_body(|| GetDatasetObjectGroupsResponse {
                    object_groups: vec![ObjectGroup {
                        id: "OG".to_string(),
                        name: "Test".to_string(),
                        description: "Test".to_string(),
                        dataset_id: DATASET_ID.to_string(),
                        project_id: PROJECT_ID.to_string(),
                        labels: vec![],
                        metadata: vec![],
                        status: 0,
                        objects: vec![Object {
                            id: "OBJ".to_string(),
                            filename: "".to_string(),
                            filetype: "".to_string(),
                            labels: vec![],
                            metadata: vec![],
                            created: None,
                            location: None,
                            origin: None,
                            content_len: 0,
                            upload_id: "".to_string(),
                            generated: None,
                            object_group_id: "OG".to_string(),
                            dataset_id: DATASET_ID.to_string(),
                            project_id: PROJECT_ID.to_string(),
                        }],
                        generated: None,
                        created: None,
                    }],
                }),
        );

        let id = DataId::External(ExternalDataId {
            provider_id: DataProviderId::from_str(PROVIDER_ID).unwrap(),
            layer_id: LayerId(DATASET_ID.to_string()),
        });

        let addr = format!("http://{}", server.address());
        let provider = new_provider_with_url(addr).await;

        let res: geoengine_operators::util::Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = provider.meta_data(&id).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_executes_loads() {
        let mut server = TestProjectServer::start_default().await;
        let dl_server = Server::run();

        let addr = format!("http://{}", server.address());
        let dl_addr = format!("http://{}/file.fgb", dl_server.addr());

        server.setup(
            MockBuilder::when()
                .path("/sciobjsdb.api.storage.services.v1.DatasetService/GetDataset")
                .then()
                .return_status(tonic::Code::Ok)
                .return_body(|| GetDatasetResponse {
                    dataset: Some(Dataset {
                        id: DATASET_ID.to_string(),
                        name: "Test".to_string(),
                        description: "Test".to_string(),
                        created: None,
                        labels: vec![],
                        metadata: vec![Metadata {
                            key: METADATA_KEY.to_string(),
                            labels: vec![],
                            metadata: vector_meta_data().to_string().into_bytes(),
                            schema: None,
                        }],
                        project_id: PROJECT_ID.to_string(),
                        is_public: true,
                        status: 0,
                        bucket: "".to_string(),
                    }),
                }),
        );

        server.setup(
            MockBuilder::when()
                .path("/sciobjsdb.api.storage.services.v1.DatasetService/GetDatasetObjectGroups")
                .then()
                .return_status(tonic::Code::Ok)
                .return_body(|| GetDatasetObjectGroupsResponse {
                    object_groups: vec![ObjectGroup {
                        id: "OG".to_string(),
                        name: "Test".to_string(),
                        description: "Test".to_string(),
                        dataset_id: DATASET_ID.to_string(),
                        project_id: PROJECT_ID.to_string(),
                        labels: vec![],
                        metadata: vec![],
                        status: 0,
                        objects: vec![Object {
                            id: "OBJ".to_string(),
                            filename: "points.fgb".to_string(),
                            filetype: "fgb".to_string(),
                            labels: vec![],
                            metadata: vec![],
                            created: None,
                            location: None,
                            origin: None,
                            content_len: 0,
                            upload_id: "".to_string(),
                            generated: None,
                            object_group_id: "OG".to_string(),
                            dataset_id: DATASET_ID.to_string(),
                            project_id: PROJECT_ID.to_string(),
                        }],
                        generated: None,
                        created: None,
                    }],
                }),
        );

        server.setup(
            MockBuilder::when()
                .path("/sciobjsdb.api.storage.services.v1.ObjectLoadService/CreateDownloadLink")
                .then()
                .return_status(tonic::Code::Ok)
                .return_body(|| CreateDownloadLinkResponse {
                    download_link: dl_addr.clone(),
                    object: None,
                }),
        );

        let mut data = vec![];
        let mut file = File::open(geoengine_datatypes::test_data!("vector/data/points.fgb"))
            .await
            .unwrap();
        let file_size = file.read_to_end(&mut data).await.unwrap();

        let responder = status_code(200)
            .append_header("content-type", "text/csv")
            .append_header("content-length", file_size)
            .body(data);

        dl_server.expect(
            Expectation::matching(httptest::matchers::request::path("/file.fgb"))
                .times(1..)
                .respond_with(responder),
        );

        let id = DataId::External(ExternalDataId {
            provider_id: DataProviderId::from_str(PROVIDER_ID).unwrap(),
            layer_id: LayerId(DATASET_ID.to_string()),
        });

        let provider = new_provider_with_url(addr).await;

        let meta: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        > = provider.meta_data(&id).await.unwrap();

        let mut context = MockExecutionContext::test_default();
        context.add_meta_data(id.clone(), meta);

        let src = OgrSource {
            params: OgrSourceParameters {
                data: id,
                attribute_projection: None,
                attribute_filters: None,
            },
        }
        .boxed();

        let initialized_op = src.initialize(&context).await.unwrap();

        let proc = initialized_op.query_processor().unwrap();
        let proc = match proc {
            TypedVectorQueryProcessor::MultiPoint(qp) => qp,
            _ => panic!("Expected MultiPoint QueryProcessor"),
        };

        let ctx = MockQueryContext::test_default();

        let qr = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
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
