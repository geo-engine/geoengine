use crate::datasets::external::nfdi::metadata::{DataType, GEMetadata, RasterInfo, VectorInfo};
use crate::datasets::listing::{DatasetListOptions, DatasetListing, DatasetProvider};
use crate::datasets::provenance::{ProvenanceOutput, ProvenanceProvider};
use crate::datasets::storage::{Dataset, DatasetProviderDefinition};
use crate::error::{Error, Result};
use crate::util::user_input::Validated;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
use geoengine_datatypes::primitives::{FeatureDataType, Measurement};
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterQueryRectangle, RasterResultDescriptor, ResultDescriptor,
    TypedResultDescriptor, VectorQueryRectangle, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetParameters, GdalLoadingInfo, GdalLoadingInfoPart,
    GdalLoadingInfoPartIterator, OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType,
    OgrSourceErrorSpec,
};
use scienceobjectsdb_rust_api::sciobjectsdbapi::models::v1::Object;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::dataset_service_client::DatasetServiceClient;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::object_load_service_client::ObjectLoadServiceClient;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::project_service_client::ProjectServiceClient;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::{
    CreateDownloadLinkRequest, GetDatasetObjectGroupsRequest, GetDatasetRequest,
    GetProjectDatasetsRequest,
};
use serde::{Deserialize, Serialize};
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
    id: DatasetProviderId,
    name: String,
    api_url: String,
    project_id: String,
    api_token: String,
}

#[typetag::serde]
#[async_trait::async_trait]
impl DatasetProviderDefinition for NFDIDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> Result<Box<dyn DatasetProvider>> {
        Ok(Box::new(NFDIDataProvider::new(self).await?))
    }

    fn type_name(&self) -> String {
        "NFDI".to_owned()
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DatasetProviderId {
        self.id
    }
}

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

pub struct NFDIDataProvider {
    id: DatasetProviderId,
    project_id: String,
    project_stub: ProjectServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    dataset_stub: DatasetServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    object_stub: ObjectLoadServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
}

impl NFDIDataProvider {
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

        let object_stub = ObjectLoadServiceClient::with_interceptor(channel, interceptor.clone());

        Ok(NFDIDataProvider {
            id: def.id,
            project_id: def.project_id,
            project_stub,
            dataset_stub,
            object_stub,
        })
    }

    /// Extracts the core store id from the given dataset id
    fn dataset_nfdi_id(id: &DatasetId) -> Result<String> {
        match id {
            DatasetId::External(id) => Ok(id.dataset_id.clone()),
            _ => Err(Error::InvalidDatasetId),
        }
    }

    /// Extracts the geoengine metadata from a Dataset returnd from the core store
    fn extract_metadata(
        ds: &scienceobjectsdb_rust_api::sciobjectsdbapi::models::v1::Dataset,
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

    async fn dataset_info(&self, id: &DatasetId) -> Result<(Dataset, GEMetadata)> {
        let id = Self::dataset_nfdi_id(id)?;
        let mut stub = self.dataset_stub.clone();

        let resp = stub
            .get_dataset(GetDatasetRequest { id })
            .await?
            .into_inner();

        resp.dataset.ok_or(Error::InvalidDatasetId).and_then(|ds| {
            // Extract and parse geoengine metadata
            let md = Self::extract_metadata(&ds)?;
            self.map_dataset(&ds, &md).map(|ds| (ds, md))
        })
    }

    fn map_dataset(
        &self,
        ds: &scienceobjectsdb_rust_api::sciobjectsdbapi::models::v1::Dataset,
        md: &GEMetadata,
    ) -> Result<Dataset> {
        let id = DatasetId::External(ExternalDatasetId {
            provider_id: self.id.clone(),
            dataset_id: ds.id.clone(),
        });

        // Create type specific infos
        let (result_descriptor, source_operator) = match &md.data_type {
            DataType::SingleVectorFile(info) => (
                TypedResultDescriptor::Vector(
                    self.create_vector_result_descriptor(md.crs.clone().into(), &info)?,
                ),
                "OgrSource".to_string(),
            ),
            DataType::SingleRasterFile(info) => (
                TypedResultDescriptor::Raster(
                    self.create_raster_result_descriptor(md.crs.clone().into(), info)?,
                ),
                "GdalSource".to_string(),
            ),
        };

        Ok(Dataset {
            id,
            name: ds.name.clone(),
            description: ds.description.clone(),
            source_operator,
            result_descriptor,
            symbology: None,
            provenance: md.provenance.clone(),
        })
    }

    fn create_vector_result_descriptor(
        &self,
        crs: SpatialReferenceOption,
        info: &VectorInfo,
    ) -> Result<VectorResultDescriptor> {
        let columns = info
            .attributes
            .iter()
            .map(|a| (a.name.clone(), a.r#type))
            .collect::<HashMap<String, FeatureDataType>>();

        Ok(VectorResultDescriptor {
            data_type: info.vector_type,
            spatial_reference: crs,
            columns,
        })
    }

    fn create_raster_result_descriptor(
        &self,
        crs: SpatialReferenceOption,
        info: &RasterInfo,
    ) -> Result<RasterResultDescriptor> {
        Ok(RasterResultDescriptor {
            data_type: info.data_type,
            spatial_reference: crs,
            measurement: info
                .measurement
                .as_ref()
                .map(Clone::clone)
                .unwrap_or(Measurement::Unitless),
            no_data_value: info.no_data_value,
        })
    }

    async fn get_single_file_object(&self, id: &DatasetId) -> Result<Object> {
        let mut ds_stub = self.dataset_stub.clone();

        let group = ds_stub
            .get_dataset_object_groups(GetDatasetObjectGroupsRequest {
                id: Self::dataset_nfdi_id(id)?,
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

    async fn vector_loading_template(
        &self,
        layer_name: String,
        rd: &VectorResultDescriptor,
    ) -> Result<OgrSourceDataset> {
        let data_type = match rd.data_type {
            VectorDataType::Data => None,
            x => Some(x),
        };

        // Map column definition
        let mut int = vec![];
        let mut float = vec![];
        let mut text = vec![];

        for (k, v) in rd.columns.iter() {
            match v {
                FeatureDataType::Category | FeatureDataType::Int => int.push(k.to_string()),
                FeatureDataType::Float => float.push(k.to_string()),
                FeatureDataType::Text => text.push(k.to_string()),
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
            rename: None,
        };

        let time = OgrSourceDatasetTimeType::None;

        Ok(OgrSourceDataset {
            file_name: PathBuf::from(link),
            layer_name,
            data_type,
            time,
            default_geometry: None,
            columns: Some(column_spec),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Abort,
            sql_query: None,
            attribute_query: None,
        })
    }

    async fn raster_loading_template(
        &self,
        info: &RasterInfo,
        rd: &RasterResultDescriptor,
    ) -> Result<GdalLoadingInfo> {
        let part = GdalLoadingInfoPart {
            time: info.time_interval.clone(),
            params: GdalDatasetParameters {
                file_path: PathBuf::from(format!("/viscurl/{}", URL_REPLACEMENT)),
                rasterband_channel: info.rasterband_channel,
                geo_transform: info.geo_transform.clone(),
                width: info.width,
                height: info.height,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: rd.no_data_value,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
            },
        };

        Ok(GdalLoadingInfo {
            info: GdalLoadingInfoPartIterator::Static {
                parts: vec![part].into_iter(),
            },
        })
    }
}

#[async_trait::async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for NFDIDataProvider
{
    async fn meta_data(
        &self,
        _dataset: &DatasetId,
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
        dataset: &DatasetId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
    > {
        let (_, md) = self.dataset_info(dataset).await.map_err(|e| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            }
        })?;

        let object = self.get_single_file_object(dataset).await.map_err(|e| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            }
        })?;

        match md.data_type {
            DataType::SingleVectorFile(info) => {
                let result_descriptor = self
                    .create_vector_result_descriptor(md.crs.into(), &info)
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?;
                let template = self
                    .vector_loading_template(info.layer_name.clone(), &result_descriptor)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?;

                let res = NFDIMetaData {
                    object_id: object.id,
                    template,
                    result_descriptor,
                    _phantom: Default::default(),
                    object_stub: self.object_stub.clone(),
                };
                Ok(Box::new(res))
            }
            _ => Err(geoengine_operators::error::Error::InvalidType {
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
        dataset: &DatasetId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
    > {
        let (_, md) = self.dataset_info(dataset).await.map_err(|e| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            }
        })?;

        let object = self.get_single_file_object(dataset).await.map_err(|e| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            }
        })?;

        match &md.data_type {
            DataType::SingleRasterFile(info) => {
                let result_descriptor =
                    self.create_raster_result_descriptor(md.crs.into(), info)
                        .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                            source: Box::new(e),
                        })?;
                let template = self
                    .raster_loading_template(info, &result_descriptor)
                    .await
                    .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(e),
                    })?;

                let res = NFDIMetaData {
                    object_id: object.id,
                    template,
                    result_descriptor,
                    _phantom: Default::default(),
                    object_stub: self.object_stub.clone(),
                };
                Ok(Box::new(res))
            }
            _ => Err(geoengine_operators::error::Error::InvalidType {
                found: md.data_type.to_string(),
                expected: "SingleRasterFile".to_string(),
            }),
        }
    }
}

#[async_trait::async_trait]
impl ProvenanceProvider for NFDIDataProvider {
    async fn provenance(&self, dataset_id: &DatasetId) -> Result<ProvenanceOutput> {
        let (ds, _) = self.dataset_info(dataset_id).await?;

        Ok(ProvenanceOutput {
            dataset: dataset_id.clone(),
            provenance: ds.provenance,
        })
    }
}

#[async_trait::async_trait]
impl DatasetProvider for NFDIDataProvider {
    async fn list(&self, _options: Validated<DatasetListOptions>) -> Result<Vec<DatasetListing>> {
        let mut project_stub = self.project_stub.clone();

        let resp = project_stub
            .get_project_datasets(GetProjectDatasetsRequest {
                id: self.project_id.clone(),
            })
            .await?
            .into_inner();

        Ok(resp
            .dataset
            .into_iter()
            .map(|ds| {
                let md = Self::extract_metadata(&ds)?;
                self.map_dataset(&ds, &md).map(|x| x.listing())
            })
            .collect::<Result<Vec<DatasetListing>>>()?)
    }

    async fn load(&self, id: &DatasetId) -> Result<Dataset> {
        Ok(self.dataset_info(id).await?.0)
    }
}

/*
 * Internal structures
 */

trait ExpiringDownloadLink {
    fn new_link(&self, url: String) -> std::result::Result<Self, geoengine_operators::error::Error>
    where
        Self: Sized;
}

impl ExpiringDownloadLink for OgrSourceDataset {
    fn new_link(
        &self,
        url: String,
    ) -> std::result::Result<Self, geoengine_operators::error::Error> {
        let path = self.file_name.to_str().ok_or(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "Could not parse original path as string {:?}",
                &self.file_name
            ),
        ))?;
        let new_path = PathBuf::from(path.replace(URL_REPLACEMENT, url.as_str()));

        Ok(Self {
            file_name: new_path,
            layer_name: self.layer_name.clone(),
            data_type: self.data_type.clone(),
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
    fn new_link(
        &self,
        url: String,
    ) -> std::prelude::rust_2015::Result<Self, geoengine_operators::error::Error>
    where
        Self: Sized,
    {
        match &self.info {
            GdalLoadingInfoPartIterator::Static { parts } if parts.as_slice().len() == 1 => {
                let new_parts: Vec<_> = parts
                    .as_slice()
                    .iter()
                    .map(|part| {
                        let mut new_part = part.clone();
                        new_part.params.file_path = PathBuf::from(url.as_str());
                        new_part
                    })
                    .collect();

                Ok(Self {
                    info: GdalLoadingInfoPartIterator::Static {
                        parts: new_parts.into_iter(),
                    },
                })
            }
            _ => {
                return Err(geoengine_operators::error::Error::InvalidType {
                    found: "GdalLoadingInfoPartIterator::Dynamic".to_string(),
                    expected: "GdalLoadingInfoPartIterator::Static".to_string(),
                })
            }
        }
    }
}

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
    use crate::datasets::external::nfdi::{NFDIDataProvider, NFDIDataProviderDefinition};
    use crate::datasets::listing::{DatasetListOptions, DatasetProvider, OrderBy};
    use crate::datasets::provenance::ProvenanceProvider;
    use crate::util::user_input::Validated;
    use futures::StreamExt;
    use geoengine_datatypes::collections::{FeatureCollectionInfos, MultiPointCollection};
    use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
    use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution, TimeInterval};
    use geoengine_operators::engine::{
        MetaData, MetaDataProvider, MockExecutionContext, MockQueryContext, QueryProcessor,
        TypedVectorQueryProcessor, VectorOperator, VectorQueryRectangle, VectorResultDescriptor,
    };
    use geoengine_operators::source::{OgrSource, OgrSourceDataset, OgrSourceParameters};
    use std::str::FromStr;

    const PROVIDER_ID: &str = "86a7f7ce-1bab-4ce9-a32b-172c0f958ee0";
    const DATASET_ID: &str = "a04ecdb8-848e-4a00-baa9-d3fb907ec37b";

    async fn new_provider() -> NFDIDataProvider {
        let def = NFDIDataProviderDefinition {
            id: DatasetProviderId::from_str(PROVIDER_ID).unwrap(),
            api_token: "vpyJ1yWSHevHoWd9PoNtJAv4H1JLiNO/Lc+T2UGmQX+XksuzHJxvJe16i2pe".to_string(),
            api_url: "https://api.core-server-dev.m1.k8s.computational.bio/".to_string(),
            project_id: "4277287c-7ad0-44b2-821b-cac50d4aae36".to_string(),
            name: "NFDI".to_string(),
        };

        NFDIDataProvider::new(Box::new(def)).await.unwrap()
    }

    #[tokio::test]
    async fn it_lists() {
        let provider = new_provider().await;

        let opts = DatasetListOptions {
            filter: None,
            limit: 100,
            offset: 0,
            order: OrderBy::NameAsc,
        };

        let res = provider.list(Validated { user_input: opts }).await;
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(!res.is_empty());
    }

    #[tokio::test]
    async fn it_loads() {
        let provider = new_provider().await;

        let id = DatasetId::External(ExternalDatasetId {
            provider_id: provider.id,
            dataset_id: DATASET_ID.to_string(),
        });

        let res = provider.load(&id).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn it_loads_provenance() {
        let provider = new_provider().await;

        let id = DatasetId::External(ExternalDatasetId {
            provider_id: provider.id,
            dataset_id: DATASET_ID.to_string(),
        });

        let res = provider.provenance(&id).await;

        assert!(res.is_ok());

        let res = res.unwrap();

        assert!(res.provenance.is_some());
    }

    #[tokio::test]
    async fn it_loads_meta_data() {
        let provider = new_provider().await;

        let id = DatasetId::External(ExternalDatasetId {
            provider_id: provider.id,
            dataset_id: DATASET_ID.to_string(),
        });

        let _res: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        > = provider.meta_data(&id).await.unwrap();
    }

    #[tokio::test]
    async fn it_executes_loads() {
        let provider = new_provider().await;

        let id = DatasetId::External(ExternalDatasetId {
            provider_id: provider.id,
            dataset_id: DATASET_ID.to_string(),
        });

        let meta: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        > = provider.meta_data(&id).await.unwrap();

        let mut context = MockExecutionContext::default();
        context.add_meta_data(id.clone(), meta);

        let src = OgrSource {
            params: OgrSourceParameters {
                dataset: id,
                attribute_projection: None,
            },
        }
        .boxed();

        let initialized_op = src.initialize(&context).await.unwrap();

        let proc = initialized_op.query_processor().unwrap();
        let proc = match proc {
            TypedVectorQueryProcessor::MultiPoint(qp) => qp,
            _ => panic!("Expected MultiPoint QueryProcessor"),
        };

        let ctx = MockQueryContext::default();

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

        let sum: usize = result.iter().map(MultiPointCollection::len).sum();
        println!("{}", sum);
    }
}
