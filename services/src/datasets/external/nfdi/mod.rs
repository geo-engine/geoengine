use crate::datasets::listing::{DatasetListOptions, DatasetListing, DatasetProvider};
use crate::datasets::provenance::{Provenance, ProvenanceOutput, ProvenanceProvider};
use crate::datasets::storage::{Dataset, DatasetProviderDefinition};
use crate::error::{Error, Result};
use crate::util::user_input::Validated;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
use geoengine_datatypes::primitives::FeatureDataType;
use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterQueryRectangle, RasterResultDescriptor, StaticMetaData,
    TypedResultDescriptor, VectorQueryRectangle, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    GdalLoadingInfo, OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType,
    OgrSourceDurationSpec, OgrSourceErrorSpec, OgrSourceTimeFormat,
};
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::dataset_service_client::DatasetServiceClient;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::object_load_service_client::ObjectLoadServiceClient;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::project_service_client::ProjectServiceClient;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::{
    CreateDownloadLinkRequest, GetDatasetObjectGroupsRequest, GetDatasetRequest,
    GetProjectDatasetsRequest,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use tonic::codegen::InterceptedService;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NFDIDataProviderDefinition {
    id: DatasetProviderId,
    name: String,
    api_url: String,
    project_id: u64,
    api_token: String,
}

#[typetag::serde]
#[async_trait::async_trait]
impl DatasetProviderDefinition for NFDIDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> Result<Box<dyn DatasetProvider>> {
        Ok(Box::new(NFDIDataProvider::new(&self).await?))
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
    project_id: u64,
    project_stub: ProjectServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    dataset_stub: DatasetServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    object_stub: ObjectLoadServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
}

impl NFDIDataProvider {
    async fn new(def: &NFDIDataProviderDefinition) -> Result<NFDIDataProvider> {
        let channel = Endpoint::from_str(def.api_url.as_str())
            .map_err(|_| Error::InvalidUri {
                uri_string: def.api_url.clone(),
            })?
            .connect()
            .await?;

        let interceptor = APITokenInterceptor::new(&def.api_token[..])?;

        let project_stub =
            ProjectServiceClient::with_interceptor(channel.clone(), interceptor.clone());
        let dataset_stub =
            DatasetServiceClient::with_interceptor(channel.clone(), interceptor.clone());

        let object_stub = ObjectLoadServiceClient::with_interceptor(channel, interceptor.clone());

        Ok(NFDIDataProvider {
            id: def.id.clone(),
            project_id: def.project_id,
            project_stub,
            dataset_stub,
            object_stub,
        })
    }

    fn dataset_id(id: &DatasetId) -> Result<u64> {
        match id {
            DatasetId::External(id) => id.dataset_id.parse().map_err(|_| Error::InvalidDatasetId),
            _ => Err(Error::InvalidDatasetId),
        }
    }

    async fn dataset_info(&self, id: &DatasetId) -> Result<Dataset> {
        let id = Self::dataset_id(id)?;
        let mut stub = self.dataset_stub.clone();

        let resp = stub
            .get_dataset(GetDatasetRequest { id })
            .await?
            .into_inner();

        match &resp.dataset {
            Some(ds) => Ok(self.map_dataset(ds)?),
            None => Err(Error::InvalidDatasetId),
        }
    }

    fn extract_provenance(
        &self,
        _ds: &scienceobjectsdb_rust_api::sciobjectsdbapi::models::v1::Dataset,
    ) -> Option<Provenance> {
        // TODO
        Some(Provenance {
            citation: "NFDI4BioDiv".to_string(),
            uri: "https://nfdi4biodiversity.org".to_string(),
            license: "Attribution 4.0 International (CC BY 4.0)".to_string(),
        })
    }

    fn map_dataset(
        &self,
        ds: &scienceobjectsdb_rust_api::sciobjectsdbapi::models::v1::Dataset,
    ) -> Result<Dataset> {
        // TODO:  These two are dataset specific
        let result_descriptor = TypedResultDescriptor::Vector(VectorResultDescriptor {
            data_type: VectorDataType::MultiPoint,
            spatial_reference: SpatialReferenceOption::SpatialReference(
                SpatialReference::epsg_4326(),
            ),
            columns: HashMap::from([
                ("event-id".to_string(), FeatureDataType::Int),
                ("visible".to_string(), FeatureDataType::Text),
                ("timestamp".to_string(), FeatureDataType::Text),
                ("location-long".to_string(), FeatureDataType::Float),
                ("location-lat".to_string(), FeatureDataType::Float),
                ("manually-marked-outlier".to_string(), FeatureDataType::Text),
                ("sensor-type".to_string(), FeatureDataType::Text),
                (
                    "individual-taxon-canonical-name".to_string(),
                    FeatureDataType::Text,
                ),
                ("tag-local-identifier".to_string(), FeatureDataType::Int),
                (
                    "individual-local-identifier".to_string(),
                    FeatureDataType::Text,
                ),
            ]),
        });
        let source_operator = "OgrSource".to_string();

        let provenance = self.extract_provenance(ds);

        Ok(Dataset {
            id: DatasetId::External(ExternalDatasetId {
                provider_id: self.id.clone(),
                dataset_id: ds.id.to_string(),
            }),
            name: ds.name.clone(),
            description: ds.description.clone(),
            source_operator,
            result_descriptor,
            symbology: None,
            provenance,
        })
    }

    async fn vector_loading_info(
        &self,
        ds: &Dataset,
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

        let mut dataset_stub = self.dataset_stub.clone();

        // TODO: Currently, we expect a single group with a single file
        let group = dataset_stub
            .get_dataset_object_groups(GetDatasetObjectGroupsRequest {
                id: Self::dataset_id(&ds.id)?,
            })
            .await?
            .into_inner()
            .object_groups
            .into_iter()
            .next()
            .ok_or(Error::NoMainFileCandidateFound)?;

        let object = group
            .objects
            .into_iter()
            .next()
            .ok_or(Error::NoMainFileCandidateFound)?;

        let mut object_stub = self.object_stub.clone();

        let link = object_stub
            .create_download_link(CreateDownloadLinkRequest { id: object.id })
            .await?
            .into_inner()
            .download_link;

        let link = format!("CSV:/vsicurl/{}", link);

        let layer = PathBuf::from(object.filename)
            .file_stem()
            .and_then(|x| x.to_str())
            .map(|x| x.to_string())
            .ok_or(Error::InvalidDatasetName)?;

        let column_spec = OgrSourceColumnSpec {
            format_specifics: None,
            x: "location-long".to_string(),
            y: Some("location-lat".to_string()),
            int,
            float,
            text,
            rename: None,
        };

        let time = OgrSourceDatasetTimeType::Start {
            start_field: "timestamp".to_string(),
            duration: OgrSourceDurationSpec::Zero,
            start_format: OgrSourceTimeFormat::Auto,
        };

        // TODO: Think about dataset types
        Ok(OgrSourceDataset {
            file_name: PathBuf::from(link),
            layer_name: layer,
            data_type,
            time,
            default_geometry: None,
            columns: Some(column_spec),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Ignore,
            sql_query: None,
            attribute_query: None,
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
        let ds = self.load(dataset).await.map_err(|e| {
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            }
        })?;

        let result_descriptor = match &ds.result_descriptor {
            TypedResultDescriptor::Vector(rd) => rd.clone(),
            x => {
                return Err(geoengine_operators::error::Error::InvalidType {
                    expected: "TypedResultDescriptor::Vector".to_owned(),
                    found: format!("{:?}", x),
                })
            }
        };

        let loading_info = self
            .vector_loading_info(&ds, &result_descriptor)
            .await
            .map_err(|e| geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(e),
            })?;

        let res = StaticMetaData {
            loading_info,
            result_descriptor,
            phantom: Default::default(),
        };

        Ok(Box::new(res))
    }
}

#[async_trait::async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for NFDIDataProvider
{
    async fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[async_trait::async_trait]
impl ProvenanceProvider for NFDIDataProvider {
    async fn provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput> {
        let ds = self.dataset_info(dataset).await?;

        Ok(ProvenanceOutput {
            dataset: dataset.clone(),
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
                id: self.project_id,
            })
            .await?
            .into_inner();

        let result: Result<Vec<_>, _> = resp
            .dataset
            .iter()
            .map(|ds| self.map_dataset(ds).map(|ds| ds.listing()))
            .collect();

        Ok(result?)
    }

    async fn load(&self, id: &DatasetId) -> Result<Dataset> {
        Ok(self.dataset_info(id).await?)
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

    async fn new_provider() -> NFDIDataProvider {
        let def = NFDIDataProviderDefinition {
            id: DatasetProviderId::from_str(PROVIDER_ID).unwrap(),
            api_token: "T4M7LtGpTHQkMAs2v9f/QXOJCzxS6OueybfMqKXA3NZOTmrZ7PdT5vT14DLa".to_string(),
            api_url: "https://api.core-server-dev.m1.k8s.computational.bio/".to_string(),
            project_id: 701931538305253378,
            name: "NFDI".to_string(),
        };

        NFDIDataProvider::new(&def).await.unwrap()
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
            dataset_id: "701939066165592066".to_string(),
        });

        let res = provider.load(&id).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn it_loads_provenance() {
        let provider = new_provider().await;

        let id = DatasetId::External(ExternalDatasetId {
            provider_id: provider.id,
            dataset_id: "701939066165592066".to_string(),
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
            dataset_id: "701939066165592066".to_string(),
        });

        let res: Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>> =
            provider.meta_data(&id).await.unwrap();
    }

    #[tokio::test]
    async fn it_executes_loads() {
        let provider = new_provider().await;

        let id = DatasetId::External(ExternalDatasetId {
            provider_id: provider.id,
            dataset_id: "701939066165592066".to_string(),
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
