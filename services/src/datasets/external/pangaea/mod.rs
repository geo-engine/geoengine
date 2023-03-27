use crate::api::model::datatypes::{DataId, DataProviderId, LayerId};
pub use crate::datasets::external::pangaea::meta::PangaeaMetaData;
use crate::datasets::listing::{Provenance, ProvenanceOutput};
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{Layer, LayerCollection, LayerCollectionListOptions};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use async_trait::async_trait;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
use reqwest::Client;
use url::Url;

use crate::error::{Error, Result};
use crate::util::user_input::Validated;
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use serde::{Deserialize, Serialize};

mod meta;

pub const PANGAEA_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0xe3b9_3bf3_1bc1_48db_80e8_97cf_b068_5e8d);

/// The pangaea provider allows to include datasets from
/// <http://pangaea.de/>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PangaeaDataProviderDefinition {
    name: String,
    base_url: Url,
}

#[typetag::serde]
#[async_trait]
impl DataProviderDefinition for PangaeaDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> Result<Box<dyn DataProvider>> {
        Ok(Box::new(PangaeaDataProvider::new(self.base_url)))
    }

    fn type_name(&self) -> &'static str {
        "Pangaea"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        PANGAEA_PROVIDER_ID
    }
}

#[derive(Debug)]
pub struct PangaeaDataProvider {
    client: Client,
    base_url: Url,
}

impl PangaeaDataProvider {
    pub fn new(base_url: Url) -> PangaeaDataProvider {
        PangaeaDataProvider {
            client: Client::new(),
            base_url,
        }
    }

    pub async fn get_provenance(
        client: &reqwest::Client,
        base_url: &Url,
        id: &DataId,
        doi: &str,
    ) -> Result<ProvenanceOutput> {
        let pmd: PangaeaMetaData = client
            .get(format!("{base_url}{doi}?format=metadata_jsonld"))
            .send()
            .await?
            .json()
            .await
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?;

        let citation_text = client
            .get(format!("{base_url}{doi}?format=citation_text"))
            .send()
            .await?
            .text()
            .await?;

        Ok(ProvenanceOutput {
            data: id.clone(),
            provenance: Some(vec![Provenance {
                citation: citation_text,
                license: pmd.license.unwrap_or_default(),
                uri: pmd.url.to_string(),
            }]),
        })
    }
}

#[async_trait]
impl DataProvider for PangaeaDataProvider {
    async fn provenance(&self, id: &DataId) -> Result<ProvenanceOutput> {
        let doi = id.external().ok_or(Error::InvalidDataId)?.layer_id.0;
        Self::get_provenance(&self.client, &self.base_url, id, &doi).await
    }
}

#[async_trait]
impl LayerCollectionProvider for PangaeaDataProvider {
    async fn load_layer_collection(
        &self,
        _collection: &LayerCollectionId,
        _options: Validated<LayerCollectionListOptions>,
    ) -> Result<LayerCollection> {
        Err(Error::ProviderDoesNotSupportBrowsing)
    }

    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        Err(Error::ProviderDoesNotSupportBrowsing)
    }

    async fn load_layer(&self, _id: &LayerId) -> Result<Layer> {
        Err(Error::NotYetImplemented)
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for PangaeaDataProvider
{
    async fn meta_data(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let id: DataId = id.clone().into();

        let doi = id
            .external()
            .ok_or(Error::InvalidDataId)
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?
            .layer_id;

        let pmd: PangaeaMetaData = self
            .client
            .get(format!("{}{}?format=metadata_jsonld", self.base_url, doi))
            .send()
            .await
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?
            .json()
            .await
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?;

        let smd = pmd.get_ogr_metadata(&self.client).await.map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })?;

        Ok(Box::new(smd))
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for PangaeaDataProvider
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for PangaeaDataProvider
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
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use crate::api::model::datatypes::{DataId, ExternalDataId, LayerId};
    use crate::datasets::external::pangaea::{PangaeaDataProviderDefinition, PANGAEA_PROVIDER_ID};
    use crate::error::Error;
    use crate::layers::external::{DataProvider, DataProviderDefinition};
    use futures::StreamExt;
    use geoengine_datatypes::collections::{
        DataCollection, FeatureCollectionInfos, IntoGeometryIterator, MultiPointCollection,
        MultiPolygonCollection, VectorDataType,
    };
    use geoengine_datatypes::primitives::{
        BoundingBox2D, Coordinate2D, MultiPointAccess, SpatialResolution, TimeInterval,
        VectorQueryRectangle,
    };
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{
        InitializedVectorOperator, MetaData, MockExecutionContext, MockQueryContext,
        QueryProcessor, TypedVectorQueryProcessor, VectorOperator, VectorResultDescriptor,
    };
    use geoengine_operators::source::{OgrSource, OgrSourceDataset, OgrSourceParameters};
    use httptest::{
        all_of,
        matchers::{contains, request, url_decoded},
        responders::status_code,
        Expectation, Server,
    };
    use std::ops::RangeInclusive;
    use std::path::PathBuf;
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncReadExt;
    use url::Url;

    pub fn test_data_path(file_name: &str) -> PathBuf {
        crate::test_data!(String::from("pangaea/") + file_name).into()
    }

    async fn create_provider(server: &Server) -> Result<Box<dyn DataProvider>, Error> {
        Box::new(PangaeaDataProviderDefinition {
            name: "Pangaea".to_string(),
            base_url: Url::parse(server.url_str("").strip_suffix('/').unwrap()).unwrap(),
        })
        .initialize()
        .await
    }

    fn create_id(doi: &str) -> DataId {
        DataId::External(ExternalDataId {
            provider_id: PANGAEA_PROVIDER_ID,
            layer_id: LayerId(doi.to_owned()),
        })
    }

    async fn setup(
        server: &mut Server,
        method: &str,
        doi: &str,
        file_name: &str,
        format_param: &str,
        content_type: &str,
        count: RangeInclusive<usize>,
    ) {
        let mut body = String::new();

        let path = test_data_path(file_name);

        OpenOptions::new()
            .read(true)
            .open(path.as_path())
            .await
            .unwrap()
            .read_to_string(&mut body)
            .await
            .unwrap();

        // Let download urls point to test server
        let body = body.replace(
            "https://doi.pangaea.de",
            server.url_str("").strip_suffix('/').unwrap(),
        );

        let responder = status_code(200)
            .append_header("content-type", content_type.to_owned())
            .append_header("content-length", body.len())
            .body(if "HEAD" == method {
                String::new()
            } else {
                body
            });

        server.expect(
            Expectation::matching(all_of![
                request::method_path(method.to_string(), format!("/{doi}")),
                request::query(url_decoded(contains(("format", format_param.to_owned()))))
            ])
            .times(count)
            .respond_with(responder),
        );
    }

    async fn setup_metadata(server: &mut Server, doi: &str, file_name: &str) {
        setup(
            server,
            "GET",
            doi,
            file_name,
            "metadata_jsonld",
            "application/json",
            1..=1,
        )
        .await;
    }

    async fn setup_vsicurl(server: &mut Server, doi: &str, file_name: &str) {
        server.expect(
            Expectation::matching(request::method_path("GET", "/10.1594/PANGAEA.prj"))
                .times(0..=1)
                .respond_with(status_code(404)),
        );

        server.expect(
            Expectation::matching(request::method_path("GET", "/10.1594/PANGAEA.csvt"))
                .times(0..=1)
                .respond_with(status_code(404)),
        );

        setup(
            server,
            "HEAD",
            doi,
            file_name,
            "textfile",
            "text/tab-separated-values; charset=UTF-8",
            0..=1,
        )
        .await;

        setup(
            server,
            "GET",
            doi,
            file_name,
            "textfile",
            "text/tab-separated-values; charset=UTF-8",
            2..=2,
        )
        .await;
    }

    async fn setup_data(server: &mut Server, doi: &str, file_name: &str) {
        setup(
            server,
            "GET",
            doi,
            file_name,
            "textfile",
            "text/tab-separated-values; charset=UTF-8",
            1..=1,
        )
        .await;
    }

    async fn setup_citation(server: &mut Server, doi: &str, file_name: &str) {
        setup(
            server,
            "GET",
            doi,
            file_name,
            "citation_text",
            "text/plain",
            1..=1,
        )
        .await;
    }

    #[tokio::test]
    async fn it_creates_meta_data() {
        let doi = "10.1594/PANGAEA.909550";

        let mut server = Server::run();
        setup_metadata(&mut server, doi, "pangaea_geo_none_meta.json").await;
        setup_data(&mut server, doi, "pangaea_geo_none.tsv").await;

        let provider = create_provider(&server).await.unwrap();
        let id = create_id(doi);

        let meta: Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
            _,
        > = provider.meta_data(&id.into()).await;

        server.verify_and_clear();

        assert!(meta.is_ok());

        if let VectorDataType::Data = meta.unwrap().result_descriptor().await.unwrap().data_type {
        } else {
            panic!("Expected None Feature type")
        }
    }

    #[tokio::test]
    async fn it_loads_no_geometry() {
        let doi = "10.1594/PANGAEA.909550";

        let mut server = Server::run();

        setup_metadata(&mut server, doi, "pangaea_geo_none_meta.json").await;
        setup_data(&mut server, doi, "pangaea_geo_none.tsv").await;

        let provider = create_provider(&server).await.unwrap();
        let id = create_id(doi);

        let meta: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        > = provider.meta_data(&id.clone().into()).await.unwrap();

        server.verify_and_clear();
        setup_vsicurl(&mut server, doi, "pangaea_geo_none.tsv").await;

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

        let initialized_op = src.initialize(&context).await.unwrap();
        let proc = initialized_op.query_processor().unwrap();

        let TypedVectorQueryProcessor::Data(proc) = proc else {
            panic!("Expected Data QueryProcessor");
        };

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::test_default();

        let result = proc.query(query_rectangle, &ctx).await;

        let result: Vec<DataCollection> = result.unwrap().map(Result::unwrap).collect().await;

        server.verify_and_clear();

        assert_eq!(1, result.len());
        assert_eq!(60, result[0].len());
    }

    #[tokio::test]
    async fn it_loads_default_point() {
        let doi = "10.1594/PANGAEA.933024";

        let mut server = Server::run();

        setup_metadata(&mut server, doi, "pangaea_geo_point_meta.json").await;
        setup_data(&mut server, doi, "pangaea_geo_point.tsv").await;

        let provider = create_provider(&server).await.unwrap();
        let id = create_id(doi);

        let meta: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        > = provider.meta_data(&id.clone().into()).await.unwrap();

        server.verify_and_clear();
        setup_vsicurl(&mut server, doi, "pangaea_geo_point.tsv").await;

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

        let initialized_op = src.initialize(&context).await.unwrap();

        let proc = initialized_op.query_processor().unwrap();

        let TypedVectorQueryProcessor::MultiPoint(proc) = proc else {
            panic!("Expected MultiPoint QueryProcessor");
        };

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::test_default();

        let result: Vec<MultiPointCollection> = proc
            .query(query_rectangle, &ctx)
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect()
            .await;

        server.verify_and_clear();

        assert_eq!(1, result.len());
        assert_eq!(84, result[0].len());

        let target: Coordinate2D = [73.283_667, 4.850_232].into();

        for g in result[0].geometries() {
            assert_eq!(1, g.points().len());
            assert_eq!(&target, g.points().get(0).unwrap());
        }
    }

    #[tokio::test]
    async fn it_loads_default_polygon() {
        let doi = "10.1594/PANGAEA.913417";

        let mut server = Server::run();

        setup_metadata(&mut server, doi, "pangaea_geo_box_meta.json").await;
        setup_data(&mut server, doi, "pangaea_geo_box.tsv").await;

        let provider = create_provider(&server).await.unwrap();
        let id = create_id(doi);

        let meta: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        > = provider.meta_data(&id.clone().into()).await.unwrap();

        server.verify_and_clear();
        setup_vsicurl(&mut server, doi, "pangaea_geo_box.tsv").await;

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

        let initialized_op = src.initialize(&context).await.unwrap();

        let proc = initialized_op.query_processor().unwrap();

        let TypedVectorQueryProcessor::MultiPolygon(proc) = proc else {
            panic!("Expected MultiPolygon QueryProcessor");
        };

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::test_default();

        let result: Vec<MultiPolygonCollection> = proc
            .query(query_rectangle, &ctx)
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect()
            .await;

        server.verify_and_clear();

        assert_eq!(3, result.len());

        let total_results: usize = result.iter().map(FeatureCollectionInfos::len).sum();

        assert_eq!(432, total_results);
    }

    #[tokio::test]
    async fn it_loads_points() {
        let doi = "10.1594/PANGAEA.921338";

        let mut server = Server::run();

        setup_metadata(&mut server, doi, "pangaea_geo_lat_lon_meta.json").await;
        setup_data(&mut server, doi, "pangaea_geo_lat_lon.tsv").await;

        let provider = create_provider(&server).await.unwrap();
        let id = create_id(doi);

        let meta: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        > = provider.meta_data(&id.clone().into()).await.unwrap();

        server.verify_and_clear();
        setup_vsicurl(&mut server, doi, "pangaea_geo_lat_lon.tsv").await;

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

        let initialized_op = src.initialize(&context).await.unwrap();

        let proc = initialized_op.query_processor().unwrap();
        let TypedVectorQueryProcessor::MultiPoint(proc) = proc else {
            panic!("Expected MultiPoint QueryProcessor");
        };

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::test_default();

        let result: Vec<MultiPointCollection> = proc
            .query(query_rectangle, &ctx)
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect()
            .await;

        server.verify_and_clear();

        assert_eq!(1, result.len());
        assert_eq!(27, result[0].len());
    }

    #[tokio::test]
    async fn it_creates_provenance() {
        let doi = "10.1594/PANGAEA.921338";

        let mut server = Server::run();

        setup_metadata(&mut server, doi, "pangaea_geo_lat_lon_meta.json").await;
        setup_citation(&mut server, doi, "pangaea_geo_lat_lon_citation.txt").await;

        let provider = create_provider(&server).await.unwrap();
        let id = create_id(doi);

        let result = provider.provenance(&id).await;

        server.verify_and_clear();

        assert!(result.is_ok());

        let result = result.unwrap();

        assert!(result.provenance.is_some());

        let provenance = result.provenance.unwrap();
        assert_eq!(1, provenance.len());
        let provenance = &provenance[0];

        assert_ne!("", provenance.license);
        assert_ne!("", provenance.citation);
        assert_ne!("", provenance.uri);
    }
}
