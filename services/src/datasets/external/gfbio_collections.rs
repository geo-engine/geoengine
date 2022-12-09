use std::collections::HashMap;
use std::marker::PhantomData;
use std::str::FromStr;

use crate::api::model::datatypes::{DataId, DataProviderId, ExternalDataId, LayerId};
use crate::datasets::listing::{Provenance, ProvenanceOutput};
use crate::error::Result;
use crate::error::{self, Error};
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerCollectionListing,
    LayerListing, ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::util::parsing::string_or_string_array;
use crate::util::postgres::DatabaseConnectionConfig;
use crate::util::user_input::Validated;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use bb8_postgres::bb8::{Pool, PooledConnection};
use bb8_postgres::tokio_postgres::{Config, NoTls};
use bb8_postgres::PostgresConnectionManager;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::primitives::{
    FeatureDataType, Measurement, RasterQueryRectangle, VectorQueryRectangle,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{
    RasterOperator, StaticMetaData, TypedOperator, VectorColumnInfo, VectorOperator,
};
use geoengine_operators::source::{
    GdalSource, GdalSourceParameters, OgrSource, OgrSourceColumnSpec, OgrSourceDatasetTimeType,
    OgrSourceErrorSpec, OgrSourceParameters,
};
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use reqwest::{header, Client};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use url::Url;
use uuid::Uuid;

use super::gfbio::GfbioDataProvider;
use super::pangaea::PangeaMetaData;

pub const GFBIO_COLLECTIONS_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0xf64e_2d5b_3b80_476a_83f5_c330_956b_2909);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GfbioCollectionsDataProviderDefinition {
    name: String,
    collection_api_url: Url,
    collection_api_auth_token: String,
    abcd_db_config: DatabaseConnectionConfig,
    pangaea_url: Url,
}

#[typetag::serde]
#[async_trait]
impl DataProviderDefinition for GfbioCollectionsDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> Result<Box<dyn DataProvider>> {
        Ok(Box::new(
            GfbioCollectionsDataProvider::new(
                self.name,
                self.collection_api_url,
                self.collection_api_auth_token,
                self.abcd_db_config,
                self.pangaea_url,
            )
            .await?,
        ))
    }

    fn type_name(&self) -> &'static str {
        "GFBioCollections"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        GFBIO_COLLECTIONS_PROVIDER_ID
    }
}

#[derive(Debug)]
pub struct GfbioCollectionsDataProvider {
    name: String,
    collection_api_url: Url,
    collection_api_auth_token: String,
    abcd_db_config: DatabaseConnectionConfig,
    pangaea_url: Url,
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

#[derive(Debug, Deserialize)]
struct CollectionResponse {
    id: Uuid,
    external_user_id: Option<String>,
    origin: String,
    set: Vec<CollectionEntry>,
    created: String, // TODO: datetime
    service: isize,
}

#[derive(Debug, Deserialize)]
struct CollectionEntry {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_type")]
    type_: String,
    #[serde(rename = "_source")]
    source: CollectionEntrySource,
    // TODO: more fields
}

#[derive(Debug, Deserialize)]
struct CollectionEntrySource {
    #[serde(rename = "type", deserialize_with = "string_or_string_array")]
    type_: Vec<String>,
    datalink: Option<String>,
    citation_title: String,
    #[serde(rename = "abcdDatasetIdentifier")]
    abcd_dataset_identifier: Option<String>,
    // TODO: more fields
}

#[derive(Debug, Clone)]
enum GfBioCollectionId {
    Collections,
    Collection { collection: String },
    AbcdLayer { collection: String, layer: String },
    PangaeaLayer { collection: String, layer: String },
}

impl FromStr for GfBioCollectionId {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let split = s.split('/').collect::<Vec<_>>();

        Ok(match *split.as_slice() {
            ["collections"] => GfBioCollectionId::Collections,
            ["collections", collection] => GfBioCollectionId::Collection {
                collection: collection.to_string(),
            },
            ["collections", collection, "abcd", layer] => GfBioCollectionId::AbcdLayer {
                collection: collection.to_string(),
                layer: layer.to_string(),
            },
            ["collections", collection, "pangaea", layer] => GfBioCollectionId::PangaeaLayer {
                collection: collection.to_string(),
                layer: layer.replace("__", "/").to_string(), // decode the DOI,
            },
            _ => return Err(crate::error::Error::InvalidLayerCollectionId),
        })
    }
}

impl TryFrom<GfBioCollectionId> for LayerCollectionId {
    type Error = crate::error::Error;

    fn try_from(id: GfBioCollectionId) -> Result<Self> {
        let s = match id {
            GfBioCollectionId::Collections => "collections".to_string(),
            GfBioCollectionId::Collection { collection } => format!("collections/{}", collection),
            _ => return Err(crate::error::Error::InvalidLayerCollectionId),
        };

        Ok(LayerCollectionId(s))
    }
}

impl TryFrom<GfBioCollectionId> for LayerId {
    type Error = crate::error::Error;

    fn try_from(id: GfBioCollectionId) -> Result<Self> {
        let s = match id {
            GfBioCollectionId::AbcdLayer { collection, layer } => {
                format!("collections/{}/abcd/{}", collection, layer,)
            }
            GfBioCollectionId::PangaeaLayer { collection, layer } => {
                format!(
                    "collections/{}/pangaea/{}",
                    collection,
                    layer.replace('/', "__") // encode the DOI so that it doesn't break parsing
                )
            }
            _ => return Err(crate::error::Error::InvalidLayerId),
        };

        Ok(LayerId(s))
    }
}

impl GfbioCollectionsDataProvider {
    async fn new(
        name: String,
        url: Url,
        auth_token: String,
        db_config: DatabaseConnectionConfig,
        pangaea_url: Url,
    ) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);
        let pool = Pool::builder().build(pg_mgr).await?;

        Ok(Self {
            name,
            collection_api_url: url,
            collection_api_auth_token: auth_token,
            abcd_db_config: db_config,
            pangaea_url,
            pool,
        })
    }

    #[allow(clippy::unused_async)] // TODO remove when properly implemented
    async fn get_collections(
        &self,
        _options: Validated<LayerCollectionListOptions>,
    ) -> Result<LayerCollection> {
        // TODO: use options

        // TODO: get collection for current user from GFBio collections API
        //       for this we need to get current user id and need test data that is associated to a user

        let mock_collections = [
            ("4917f014-834f-432f-9dbc-66b2d794036f", "1 ABCD Collection"),
            (
                "63cf68e4-6e11-469d-8f35-af83ee6586dc",
                "2 ABCD Units + 2 PANGAEA Units",
            ),
        ];

        let items: Result<Vec<CollectionItem>> = mock_collections
            .into_iter()
            .map(|(id, name)| {
                Ok(CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                        collection_id: (GfBioCollectionId::Collection {
                            collection: id.to_string(),
                        })
                        .try_into()?,
                    },
                    name: name.to_string(),
                    description: String::new(),
                }))
            })
            .collect();

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                collection_id: GfBioCollectionId::Collections.try_into()?,
            },
            name: "GFBio Collections".to_owned(),
            description: String::new(),
            items: items?,
            entry_label: None,
            properties: vec![],
        })
    }

    async fn get_collection(
        &self,
        collection: &str,
        _options: Validated<LayerCollectionListOptions>,
    ) -> Result<LayerCollection> {
        let client = Client::new();

        let response = client
            .get(
                self.collection_api_url
                    .join(&format!("collections/{}/", collection))?,
            )
            .header(header::AUTHORIZATION, &self.collection_api_auth_token)
            .header(header::ACCEPT, "application/json")
            .send()
            .await?
            .text()
            .await?;

        let response: CollectionResponse = serde_json::from_str(&response)?;

        let items = response
            .set
            .into_iter()
            .map(|entry| {
                let layer_id = match entry.source.abcd_dataset_identifier {
                    Some(id) => GfBioCollectionId::AbcdLayer {
                        collection: collection.to_string(),
                        layer: id, // TODO: maybe use entry.id and extract the relevant part instead?
                    },
                    None => GfBioCollectionId::PangaeaLayer {
                        collection: collection.to_string(),
                        layer: entry.id,
                    },
                };
                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                        layer_id: layer_id
                            .try_into()
                            .expect("AbcdLayer and PangaeaLayer should be valid LayerIds"),
                    },
                    name: entry.source.citation_title,
                    description: String::new(),
                })
            })
            .collect();

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                collection_id: GfBioCollectionId::Collection {
                    collection: collection.to_string(),
                }
                .try_into()?,
            },
            name: "Collection Name".to_string(), // TODO
            description: String::new(),
            items,
            entry_label: None,
            properties: vec![],
        })
    }

    fn extract_gfbio_dataset_id_from_collection_entry_id(entry_id: &str) -> Result<String> {
        // TODO: do we need this or not?
        // // the entry id is of the form "urn:gfbio.org:abcd:3_259_402:ZFMK+Sc0612"
        // // but we need to extract the dataset id "urn:gfbio.org:abcd:3_259_402"
        // let entry_id = entry_id
        //     .strip_prefix("urn:gfbio.org:abcd:")
        //     .ok_or(crate::error::Error::InvalidLayerId)?;

        // Ok(match entry_id.find(":") {
        //     Some(pos) => &entry_id[..pos],
        //     None => entry_id,
        // }
        // .to_string())

        Ok(entry_id.to_string())
    }

    async fn get_surrogate_key_for_gfbio_dataset(&self, dataset_id: &str) -> Result<i32> {
        let conn = self.pool.get().await?;

        let stmt = conn
            .prepare(&format!(
                r#"
            SELECT surrogate_key
            FROM {}.abcd_datasets
            WHERE dataset_id = $1;"#,
                self.abcd_db_config.schema
            ))
            .await?;

        let row = conn.query_one(&stmt, &[&dataset_id]).await.unwrap(); // TODO: handle error

        Ok(row.get(0))
    }

    async fn create_gfbio_loading_info(
        &self,
        layer: &str,
    ) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>>
    {
        let abcd_dataset_id =
            Self::extract_gfbio_dataset_id_from_collection_entry_id(layer).unwrap(); // TODO: handle error

        let surrogate_key = self
            .get_surrogate_key_for_gfbio_dataset(&abcd_dataset_id)
            .await
            .unwrap(); // TODO: handle error

        let (column_hash_to_name, column_name_to_hash) =
            GfbioDataProvider::resolve_columns(self.pool.get().await?, &self.abcd_db_config.schema)
                .await
                .unwrap(); // TODO: handle error

        Ok(Box::new(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: self.abcd_db_config.ogr_pg_config().into(),
                layer_name: format!("{}.abcd_units", self.abcd_db_config.schema),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::None, // TODO
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: String::new(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: column_name_to_hash
                        .iter()
                        .filter(|(name, _)| name.starts_with("/DataSets/DataSet/Units/Unit/"))
                        .map(|(_, hash)| hash.clone())
                        .collect(),
                    bool: vec![],
                    datetime: vec![],
                    rename: Some(
                        column_hash_to_name
                            .iter()
                            .filter(|(_hash, name)| {
                                name.starts_with("/DataSets/DataSet/Units/Unit/")
                            })
                            .map(|(hash, name)| (hash.clone(), name.clone()))
                            .collect(),
                    ),
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: true,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: Some(GfbioDataProvider::build_attribute_query(surrogate_key)),
            },
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: column_hash_to_name
                    .iter()
                    .filter(|(_, name)| name.starts_with("/DataSets/DataSet/Units/Unit"))
                    .map(|(_, name)| {
                        (
                            name.clone(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless,
                            },
                        )
                    })
                    .collect(),
                time: None,
                bbox: None,
            },
            phantom: PhantomData::default(),
        }))
    }

    async fn create_pangaea_loading_info(
        &self,
        layer: &str,
    ) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>>
    {
        let doi = layer.strip_prefix("oai:pangaea.de:doi:").unwrap(); // TODO: handle error

        let client = reqwest::Client::new();

        let pmd: PangeaMetaData = client
            .get(format!(
                "{}/{}?format=metadata_jsonld",
                self.pangaea_url, doi
            ))
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

        let smd = pmd.get_ogr_metadata(&client).await.map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })?;

        Ok(Box::new(smd))
    }
}

#[async_trait]
impl LayerCollectionProvider for GfbioCollectionsDataProvider {
    async fn collection(
        &self,
        collection: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<LayerCollection> {
        // TODO: offset & limit
        let collection = GfBioCollectionId::from_str(&collection.0)?;

        match collection {
            GfBioCollectionId::Collections => self.get_collections(options).await,
            GfBioCollectionId::Collection { collection } => {
                self.get_collection(&collection, options).await
            }
            _ => Err(crate::error::Error::InvalidLayerCollectionId),
        }
    }

    async fn root_collection_id(&self) -> Result<LayerCollectionId> {
        GfBioCollectionId::Collections.try_into()
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        let id = GfBioCollectionId::from_str(&id.0)?;
        dbg!(&id);

        // TODO: maybe dispatch to existing Abcd/Pangaea providers? Or just delegate to the code
        match &id {
            GfBioCollectionId::AbcdLayer { collection, layer }
            | GfBioCollectionId::PangaeaLayer { collection, layer } => Ok(Layer {
                id: ProviderLayerId {
                    provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                    layer_id: id.clone().try_into()?,
                },
                name: "Layer Name".to_string(), // TODO
                description: String::new(),
                workflow: Workflow {
                    operator: TypedOperator::Vector(
                        OgrSource {
                            params: OgrSourceParameters {
                                data: DataId::External(ExternalDataId {
                                    provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                                    layer_id: id.try_into()?,
                                })
                                .into(),
                                attribute_filters: None,
                                attribute_projection: None,
                            },
                        }
                        .boxed(),
                    ),
                },
                symbology: None,
                properties: vec![],
                metadata: HashMap::new(),
            }),
            _ => return Err(crate::error::Error::InvalidLayerId),
        }
    }
}

#[async_trait]
impl DataProvider for GfbioCollectionsDataProvider {
    async fn provenance(&self, id: &DataId) -> Result<ProvenanceOutput> {
        todo!()
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for GfbioCollectionsDataProvider
{
    async fn meta_data(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
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
    for GfbioCollectionsDataProvider
{
    async fn meta_data(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let id = if let geoengine_datatypes::dataset::DataId::External(id) = id {
            id
        } else {
            return Err(geoengine_operators::error::Error::InvalidDataId);
        };

        let id = GfBioCollectionId::from_str(&id.layer_id.0).unwrap(); // TODO: handle Error

        match id {
            GfBioCollectionId::AbcdLayer {
                collection: _,
                layer,
            } => self.create_gfbio_loading_info(&layer).await.map_err(|e| {
                geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                }
            }),
            GfBioCollectionId::PangaeaLayer {
                collection: _,
                layer,
            } => self
                .create_pangaea_loading_info(&dbg!(layer))
                .await
                .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                }),
            _ => Err(geoengine_operators::error::Error::InvalidDataId),
        }
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for GfbioCollectionsDataProvider
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[cfg(test)]
mod tests {
    use crate::util::user_input::UserInput;

    use super::*;

    #[tokio::test]
    async fn test() {
        // TODO: mock GFBio Colletions API

        // TODO: create test database schema instead of connecting to abcd database

        // TODO: mock the Pangaea API

        let provider = GfbioCollectionsDataProvider::new(
            "GFBIO Collections".to_string(),
            "https://collections.gfbio.dev/api/".parse().unwrap(),
            "Token 6bc06a951394f222eeb576c6f86a4ad73ab805f6".to_string(),
            DatabaseConnectionConfig {
                // TODO: load from config
                user: "geoengine".to_string(),
                password: "geoengine".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                database: "abcd".to_string(),
                schema: "public".to_string(),
            },
            "https://doi.pangaea.de".parse().unwrap(),
        )
        .await
        .unwrap();

        let collections = provider
            .collection(
                &LayerCollectionId("collections".to_string()),
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let child = if let CollectionItem::Collection(c) = &collections.items[0] {
            &c.id
        } else {
            panic!("expected collection");
        };

        let collections = provider
            .collection(
                &child.collection_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        // provider.meta_data(id)
        // urn:gfbio.org:abcd:3_259_402
    }
}
