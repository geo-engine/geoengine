use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use crate::api::model::datatypes::{DataId, DataProviderId, ExternalDataId, LayerId};
use crate::datasets::listing::ProvenanceOutput;
use crate::error::Result;
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerListing,
    ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::util::parsing::string_or_string_array;
use crate::util::postgres::DatabaseConnectionConfig;
use crate::util::user_input::Validated;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use bb8_postgres::bb8::Pool;
use bb8_postgres::tokio_postgres::NoTls;
use bb8_postgres::PostgresConnectionManager;
use futures::future::join_all;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::primitives::{
    FeatureDataType, Measurement, RasterQueryRectangle, VectorQueryRectangle,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{
    StaticMetaData, TypedOperator, VectorColumnInfo, VectorOperator,
};
use geoengine_operators::source::{
    OgrSource, OgrSourceColumnSpec, OgrSourceDatasetTimeType, OgrSourceErrorSpec,
    OgrSourceParameters,
};
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use reqwest::{header, Client};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use super::gfbio_abcd::GfbioAbcdDataProvider;
use super::pangaea::{PangaeaDataProvider, PangaeaMetaData};

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
    collection_api_url: Url,
    collection_api_auth_token: String,
    abcd_db_config: DatabaseConnectionConfig,
    pangaea_url: Url,
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

#[derive(Debug, Deserialize)]
pub struct CollectionResponse {
    pub id: Uuid,
    pub external_user_id: Option<String>,
    pub origin: String,
    pub set: Vec<CollectionEntry>,
    pub created: String, // TODO: datetime
    pub service: isize,
}

#[derive(Debug, Deserialize)]
pub struct CollectionEntry {
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(rename = "_type")]
    pub type_: String,
    #[serde(rename = "_source")]
    pub source: CollectionEntrySource,
}

#[derive(Debug, Deserialize)]
pub struct CollectionEntrySource {
    #[serde(rename = "type", deserialize_with = "string_or_string_array")]
    pub type_: Vec<String>,
    pub datalink: Option<String>,
    pub citation_title: String,
    #[serde(rename = "abcdDatasetIdentifier")]
    pub abcd_dataset_identifier: Option<String>,
    #[serde(rename = "vatVisualizable")]
    pub vat_visualizable: bool,
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
                layer: layer.replace("__", "/"), // decode the DOI,
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
            GfBioCollectionId::Collection { collection } => format!("collections/{collection}"),
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
                format!("collections/{collection}/abcd/{layer}")
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

#[derive(Debug, Clone)]
enum LayerStatus {
    Ok,
    Unavailable,
    Error,
}

impl fmt::Display for LayerStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LayerStatus::Ok => write!(f, "ok"),
            LayerStatus::Unavailable => write!(f, "unavailable"),
            LayerStatus::Error => write!(f, "error"),
        }
    }
}

impl GfbioCollectionsDataProvider {
    async fn new(
        url: Url,
        auth_token: String,
        db_config: DatabaseConnectionConfig,
        pangaea_url: Url,
    ) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);
        let pool = Pool::builder().build(pg_mgr).await?;

        Ok(Self {
            collection_api_url: url,
            collection_api_auth_token: auth_token,
            abcd_db_config: db_config,
            pangaea_url,
            pool,
        })
    }

    fn get_collections() -> Result<LayerCollection> {
        // return an empty collection because we do not support browsing the collections
        // but rather accessing them directly via their ID
        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                collection_id: GfBioCollectionId::Collections.try_into()?,
            },
            name: "GFBio Collections".to_owned(),
            description: String::new(),
            items: vec![],
            entry_label: None,
            properties: vec![],
        })
    }

    async fn get_collection(
        &self,
        collection: &str,
        offset: u32,
        limit: u32,
    ) -> Result<LayerCollection> {
        let client = Client::new();

        let response = client
            .get(
                self.collection_api_url
                    .join(&format!("collections/{collection}/"))?,
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
            .skip(offset as usize)
            .take(limit as usize)
            .map(|entry| self.create_layer_listing(collection, entry));

        let items = join_all(items)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                collection_id: GfBioCollectionId::Collection {
                    collection: collection.to_string(),
                }
                .try_into()?,
            },
            name: "GfBio Collection".to_string(), // no info available via API
            description: String::new(),
            items,
            entry_label: None,
            properties: vec![],
        })
    }

    async fn create_layer_listing(
        &self,
        collection: &str,
        entry: CollectionEntry,
    ) -> Result<CollectionItem> {
        match entry.source.abcd_dataset_identifier.as_ref() {
            Some(_) => self.create_abcd_layer_listing(collection, entry).await,
            None => Ok(Self::create_pangaea_layer_listing(collection, entry)),
        }
    }

    async fn create_abcd_layer_listing(
        &self,
        collection: &str,
        entry: CollectionEntry,
    ) -> Result<CollectionItem> {
        let abcd_dataset_identifier = entry
            .source
            .abcd_dataset_identifier
            .expect("abcd_dataset_identifier should be present because it was checked in `create_layer_listing`");

        let status = if entry.source.vat_visualizable {
            let in_database = self
                .get_surrogate_key_for_gfbio_dataset(&abcd_dataset_identifier)
                .await
                .is_ok();

            if in_database {
                LayerStatus::Ok
            } else {
                LayerStatus::Error
            }
        } else {
            LayerStatus::Unavailable
        };

        Ok(CollectionItem::Layer(LayerListing {
            id: ProviderLayerId {
                provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                layer_id: GfBioCollectionId::AbcdLayer {
                    collection: collection.to_string(),
                    layer: abcd_dataset_identifier,
                }
                .try_into()
                .expect("AbcdLayer should be a valid LayerId"),
            },
            name: entry.source.citation_title,
            description: String::new(),
            properties: vec![("status".to_string(), status.to_string()).into()],
        }))
    }

    fn create_pangaea_layer_listing(collection: &str, entry: CollectionEntry) -> CollectionItem {
        let status = if entry.source.vat_visualizable {
            LayerStatus::Ok
        } else {
            LayerStatus::Unavailable
        };

        CollectionItem::Layer(LayerListing {
            id: ProviderLayerId {
                provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                layer_id: GfBioCollectionId::PangaeaLayer {
                    collection: collection.to_string(),
                    layer: entry.id,
                }
                .try_into()
                .expect("PangaeaLayer should be a valid LayerId"),
            },
            name: entry.source.citation_title,
            description: String::new(),
            properties: vec![("status".to_string(), status.to_string()).into()],
        })
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

        let row = conn.query_one(&stmt, &[&dataset_id]).await?;

        Ok(row.get(0))
    }

    async fn create_gfbio_loading_info(
        &self,
        abcd_dataset_id: &str,
    ) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>>
    {
        let surrogate_key = self
            .get_surrogate_key_for_gfbio_dataset(abcd_dataset_id)
            .await?;

        let (column_hash_to_name, column_name_to_hash) = GfbioAbcdDataProvider::resolve_columns(
            &self.pool.get().await?,
            &self.abcd_db_config.schema,
        )
        .await?;

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
                attribute_query: Some(GfbioAbcdDataProvider::build_attribute_query(surrogate_key)),
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
        let doi = layer
            .strip_prefix("oai:pangaea.de:doi:")
            .ok_or(crate::error::Error::InvalidLayerId)?;

        let client = reqwest::Client::new();

        let pmd: PangaeaMetaData = client
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
        let collection = GfBioCollectionId::from_str(&collection.0)?;

        let options = options.user_input;

        match collection {
            GfBioCollectionId::Collections => Self::get_collections(),
            GfBioCollectionId::Collection { collection } => {
                self.get_collection(&collection, options.offset, options.limit)
                    .await
            }
            _ => Err(crate::error::Error::InvalidLayerCollectionId),
        }
    }

    async fn root_collection_id(&self) -> Result<LayerCollectionId> {
        GfBioCollectionId::Collections.try_into()
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        let gfbio_collection_id = GfBioCollectionId::from_str(&id.0)?;

        match &gfbio_collection_id {
            GfBioCollectionId::AbcdLayer {
                collection,
                layer: _,
            }
            | GfBioCollectionId::PangaeaLayer {
                collection,
                layer: _,
            } => {
                // get the layer information by reusing the code for listing the collections
                let collection = self.get_collection(collection, 0, std::u32::MAX).await?;

                let layer = collection
                    .items
                    .into_iter()
                    .filter_map(|l| match l {
                        CollectionItem::Layer(l) => Some(l),
                        CollectionItem::Collection(_) => None,
                    })
                    .find(|l| {
                        l.id == ProviderLayerId {
                            provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                            layer_id: id.clone(),
                        }
                    })
                    .ok_or(crate::error::Error::UnknownLayerId { id: id.clone() })?;

                Ok(Layer {
                    id: ProviderLayerId {
                        provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                        layer_id: id.clone(),
                    },
                    name: layer.name,
                    description: String::new(),
                    workflow: Workflow {
                        operator: TypedOperator::Vector(
                            OgrSource {
                                params: OgrSourceParameters {
                                    data: DataId::External(ExternalDataId {
                                        provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                                        layer_id: id.clone(),
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
                    properties: layer.properties,
                    metadata: HashMap::new(),
                })
            }
            _ => return Err(crate::error::Error::InvalidLayerId),
        }
    }
}

#[async_trait]
impl DataProvider for GfbioCollectionsDataProvider {
    async fn provenance(&self, id: &DataId) -> Result<ProvenanceOutput> {
        let external_id = id.external().ok_or(crate::error::Error::InvalidDataId)?;
        let gfbio_id = GfBioCollectionId::from_str(&external_id.layer_id.0)?;

        match gfbio_id {
            GfBioCollectionId::AbcdLayer {
                collection: _,
                layer,
            } => {
                let conn = self.pool.get().await?;

                let (_column_hash_to_name, column_name_to_hash) =
                    GfbioAbcdDataProvider::resolve_columns(&conn, &self.abcd_db_config.schema)
                        .await?;

                let surrogate_key = self.get_surrogate_key_for_gfbio_dataset(&layer).await?;

                GfbioAbcdDataProvider::get_provenance(
                    id,
                    surrogate_key,
                    &column_name_to_hash,
                    &conn,
                    &self.abcd_db_config.schema,
                )
                .await
            }
            GfBioCollectionId::PangaeaLayer {
                collection: _,
                layer,
            } => {
                let doi = layer
                    .strip_prefix("oai:pangaea.de:doi:")
                    .ok_or(crate::error::Error::InvalidLayerId)?;
                PangaeaDataProvider::get_provenance(
                    &reqwest::Client::new(),
                    &self.pangaea_url,
                    id,
                    doi,
                )
                .await
            }
            _ => Err(crate::error::Error::InvalidDataId),
        }
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for GfbioCollectionsDataProvider
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
    for GfbioCollectionsDataProvider
{
    async fn meta_data(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let geoengine_datatypes::dataset::DataId::External(id) = id else {
            return Err(geoengine_operators::error::Error::InvalidDataId);
        };

        let id = GfBioCollectionId::from_str(&id.layer_id.0)
            .map_err(|_| geoengine_operators::error::Error::InvalidDataId)?;

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
            } => self.create_pangaea_loading_info(&layer).await.map_err(|e| {
                geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                }
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
    use std::{fs::File, io::Read};

    use bb8_postgres::bb8::ManageConnection;
    use geoengine_datatypes::test_data;
    use httptest::{
        all_of,
        matchers::{contains, lowercase, request},
        responders::status_code,
        Expectation, Server,
    };
    use rand::RngCore;
    use tokio_postgres::Config;

    use crate::{util::config, util::user_input::UserInput};

    use super::*;

    /// Create a schema with test tables and return the schema name
    async fn create_test_data(db_config: &config::Postgres) -> String {
        let mut pg_config = Config::new();
        pg_config
            .user(&db_config.user)
            .password(&db_config.password)
            .host(&db_config.host)
            .dbname(&db_config.database);
        let pg_mgr = PostgresConnectionManager::new(pg_config, NoTls);
        let conn = pg_mgr.connect().await.unwrap();

        let mut sql = String::new();
        File::open(test_data!("gfbio/test_data.sql"))
            .unwrap()
            .read_to_string(&mut sql)
            .unwrap();

        let schema = format!("geoengine_test_{}", rand::thread_rng().next_u64());

        // basic schema
        conn.batch_execute(&format!(
            "CREATE SCHEMA {schema}; 
            SET SEARCH_PATH TO {schema}, public;
            {sql}"
        ))
        .await
        .unwrap();

        // dataset from the collection API
        conn.batch_execute(r#"
        INSERT INTO abcd_datasets 
            (surrogate_key, dataset_id, dataset_path, dataset_landing_page, dataset_provider, ac33b09f59554c61c14db1c2ae1a635cb06c8436, 
                "8fdde5ff4759fdaa7c55fb172e68527671a2240a", c6b8d2982cdf2e80fa6882734630ec735e9ea05d, b680a531f806d3a31ff486fdad601956d30eff39, 
                "0a58413cb55a2ddefa3aa83de465fb5d58e4f1df", "9848409ccf22cbd3b5aeebfe6592677478304a64", "9df7aa344cb18001c7c4f173a700f72904bb64af", 
                e814cff84791402aef987219e408c6957c076e5a, "118bb6a92bc934803314ce2711baca3d8232e4dc", "3375d84219d930ef640032f6993fee32b38e843d", 
                abbd35a33f3fef7e2e96e1be66daf8bbe26c17f5, "5a3c23f17987c03c35912805398b491cbfe03751", b7f24b1e9e8926c974387814a38d936aacf0aac8
            ) 
        VALUES (
            17,
            'urn:gfbio.org:abcd:3_259_402',
            'https://biocase.zfmk.de/biocase/downloads/ZFMK-Collections-v2/ZFMK%20Scorpiones%20collection.ABCD_GGBN.zip',
            'https://www.zfmk.de/en/research/collections/basal-arthropods',
            'Data Center ZFMK',
            'ZFMKDatacenter@leibniz-zfmk.de',
            'The collections of basal arthropods including Scorpiones at the Zoological Research Museum Alexander Koenig Bonn',
            'https://creativecommons.org/licenses/by-sa/4.0/',
            'https://www.zfmk.de/en/research/collections/basal-arthropods',
            'ZFMK Arthropoda Working Group. (2021). ZFMK Scorpiones collection. [Dataset]. Version: 1.2. Data Publisher: Data Center ZFMK. https://doi.org/10.20363/zfmk-coll.scorpiones-2018-11.',
            'b.huber@leibniz-zfmk.de',
            'https://id.zfmk.de/dataset_ZFMK/655',
            '2021-02-04 16:08:11',
            'ZFMK Scorpiones collection',
            'Data Center ZFMK',
            'Dr. B. Huber',
            'CC-BY-SA',
            ''
        );
        "#)
        .await
        .unwrap();

        schema
    }

    /// Drop the schema created by `create_test_data`
    async fn cleanup_test_data(db_config: &config::Postgres, schema: String) {
        let mut pg_config = Config::new();
        pg_config
            .user(&db_config.user)
            .password(&db_config.password)
            .host(&db_config.host)
            .dbname(&db_config.database);
        let pg_mgr = PostgresConnectionManager::new(pg_config, NoTls);
        let conn = pg_mgr.connect().await.unwrap();

        conn.batch_execute(&format!("DROP SCHEMA {schema} CASCADE;"))
            .await
            .unwrap();
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn it_lists() {
        let db_config = config::get_config_element::<config::Postgres>().unwrap();
        let test_schema = create_test_data(&db_config).await;

        let gfbio_collections_server = Server::run();
        let gfbio_collections_server_token = "Token 6bc06a951394f222eeb576c6f86a4ad73ab805f6";

        let mut gfbio_collection_response = vec![];
        File::open(test_data!("gfbio/collections_api_response.json"))
            .unwrap()
            .read_to_end(&mut gfbio_collection_response)
            .unwrap();

        gfbio_collections_server.expect(
            Expectation::matching(all_of![
                request::headers(contains((
                    lowercase("authorization"),
                    gfbio_collections_server_token
                ))),
                request::headers(contains(("accept", "application/json"))),
                request::method_path(
                    "GET",
                    "/api/collections/63cf68e4-6e11-469d-8f35-af83ee6586dc/",
                ),
            ])
            .times(1)
            .respond_with(status_code(200).body(gfbio_collection_response)),
        );

        let provider = GfbioCollectionsDataProvider::new(
            Url::parse(&gfbio_collections_server.url("/api/").to_string()).unwrap(),
            gfbio_collections_server_token.to_string(),
            DatabaseConnectionConfig {
                host: db_config.host.clone(),
                port: db_config.port,
                database: db_config.database.clone(),
                schema: test_schema.clone(),
                user: db_config.user.clone(),
                password: db_config.password.clone(),
            },
            "https://doi.pangaea.de".parse().unwrap(),
        )
        .await
        .unwrap();

        let root_id = provider.root_collection_id().await.unwrap();

        let collection = provider
            .collection(
                &root_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                }
                .validated()
                .unwrap(),
            )
            .await;

        let collection = collection.unwrap();

        // root collection should be empty because we don't support browsing
        assert_eq!(collection.items.len(), 0);

        let collection = provider
            .collection(
                &LayerCollectionId("collections/63cf68e4-6e11-469d-8f35-af83ee6586dc".to_string()),
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            collection.items,
            vec![
            CollectionItem::Layer(LayerListing {
                 id: ProviderLayerId {
                    provider_id: DataProviderId::from_str("f64e2d5b-3b80-476a-83f5-c330956b2909").unwrap(),
                    layer_id: LayerId("collections/63cf68e4-6e11-469d-8f35-af83ee6586dc/abcd/urn:gfbio.org:abcd:3_259_402".to_string())
                },
                name: "Scorpiones, a preserved specimen record of the ZFMK Scorpiones collection dataset [ID: ZFMK Sc0602 ]".to_string(),
                description: String::new(),
                properties: vec![("status".to_string(), "ok".to_string()).into()]
                }),
                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: DataProviderId::from_str("f64e2d5b-3b80-476a-83f5-c330956b2909").unwrap(),
                        layer_id: LayerId("collections/63cf68e4-6e11-469d-8f35-af83ee6586dc/abcd/urn:gfbio.org:abcd:3_259_402".to_string())
                    },
                    name: "Scorpiones, a preserved specimen record of the ZFMK Scorpiones collection dataset [ID: ZFMK Sc0612 ]".to_string(),
                    description: String::new(),
                    properties: vec![("status".to_string(), "ok".to_string()).into()]
                    }),
                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: DataProviderId::from_str("f64e2d5b-3b80-476a-83f5-c330956b2909").unwrap(),
                        layer_id: LayerId("collections/63cf68e4-6e11-469d-8f35-af83ee6586dc/pangaea/oai:pangaea.de:doi:10.1594__PANGAEA.747054".to_string())
                    },
                    name: "Meteorological observations during SCORPION cruise from Brunswick to Cape Fear started at 1750-07-01".to_string(),
                    description: String::new(),
                    properties: vec![("status".to_string(), "ok".to_string()).into()]
                    }),
                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: DataProviderId::from_str("f64e2d5b-3b80-476a-83f5-c330956b2909").unwrap(),
                        layer_id: LayerId("collections/63cf68e4-6e11-469d-8f35-af83ee6586dc/pangaea/oai:pangaea.de:doi:10.1594__PANGAEA.747056".to_string()) 
                    },
                    name: "Meteorological observations during SCORPION cruise from Ocracoke to Southport started at 1750-11-04".to_string(), 
                    description: String::new(),
                    properties: vec![("status".to_string(), "ok".to_string()).into()] 
                    }
                )]
        );

        cleanup_test_data(&db_config, test_schema).await;
    }
}
