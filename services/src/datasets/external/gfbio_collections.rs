use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use crate::api::model::datatypes::{DataId, DataProviderId, ExternalDataId, LayerId};
use crate::datasets::listing::ProvenanceOutput;
use crate::error::Error::ProviderDoesNotSupportBrowsing;
use crate::error::{Error, Result};
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
    Collection {
        collection: String,
    },
    AbcdLayer {
        collection: String,
        dataset: String,
        unit: Option<String>,
    },
    PangaeaLayer {
        collection: String,
        dataset: String,
    },
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
            ["collections", collection, "abcd", layer] => {
                let (dataset, unit) = gfbio_dataset_identifier_to_dataset_unit(layer)?;

                GfBioCollectionId::AbcdLayer {
                    collection: collection.to_string(),
                    dataset,
                    unit,
                }
            }
            ["collections", collection, "pangaea", layer] => GfBioCollectionId::PangaeaLayer {
                collection: collection.to_string(),
                dataset: layer.replace("__", "/"), // decode the DOI,
            },
            _ => return Err(crate::error::Error::InvalidLayerCollectionId),
        })
    }
}

fn gfbio_dataset_identifier_to_dataset_unit(
    dataset_identifier: &str,
) -> Result<(String, Option<String>)> {
    // urn:gfbio.org:abcd:{dataset}:{unit} (unit is optional)
    let id_parts: Vec<_> = dataset_identifier.split(':').collect();

    Ok(match id_parts.as_slice() {
        [_, _, _, dataset, unit] => (
            format!("urn:gfbio.org:abcd:{dataset}"),
            Some((*unit).to_string()),
        ),
        [_, _, _, dataset] => (format!("urn:gfbio.org:abcd:{dataset}"), None),
        _ => return Err(crate::error::Error::InvalidLayerId),
    })
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
            GfBioCollectionId::AbcdLayer {
                collection,
                dataset,
                unit: Some(unit),
            } => {
                format!("collections/{collection}/abcd/{dataset}:{unit}")
            }
            GfBioCollectionId::AbcdLayer {
                collection,
                dataset,
                unit: None,
            } => {
                format!("collections/{collection}/abcd/{dataset}")
            }
            GfBioCollectionId::PangaeaLayer {
                collection,
                dataset: layer,
            } => {
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
        // return an error because we do not support browsing the collections
        // but rather accessing them directly via their ID
        Err(ProviderDoesNotSupportBrowsing)
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

        let (dataset, unit) = gfbio_dataset_identifier_to_dataset_unit(&entry.id)?;

        Ok(CollectionItem::Layer(LayerListing {
            id: ProviderLayerId {
                provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                layer_id: GfBioCollectionId::AbcdLayer {
                    collection: collection.to_string(),
                    dataset,
                    unit,
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
                    dataset: entry.id,
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
        abcd_unit_id: Option<&str>,
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
                attribute_query: Some(Self::build_attribute_query(
                    surrogate_key,
                    abcd_unit_id,
                    &column_name_to_hash,
                )?),
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

    fn build_attribute_query(
        surrogate_key: i32,
        unit_id: Option<&str>,
        column_name_to_hash: &HashMap<String, String>,
    ) -> Result<String> {
        if let Some(unit_id) = unit_id {
            let id_column = column_name_to_hash
                .get("/DataSets/DataSet/Units/Unit/UnitID")
                .ok_or(Error::AbcdUnitIdColumnMissingInDatabase)?;

            // in the collection API the IDs use "+" but in the database we use " "
            let unit_id = unit_id.replace('+', " ");

            Ok(format!(
                "surrogate_key = {surrogate_key} AND {id_column} = '{unit_id}'"
            ))
        } else {
            Ok(format!("surrogate_key = {surrogate_key}"))
        }
    }
}

#[async_trait]
impl LayerCollectionProvider for GfbioCollectionsDataProvider {
    async fn load_layer_collection(
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

    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        // return an error because we do not support browsing the collections
        // but rather accessing them directly via their ID
        Err(ProviderDoesNotSupportBrowsing)
    }

    async fn load_layer(&self, id: &LayerId) -> Result<Layer> {
        let gfbio_collection_id = GfBioCollectionId::from_str(&id.0)?;

        match &gfbio_collection_id {
            GfBioCollectionId::AbcdLayer {
                collection,
                dataset: _,
                unit: _,
            }
            | GfBioCollectionId::PangaeaLayer {
                collection,
                dataset: _,
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
                dataset,
                unit: _,
            } => {
                let conn = self.pool.get().await?;

                let (_column_hash_to_name, column_name_to_hash) =
                    GfbioAbcdDataProvider::resolve_columns(&conn, &self.abcd_db_config.schema)
                        .await?;

                let surrogate_key = self.get_surrogate_key_for_gfbio_dataset(&dataset).await?;

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
                dataset,
            } => {
                let doi = dataset
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
                dataset,
                unit,
            } => self
                .create_gfbio_loading_info(&dataset, unit.as_deref())
                .await
                .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                }),
            GfBioCollectionId::PangaeaLayer {
                collection: _,
                dataset,
            } => self
                .create_pangaea_loading_info(&dataset)
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
    use std::{fs::File, io::Read, path::PathBuf};

    use bb8_postgres::bb8::ManageConnection;
    use geoengine_datatypes::{
        primitives::{BoundingBox2D, SpatialResolution, TimeInterval},
        test_data,
    };
    use httptest::{
        all_of,
        matchers::{contains, lowercase, request},
        responders::status_code,
        Expectation, Server,
    };
    use rand::RngCore;
    use tokio_postgres::Config;

    use crate::{datasets::listing::Provenance, util::config, util::user_input::UserInput};

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
        async fn test(db_config: &config::Postgres, test_schema: &str) -> Result<(), String> {
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
                    schema: test_schema.to_string(),
                    user: db_config.user.clone(),
                    password: db_config.password.clone(),
                },
                "https://doi.pangaea.de".parse().unwrap(),
            )
            .await
            .unwrap();

            let root_id = provider.get_root_layer_collection_id().await;

            // root collection id should be an error because we don't support browsing
            assert!(root_id.is_err());

            let collection = provider
                .load_layer_collection(
                    &LayerCollectionId(
                        "collections/63cf68e4-6e11-469d-8f35-af83ee6586dc".to_string(),
                    ),
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
                        layer_id: LayerId("collections/63cf68e4-6e11-469d-8f35-af83ee6586dc/abcd/urn:gfbio.org:abcd:3_259_402:ZFMK+Sc0602".to_string())
                    },
                    name: "Scorpiones, a preserved specimen record of the ZFMK Scorpiones collection dataset [ID: ZFMK Sc0602 ]".to_string(),
                    description: String::new(),
                    properties: vec![("status".to_string(), "ok".to_string()).into()]
                    }),
                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: DataProviderId::from_str("f64e2d5b-3b80-476a-83f5-c330956b2909").unwrap(),
                        layer_id: LayerId("collections/63cf68e4-6e11-469d-8f35-af83ee6586dc/abcd/urn:gfbio.org:abcd:3_259_402:ZFMK+Sc0612".to_string())
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
            Ok(())
        }

        let db_config = config::get_config_element::<config::Postgres>().unwrap();

        let test_schema = create_test_data(&db_config).await;

        let test = test(&db_config, &test_schema).await;

        cleanup_test_data(&db_config, test_schema).await;

        assert!(test.is_ok());
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn it_creates_abcd_meta_data() {
        async fn test(db_config: &config::Postgres, test_schema: &str) -> Result<(), String> {
            let gfbio_collections_server_token = "Token 6bc06a951394f222eeb576c6f86a4ad73ab805f6";

            let provider_db_config = DatabaseConnectionConfig {
                host: db_config.host.clone(),
                port: db_config.port,
                database: db_config.database.clone(),
                schema: test_schema.to_string(),
                user: db_config.user.clone(),
                password: db_config.password.clone(),
            };

            let ogr_pg_string = provider_db_config.ogr_pg_config();

            let provider = GfbioCollectionsDataProvider::new(
                Url::parse("http://foo.bar/api").unwrap(),
                gfbio_collections_server_token.to_string(),
                provider_db_config,
                "https://doi.pangaea.de".parse().unwrap(),
            )
            .await
            .unwrap();

            let meta: Box<
                dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
            > = provider
                .meta_data(
                    &DataId::External(ExternalDataId {
                        provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                        layer_id: LayerId("collections/63cf68e4-6e11-469d-8f35-af83ee6586dc/abcd/urn:gfbio.org:abcd:3_259_402:ZFMK+Sc0602".to_string()),
                    })
                    .into(),
                )
                .await
                .map_err(|e| e.to_string())?;

            let text_column = VectorColumnInfo {
                data_type: FeatureDataType::Text,
                measurement: Measurement::Unitless,
            };

            let expected = VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns:  [
                    ("/DataSets/DataSet/Units/Unit/DateLastEdited".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/Gathering/Agents/GatheringAgent/AgentText".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/Gathering/Country/ISO3166Code".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/Gathering/Country/Name".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/Gathering/DateTime/ISODateTimeBegin".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/Gathering/LocalityText".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/SpatialDatum".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/HigherTaxa/HigherTaxon/HigherTaxonName".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/HigherTaxa/HigherTaxon/HigherTaxonRank".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/ScientificName/FullScientificNameString".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/Creator".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/FileURI".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/Format".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/Details".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/Text".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/URI".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/RecordBasis".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/RecordURI".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/SourceID".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/SourceInstitutionID".to_owned(), text_column.clone()),
                    ("/DataSets/DataSet/Units/Unit/UnitID".to_owned(), text_column.clone()),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
            };

            let result_descriptor = meta.result_descriptor().await.map_err(|e| e.to_string())?;

            if result_descriptor != expected {
                return Err(format!("{result_descriptor:?} != {expected:?}"));
            }

            let mut loading_info = meta
                .loading_info(VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new_unchecked(
                        (-180., -90.).into(),
                        (180., 90.).into(),
                    ),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::zero_point_one(),
                })
                .await
                .map_err(|e| e.to_string())?;

            loading_info
                .columns
                .as_mut()
                .ok_or_else(|| "missing columns".to_owned())?
                .text
                .sort();

            let expected = OgrSourceDataset {
                file_name: PathBuf::from(ogr_pg_string),
                layer_name: format!("{test_schema}.abcd_units"),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::None,
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: String::new(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: vec![
                        "09e05cff5522bf112eedf91c5c2f1432539e59aa".to_owned(),
                        "0dcf8788cadda41eaa5831f44227d8c531411953".to_owned(),                        
                        "150ac8760faba3bbf29ee77713fc0402641eea82".to_owned(),                        
                        "2598ba17aa170832b45c3c206f8133ddddc52c6e".to_owned(),                        
                        "2b603312fc185489ffcffd5763bcd47c4b126f31".to_owned(),                        
                        "46b0ed7a1faa8d25b0c681fbbdc2cca60cecbdf0".to_owned(),                        
                        "4f885a9545b143d322f3bf34bf2c5148e07d578a".to_owned(),                        
                        "54a52959a34f3c19fa1b0e22cea2ae5c8ce78602".to_owned(),                        
                        "624516976f697c1eacc7bccfb668d2c25ae7756e".to_owned(),                        
                        "6df446e57190f19d63fcf99ba25476510c5c8ce6".to_owned(),                        
                        "7fdf1ed68add3ac2f4a1b2c89b75245260890dfe".to_owned(),                        
                        "8003ddd80b42736ebf36b87018e51db3ee84efaf".to_owned(),                        
                        "83fb54d8cfa58d729125f3dccac3a6820d95ccaa".to_owned(),                        
                        "8603069b15071933545a8ce6563308da4d8ee019".to_owned(),                        
                        "9691f318c0f84b4e71e3c125492902af3ad22a81".to_owned(),                        
                        "abc0ceb08b2723a43274e1db093dfe1f333fe453".to_owned(),                        
                        "adf8c075f2c6b97eaab5cee8f22e97abfdaf6b71".to_owned(),                        
                        "bad2f7cae88e4219f2c3b186628189c5380f3c52".to_owned(),                        
                        "d22ecb7dd0e5de6e8b2721977056d30aefda1b75".to_owned(),                        
                        "f2374ad051911a65bc0d0a46c13ada2625f55a10".to_owned(),                        
                        "f65b72bbbd0b17e7345821a34c1da49d317ca28b".to_owned()
                    ],
                    bool: vec![],
                    datetime: vec![],
                    rename: Some([
                        ("8003ddd80b42736ebf36b87018e51db3ee84efaf".to_owned(), "/DataSets/DataSet/Units/Unit/Gathering/Country/Name".to_owned()),
                        ("f2374ad051911a65bc0d0a46c13ada2625f55a10".to_owned(), "/DataSets/DataSet/Units/Unit/SourceID".to_owned()),
                        ("d22ecb7dd0e5de6e8b2721977056d30aefda1b75".to_owned(), "/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/HigherTaxa/HigherTaxon/HigherTaxonName".to_owned()),
                        ("6df446e57190f19d63fcf99ba25476510c5c8ce6".to_owned(), "/DataSets/DataSet/Units/Unit/Gathering/Agents/GatheringAgent/AgentText".to_owned()),
                        ("09e05cff5522bf112eedf91c5c2f1432539e59aa".to_owned(), "/DataSets/DataSet/Units/Unit/Gathering/Country/ISO3166Code".to_owned(),),
                        ("46b0ed7a1faa8d25b0c681fbbdc2cca60cecbdf0".to_owned(), "/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/Details".to_owned()),
                        ("bad2f7cae88e4219f2c3b186628189c5380f3c52".to_owned(), "/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/HigherTaxa/HigherTaxon/HigherTaxonRank".to_owned()),
                        ("f65b72bbbd0b17e7345821a34c1da49d317ca28b".to_owned(), "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/SpatialDatum".to_owned()),
                        ("2598ba17aa170832b45c3c206f8133ddddc52c6e".to_owned(), "/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/Text".to_owned()),
                        ("54a52959a34f3c19fa1b0e22cea2ae5c8ce78602".to_owned(), "/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/URI".to_owned()),
                        ("abc0ceb08b2723a43274e1db093dfe1f333fe453".to_owned(), "/DataSets/DataSet/Units/Unit/Gathering/LocalityText".to_owned()),
                        ("9691f318c0f84b4e71e3c125492902af3ad22a81".to_owned(), "/DataSets/DataSet/Units/Unit/Gathering/DateTime/ISODateTimeBegin".to_owned()),
                        ("2b603312fc185489ffcffd5763bcd47c4b126f31".to_owned(), "/DataSets/DataSet/Units/Unit/SourceInstitutionID".to_owned()),
                        ("624516976f697c1eacc7bccfb668d2c25ae7756e".to_owned(), "/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/ScientificName/FullScientificNameString".to_owned()),
                        ("8603069b15071933545a8ce6563308da4d8ee019".to_owned(), "/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/FileURI".to_owned()),
                        ("150ac8760faba3bbf29ee77713fc0402641eea82".to_owned(), "/DataSets/DataSet/Units/Unit/DateLastEdited".to_owned(),),
                        ("adf8c075f2c6b97eaab5cee8f22e97abfdaf6b71".to_owned(), "/DataSets/DataSet/Units/Unit/UnitID".to_owned()),
                        ("0dcf8788cadda41eaa5831f44227d8c531411953".to_owned(), "/DataSets/DataSet/Units/Unit/RecordURI".to_owned()),
                        ("7fdf1ed68add3ac2f4a1b2c89b75245260890dfe".to_owned(), "/DataSets/DataSet/Units/Unit/RecordBasis".to_owned()),
                        ("83fb54d8cfa58d729125f3dccac3a6820d95ccaa".to_owned(), "/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/Format".to_owned()),
                        ("4f885a9545b143d322f3bf34bf2c5148e07d578a".to_owned(), "/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/Creator".to_owned())
                    ].iter().cloned().collect()),
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: true,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: Some("surrogate_key = 17 AND adf8c075f2c6b97eaab5cee8f22e97abfdaf6b71 = 'ZFMK Sc0602'".to_string()),
            };

            if loading_info != expected {
                return Err(format!("{loading_info:?} != {expected:?}"));
            }

            Ok(())
        }

        let db_config = config::get_config_element::<config::Postgres>().unwrap();

        let test_schema = create_test_data(&db_config).await;

        let test = test(&db_config, &test_schema).await;

        cleanup_test_data(&db_config, test_schema).await;

        assert!(test.is_ok());
    }

    #[tokio::test]
    async fn it_cites_abcd() {
        async fn test(db_config: &config::Postgres, test_schema: &str) -> Result<(), String> {
            let gfbio_collections_server_token = "Token 6bc06a951394f222eeb576c6f86a4ad73ab805f6";

            let provider_db_config = DatabaseConnectionConfig {
                host: db_config.host.clone(),
                port: db_config.port,
                database: db_config.database.clone(),
                schema: test_schema.to_string(),
                user: db_config.user.clone(),
                password: db_config.password.clone(),
            };

            let provider = GfbioCollectionsDataProvider::new(
                Url::parse("http://foo.bar/api").unwrap(),
                gfbio_collections_server_token.to_string(),
                provider_db_config,
                "https://doi.pangaea.de".parse().unwrap(),
            )
            .await
            .unwrap();

            let dataset = DataId::External(ExternalDataId {
                provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                layer_id: LayerId("collections/63cf68e4-6e11-469d-8f35-af83ee6586dc/abcd/urn:gfbio.org:abcd:3_259_402:ZFMK+Sc0602".to_string()),
            });

            let result = provider
                .provenance(&dataset)
                .await
                .map_err(|e| e.to_string())?;

            let expected = ProvenanceOutput {
                data: DataId::External(ExternalDataId {
                    provider_id: GFBIO_COLLECTIONS_PROVIDER_ID,
                    layer_id: LayerId("collections/63cf68e4-6e11-469d-8f35-af83ee6586dc/abcd/urn:gfbio.org:abcd:3_259_402:ZFMK+Sc0602".to_string()),
                }),
                provenance: Some(vec![Provenance {
                    citation: "ZFMK Arthropoda Working Group. (2021). ZFMK Scorpiones collection. [Dataset]. Version: 1.2. Data Publisher: Data Center ZFMK. https://doi.org/10.20363/zfmk-coll.scorpiones-2018-11.".to_owned(),
                    license: "CC-BY-SA".to_owned(),
                    uri: "https://www.zfmk.de/en/research/collections/basal-arthropods".to_owned(),
                }]),
            };

            if result != expected {
                return Err(format!("{result:?} != {expected:?}"));
            }

            Ok(())
        }

        let db_config = config::get_config_element::<config::Postgres>().unwrap();

        let test_schema = create_test_data(&db_config).await;

        let result = test(&db_config, &test_schema).await;

        cleanup_test_data(&db_config, test_schema).await;

        assert!(result.is_ok());
    }
}
