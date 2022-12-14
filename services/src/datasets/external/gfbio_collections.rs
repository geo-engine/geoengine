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

use super::gfbio::GfbioDataProvider;
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

        let (column_hash_to_name, column_name_to_hash) = GfbioDataProvider::resolve_columns(
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
                    GfbioDataProvider::resolve_columns(&conn, &self.abcd_db_config.schema).await?;

                let surrogate_key = self.get_surrogate_key_for_gfbio_dataset(&layer).await?;

                GfbioDataProvider::get_provenance(
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
        let id = if let geoengine_datatypes::dataset::DataId::External(id) = id {
            id
        } else {
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
    use super::*;

    #[tokio::test]
    async fn test() {
        // TODO: mock GFBio Colletions API

        // TODO: create test database schema instead of connecting to abcd database

        // TODO: mock the Pangaea API

        let _provider = GfbioCollectionsDataProvider::new(
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
    }
}
