use std::collections::HashMap;
use std::marker::PhantomData;

use crate::datasets::listing::{Provenance, ProvenanceOutput};
use crate::error::Result;
use crate::error::{self, Error};
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerListing,
    ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::util::user_input::Validated;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use bb8_postgres::bb8::{Pool, PooledConnection};
use bb8_postgres::tokio_postgres::{Config, NoTls};
use bb8_postgres::PostgresConnectionManager;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::dataset::{DataId, DataProviderId, ExternalDataId, LayerId};
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
use serde::{Deserialize, Serialize};
use snafu::ensure;

pub const GFBIO_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0x907f_9f5b_0304_4a0e_a5ef_28de_62d1_c0f9);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DatabaseConnectionConfig {
    host: String,
    port: u16,
    database: String,
    schema: String,
    user: String,
    password: String,
}

impl DatabaseConnectionConfig {
    fn pg_config(&self) -> Config {
        let mut config = Config::new();
        config
            .user(&self.user)
            .password(&self.password)
            .host(&self.host)
            .dbname(&self.database);
        config
    }

    fn ogr_pg_config(&self) -> String {
        format!(
            "PG:host={} port={} dbname={} user={} password={}",
            self.host, self.port, self.database, self.user, self.password
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GfbioDataProviderDefinition {
    name: String,
    db_config: DatabaseConnectionConfig,
}

#[typetag::serde]
#[async_trait]
impl DataProviderDefinition for GfbioDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> Result<Box<dyn DataProvider>> {
        Ok(Box::new(GfbioDataProvider::new(self.db_config).await?))
    }

    fn type_name(&self) -> &'static str {
        "GFBio"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        GFBIO_PROVIDER_ID
    }
}

// TODO: make schema, table names and column names configurable like in crawler
#[derive(Debug)]
pub struct GfbioDataProvider {
    db_config: DatabaseConnectionConfig,
    pool: Pool<PostgresConnectionManager<NoTls>>,
    column_hash_to_name: HashMap<String, String>,
    column_name_to_hash: HashMap<String, String>,
}

impl GfbioDataProvider {
    const COLUMN_NAME_LONGITUDE: &'static str = "e9eefbe81d4343c6a114b7d522017bf493b89cef";
    const COLUMN_NAME_LATITUDE: &'static str = "506e190d0ad979d1c7a816223d1ded3604907d91";

    async fn new(db_config: DatabaseConnectionConfig) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);
        let pool = Pool::builder().build(pg_mgr).await?;

        let (column_hash_to_name, column_name_to_hash) =
            Self::resolve_columns(pool.get().await?, &db_config.schema).await?;

        Ok(Self {
            db_config,
            pool,
            column_hash_to_name,
            column_name_to_hash,
        })
    }

    async fn resolve_columns(
        conn: PooledConnection<'_, PostgresConnectionManager<NoTls>>,
        schema: &str,
    ) -> Result<(HashMap<String, String>, HashMap<String, String>)> {
        let stmt = conn
            .prepare(&format!(
                r#"
            SELECT hash, name
            FROM {}.abcd_datasets_translation
            WHERE hash <> $1 AND hash <> $2;"#,
                schema
            ))
            .await?;

        let rows = conn
            .query(
                &stmt,
                &[&Self::COLUMN_NAME_LONGITUDE, &Self::COLUMN_NAME_LATITUDE],
            )
            .await?;

        Ok((
            rows.iter().map(|row| (row.get(0), row.get(1))).collect(),
            rows.iter().map(|row| (row.get(1), row.get(0))).collect(),
        ))
    }

    fn build_attribute_query(surrogate_key: i32) -> String {
        format!("surrogate_key = {surrogate}", surrogate = surrogate_key)
    }

    pub async fn resolve_surrogate_key(&self, dataset_id: &str) -> Result<Option<i32>> {
        let conn = self.pool.get().await?;

        let stmt = conn
            .prepare(&format!(
                r#"
            SELECT surrogate_key
            FROM {schema}.abcd_datasets WHERE dataset_id = $1;"#,
                schema = self.db_config.schema,
            ))
            .await?;

        let row = conn.query_one(&stmt, &[&dataset_id]).await.ok();

        Ok(row.map(|r| r.get::<usize, i32>(0)))
    }
}

#[async_trait]
impl LayerCollectionProvider for GfbioDataProvider {
    async fn collection(
        &self,
        collection: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<LayerCollection> {
        ensure!(
            *collection == self.root_collection_id().await?,
            error::UnknownLayerCollectionId {
                id: collection.clone()
            }
        );

        let conn = self.pool.get().await?;

        let options = options.user_input;

        let stmt = conn
            .prepare(&format!(
                r#"
                SELECT surrogate_key, "{title}", "{details}"
                FROM {schema}.abcd_datasets
                ORDER BY surrogate_key
                LIMIT $1
                OFFSET $2;"#,
                title = self
                    .column_name_to_hash
                    .get("/DataSets/DataSet/Metadata/Description/Representation/Title")
                    .ok_or(Error::GfbioMissingAbcdField)?,
                details = self
                    .column_name_to_hash
                    .get("/DataSets/DataSet/Metadata/Description/Representation/Details")
                    .ok_or(Error::GfbioMissingAbcdField)?,
                schema = self.db_config.schema
            ))
            .await?;

        let rows = conn
            .query(
                &stmt,
                &[&i64::from(options.limit), &i64::from(options.offset)],
            )
            .await?;

        let items: Vec<_> = rows
            .into_iter()
            .map(|row| {
                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: GFBIO_PROVIDER_ID,
                        layer_id: LayerId(row.get::<usize, i32>(0).to_string()),
                    },
                    name: row.get(1),
                    description: row.try_get(2).unwrap_or_else(|_| "".to_owned()),
                })
            })
            .collect();

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: GFBIO_PROVIDER_ID,
                collection_id: collection.clone(),
            },
            name: "GFBio".to_owned(),
            description: "GFBio".to_owned(),
            items,
            entry_label: None,
            properties: vec![],
        })
    }

    async fn root_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId("abcd".to_owned()))
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        let surrogate_key: i32 = id.0.parse().map_err(|_| Error::InvalidDataId)?;

        let conn = self.pool.get().await?;

        let stmt = conn
            .prepare(&format!(
                r#"
                SELECT "{title}", "{details}"
                FROM {schema}.abcd_datasets
                WHERE surrogate_key = $1;"#,
                title = self
                    .column_name_to_hash
                    .get("/DataSets/DataSet/Metadata/Description/Representation/Title")
                    .ok_or(Error::GfbioMissingAbcdField)?,
                details = self
                    .column_name_to_hash
                    .get("/DataSets/DataSet/Metadata/Description/Representation/Details")
                    .ok_or(Error::GfbioMissingAbcdField)?,
                schema = self.db_config.schema
            ))
            .await?;

        let row = conn.query_one(&stmt, &[&surrogate_key]).await?;

        Ok(Layer {
            id: ProviderLayerId {
                provider_id: GFBIO_PROVIDER_ID,
                layer_id: id.clone(),
            },
            name: row.get(0),
            description: row.try_get(1).unwrap_or_else(|_| "".to_owned()),
            workflow: Workflow {
                operator: TypedOperator::Vector(
                    OgrSource {
                        params: OgrSourceParameters {
                            data: DataId::External(ExternalDataId {
                                provider_id: GFBIO_PROVIDER_ID,
                                layer_id: id.clone(),
                            }),
                            attribute_projection: None,
                            attribute_filters: None,
                        },
                    }
                    .boxed(),
                ),
            },
            symbology: None, // TODO
            properties: vec![],
            metadata: HashMap::new(),
        })
    }
}

#[async_trait]
impl DataProvider for GfbioDataProvider {
    async fn provenance(&self, id: &DataId) -> Result<ProvenanceOutput> {
        let surrogate_key: i32 = id
            .external()
            .ok_or(Error::InvalidDataId)
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?
            .layer_id
            .0
            .parse()
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?;

        let conn = self.pool.get().await?;

        let stmt = conn
            .prepare(&format!(
                r#"
            SELECT "{citation}", "{license}", "{uri}"
            FROM {schema}.abcd_datasets WHERE surrogate_key = $1;"#,
                citation = self
                    .column_name_to_hash
                    .get("/DataSets/DataSet/Metadata/IPRStatements/Citations/Citation/Text")
                    .ok_or(Error::GfbioMissingAbcdField)?,
                uri = self
                    .column_name_to_hash
                    .get("/DataSets/DataSet/Metadata/Description/Representation/URI")
                    .ok_or(Error::GfbioMissingAbcdField)?,
                license = self
                    .column_name_to_hash
                    .get("/DataSets/DataSet/Metadata/IPRStatements/Licenses/License/Text")
                    .ok_or(Error::GfbioMissingAbcdField)?,
                schema = self.db_config.schema
            ))
            .await?;

        let row = conn.query_one(&stmt, &[&surrogate_key]).await?;

        Ok(ProvenanceOutput {
            data: id.clone(),
            provenance: Some(Provenance {
                citation: row.try_get(0).unwrap_or_else(|_| "".to_owned()),
                license: row.try_get(1).unwrap_or_else(|_| "".to_owned()),
                uri: row.try_get(2).unwrap_or_else(|_| "".to_owned()),
            }),
        })
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for GfbioDataProvider
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let surrogate_key: i32 = id
            .external()
            .ok_or(Error::InvalidDataId)
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?
            .layer_id
            .0
            .parse()
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?;

        Ok(Box::new(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: self.db_config.ogr_pg_config().into(),
                layer_name: format!("{}.abcd_units", self.db_config.schema),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::None, // TODO
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: "".to_owned(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: self
                        .column_name_to_hash
                        .iter()
                        .filter(|(name, _)| name.starts_with("/DataSets/DataSet/Units/Unit/"))
                        .map(|(_, hash)| hash.clone())
                        .collect(),
                    bool: vec![],
                    datetime: vec![],
                    rename: Some(
                        self.column_hash_to_name
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
                columns: self
                    .column_hash_to_name
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
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for GfbioDataProvider
{
    async fn meta_data(
        &self,
        _id: &DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for GfbioDataProvider
{
    async fn meta_data(
        &self,
        _id: &DataId,
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

#[cfg(test)]
mod tests {
    use bb8_postgres::bb8::ManageConnection;
    use futures::StreamExt;
    use geoengine_datatypes::collections::MultiPointCollection;
    use geoengine_datatypes::dataset::{ExternalDataId, LayerId};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, FeatureData, MultiPoint, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::QueryProcessor;
    use geoengine_operators::{engine::MockQueryContext, source::OgrSourceProcessor};
    use rand::RngCore;

    use crate::layers::layer::ProviderLayerCollectionId;
    use crate::test_data;
    use crate::util::{config, user_input::UserInput};
    use std::{fs::File, io::Read, path::PathBuf};

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

        conn.batch_execute(&format!(
            "CREATE SCHEMA {schema}; 
            SET SEARCH_PATH TO {schema}, public;
            {sql}",
            schema = schema,
            sql = sql
        ))
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

        conn.batch_execute(&format!("DROP SCHEMA {} CASCADE;", schema))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn it_lists() {
        let db_config = config::get_config_element::<config::Postgres>().unwrap();

        let test_schema = create_test_data(&db_config).await;

        let provider = Box::new(GfbioDataProviderDefinition {
            name: "GFBio".to_string(),
            db_config: DatabaseConnectionConfig {
                host: db_config.host.clone(),
                port: db_config.port,
                database: db_config.database.clone(),
                schema: test_schema.clone(),
                user: db_config.user.clone(),
                password: db_config.password.clone(),
            },
        })
        .initialize()
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

        cleanup_test_data(&db_config, test_schema).await;

        let collection = collection.unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GFBIO_PROVIDER_ID,
                    collection_id: root_id,
                },
                name: "GFBio".to_string(),
                description: "GFBio".to_string(),
                items: vec![CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: GFBIO_PROVIDER_ID,
                        layer_id: LayerId("1".to_string()),
                    },
                    name: "Example Title".to_string(),
                    description: "".to_string(),
                })],
                entry_label: None,
                properties: vec![],
            }
        );
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn it_creates_meta_data() {
        async fn test(db_config: &config::Postgres, test_schema: &str) -> Result<(), String> {
            let provider_db_config = DatabaseConnectionConfig {
                host: db_config.host.clone(),
                port: db_config.port,
                database: db_config.database.clone(),
                schema: test_schema.to_owned(),
                user: db_config.user.clone(),
                password: db_config.password.clone(),
            };

            let ogr_pg_string = provider_db_config.ogr_pg_config();

            let provider = Box::new(GfbioDataProviderDefinition {
                name: "GFBio".to_string(),
                db_config: provider_db_config,
            })
            .initialize()
            .await
            .map_err(|e| e.to_string())?;

            let meta: Box<
                dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
            > = provider
                .meta_data(&DataId::External(ExternalDataId {
                    provider_id: GFBIO_PROVIDER_ID,
                    layer_id: LayerId("1".to_string()),
                }))
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
                return Err(format!("{:?} != {:?}", result_descriptor, expected));
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
                layer_name: format!("{}.abcd_units", test_schema),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::None,
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: "".to_owned(),
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
                attribute_query: Some("surrogate_key = 1".to_string()),
            };

            if loading_info != expected {
                return Err(format!("{:?} != {:?}", loading_info, expected));
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
    async fn it_loads() {
        async fn test(db_config: &config::Postgres, test_schema: &str) -> Result<(), String> {
            let provider = Box::new(GfbioDataProviderDefinition {
                name: "GFBio".to_string(),
                db_config: DatabaseConnectionConfig {
                    host: db_config.host.clone(),
                    port: db_config.port,
                    database: db_config.database.clone(),
                    schema: test_schema.to_owned(),
                    user: db_config.user.clone(),
                    password: db_config.password.clone(),
                },
            })
            .initialize()
            .await
            .map_err(|e| e.to_string())?;

            let meta: Box<
                dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
            > = provider
                .meta_data(&DataId::External(ExternalDataId {
                    provider_id: GFBIO_PROVIDER_ID,
                    layer_id: LayerId("1".to_string()),
                }))
                .await
                .map_err(|e| e.to_string())?;

            let processor: OgrSourceProcessor<MultiPoint> = OgrSourceProcessor::new(meta, vec![]);

            let query_rectangle = VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new((0., -90.).into(), (180., 90.).into()).unwrap(),
                time_interval: TimeInterval::default(),
                spatial_resolution: SpatialResolution::zero_point_one(),
            };
            let ctx = MockQueryContext::test_default();

            let result: Vec<_> = processor
                .query(query_rectangle, &ctx)
                .await
                .map_err(|e| e.to_string())?
                .collect()
                .await;

            if result.len() != 1 {
                return Err("result.len() != 1".to_owned());
            }

            if result[0].is_err() {
                return Err("result[0].is_err()".to_owned());
            }

            let result = result[0].as_ref().unwrap();

            let expected = MultiPointCollection::from_data(
                MultiPoint::many(vec![(0.20972, -13.27737), (176.20972, 13.27737)]).unwrap(),
                vec![TimeInterval::default(); 2],
                [
                ("/DataSets/DataSet/Units/Unit/DateLastEdited".to_owned(),FeatureData::NullableText(vec![Some("2014-01-01T00:00:00".to_owned()); 2])),
                ("/DataSets/DataSet/Units/Unit/Gathering/Agents/GatheringAgent/AgentText".to_owned(),FeatureData::NullableText(vec![None; 2])),
                ("/DataSets/DataSet/Units/Unit/Gathering/Country/ISO3166Code".to_owned(),FeatureData::NullableText(vec![None; 2])),
                ("/DataSets/DataSet/Units/Unit/Gathering/Country/Name".to_owned(),FeatureData::NullableText(vec![Some("Country".to_owned()); 2])),
                ("/DataSets/DataSet/Units/Unit/Gathering/DateTime/ISODateTimeBegin".to_owned(),FeatureData::NullableText(vec![None; 2])),
                ("/DataSets/DataSet/Units/Unit/Gathering/LocalityText".to_owned(),FeatureData::NullableText(vec![Some("Locality text".to_owned()); 2])),
                ("/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/SpatialDatum".to_owned(),FeatureData::NullableText(vec![None; 2])),
                ("/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/HigherTaxa/HigherTaxon/HigherTaxonName".to_owned(),FeatureData::NullableText(vec![Some("Higher Taxon Name".to_owned()); 2])),
                ("/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/HigherTaxa/HigherTaxon/HigherTaxonRank".to_owned(),FeatureData::NullableText(vec![Some("Taxon Rank".to_owned()); 2])),
                ("/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/ScientificName/FullScientificNameString".to_owned(),FeatureData::NullableText(vec![Some("Full Scientific Name".to_owned());2])),
                ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/Creator".to_owned(),FeatureData::NullableText(vec![None; 2])),
                ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/FileURI".to_owned(),FeatureData::NullableText(vec![None; 2])),
                ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/Format".to_owned(),FeatureData::NullableText(vec![None; 2])),
                ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/Details".to_owned(),FeatureData::NullableText(vec![None; 2])),
                ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/Text".to_owned(),FeatureData::NullableText(vec![None; 2])),
                ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/URI".to_owned(),FeatureData::NullableText(vec![None; 2])),
                ("/DataSets/DataSet/Units/Unit/RecordBasis".to_owned(),FeatureData::NullableText(vec![Some("Record Basis".to_owned()); 2])),
                ("/DataSets/DataSet/Units/Unit/RecordURI".to_owned(),FeatureData::NullableText(vec![None; 2])),
                ("/DataSets/DataSet/Units/Unit/SourceID".to_owned(),FeatureData::NullableText(vec![Some("Source ID".to_owned()); 2])),
                ("/DataSets/DataSet/Units/Unit/SourceInstitutionID".to_owned(),FeatureData::NullableText(vec![Some("Institution Id".to_owned()); 2])),
                ("/DataSets/DataSet/Units/Unit/UnitID".to_owned(),FeatureData::NullableText(vec![Some("Unit ID".to_owned()); 2]))]
                .iter()
                .cloned()
                .collect(),
            )
            .unwrap();

            if result != &expected {
                return Err(format!("{:?} != {:?}", result, expected));
            }

            Ok(())
        }

        let db_config = config::get_config_element::<config::Postgres>().unwrap();

        let test_schema = create_test_data(&db_config).await;

        let result = test(&db_config, &test_schema).await;

        cleanup_test_data(&db_config, test_schema).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn it_cites() {
        async fn test(db_config: &config::Postgres, test_schema: &str) -> Result<(), String> {
            let provider = Box::new(GfbioDataProviderDefinition {
                name: "GFBio".to_string(),
                db_config: DatabaseConnectionConfig {
                    host: db_config.host.clone(),
                    port: db_config.port,
                    database: db_config.database.clone(),
                    schema: test_schema.to_owned(),
                    user: db_config.user.clone(),
                    password: db_config.password.clone(),
                },
            })
            .initialize()
            .await
            .map_err(|e| e.to_string())?;

            let dataset = DataId::External(ExternalDataId {
                provider_id: GFBIO_PROVIDER_ID,
                layer_id: LayerId("1".to_owned()),
            });

            let result = provider
                .provenance(&dataset)
                .await
                .map_err(|e| e.to_string())?;

            let expected = ProvenanceOutput {
                data: DataId::External(ExternalDataId {
                    provider_id: GFBIO_PROVIDER_ID,
                    layer_id: LayerId("1".to_owned()),
                }),
                provenance: Some(Provenance {
                    citation: "Example Description".to_owned(),
                    license: "CC-BY-SA".to_owned(),
                    uri: "http://example.org".to_owned(),
                }),
            };

            if result != expected {
                return Err(format!("{:?} != {:?}", result, expected));
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
