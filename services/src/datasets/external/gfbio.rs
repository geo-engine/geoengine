use std::collections::HashMap;
use std::marker::PhantomData;

use crate::error::Error;
use crate::{datasets::listing::DatasetListOptions, error::Result};
use crate::{
    datasets::{
        listing::{DatasetListing, DatasetProvider},
        storage::DatasetProviderDefinition,
    },
    error,
    util::user_input::Validated,
};
use async_trait::async_trait;
use bb8_postgres::bb8::{Pool, PooledConnection};
use bb8_postgres::tokio_postgres::{Config, NoTls};
use bb8_postgres::PostgresConnectionManager;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
use geoengine_datatypes::primitives::FeatureDataType;
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{StaticMetaData, TypedResultDescriptor};
use geoengine_operators::source::{
    OgrSourceColumnSpec, OgrSourceDatasetTimeType, OgrSourceErrorSpec,
};
use geoengine_operators::{
    engine::{
        MetaData, MetaDataProvider, RasterQueryRectangle, RasterResultDescriptor,
        VectorQueryRectangle, VectorResultDescriptor,
    },
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use serde::{Deserialize, Serialize};

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
    id: DatasetProviderId,
    name: String,
    db_config: DatabaseConnectionConfig,
}

#[typetag::serde]
#[async_trait]
impl DatasetProviderDefinition for GfbioDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> Result<Box<dyn DatasetProvider>> {
        Ok(Box::new(
            GfbioDataProvider::new(self.id, self.db_config).await?,
        ))
    }

    fn type_name(&self) -> String {
        "GFBio".to_owned()
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DatasetProviderId {
        self.id
    }
}

// TODO: make schema, table names and column names configurable like in crawler
pub struct GfbioDataProvider {
    id: DatasetProviderId,
    db_config: DatabaseConnectionConfig,
    pool: Pool<PostgresConnectionManager<NoTls>>,
    column_hash_to_name: HashMap<String, String>,
    column_name_to_hash: HashMap<String, String>,
}

impl GfbioDataProvider {
    const COLUMN_NAME_LONGITUDE: &'static str = "e9eefbe81d4343c6a114b7d522017bf493b89cef";
    const COLUMN_NAME_LATITUDE: &'static str = "506e190d0ad979d1c7a816223d1ded3604907d91";

    async fn new(id: DatasetProviderId, db_config: DatabaseConnectionConfig) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);
        let pool = Pool::builder().build(pg_mgr).await?;

        let (column_hash_to_name, column_name_to_hash) =
            Self::resolve_columns(pool.get().await?, &db_config.schema).await?;

        Ok(Self {
            id,
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

    fn build_query(&self, surrogate_key: i32) -> String {
        let columns = self
            .column_hash_to_name
            .iter()
            .filter(|(_, name)| name.starts_with("/DataSets/DataSet/Units/Unit"))
            .fold(String::new(), |query, (hash, name)| {
                format!(r#"{}, "{}" AS "{}""#, query, hash, name)
            });

        format!(
            r#"SELECT surrogate_key, geom {columns} FROM {schema}.abcd_units WHERE surrogate_key = {surrogate}"#,
            columns = columns,
            schema = self.db_config.schema,
            surrogate = surrogate_key
        )
    }
}

#[async_trait]
impl DatasetProvider for GfbioDataProvider {
    async fn list(&self, _options: Validated<DatasetListOptions>) -> Result<Vec<DatasetListing>> {
        let conn = self.pool.get().await?;

        let stmt = conn
            .prepare(&format!(
                r#"
            SELECT surrogate_key, "{title}", "{details}"
            FROM {schema}.abcd_datasets;"#,
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

        let rows = conn.query(&stmt, &[]).await?;

        let listings: Vec<_> = rows
            .into_iter()
            .map(|row| DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id: self.id,
                    dataset_id: row.get::<usize, i32>(0).to_string(),
                }),
                name: row.get(1),
                description: row.try_get(2).unwrap_or_else(|_| "".to_owned()),
                tags: vec![],
                source_operator: "OgrSource".to_owned(),
                result_descriptor: TypedResultDescriptor::Vector(VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: self
                        .column_hash_to_name
                        .iter()
                        .map(|(_, name)| (name.clone(), FeatureDataType::Text))
                        .collect(),
                }),
                symbology: None,
            })
            .collect();

        Ok(listings)
    }

    async fn load(
        &self,
        _dataset: &geoengine_datatypes::dataset::DatasetId,
    ) -> crate::error::Result<crate::datasets::storage::Dataset> {
        Err(error::Error::NotYetImplemented)
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for GfbioDataProvider
{
    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let surrogate_key: i32 = dataset
            .external()
            .ok_or(Error::InvalidDatasetId)
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?
            .dataset_id
            .parse()
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?;

        Ok(Box::new(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: self.db_config.ogr_pg_config().into(),
                layer_name: "".to_owned(),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::None, // TODO
                columns: Some(OgrSourceColumnSpec {
                    x: "".to_owned(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: self.column_name_to_hash.keys().cloned().collect(),
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: true,
                on_error: OgrSourceErrorSpec::Ignore,
                provenance: None,
                sql_query: Some(self.build_query(surrogate_key)),
            },
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: self
                    .column_hash_to_name
                    .iter()
                    .map(|(_, name)| (name.clone(), FeatureDataType::Text))
                    .collect(),
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
        _dataset: &DatasetId,
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
        _dataset: &DatasetId,
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
    use rand::RngCore;

    use crate::{
        datasets::listing::OrderBy,
        util::{config, user_input::UserInput},
    };
    use std::{fs::File, io::Read, str::FromStr};

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
        File::open("test-data/gfbio/test_data.sql")
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
            id: DatasetProviderId::from_str("d29f2430-5c5e-4748-a2fa-6423aa2af42d").unwrap(),
            name: "Gfbio".to_string(),
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

        let listing = provider
            .list(
                DatasetListOptions {
                    filter: None,
                    order: OrderBy::NameAsc,
                    offset: 0,
                    limit: 10,
                }
                .validated()
                .unwrap(),
            )
            .await;

        cleanup_test_data(&db_config, test_schema).await;

        let listing = listing.unwrap();

        assert_eq!(
            listing,
            vec![DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id: DatasetProviderId::from_str(
                        "d29f2430-5c5e-4748-a2fa-6423aa2af42d"
                    )
                    .unwrap(),
                    dataset_id: "1".to_string(),
                }),
                name: "Example Title".to_string(),
                description: "".to_string(),
                tags: vec![],
                source_operator: "OgrSource".to_string(),
                result_descriptor: TypedResultDescriptor::Vector(VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        ("/DataSets/DataSet/Units/Unit/Gathering/Agents/GatheringAgent/AgentText".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/SourceID".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Metadata/IPRStatements/Citations/Citation/Text".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/ContentContacts/ContentContact/Email".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/Gathering/DateTime/ISODateTimeBegin".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/ScientificName/FullScientificNameString".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/HigherTaxa/HigherTaxon/HigherTaxonRank".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Metadata/Description/Representation/Details".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Metadata/IPRStatements/Licenses/License/URI".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Metadata/RevisionData/DateModified".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/DatasetGUID".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/Creator".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/Text".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Metadata/IPRStatements/Licenses/License/Text".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/SpatialDatum".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/FileURI".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/ContentContacts/ContentContact/Name".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/Gathering/Country/Name".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/SourceInstitutionID".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/URI".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/Format".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/RecordURI".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Metadata/IPRStatements/Licenses/License/Details".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/HigherTaxa/HigherTaxon/HigherTaxonName".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/RecordBasis".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/DateLastEdited".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/Gathering/Country/ISO3166Code".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/MultiMediaObjects/MultiMediaObject/IPR/Licenses/License/Details".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/TechnicalContacts/TechnicalContact/Name".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Metadata/Description/Representation/URI".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/TechnicalContacts/TechnicalContact/Email".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/UnitID".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Metadata/Description/Representation/Title".to_owned(), FeatureDataType::Text),
                        ("/DataSets/DataSet/Units/Unit/Gathering/LocalityText".to_owned(), FeatureDataType::Text)]
                        .iter()
                        .cloned()
                        .collect(),
                }),
                symbology: None,
            }]
        );
    }
}
