use crate::contexts::GeoEngineDb;
use crate::datasets::listing::{Provenance, ProvenanceOutput};
use crate::error::{Error, Result};
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerCollectionListing,
    LayerListing, ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{
    LayerCollectionId, LayerCollectionProvider, ProviderCapabilities, SearchCapabilities,
    SearchParameters, SearchType, SearchTypes,
};
use crate::util::postgres::DatabaseConnectionConfig;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::bb8::Pool;
use chrono::NaiveDateTime;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::dataset::{DataId, DataProviderId, LayerId};
use geoengine_datatypes::primitives::{BoundingBox2D, CacheTtlSeconds, Coordinate2D, TimeInterval};
use geoengine_datatypes::primitives::{
    FeatureDataType, Measurement, RasterQueryRectangle, VectorQueryRectangle,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterResultDescriptor, StaticMetaData, TypedOperator,
    VectorColumnInfo, VectorOperator, VectorResultDescriptor,
};
use geoengine_operators::error::Error::{Bb8Postgres, Postgres};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    GdalLoadingInfo, OgrSource, OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType,
    OgrSourceDurationSpec, OgrSourceErrorSpec, OgrSourceParameters, OgrSourceTimeFormat,
};
use itertools::Itertools;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::fmt::Write;
use tokio::time::{Duration, timeout};
use tokio_postgres::NoTls;

pub const GBIF_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0x1c01_dbb9_e3ab_f9a2_06f5_228b_a4b6_bf7a);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct GbifDataProviderDefinition {
    pub name: String,
    pub description: String,
    pub priority: Option<i16>,
    pub db_config: DatabaseConnectionConfig,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
    pub autocomplete_timeout: i32,
    pub columns: Vec<String>,
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for GbifDataProviderDefinition {
    async fn initialize(self: Box<Self>, _db: D) -> Result<Box<dyn DataProvider>> {
        Ok(Box::new(
            GbifDataProvider::new(
                self.name,
                self.description,
                self.db_config,
                self.cache_ttl,
                self.autocomplete_timeout,
                self.columns,
            )
            .await?,
        ))
    }

    fn type_name(&self) -> &'static str {
        "GBIF"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        GBIF_PROVIDER_ID
    }

    fn priority(&self) -> i16 {
        self.priority.unwrap_or(0)
    }
}

#[derive(Debug)]
pub struct GbifDataProvider {
    name: String,
    description: String,
    db_config: DatabaseConnectionConfig,
    pool: Pool<PostgresConnectionManager<NoTls>>,
    cache_ttl: CacheTtlSeconds,
    autocomplete_timeout: u64,
    columns: Vec<String>,
    occurrence_table: String,
}

impl GbifDataProvider {
    const LEVELS: [&'static str; 7] = [
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "canonicalname",
    ];

    const LISTABLE_RANKS: [&'static str; 3] = ["family", "genus", "species"];

    const OCCURRENCE_COLUMNS: [&'static str; 49] = [
        "gbifid",
        "datasetkey",
        "occurrenceid",
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "species",
        "infraspecificepithet",
        "taxonrank",
        "scientificname",
        "verbatimscientificname",
        "verbatimscientificnameauthorship",
        "countrycode",
        "locality",
        "stateprovince",
        "occurrencestatus",
        "individualcount",
        "publishingorgkey",
        "decimallatitude",
        "decimallongitude",
        "coordinateuncertaintyinmeters",
        "coordinateprecision",
        "elevation",
        "elevationaccuracy",
        "depth",
        "depthaccuracy",
        "day",
        "month",
        "year",
        "taxonkey",
        "specieskey",
        "basisofrecord",
        "institutioncode",
        "collectioncode",
        "catalognumber",
        "recordnumber",
        "identifiedby",
        "dateidentified",
        "license",
        "rightsholder",
        "recordedby",
        "typestatus",
        "establishmentmeans",
        "lastinterpreted",
        "mediatype",
        "issue",
    ];
    const OCCURRENCE_LITE_COLUMNS: [&'static str; 3] =
        ["gbifid", "basisofrecord", "scientificname"];

    async fn new(
        name: String,
        description: String,
        db_config: DatabaseConnectionConfig,
        cache_ttl: CacheTtlSeconds,
        autocomplete_timeout: i32,
        columns: Vec<String>,
    ) -> Result<Self> {
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);
        let pool = Pool::builder().build(pg_mgr).await?;

        let columns: Vec<_> = columns
            .into_iter()
            .filter(|column| Self::OCCURRENCE_COLUMNS.contains(&column.as_str()))
            .collect();

        let occurrence_table = if columns
            .iter()
            .all(|column| Self::OCCURRENCE_LITE_COLUMNS.contains(&column.as_str()))
        {
            "occurrences_lite".to_string()
        } else {
            "occurrences".to_string()
        };

        Ok(Self {
            name,
            description,
            db_config,
            pool,
            cache_ttl,
            autocomplete_timeout: u64::try_from(autocomplete_timeout).unwrap_or(u64::MAX),
            columns,
            occurrence_table,
        })
    }

    pub fn all_columns() -> Vec<String> {
        Self::OCCURRENCE_COLUMNS
            .into_iter()
            .map(String::from)
            .collect()
    }

    fn filter_columns(&self, columns: Vec<String>) -> Vec<String> {
        columns
            .into_iter()
            .filter(|column| self.columns.contains(column))
            .collect()
    }

    fn level_name(path: &str) -> String {
        if path.is_empty() {
            return Self::level_name_from_depth(0);
        }
        Self::level_name_from_depth(path.split('/').count())
    }

    fn level_name_from_depth(length: usize) -> String {
        if length > Self::LEVELS.len() {
            String::new()
        } else {
            Self::LEVELS[length].to_string()
        }
    }

    fn get_filters(path: &str) -> Vec<(String, String)> {
        if path.is_empty() {
            vec![]
        } else {
            path.split('/')
                .enumerate()
                .map(|(level, filter)| (Self::level_name_from_depth(level), filter.to_string()))
                .collect()
        }
    }

    fn extend_path(path: String, selection: &str) -> String {
        if path.is_empty() {
            selection.to_string()
        } else {
            path + "/" + selection
        }
    }

    fn create_taxonrank_filter(taxonranks: &[String]) -> String {
        format!(
            "taxonrank IN ({})",
            taxonranks
                .iter()
                .enumerate()
                .map(|(index, rank)| format!(
                    "{delimiter}'{rank}'",
                    delimiter = if index > 0 {
                        ",".to_string()
                    } else {
                        String::new()
                    }
                ))
                .reduce(|a, b| a + &b)
                .unwrap_or_default()
        )
    }

    async fn get_datasets_items(
        &self,
        options: &LayerCollectionListOptions,
        path: &str,
    ) -> Result<Vec<CollectionItem>> {
        let taxonrank = path
            .split_once('/')
            .map_or_else(String::new, |(taxonrank, _)| taxonrank.to_string());
        ensure!(
            Self::LISTABLE_RANKS.contains(&taxonrank.as_str()),
            crate::error::InvalidPath
        );
        let path = path.split_once('/').map_or_else(|| "", |(_, path)| path);
        let filters = GbifDataProvider::get_filters(path);
        let conn = self.pool.get().await?;
        let query = &format!(
            "
            SELECT name, count
            FROM {schema}.{taxonrank}_stats
            WHERE name IN
                (
                    SELECT canonicalname
                    FROM {schema}.species
                    WHERE taxonrank = '{taxonrank}'{filter}
                )
            ORDER BY name
            LIMIT $1
            OFFSET $2;
            ",
            schema = self.db_config.schema,
            filter = filters.iter().enumerate().fold(
                String::new(),
                |mut output, (index, (column, _))| {
                    let _ = write!(output, r#" AND "{column}" = ${index}"#, index = index + 3);
                    output
                }
            )
        );

        let stmt = conn.prepare(query).await?;

        let limit = &i64::from(options.limit);
        let offset = &i64::from(options.offset);
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![limit, offset];
        filters.iter().for_each(|(_, value)| params.push(value));
        let rows = conn.query(&stmt, params.as_slice()).await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let canonicalname = row.get::<usize, String>(0);
                let num_points = row.get::<usize, i64>(1);

                CollectionItem::Layer(LayerListing {
                    r#type: Default::default(),
                    id: ProviderLayerId {
                        provider_id: GBIF_PROVIDER_ID,
                        layer_id: LayerId(taxonrank.clone() + "/" + canonicalname.as_str()),
                    },
                    name: canonicalname.clone(),
                    description: format!("{num_points} occurrences"),
                    properties: vec![],
                })
            })
            .collect::<Vec<_>>())
    }

    async fn get_datasets_search_items(
        &self,
        limit: &u32,
        offset: &u32,
        path: &str,
        search_string: &str,
        taxonranks: Vec<String>,
    ) -> Result<Vec<CollectionItem>> {
        let path = path.split_once('/').map_or_else(|| "", |(_, path)| path);
        let filters = GbifDataProvider::get_filters(path);
        let taxonrank_filter = Self::create_taxonrank_filter(&taxonranks);
        let taxonrank_queries = taxonranks
            .iter()
            .enumerate()
            .map(|(index, rank)| {
                format!(
                    r"
                    {union}
                    (
                        SELECT count, name, '{rank}' AS rank
                        FROM {schema}.{rank}_stats
                        WHERE name IN (SELECT name FROM names)
                        ORDER BY name
                    )
                ",
                    schema = self.db_config.schema,
                    union = if index > 0 {
                        "UNION ALL".to_string()
                    } else {
                        String::new()
                    }
                )
            })
            .reduce(|a, b| a + &b)
            .unwrap_or_default();
        let conn = self.pool.get().await?;
        let query = &format!(
            r"
            WITH names AS (
                SELECT canonicalname AS name
                FROM {schema}.species
                WHERE {taxonrank_filter}{filter} AND canonicalname ILIKE $3
            )
            SELECT count, name, rank
            FROM (
                {taxonrank_queries}
            ) AS T
            ORDER BY name, rank
            LIMIT $1
            OFFSET $2;
            ",
            schema = self.db_config.schema,
            filter = filters.iter().enumerate().fold(
                String::new(),
                |mut output, (index, (column, _))| {
                    let _ = write!(output, r#" AND "{column}" = ${index}"#, index = index + 4);
                    output
                }
            )
        );

        let stmt = conn.prepare(query).await?;

        let limit = &i64::from(*limit);
        let offset = &i64::from(*offset);
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![limit, offset, &search_string];
        filters.iter().for_each(|(_, value)| params.push(value));
        let rows = conn.query(&stmt, params.as_slice()).await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let num_points = row.get::<usize, i64>(0);
                let name = row.get::<usize, String>(1);
                let rank = row.get::<usize, String>(2);

                CollectionItem::Layer(LayerListing {
                    r#type: Default::default(),
                    id: ProviderLayerId {
                        provider_id: GBIF_PROVIDER_ID,
                        layer_id: LayerId(rank.clone() + "/" + name.as_str()),
                    },
                    name: name.clone(),
                    description: format!(
                        "{rank}, {num_points} occurrences",
                        rank = rank[..1].to_ascii_uppercase().to_string() + &rank[1..]
                    ),
                    properties: vec![],
                })
            })
            .collect::<Vec<_>>())
    }

    async fn get_datasets_autocomplete_items(
        &self,
        limit: &u32,
        offset: &u32,
        path: &str,
        search_string: &str,
        taxonranks: Vec<String>,
    ) -> Result<Vec<String>> {
        let path = path.split_once('/').map_or_else(|| "", |(_, path)| path);
        let filters = GbifDataProvider::get_filters(path);
        let taxonrank_filter = Self::create_taxonrank_filter(&taxonranks);
        let conn = self.pool.get().await?;
        let query = &format!(
            r"
            SELECT DISTINCT canonicalname
            FROM {schema}.species
            WHERE {taxonrank_filter}{filter} AND canonicalname ILIKE $3
            ORDER BY canonicalname
            LIMIT $1
            OFFSET $2;
            ",
            schema = self.db_config.schema,
            filter = filters.iter().enumerate().fold(
                String::new(),
                |mut output, (index, (column, _))| {
                    let _ = write!(output, r#" AND "{column}" = ${index}"#, index = index + 4);
                    output
                }
            )
        );

        let stmt = conn.prepare(query).await?;

        let limit = &i64::from(*limit);
        let offset = &i64::from(*offset);
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![limit, offset, &search_string];
        filters.iter().for_each(|(_, value)| params.push(value));
        let rows = timeout(
            Duration::from_secs(self.autocomplete_timeout),
            conn.query(&stmt, params.as_slice()),
        )
        .await;

        match rows {
            Ok(rows) => Ok(rows?
                .into_iter()
                .map(|row| row.get::<usize, String>(0))
                .collect::<Vec<_>>()),
            Err(_) => Ok(Vec::<String>::new()),
        }
    }

    async fn get_filter_items(
        &self,
        options: &LayerCollectionListOptions,
        path: &str,
    ) -> Result<Vec<CollectionItem>> {
        let column = GbifDataProvider::level_name(path);
        if !Self::LEVELS.contains(&column.as_str()) {
            return Err(Error::InvalidLayerCollectionId);
        }
        let filters = GbifDataProvider::get_filters(path);
        let conn = self.pool.get().await?;
        let query = &format!(
            r#"
            SELECT DISTINCT "{column}"
            FROM {schema}.species
            WHERE "{column}" IS NOT NULL{filter}
            ORDER BY "{column}"
            LIMIT $1
            OFFSET $2
            "#,
            schema = self.db_config.schema,
            column = column,
            filter = filters.iter().enumerate().fold(
                String::new(),
                |mut output, (index, (column, _))| {
                    let _ = write!(output, r#" AND "{column}" = ${index}"#, index = index + 3);
                    output
                }
            )
        );
        let stmt = conn.prepare(query).await?;
        let limit = &i64::from(options.limit);
        let offset = &i64::from(options.offset);
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![limit, offset];
        filters.iter().for_each(|(_, value)| params.push(value));
        let rows = conn.query(&stmt, params.as_slice()).await?;

        let items = rows
            .into_iter()
            .map(|row| {
                let name = row.get::<usize, String>(0);
                let new_path = GbifDataProvider::extend_path((*path).to_string(), &name);
                let new_id = "select/".to_string() + &new_path;

                CollectionItem::Collection(LayerCollectionListing {
                    r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: GBIF_PROVIDER_ID,
                        collection_id: LayerCollectionId(new_id),
                    },
                    name,
                    description: String::new(),
                    properties: Default::default(),
                })
            })
            .collect::<Vec<_>>();
        Ok(items)
    }

    async fn get_filter_search_items(
        &self,
        limit: &u32,
        offset: &u32,
        path: &str,
        search_string: &str,
    ) -> Result<Vec<CollectionItem>> {
        let items = self
            .query_filter_items(limit, offset, path, search_string)
            .await?
            .into_iter()
            .map(|name| {
                let new_path = GbifDataProvider::extend_path((*path).to_string(), &name);
                let new_id = "select/".to_string() + &new_path;

                CollectionItem::Collection(LayerCollectionListing {
                    r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: GBIF_PROVIDER_ID,
                        collection_id: LayerCollectionId(new_id),
                    },
                    name,
                    description: String::new(),
                    properties: Default::default(),
                })
            })
            .collect::<Vec<_>>();

        Ok(items)
    }

    async fn get_filter_autocomplete_items(
        &self,
        limit: &u32,
        offset: &u32,
        path: &str,
        search_string: &str,
    ) -> Result<Vec<String>> {
        let filter_items = timeout(
            Duration::from_secs(self.autocomplete_timeout),
            self.query_filter_items(limit, offset, path, search_string),
        )
        .await;

        match filter_items {
            Ok(filter_items) => Ok(filter_items?),
            Err(_) => Ok(Vec::<String>::new()),
        }
    }

    async fn query_filter_items(
        &self,
        limit: &u32,
        offset: &u32,
        path: &str,
        search_string: &str,
    ) -> Result<Vec<String>> {
        let column = GbifDataProvider::level_name(path);
        if !Self::LEVELS.contains(&column.as_str()) {
            return Err(Error::InvalidLayerCollectionId);
        }
        let filters = GbifDataProvider::get_filters(path);
        let conn = self.pool.get().await?;
        let query = &format!(
            r#"
            SELECT DISTINCT "{column}"
            FROM {schema}.species
            WHERE "{column}" IS NOT NULL{filter} AND "{column}" ILIKE $3
            ORDER BY "{column}"
            LIMIT $1
            OFFSET $2
            "#,
            schema = self.db_config.schema,
            column = column,
            filter = filters.iter().enumerate().fold(
                String::new(),
                |mut output, (index, (column, _))| {
                    let _ = write!(output, r#" AND "{column}" = ${index}"#, index = index + 4);
                    output
                }
            )
        );
        let stmt = conn.prepare(query).await?;

        let limit = &i64::from(*limit);
        let offset = &i64::from(*offset);
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![limit, offset, &search_string];
        filters.iter().for_each(|(_, value)| params.push(value));
        let rows = conn.query(&stmt, params.as_slice()).await?;

        let items = rows
            .into_iter()
            .map(|row| row.get::<usize, String>(0))
            .collect::<Vec<_>>();
        Ok(items)
    }

    fn get_select_items(path: &str) -> Vec<CollectionItem> {
        let level_name = GbifDataProvider::level_name(path);
        let mut items = vec![];

        if !GbifDataProvider::LEVELS[6..].contains(&level_name.as_str()) {
            items.push(CollectionItem::Collection(LayerCollectionListing {
                r#type: Default::default(),
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: LayerCollectionId("filter/".to_string() + path),
                },
                name: "Select ".to_string() + &level_name,
                description: "Refine the current filter".to_string(),
                properties: Default::default(),
            }));
        }
        if !GbifDataProvider::LEVELS[5..].contains(&level_name.as_str()) {
            items.push(CollectionItem::Collection(LayerCollectionListing {
                r#type: Default::default(),
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: LayerCollectionId("datasets/family/".to_string() + path),
                },
                name: "Family datasets".to_string(),
                description: "Apply the current filter".to_string(),
                properties: Default::default(),
            }));
        }
        if !GbifDataProvider::LEVELS[6..].contains(&level_name.as_str()) {
            items.push(CollectionItem::Collection(LayerCollectionListing {
                r#type: Default::default(),
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: LayerCollectionId("datasets/genus/".to_string() + path),
                },
                name: "Genus datasets".to_string(),
                description: "Apply the current filter".to_string(),
                properties: Default::default(),
            }));
        }
        items.push(CollectionItem::Collection(LayerCollectionListing {
            r#type: Default::default(),
            id: ProviderLayerCollectionId {
                provider_id: GBIF_PROVIDER_ID,
                collection_id: LayerCollectionId("datasets/species/".to_string() + path),
            },
            name: "Species datasets".to_string(),
            description: "Apply the current filter".to_string(),
            properties: Default::default(),
        }));

        items
    }
}

#[async_trait]
impl LayerCollectionProvider for GbifDataProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    async fn load_layer_collection(
        &self,
        collection: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        let selector = collection.0.split_once('/').map_or_else(
            || collection.clone().0,
            |(selector, _)| selector.to_string(),
        );
        let path = collection
            .0
            .split_once('/')
            .map_or_else(|| "", |(_, path)| path);

        let items = match selector.as_str() {
            "datasets" => self.get_datasets_items(&options, path).await?,
            "filter" => self.get_filter_items(&options, path).await?,
            "select" => Self::get_select_items(path),
            _ => vec![],
        };

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: GBIF_PROVIDER_ID,
                collection_id: collection.clone(),
            },
            name: "GBIF".to_owned(),
            description: "GBIF occurrence datasets".to_owned(),
            items,
            entry_label: None,
            properties: vec![],
        })
    }

    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId("select".to_owned()))
    }

    async fn load_layer(&self, id: &LayerId) -> Result<Layer> {
        let key = id.clone().0;

        let (taxonrank, canonicalname) =
            key.split_once('/')
                .ok_or(Error::InvalidLayerId)
                .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                })?;

        Ok(Layer {
            id: ProviderLayerId {
                provider_id: GBIF_PROVIDER_ID,
                layer_id: id.clone(),
            },
            name: canonicalname.to_string(),
            description: format!("All occurrences with a {taxonrank} of {canonicalname}"),
            workflow: Workflow {
                operator: TypedOperator::Vector(
                    OgrSource {
                        params: OgrSourceParameters {
                            data: geoengine_datatypes::dataset::NamedData::with_system_provider(
                                GBIF_PROVIDER_ID.to_string(),
                                id.to_string(),
                            ),
                            attribute_projection: None,
                            attribute_filters: None,
                        },
                    }
                    .boxed(),
                ),
            },
            symbology: None,
            properties: vec![],
            metadata: Default::default(),
        })
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            listing: true,
            search: SearchCapabilities {
                search_types: SearchTypes {
                    fulltext: true,
                    prefix: true,
                },
                autocomplete: true,
                filters: None,
            },
        }
    }

    async fn search(
        &self,
        collection_id: &LayerCollectionId,
        search: SearchParameters,
    ) -> Result<LayerCollection> {
        let selector = collection_id.0.split_once('/').map_or_else(
            || collection_id.clone().0,
            |(selector, _)| selector.to_string(),
        );
        let path = collection_id
            .0
            .split_once('/')
            .map_or_else(|| "", |(_, path)| path);

        let search_string = match search.search_type {
            SearchType::Fulltext => format!("%{}%", search.search_string),
            SearchType::Prefix => format!("{}%", search.search_string),
        };

        let items = match selector.as_str() {
            "datasets" => {
                let taxonrank = path
                    .split_once('/')
                    .map_or_else(String::new, |(taxonrank, _)| taxonrank.to_string());
                ensure!(
                    Self::LISTABLE_RANKS.contains(&taxonrank.as_str()),
                    crate::error::InvalidPath
                );
                self.get_datasets_search_items(
                    &search.limit,
                    &search.offset,
                    path,
                    &search_string,
                    vec![taxonrank],
                )
                .await?
            }
            "filter" => {
                self.get_filter_search_items(&search.limit, &search.offset, path, &search_string)
                    .await?
            }
            "select" => {
                self.get_datasets_search_items(
                    &search.limit,
                    &search.offset,
                    path,
                    &search_string,
                    Self::LISTABLE_RANKS
                        .map(std::string::ToString::to_string)
                        .to_vec(),
                )
                .await?
            }
            _ => vec![],
        };

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: GBIF_PROVIDER_ID,
                collection_id: collection_id.clone(),
            },
            name: "GBIF".to_owned(),
            description: "GBIF occurrence datasets".to_owned(),
            items,
            entry_label: None,
            properties: vec![],
        })
    }

    async fn autocomplete_search(
        &self,
        collection_id: &LayerCollectionId,
        search: SearchParameters,
    ) -> Result<Vec<String>> {
        let selector = collection_id.0.split_once('/').map_or_else(
            || collection_id.clone().0,
            |(selector, _)| selector.to_string(),
        );
        let path = collection_id
            .0
            .split_once('/')
            .map_or_else(|| "", |(_, path)| path);

        let search_string = match search.search_type {
            SearchType::Fulltext => format!("%{}%", search.search_string),
            SearchType::Prefix => format!("{}%", search.search_string),
        };

        let items = match selector.as_str() {
            "datasets" => {
                let taxonrank = path
                    .split_once('/')
                    .map_or_else(String::new, |(taxonrank, _)| taxonrank.to_string());
                ensure!(
                    Self::LISTABLE_RANKS.contains(&taxonrank.as_str()),
                    crate::error::InvalidPath
                );
                self.get_datasets_autocomplete_items(
                    &search.limit,
                    &search.offset,
                    path,
                    &search_string,
                    vec![taxonrank],
                )
                .await?
            }
            "filter" => {
                self.get_filter_autocomplete_items(
                    &search.limit,
                    &search.offset,
                    path,
                    &search_string,
                )
                .await?
            }
            "select" => {
                self.get_datasets_autocomplete_items(
                    &search.limit,
                    &search.offset,
                    path,
                    &search_string,
                    Self::LISTABLE_RANKS
                        .map(std::string::ToString::to_string)
                        .to_vec(),
                )
                .await?
            }
            _ => vec![],
        };

        Ok(items)
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for GbifDataProvider
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
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

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for GbifDataProvider
{
    #[allow(clippy::too_many_lines)]
    async fn meta_data(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
    > {
        let key = id
            .external()
            .ok_or(Error::InvalidDataId)
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?
            .layer_id
            .0;

        let (taxonrank, canonicalname) =
            key.split_once('/')
                .ok_or(Error::InvalidLayerId)
                .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                })?;

        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Bb8Postgres { source: e })?;

        let stats = conn
            .query_one(
                &format!(
                    r"
                SELECT
                    lower(time) AS tmin,
                    upper(time) AS tmax,
                    public.ST_XMIN(extent) AS xmin,
                    public.ST_XMAX(extent) AS xmax,
                    public.ST_YMIN(extent) AS ymin,
                    public.ST_YMAX(extent) AS ymax
                FROM {schema}.{taxonrank}_stats
                WHERE name = '{canonicalname}'",
                    schema = self.db_config.schema
                ),
                &[],
            )
            .await
            .map_err(|e| Postgres { source: e })?;

        let tmin = stats.get::<usize, NaiveDateTime>(0);
        let tmax = stats.get::<usize, NaiveDateTime>(1);
        let xmin = stats.get::<usize, f64>(2);
        let xmax = stats.get::<usize, f64>(3);
        let ymin = stats.get::<usize, f64>(4);
        let ymax = stats.get::<usize, f64>(5);

        Ok(Box::new(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: self.db_config.ogr_pg_config().into(),
                layer_name: format!("{}.{}", self.db_config.schema, self.occurrence_table),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::Start {
                    start_field: "eventdate".to_string(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                },
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: String::new(),
                    y: None,
                    int: self.filter_columns(vec![
                        "gbifid".to_string(),
                        "individualcount".to_string(),
                        "day".to_string(),
                        "month".to_string(),
                        "year".to_string(),
                        "taxonkey".to_string(),
                    ]),
                    float: self.filter_columns(vec![
                        "decimallatitude".to_string(),
                        "decimallongitude".to_string(),
                        "coordinateuncertaintyinmeters".to_string(),
                        "elevation".to_string(),
                    ]),
                    text: self.filter_columns(vec![
                        "datasetkey".to_string(),
                        "occurrenceid".to_string(),
                        "kingdom".to_string(),
                        "phylum".to_string(),
                        "class".to_string(),
                        "order".to_string(),
                        "family".to_string(),
                        "genus".to_string(),
                        "species".to_string(),
                        "infraspecificepithet".to_string(),
                        "taxonrank".to_string(),
                        "scientificname".to_string(),
                        "verbatimscientificname".to_string(),
                        "verbatimscientificnameauthorship".to_string(),
                        "countrycode".to_string(),
                        "locality".to_string(),
                        "stateprovince".to_string(),
                        "occurrencestatus".to_string(),
                        "publishingorgkey".to_string(),
                        "coordinateprecision".to_string(),
                        "elevationaccuracy".to_string(),
                        "depthaccuracy".to_string(),
                        "specieskey".to_string(),
                        "basisofrecord".to_string(),
                        "institutioncode".to_string(),
                        "collectioncode".to_string(),
                        "catalognumber".to_string(),
                        "recordnumber".to_string(),
                        "identifiedby".to_string(),
                        "dateidentified".to_string(),
                        "license".to_string(),
                        "rightsholder".to_string(),
                        "recordedby".to_string(),
                        "typestatus".to_string(),
                        "establishmentmeans".to_string(),
                        "lastinterpreted".to_string(),
                        "mediatype".to_string(),
                        "issue".to_string(),
                    ]),
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: true,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: Some(format!(
                    "SELECT {} geom, eventdate FROM {}.{} WHERE {taxonrank} = '{canonicalname}'",
                    self.columns
                        .iter()
                        .map(|column| format!(r#""{column}","#))
                        .join(""),
                    self.db_config.schema,
                    self.occurrence_table
                )),
                attribute_query: None,
                cache_ttl: self.cache_ttl,
            },
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: [
                    (
                        "gbifid".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "datasetkey".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "occurrenceid".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "kingdom".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "phylum".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "class".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "order".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "family".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "genus".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "species".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "infraspecificepithet".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "taxonrank".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "scientificname".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "verbatimscientificname".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "verbatimscientificnameauthorship".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "countrycode".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "locality".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "stateprovince".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "occurrencestatus".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "individualcount".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "publishingorgkey".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "decimallatitude".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Float,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "decimallongitude".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Float,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "coordinateuncertaintyinmeters".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Float,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "coordinateprecision".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "elevation".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Float,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "elevationaccuracy".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "depthaccuracy".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "day".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "month".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "year".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "taxonkey".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "specieskey".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "basisofrecord".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "institutioncode".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "collectioncode".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "catalognumber".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "recordnumber".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "identifiedby".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "dateidentified".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "license".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "rightsholder".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "recordedby".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "typestatus".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "establishmentmeans".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "lastinterpreted".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "mediatype".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "issue".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                ]
                .into_iter()
                .filter(|(column, _)| self.columns.contains(column))
                .collect(),
                time: Some(TimeInterval::new(
                    tmin.and_utc().timestamp_millis(),
                    tmax.and_utc().timestamp_millis(),
                )?),
                bbox: Some(BoundingBox2D::new(
                    Coordinate2D::new(xmin, ymin),
                    Coordinate2D::new(xmax, ymax),
                )?),
            },
            phantom: Default::default(),
        }))
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for GbifDataProvider
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[async_trait]
impl DataProvider for GbifDataProvider {
    async fn provenance(&self, id: &DataId) -> Result<ProvenanceOutput> {
        let key = id
            .external()
            .ok_or(Error::InvalidDataId)
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?
            .layer_id
            .0;

        let (taxonrank, canonicalname) =
            key.split_once('/')
                .ok_or(Error::InvalidLayerId)
                .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(e),
                })?;

        if !GbifDataProvider::LEVELS.contains(&taxonrank) && !taxonrank.eq("species") {
            return Err(Error::InvalidDataId);
        }

        let conn = self.pool.get().await?;
        let query = &format!(
            r#"
            SELECT citation, license, uri
            FROM (
                {schema}.gbif_datasets
                JOIN
                (
                    SELECT DISTINCT datasetkey
                    FROM {schema}.occurrences
                    WHERE "{taxonrank}" = '{canonicalname}'
                ) AS keys ON "key" = datasetkey
            );
            "#,
            schema = self.db_config.schema
        );
        let stmt = conn.prepare(query).await?;

        let rows = conn.query(&stmt, &[]).await?;

        let provenance = rows
            .into_iter()
            .map(|row| {
                let citation = row.get::<usize, String>(0);
                let license = row.get::<usize, String>(1);
                let uri = row.get(2);
                Provenance {
                    citation,
                    license,
                    uri,
                }
            })
            .collect();

        Ok(ProvenanceOutput {
            data: id.clone(),
            provenance: Some(provenance),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::PostgresSessionContext;
    use crate::contexts::SessionContext;
    use crate::ge_context;
    use crate::layers::layer::Layer;
    use crate::layers::layer::ProviderLayerCollectionId;
    use crate::test_data;
    use bb8_postgres::bb8::ManageConnection;
    use futures::StreamExt;
    use geoengine_datatypes::collections::{ChunksEqualIgnoringCacheHint, MultiPointCollection};
    use geoengine_datatypes::dataset::ExternalDataId;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, CacheHint, FeatureData, MultiPoint, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::primitives::{ColumnSelection, TimeInstance};
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::QueryProcessor;
    use geoengine_operators::{engine::MockQueryContext, source::OgrSourceProcessor};
    use std::collections::HashMap;
    use std::{fs::File, io::Read, path::PathBuf};

    /// Add test data to the database
    async fn add_test_data(db_config: &DatabaseConnectionConfig) {
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);
        let conn = pg_mgr.connect().await.unwrap();

        let mut sql = String::new();
        File::open(test_data!("gbif/test_data.sql"))
            .unwrap()
            .read_to_string(&mut sql)
            .unwrap();

        let schema = db_config.schema.clone();

        conn.batch_execute(&format!(
            "CREATE EXTENSION IF NOT EXISTS postgis;
            SET SEARCH_PATH TO {schema}, public;
            {sql}"
        ))
        .await
        .unwrap();
    }

    #[ge_context::test]
    async fn it_lists_select_items(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let root_id = provider.get_root_layer_collection_id().await.unwrap();

        let collection = provider
            .load_layer_collection(
                &root_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                },
            )
            .await;

        let collection = collection.unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: root_id,
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![
                    CollectionItem::Collection(LayerCollectionListing {
                        r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: GBIF_PROVIDER_ID,
                            collection_id: LayerCollectionId("filter/".to_string()),
                        },
                        name: "Select kingdom".to_string(),
                        description: "Refine the current filter".to_string(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing {
                        r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: GBIF_PROVIDER_ID,
                            collection_id: LayerCollectionId("datasets/family/".to_string()),
                        },
                        name: "Family datasets".to_string(),
                        description: "Apply the current filter".to_string(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing {
                        r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: GBIF_PROVIDER_ID,
                            collection_id: LayerCollectionId("datasets/genus/".to_string()),
                        },
                        name: "Genus datasets".to_string(),
                        description: "Apply the current filter".to_string(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing {
                        r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: GBIF_PROVIDER_ID,
                            collection_id: LayerCollectionId("datasets/species/".to_string()),
                        },
                        name: "Species datasets".to_string(),
                        description: "Apply the current filter".to_string(),
                        properties: Default::default(),
                    })
                ],
                entry_label: None,
                properties: vec![],
            }
        );
    }

    #[ge_context::test]
    async fn it_lists_select_items_filtered(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let root_id = LayerCollectionId("select/Animalia/Chordata".to_string());

        let collection = provider
            .load_layer_collection(
                &root_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                },
            )
            .await;

        let collection = collection.unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: root_id,
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![
                    CollectionItem::Collection(LayerCollectionListing {
                        r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: GBIF_PROVIDER_ID,
                            collection_id: LayerCollectionId(
                                "filter/Animalia/Chordata".to_string()
                            ),
                        },
                        name: "Select class".to_string(),
                        description: "Refine the current filter".to_string(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing {
                        r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: GBIF_PROVIDER_ID,
                            collection_id: LayerCollectionId(
                                "datasets/family/Animalia/Chordata".to_string()
                            ),
                        },
                        name: "Family datasets".to_string(),
                        description: "Apply the current filter".to_string(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing {
                        r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: GBIF_PROVIDER_ID,
                            collection_id: LayerCollectionId(
                                "datasets/genus/Animalia/Chordata".to_string()
                            ),
                        },
                        name: "Genus datasets".to_string(),
                        description: "Apply the current filter".to_string(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing {
                        r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: GBIF_PROVIDER_ID,
                            collection_id: LayerCollectionId(
                                "datasets/species/Animalia/Chordata".to_string()
                            ),
                        },
                        name: "Species datasets".to_string(),
                        description: "Apply the current filter".to_string(),
                        properties: Default::default(),
                    })
                ],
                entry_label: None,
                properties: vec![],
            }
        );
    }

    #[ge_context::test]
    async fn it_lists_correct_select_items(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let mut collections = vec![];

        for levels in 0..7 {
            let mut id = "select/".to_string();
            for level in 0..levels {
                if level > 0 {
                    id += "/";
                }
                id += "-";
            }
            let id = LayerCollectionId(id);

            let collection = provider
                .load_layer_collection(
                    &id,
                    LayerCollectionListOptions {
                        offset: 0,
                        limit: 10,
                    },
                )
                .await;

            collections.push(
                collection
                    .unwrap()
                    .items
                    .iter()
                    .map(|item| item.name().to_string())
                    .collect::<Vec<_>>(),
            );
        }

        assert_eq!(
            collections,
            vec![
                vec![
                    "Select kingdom",
                    "Family datasets",
                    "Genus datasets",
                    "Species datasets",
                ],
                vec![
                    "Select phylum",
                    "Family datasets",
                    "Genus datasets",
                    "Species datasets",
                ],
                vec![
                    "Select class",
                    "Family datasets",
                    "Genus datasets",
                    "Species datasets",
                ],
                vec![
                    "Select order",
                    "Family datasets",
                    "Genus datasets",
                    "Species datasets",
                ],
                vec![
                    "Select family",
                    "Family datasets",
                    "Genus datasets",
                    "Species datasets",
                ],
                vec!["Select genus", "Genus datasets", "Species datasets",],
                vec!["Species datasets",],
            ]
        );
    }

    #[ge_context::test]
    async fn it_lists_result_items(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        add_test_data(&db_config).await;

        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let invalid_root_id = LayerCollectionId("datasets/families/".to_string());

        let collection = provider
            .load_layer_collection(
                &invalid_root_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                },
            )
            .await;

        assert!(collection.is_err());
        matches!(collection, Err(Error::InvalidPath));

        let root_id = LayerCollectionId("datasets/family/".to_string());

        let collection = provider
            .load_layer_collection(
                &root_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                },
            )
            .await;

        let collection = collection.unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: root_id,
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![CollectionItem::Layer(LayerListing {
                    r#type: Default::default(),
                    id: ProviderLayerId {
                        provider_id: GBIF_PROVIDER_ID,
                        layer_id: LayerId("family/Limoniidae".to_string()),
                    },
                    name: "Limoniidae".to_string(),
                    description: "3 occurrences".to_string(),
                    properties: vec![]
                }),],
                entry_label: None,
                properties: vec![],
            }
        );
    }

    #[ge_context::test]
    async fn it_lists_result_items_filtered(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        add_test_data(&db_config).await;

        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let root_id = LayerCollectionId("datasets/family/Plantae/".to_string());

        let collection = provider
            .load_layer_collection(
                &root_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                },
            )
            .await;

        let collection = collection.unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: root_id,
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![],
                entry_label: None,
                properties: vec![],
            }
        );
    }

    #[ge_context::test]
    async fn it_lists_filter_items(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        add_test_data(&db_config).await;

        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let layer_collection_id = LayerCollectionId("filter/".to_string());

        let collection = provider
            .load_layer_collection(
                &layer_collection_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                },
            )
            .await;

        let collection = collection.unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: layer_collection_id,
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![CollectionItem::Collection(LayerCollectionListing {
                    r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: GBIF_PROVIDER_ID,
                        collection_id: LayerCollectionId("select/Animalia".to_string()),
                    },
                    name: "Animalia".to_string(),
                    description: String::new(),
                    properties: Default::default(),
                })],
                entry_label: None,
                properties: vec![],
            }
        );
    }

    #[ge_context::test]
    async fn it_lists_filter_items_filtered(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        add_test_data(&db_config).await;

        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let layer_collection_id = LayerCollectionId("filter/Plantae".to_string());

        let collection = provider
            .load_layer_collection(
                &layer_collection_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                },
            )
            .await;

        let collection = collection.unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: layer_collection_id,
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![],
                entry_label: None,
                properties: vec![],
            }
        );
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_creates_meta_data(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        async fn test(
            ctx: PostgresSessionContext<NoTls>,
            db_config: DatabaseConnectionConfig,
        ) -> Result<(), String> {
            let ogr_pg_string = db_config.ogr_pg_config();

            let provider = Box::new(GbifDataProviderDefinition {
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                priority: Some(12),
                db_config: db_config.clone(),
                cache_ttl: Default::default(),
                autocomplete_timeout: 5,
                columns: GbifDataProvider::all_columns(),
            })
            .initialize(ctx.db())
            .await
            .map_err(|e| e.to_string())?;

            let meta: Box<
                dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
            > = provider
                .meta_data(&DataId::External(ExternalDataId {
                    provider_id: GBIF_PROVIDER_ID,
                    layer_id: LayerId("species/Rhipidia willistoniana".to_string()),
                }))
                .await
                .map_err(|e| e.to_string())?;

            let int_column = VectorColumnInfo {
                data_type: FeatureDataType::Int,
                measurement: Measurement::Unitless,
            };
            let float_column = VectorColumnInfo {
                data_type: FeatureDataType::Float,
                measurement: Measurement::Unitless,
            };
            let text_column = VectorColumnInfo {
                data_type: FeatureDataType::Text,
                measurement: Measurement::Unitless,
            };

            let expected = VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: HashMap::from([
                    ("gbifid".to_string(), int_column.clone()),
                    ("individualcount".to_string(), int_column.clone()),
                    ("day".to_string(), int_column.clone()),
                    ("month".to_string(), int_column.clone()),
                    ("year".to_string(), int_column.clone()),
                    ("taxonkey".to_string(), int_column.clone()),
                    ("decimallatitude".to_string(), float_column.clone()),
                    ("decimallongitude".to_string(), float_column.clone()),
                    (
                        "coordinateuncertaintyinmeters".to_string(),
                        float_column.clone(),
                    ),
                    ("elevation".to_string(), float_column.clone()),
                    ("datasetkey".to_string(), text_column.clone()),
                    ("occurrenceid".to_string(), text_column.clone()),
                    ("kingdom".to_string(), text_column.clone()),
                    ("phylum".to_string(), text_column.clone()),
                    ("class".to_string(), text_column.clone()),
                    ("order".to_string(), text_column.clone()),
                    ("family".to_string(), text_column.clone()),
                    ("genus".to_string(), text_column.clone()),
                    ("species".to_string(), text_column.clone()),
                    ("infraspecificepithet".to_string(), text_column.clone()),
                    ("taxonrank".to_string(), text_column.clone()),
                    ("scientificname".to_string(), text_column.clone()),
                    ("verbatimscientificname".to_string(), text_column.clone()),
                    (
                        "verbatimscientificnameauthorship".to_string(),
                        text_column.clone(),
                    ),
                    ("countrycode".to_string(), text_column.clone()),
                    ("locality".to_string(), text_column.clone()),
                    ("stateprovince".to_string(), text_column.clone()),
                    ("occurrencestatus".to_string(), text_column.clone()),
                    ("publishingorgkey".to_string(), text_column.clone()),
                    ("coordinateprecision".to_string(), text_column.clone()),
                    ("elevationaccuracy".to_string(), text_column.clone()),
                    ("depthaccuracy".to_string(), text_column.clone()),
                    ("specieskey".to_string(), text_column.clone()),
                    ("basisofrecord".to_string(), text_column.clone()),
                    ("institutioncode".to_string(), text_column.clone()),
                    ("collectioncode".to_string(), text_column.clone()),
                    ("catalognumber".to_string(), text_column.clone()),
                    ("recordnumber".to_string(), text_column.clone()),
                    ("identifiedby".to_string(), text_column.clone()),
                    ("dateidentified".to_string(), text_column.clone()),
                    ("license".to_string(), text_column.clone()),
                    ("rightsholder".to_string(), text_column.clone()),
                    ("recordedby".to_string(), text_column.clone()),
                    ("typestatus".to_string(), text_column.clone()),
                    ("establishmentmeans".to_string(), text_column.clone()),
                    ("lastinterpreted".to_string(), text_column.clone()),
                    ("mediatype".to_string(), text_column.clone()),
                    ("issue".to_string(), text_column.clone()),
                ]),
                time: Some(TimeInterval::new(1_517_011_200_000, 1_517_443_200_000).unwrap()),
                #[allow(clippy::excessive_precision)]
                bbox: Some(
                    BoundingBox2D::new(
                        Coordinate2D::new(-61.114_469_999_999_997, 14.679_15),
                        Coordinate2D::new(-61.065_219_999_999_997, 14.775_33),
                    )
                    .unwrap(),
                ),
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
                    attributes: ColumnSelection::all(),
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
                layer_name: format!("{0}.occurrences", db_config.schema),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::Start {
                    start_field: "eventdate".to_string(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                },
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: String::new(),
                    y: None,
                    int: vec![
                        "gbifid".to_string(),
                        "individualcount".to_string(),
                        "day".to_string(),
                        "month".to_string(),
                        "year".to_string(),
                        "taxonkey".to_string(),
                    ],
                    float: vec![
                        "decimallatitude".to_string(),
                        "decimallongitude".to_string(),
                        "coordinateuncertaintyinmeters".to_string(),
                        "elevation".to_string(),
                    ],
                    text: vec![
                        "basisofrecord".to_string(),
                        "catalognumber".to_string(),
                        "class".to_string(),
                        "collectioncode".to_string(),
                        "coordinateprecision".to_string(),
                        "countrycode".to_string(),
                        "datasetkey".to_string(),
                        "dateidentified".to_string(),
                        "depthaccuracy".to_string(),
                        "elevationaccuracy".to_string(),
                        "establishmentmeans".to_string(),
                        "family".to_string(),
                        "genus".to_string(),
                        "identifiedby".to_string(),
                        "infraspecificepithet".to_string(),
                        "institutioncode".to_string(),
                        "issue".to_string(),
                        "kingdom".to_string(),
                        "lastinterpreted".to_string(),
                        "license".to_string(),
                        "locality".to_string(),
                        "mediatype".to_string(),
                        "occurrenceid".to_string(),
                        "occurrencestatus".to_string(),
                        "order".to_string(),
                        "phylum".to_string(),
                        "publishingorgkey".to_string(),
                        "recordedby".to_string(),
                        "recordnumber".to_string(),
                        "rightsholder".to_string(),
                        "scientificname".to_string(),
                        "species".to_string(),
                        "specieskey".to_string(),
                        "stateprovince".to_string(),
                        "taxonrank".to_string(),
                        "typestatus".to_string(),
                        "verbatimscientificname".to_string(),
                        "verbatimscientificnameauthorship".to_string(),
                    ],
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: true,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: Some(format!(
                    "SELECT {} geom, eventdate FROM {}.occurrences WHERE species = 'Rhipidia willistoniana'",
                    GbifDataProvider::all_columns()
                        .iter()
                        .map(|column| format!(r#""{column}","#))
                        .join(""),
                    db_config.schema
                )),
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            };

            if loading_info != expected {
                return Err(format!("{result_descriptor:?} != {expected:?}"));
            }

            Ok(())
        }

        add_test_data(&db_config).await;

        let test = test(ctx, db_config).await;

        assert!(test.is_ok());
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_creates_meta_data_for_selected_columns(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        async fn test(
            ctx: PostgresSessionContext<NoTls>,
            db_config: DatabaseConnectionConfig,
        ) -> Result<(), String> {
            let ogr_pg_string = db_config.ogr_pg_config();

            let provider = Box::new(GbifDataProviderDefinition {
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                priority: Some(12),
                db_config: db_config.clone(),
                cache_ttl: Default::default(),
                autocomplete_timeout: 5,
                columns: vec![
                    "gbifid".to_string(),
                    "scientificname".to_string(),
                    "elevation".to_string(),
                    "nonexistent".to_string(),
                ],
            })
            .initialize(ctx.db())
            .await
            .map_err(|e| e.to_string())?;

            let meta: Box<
                dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
            > = provider
                .meta_data(&DataId::External(ExternalDataId {
                    provider_id: GBIF_PROVIDER_ID,
                    layer_id: LayerId("species/Rhipidia willistoniana".to_string()),
                }))
                .await
                .map_err(|e| e.to_string())?;

            let int_column = VectorColumnInfo {
                data_type: FeatureDataType::Int,
                measurement: Measurement::Unitless,
            };
            let float_column = VectorColumnInfo {
                data_type: FeatureDataType::Float,
                measurement: Measurement::Unitless,
            };
            let text_column = VectorColumnInfo {
                data_type: FeatureDataType::Text,
                measurement: Measurement::Unitless,
            };

            let expected = VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: HashMap::from([
                    ("gbifid".to_string(), int_column.clone()),
                    ("elevation".to_string(), float_column.clone()),
                    ("scientificname".to_string(), text_column.clone()),
                ]),
                time: Some(TimeInterval::new(1_517_011_200_000, 1_517_443_200_000).unwrap()),
                #[allow(clippy::excessive_precision)]
                bbox: Some(
                    BoundingBox2D::new(
                        Coordinate2D::new(-61.114_469_999_999_997, 14.679_15),
                        Coordinate2D::new(-61.065_219_999_999_997, 14.775_33),
                    )
                    .unwrap(),
                ),
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
                    attributes: ColumnSelection::all(),
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
                layer_name: format!("{0}.occurrences", db_config.schema),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::Start {
                    start_field: "eventdate".to_string(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                },
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: String::new(),
                    y: None,
                    int: vec!["gbifid".to_string()],
                    float: vec!["elevation".to_string()],
                    text: vec!["scientificname".to_string()],
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: true,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: Some(format!(
                    r#"SELECT "gbifid","scientificname","elevation", geom, eventdate FROM {}.occurrences WHERE species = 'Rhipidia willistoniana'"#,
                    db_config.schema
                )),
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            };

            if loading_info != expected {
                return Err(format!("{result_descriptor:?} != {expected:?}"));
            }

            Ok(())
        }

        add_test_data(&db_config).await;

        let test = test(ctx, db_config).await;

        assert!(test.is_ok());
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_creates_meta_data_for_lite_subset_selected_columns(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        async fn test(
            ctx: PostgresSessionContext<NoTls>,
            db_config: DatabaseConnectionConfig,
        ) -> Result<(), String> {
            let ogr_pg_string = db_config.ogr_pg_config();

            let provider = Box::new(GbifDataProviderDefinition {
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                priority: Some(12),
                db_config: db_config.clone(),
                cache_ttl: Default::default(),
                autocomplete_timeout: 5,
                columns: vec![
                    "gbifid".to_string(),
                    "scientificname".to_string(),
                    "nonexistent".to_string(),
                ],
            })
            .initialize(ctx.db())
            .await
            .map_err(|e| e.to_string())?;

            let meta: Box<
                dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
            > = provider
                .meta_data(&DataId::External(ExternalDataId {
                    provider_id: GBIF_PROVIDER_ID,
                    layer_id: LayerId("species/Rhipidia willistoniana".to_string()),
                }))
                .await
                .map_err(|e| e.to_string())?;

            let int_column = VectorColumnInfo {
                data_type: FeatureDataType::Int,
                measurement: Measurement::Unitless,
            };
            let text_column = VectorColumnInfo {
                data_type: FeatureDataType::Text,
                measurement: Measurement::Unitless,
            };

            let expected = VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: HashMap::from([
                    ("gbifid".to_string(), int_column.clone()),
                    ("scientificname".to_string(), text_column.clone()),
                ]),
                time: Some(TimeInterval::new(1_517_011_200_000, 1_517_443_200_000).unwrap()),
                #[allow(clippy::excessive_precision)]
                bbox: Some(
                    BoundingBox2D::new(
                        Coordinate2D::new(-61.114_469_999_999_997, 14.679_15),
                        Coordinate2D::new(-61.065_219_999_999_997, 14.775_33),
                    )
                    .unwrap(),
                ),
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
                    attributes: ColumnSelection::all(),
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
                layer_name: format!("{0}.occurrences_lite", db_config.schema),
                data_type: Some(VectorDataType::MultiPoint),
                time: OgrSourceDatasetTimeType::Start {
                    start_field: "eventdate".to_string(),
                    start_format: OgrSourceTimeFormat::Auto,
                    duration: OgrSourceDurationSpec::Zero,
                },
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: String::new(),
                    y: None,
                    int: vec!["gbifid".to_string()],
                    float: vec![],
                    text: vec!["scientificname".to_string()],
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: true,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: Some(format!(
                    r#"SELECT "gbifid","scientificname", geom, eventdate FROM {}.occurrences_lite WHERE species = 'Rhipidia willistoniana'"#,
                    db_config.schema
                )),
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            };

            if loading_info != expected {
                return Err(format!("{result_descriptor:?} != {expected:?}"));
            }

            Ok(())
        }

        add_test_data(&db_config).await;

        let test = test(ctx, db_config).await;

        assert!(test.is_ok());
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_loads(db_config: DatabaseConnectionConfig, ctx: PostgresSessionContext<NoTls>) {
        async fn test(
            ctx: PostgresSessionContext<NoTls>,
            db_config: DatabaseConnectionConfig,
        ) -> Result<(), String> {
            let provider = Box::new(GbifDataProviderDefinition {
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                priority: Some(12),
                db_config,
                cache_ttl: Default::default(),
                autocomplete_timeout: 5,
                columns: GbifDataProvider::all_columns(),
            })
            .initialize(ctx.db())
            .await
            .map_err(|e| e.to_string())?;

            let meta: Box<
                dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
            > = provider
                .meta_data(&DataId::External(ExternalDataId {
                    provider_id: GBIF_PROVIDER_ID,
                    layer_id: LayerId("species/Rhipidia willistoniana".to_string()),
                }))
                .await
                .map_err(|e| e.to_string())?;

            let processor: OgrSourceProcessor<MultiPoint> = OgrSourceProcessor::new(
                VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: HashMap::new(),
                    time: None,
                    bbox: None,
                },
                meta,
                vec![],
            );

            let query_rectangle = VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    (-61.065_22, 14.775_33).into(),
                    (-61.065_22, 14.775_33).into(),
                )
                .unwrap(),
                time_interval: TimeInterval::default(),
                spatial_resolution: SpatialResolution::zero_point_one(),
                attributes: ColumnSelection::all(),
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
                MultiPoint::many(vec![(-61.065_22, 14.775_33), (-61.065_22, 14.775_33)]).unwrap(),
                vec![TimeInterval::new_instant(1_517_011_200_000).unwrap(); 2],
                [
                    (
                        "gbifid".to_string(),
                        FeatureData::NullableInt(vec![Some(4_021_925_301), Some(4_021_925_303)]),
                    ),
                    (
                        "individualcount".to_string(),
                        FeatureData::NullableInt(vec![Some(156), Some(213)]),
                    ),
                    (
                        "day".to_string(),
                        FeatureData::NullableInt(vec![Some(27), Some(27)]),
                    ),
                    (
                        "month".to_string(),
                        FeatureData::NullableInt(vec![Some(1), Some(1)]),
                    ),
                    (
                        "year".to_string(),
                        FeatureData::NullableInt(vec![Some(2018), Some(2018)]),
                    ),
                    (
                        "taxonkey".to_string(),
                        FeatureData::NullableInt(vec![Some(5_066_840), Some(5_066_840)]),
                    ),
                    (
                        "decimallatitude".to_string(),
                        FeatureData::NullableFloat(vec![Some(14.775_33), Some(14.775_33)]),
                    ),
                    (
                        "decimallongitude".to_string(),
                        FeatureData::NullableFloat(vec![Some(-61.065_22), Some(-61.065_22)]),
                    ),
                    (
                        "coordinateuncertaintyinmeters".to_string(),
                        FeatureData::NullableFloat(vec![Some(30.0), Some(30.0)]),
                    ),
                    (
                        "elevation".to_string(),
                        FeatureData::NullableFloat(vec![None, None]),
                    ),
                    (
                        "basisofrecord".to_string(),
                        FeatureData::NullableText(vec![
                            Some("PRESERVED_SPECIMEN".to_string()),
                            Some("PRESERVED_SPECIMEN".to_string()),
                        ]),
                    ),
                    (
                        "catalognumber".to_string(),
                        FeatureData::NullableText(vec![None, None]),
                    ),
                    (
                        "class".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Insecta".to_string()),
                            Some("Insecta".to_string()),
                        ]),
                    ),
                    (
                        "collectioncode".to_string(),
                        FeatureData::NullableText(vec![None, None]),
                    ),
                    (
                        "coordinateprecision".to_string(),
                        FeatureData::NullableText(vec![None, None]),
                    ),
                    (
                        "countrycode".to_string(),
                        FeatureData::NullableText(vec![
                            Some("MQ".to_string()),
                            Some("MQ".to_string()),
                        ]),
                    ),
                    (
                        "datasetkey".to_string(),
                        FeatureData::NullableText(vec![
                            Some("92827b65-9987-4479-b135-7ec1bf9cf3d1".to_string()),
                            Some("92827b65-9987-4479-b135-7ec1bf9cf3d1".to_string()),
                        ]),
                    ),
                    (
                        "dateidentified".to_string(),
                        FeatureData::NullableText(vec![None, None]),
                    ),
                    (
                        "depthaccuracy".to_string(),
                        FeatureData::NullableText(vec![None, None]),
                    ),
                    (
                        "elevationaccuracy".to_string(),
                        FeatureData::NullableText(vec![None, None]),
                    ),
                    (
                        "establishmentmeans".to_string(),
                        FeatureData::NullableText(vec![None, None]),
                    ),
                    (
                        "family".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Limoniidae".to_string()),
                            Some("Limoniidae".to_string()),
                        ]),
                    ),
                    (
                        "genus".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Rhipidia".to_string()),
                            Some("Rhipidia".to_string()),
                        ]),
                    ),
                    (
                        "identifiedby".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Jorge Mederos".to_string()),
                            Some("Jorge Mederos".to_string()),
                        ]),
                    ),
                    (
                        "infraspecificepithet".to_string(),
                        FeatureData::NullableText(vec![None, None]),
                    ),
                    (
                        "institutioncode".to_string(),
                        FeatureData::NullableText(vec![None, None]),
                    ),
                    (
                        "issue".to_string(),
                        FeatureData::NullableText(vec![None, None]),
                    ),
                    (
                        "kingdom".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Animalia".to_string()),
                            Some("Animalia".to_string()),
                        ]),
                    ),
                    (
                        "lastinterpreted".to_string(),
                        FeatureData::NullableText(vec![
                            Some("2023-01-31 08:47:46 +00:00".to_string()),
                            Some("2023-01-31 08:47:46 +00:00".to_string()),
                        ]),
                    ),
                    (
                        "license".to_string(),
                        FeatureData::NullableText(vec![
                            Some("CC0_1_0".to_string()),
                            Some("CC0_1_0".to_string()),
                        ]),
                    ),
                    (
                        "locality".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Rivière Sylvestre (Le Lorrain)".to_string()),
                            Some("Rivière Sylvestre (Le Lorrain)".to_string()),
                        ]),
                    ),
                    (
                        "mediatype".to_string(),
                        FeatureData::NullableText(vec![None, None]),
                    ),
                    (
                        "occurrenceid".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Mart:tipu:13801".to_string()),
                            Some("Mart:tipu:13803".to_string()),
                        ]),
                    ),
                    (
                        "occurrencestatus".to_string(),
                        FeatureData::NullableText(vec![
                            Some("PRESENT".to_string()),
                            Some("PRESENT".to_string()),
                        ]),
                    ),
                    (
                        "order".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Diptera".to_string()),
                            Some("Diptera".to_string()),
                        ]),
                    ),
                    (
                        "phylum".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Arthropoda".to_string()),
                            Some("Arthropoda".to_string()),
                        ]),
                    ),
                    (
                        "publishingorgkey".to_string(),
                        FeatureData::NullableText(vec![
                            Some("1cd669d0-80ea-11de-a9d0-f1765f95f18b".to_string()),
                            Some("1cd669d0-80ea-11de-a9d0-f1765f95f18b".to_string()),
                        ]),
                    ),
                    (
                        "recordedby".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Marc Pollet".to_string()),
                            Some("Marc Pollet".to_string()),
                        ]),
                    ),
                    (
                        "recordnumber".to_string(),
                        FeatureData::NullableText(vec![None, None]),
                    ),
                    (
                        "rightsholder".to_string(),
                        FeatureData::NullableText(vec![
                            Some("dataset authors".to_string()),
                            Some("dataset authors".to_string()),
                        ]),
                    ),
                    (
                        "scientificname".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Rhipidia willistoniana (Alexander, 1929)".to_string()),
                            Some("Rhipidia willistoniana (Alexander, 1929)".to_string()),
                        ]),
                    ),
                    (
                        "species".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Rhipidia willistoniana".to_string()),
                            Some("Rhipidia willistoniana".to_string()),
                        ]),
                    ),
                    (
                        "specieskey".to_string(),
                        FeatureData::NullableText(vec![
                            Some("5066840".to_string()),
                            Some("5066840".to_string()),
                        ]),
                    ),
                    (
                        "stateprovince".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Martinique".to_string()),
                            Some("Martinique".to_string()),
                        ]),
                    ),
                    (
                        "taxonrank".to_string(),
                        FeatureData::NullableText(vec![
                            Some("SPECIES".to_string()),
                            Some("SPECIES".to_string()),
                        ]),
                    ),
                    (
                        "typestatus".to_string(),
                        FeatureData::NullableText(vec![None, None]),
                    ),
                    (
                        "verbatimscientificname".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Rhipidia (Rhipidia) willistoniana (Alexander, 1929)".to_string()),
                            Some("Rhipidia (Rhipidia) willistoniana (Alexander, 1929)".to_string()),
                        ]),
                    ),
                    (
                        "verbatimscientificnameauthorship".to_string(),
                        FeatureData::NullableText(vec![None, None]),
                    ),
                ]
                .iter()
                .cloned()
                .collect(),
                CacheHint::default(),
            )
            .unwrap();

            if !result.chunks_equal_ignoring_cache_hint(&expected) {
                return Err(format!("{result:?} != {expected:?}"));
            }

            Ok(())
        }

        add_test_data(&db_config).await;

        let result = test(ctx, db_config).await;

        assert!(result.is_ok());
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_loads_for_selected_columns(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        async fn test(
            ctx: PostgresSessionContext<NoTls>,
            db_config: DatabaseConnectionConfig,
        ) -> Result<(), String> {
            let provider = Box::new(GbifDataProviderDefinition {
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                priority: Some(12),
                db_config,
                cache_ttl: Default::default(),
                autocomplete_timeout: 5,
                columns: vec![
                    "gbifid".to_string(),
                    "scientificname".to_string(),
                    "elevation".to_string(),
                    "nonexistent".to_string(),
                ],
            })
            .initialize(ctx.db())
            .await
            .map_err(|e| e.to_string())?;

            let meta: Box<
                dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
            > = provider
                .meta_data(&DataId::External(ExternalDataId {
                    provider_id: GBIF_PROVIDER_ID,
                    layer_id: LayerId("species/Rhipidia willistoniana".to_string()),
                }))
                .await
                .map_err(|e| e.to_string())?;

            let processor: OgrSourceProcessor<MultiPoint> = OgrSourceProcessor::new(
                VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: HashMap::new(),
                    time: None,
                    bbox: None,
                },
                meta,
                vec![],
            );

            let query_rectangle = VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    (-61.065_22, 14.775_33).into(),
                    (-61.065_22, 14.775_33).into(),
                )
                .unwrap(),
                time_interval: TimeInterval::default(),
                spatial_resolution: SpatialResolution::zero_point_one(),
                attributes: ColumnSelection::all(),
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
                MultiPoint::many(vec![(-61.065_22, 14.775_33), (-61.065_22, 14.775_33)]).unwrap(),
                vec![TimeInterval::new_instant(1_517_011_200_000).unwrap(); 2],
                [
                    (
                        "gbifid".to_string(),
                        FeatureData::NullableInt(vec![Some(4_021_925_301), Some(4_021_925_303)]),
                    ),
                    (
                        "elevation".to_string(),
                        FeatureData::NullableFloat(vec![None, None]),
                    ),
                    (
                        "scientificname".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Rhipidia willistoniana (Alexander, 1929)".to_string()),
                            Some("Rhipidia willistoniana (Alexander, 1929)".to_string()),
                        ]),
                    ),
                ]
                .iter()
                .cloned()
                .collect(),
                CacheHint::default(),
            )
            .unwrap();

            if !result.chunks_equal_ignoring_cache_hint(&expected) {
                return Err(format!("{result:?} != {expected:?}"));
            }

            Ok(())
        }

        add_test_data(&db_config).await;

        let result = test(ctx, db_config).await;

        assert!(result.is_ok());
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_loads_for_lite_subset_selected_columns(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        async fn test(
            ctx: PostgresSessionContext<NoTls>,
            db_config: DatabaseConnectionConfig,
        ) -> Result<(), String> {
            let provider = Box::new(GbifDataProviderDefinition {
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                priority: Some(12),
                db_config,
                cache_ttl: Default::default(),
                autocomplete_timeout: 5,
                columns: vec![
                    "gbifid".to_string(),
                    "scientificname".to_string(),
                    "nonexistent".to_string(),
                ],
            })
            .initialize(ctx.db())
            .await
            .map_err(|e| e.to_string())?;

            let meta: Box<
                dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
            > = provider
                .meta_data(&DataId::External(ExternalDataId {
                    provider_id: GBIF_PROVIDER_ID,
                    layer_id: LayerId("species/Rhipidia willistoniana".to_string()),
                }))
                .await
                .map_err(|e| e.to_string())?;

            let processor: OgrSourceProcessor<MultiPoint> = OgrSourceProcessor::new(
                VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: HashMap::new(),
                    time: None,
                    bbox: None,
                },
                meta,
                vec![],
            );

            let query_rectangle = VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    (-61.065_22, 14.775_33).into(),
                    (-61.065_22, 14.775_33).into(),
                )
                .unwrap(),
                time_interval: TimeInterval::default(),
                spatial_resolution: SpatialResolution::zero_point_one(),
                attributes: ColumnSelection::all(),
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
                MultiPoint::many(vec![(-61.065_22, 14.775_33), (-61.065_22, 14.775_33)]).unwrap(),
                vec![TimeInterval::new_instant(1_517_011_200_000).unwrap(); 2],
                [
                    (
                        "gbifid".to_string(),
                        FeatureData::NullableInt(vec![Some(4_021_925_301), Some(4_021_925_303)]),
                    ),
                    (
                        "scientificname".to_string(),
                        FeatureData::NullableText(vec![
                            Some("Rhipidia willistoniana (Alexander, 1929)".to_string()),
                            Some("Rhipidia willistoniana (Alexander, 1929)".to_string()),
                        ]),
                    ),
                ]
                .iter()
                .cloned()
                .collect(),
                CacheHint::default(),
            )
            .unwrap();

            if !result.chunks_equal_ignoring_cache_hint(&expected) {
                return Err(format!("{result:?} != {expected:?}"));
            }

            Ok(())
        }

        add_test_data(&db_config).await;

        let result = test(ctx, db_config).await;

        assert!(result.is_ok());
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_loads_for_lite_subset_selected_columns_with_time_instant_filter(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        async fn test(
            ctx: PostgresSessionContext<NoTls>,
            db_config: DatabaseConnectionConfig,
        ) -> Result<(), String> {
            let provider = Box::new(GbifDataProviderDefinition {
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                priority: Some(12),
                db_config,
                cache_ttl: Default::default(),
                autocomplete_timeout: 5,
                columns: vec!["gbifid".to_string()],
            })
            .initialize(ctx.db())
            .await
            .map_err(|e| e.to_string())?;

            let meta: Box<
                dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
            > = provider
                .meta_data(&DataId::External(ExternalDataId {
                    provider_id: GBIF_PROVIDER_ID,
                    layer_id: LayerId("species/Rhipidia willistoniana".to_string()),
                }))
                .await
                .map_err(|e| e.to_string())?;

            let processor: OgrSourceProcessor<MultiPoint> = OgrSourceProcessor::new(
                VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: HashMap::new(),
                    time: None,
                    bbox: None,
                },
                meta,
                vec![],
            );

            let query_rectangle = VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                    .unwrap(),
                time_interval: TimeInterval::new_instant(1_517_011_200_000).unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
                attributes: ColumnSelection::all(),
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
                MultiPoint::many(vec![(-61.065_22, 14.775_33), (-61.065_22, 14.775_33)]).unwrap(),
                vec![TimeInterval::new_instant(1_517_011_200_000).unwrap(); 2],
                [(
                    "gbifid".to_string(),
                    FeatureData::NullableInt(vec![Some(4_021_925_301), Some(4_021_925_303)]),
                )]
                .iter()
                .cloned()
                .collect(),
                CacheHint::default(),
            )
            .unwrap();

            if !result.chunks_equal_ignoring_cache_hint(&expected) {
                return Err(format!("{result:?} != {expected:?}"));
            }

            let query_rectangle = VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                    .unwrap(),
                time_interval: TimeInterval::new_instant(1_517_443_200_000).unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
                attributes: ColumnSelection::all(),
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
                MultiPoint::many(vec![(-61.114_47, 14.679_15)]).unwrap(),
                vec![TimeInterval::new_instant(1_517_443_200_000).unwrap()],
                [(
                    "gbifid".to_string(),
                    FeatureData::NullableInt(vec![Some(4_021_925_302)]),
                )]
                .iter()
                .cloned()
                .collect(),
                CacheHint::default(),
            )
            .unwrap();

            if !result.chunks_equal_ignoring_cache_hint(&expected) {
                return Err(format!("{result:?} != {expected:?}"));
            }

            Ok(())
        }

        add_test_data(&db_config).await;

        let result = test(ctx, db_config).await;

        assert!(result.is_ok());
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_loads_for_lite_subset_selected_columns_with_time_range_filter(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        async fn test(
            ctx: PostgresSessionContext<NoTls>,
            db_config: DatabaseConnectionConfig,
        ) -> Result<(), String> {
            let provider = Box::new(GbifDataProviderDefinition {
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                priority: Some(12),
                db_config,
                cache_ttl: Default::default(),
                autocomplete_timeout: 5,
                columns: vec!["gbifid".to_string()],
            })
            .initialize(ctx.db())
            .await
            .map_err(|e| e.to_string())?;

            let meta: Box<
                dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
            > = provider
                .meta_data(&DataId::External(ExternalDataId {
                    provider_id: GBIF_PROVIDER_ID,
                    layer_id: LayerId("species/Rhipidia willistoniana".to_string()),
                }))
                .await
                .map_err(|e| e.to_string())?;

            let processor: OgrSourceProcessor<MultiPoint> = OgrSourceProcessor::new(
                VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: HashMap::new(),
                    time: None,
                    bbox: None,
                },
                meta,
                vec![],
            );

            let query_rectangle = VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                    .unwrap(),
                time_interval: TimeInterval::new(
                    TimeInstance::from_millis_unchecked(1_517_011_200_000),
                    TimeInstance::from_millis_unchecked(1_517_443_200_000),
                )
                .unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
                attributes: ColumnSelection::all(),
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
                MultiPoint::many(vec![(-61.065_22, 14.775_33), (-61.065_22, 14.775_33)]).unwrap(),
                vec![TimeInterval::new_instant(1_517_011_200_000).unwrap(); 2],
                [(
                    "gbifid".to_string(),
                    FeatureData::NullableInt(vec![Some(4_021_925_301), Some(4_021_925_303)]),
                )]
                .iter()
                .cloned()
                .collect(),
                CacheHint::default(),
            )
            .unwrap();

            if !result.chunks_equal_ignoring_cache_hint(&expected) {
                return Err(format!("{result:?} != {expected:?}"));
            }

            let query_rectangle = VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                    .unwrap(),
                time_interval: TimeInterval::new(
                    TimeInstance::from_millis_unchecked(1_517_011_200_000),
                    TimeInstance::from_millis_unchecked(1_517_443_200_001),
                )
                .unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
                attributes: ColumnSelection::all(),
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
                MultiPoint::many(vec![
                    (-61.065_22, 14.775_33),
                    (-61.114_47, 14.679_15),
                    (-61.065_22, 14.775_33),
                ])
                .unwrap(),
                vec![
                    TimeInterval::new_instant(1_517_011_200_000).unwrap(),
                    TimeInterval::new_instant(1_517_443_200_000).unwrap(),
                    TimeInterval::new_instant(1_517_011_200_000).unwrap(),
                ],
                [(
                    "gbifid".to_string(),
                    FeatureData::NullableInt(vec![
                        Some(4_021_925_301),
                        Some(4_021_925_302),
                        Some(4_021_925_303),
                    ]),
                )]
                .iter()
                .cloned()
                .collect(),
                CacheHint::default(),
            )
            .unwrap();

            if !result.chunks_equal_ignoring_cache_hint(&expected) {
                return Err(format!("{result:?} != {expected:?}"));
            }

            Ok(())
        }

        add_test_data(&db_config).await;

        let result = test(ctx, db_config).await;

        assert!(result.is_ok());
    }

    #[ge_context::test]
    async fn it_cites(db_config: DatabaseConnectionConfig, ctx: PostgresSessionContext<NoTls>) {
        async fn test(
            ctx: PostgresSessionContext<NoTls>,
            db_config: DatabaseConnectionConfig,
        ) -> Result<(), String> {
            let provider = Box::new(GbifDataProviderDefinition {
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                priority: Some(12),
                db_config,
                cache_ttl: Default::default(),
                autocomplete_timeout: 5,
                columns: GbifDataProvider::all_columns(),
            })
            .initialize(ctx.db())
            .await
            .map_err(|e| e.to_string())?;

            let dataset = DataId::External(ExternalDataId {
                provider_id: GBIF_PROVIDER_ID,
                layer_id: LayerId("species/Rhipidia willistoniana".to_owned()),
            });

            let result = provider
                .provenance(&dataset)
                .await
                .map_err(|e| e.to_string())?;

            let expected = ProvenanceOutput {
                    data: DataId::External(ExternalDataId {
                        provider_id: GBIF_PROVIDER_ID,
                        layer_id: LayerId("species/Rhipidia willistoniana".to_owned()),
                    }),
                    provenance: Some(vec![Provenance {
                        citation: "Mederos J, Pollet M, Oosterbroek P, Brosens D (2023). Tipuloidea of Martinique - 2016-2018. Version 1.10. Research Institute for Nature and Forest (INBO). Occurrence dataset https://doi.org/10.15468/s8h9pg accessed via GBIF.org on 2023-01-31.".to_owned(),
                        license: "http://creativecommons.org/publicdomain/zero/1.0/legalcode".to_owned(),
                        uri: "http://www.gbif.org/dataset/92827b65-9987-4479-b135-7ec1bf9cf3d1".to_owned(),
                    }]),
                };

            if result != expected {
                return Err(format!("{result:?} != {expected:?}"));
            }

            Ok(())
        }

        add_test_data(&db_config).await;

        let result = test(ctx, db_config).await;

        assert!(result.is_ok());
    }

    #[ge_context::test]
    async fn it_creates_layer(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        async fn test(
            ctx: PostgresSessionContext<NoTls>,
            db_config: DatabaseConnectionConfig,
        ) -> Result<(), String> {
            let provider = Box::new(GbifDataProviderDefinition {
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                priority: Some(12),
                db_config,
                cache_ttl: Default::default(),
                autocomplete_timeout: 5,
                columns: GbifDataProvider::all_columns(),
            })
            .initialize(ctx.db())
            .await
            .map_err(|e| e.to_string())?;

            let layer_id = LayerId("species/Rhipidia willistoniana".to_owned());

            let result = provider
                .load_layer(&layer_id)
                .await
                .map_err(|e| e.to_string())?;

            let expected = Layer {
                id: ProviderLayerId {
                    provider_id: GBIF_PROVIDER_ID,
                    layer_id: layer_id.clone(),
                },
                name: "Rhipidia willistoniana".to_string(),
                description: "All occurrences with a species of Rhipidia willistoniana".to_string(),
                workflow: Workflow {
                    operator: TypedOperator::Vector(
                        OgrSource {
                            params: OgrSourceParameters {
                                data: geoengine_datatypes::dataset::NamedData::with_system_provider(
                                    GBIF_PROVIDER_ID.to_string(),
                                    layer_id.to_string(),
                                ),
                                attribute_projection: None,
                                attribute_filters: None,
                            },
                        }
                        .boxed(),
                    ),
                },
                symbology: None,
                properties: vec![],
                metadata: Default::default(),
            };

            if result != expected {
                return Err(format!("{result:?} != {expected:?}"));
            }

            Ok(())
        }

        add_test_data(&db_config).await;

        let result = test(ctx, db_config).await;

        assert!(result.is_ok());
    }

    #[ge_context::test]
    async fn it_performs_global_search_on_select_items(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        add_test_data(&db_config).await;

        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let root_id = provider.get_root_layer_collection_id().await.unwrap();

        let collection = provider
            .search(
                &root_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "x".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: root_id.clone(),
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![],
                entry_label: None,
                properties: vec![],
            }
        );

        let collection = provider
            .search(
                &root_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "i".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: root_id,
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![
                    CollectionItem::Layer(LayerListing {
                        r#type: Default::default(),
                        id: ProviderLayerId {
                            provider_id: GBIF_PROVIDER_ID,
                            layer_id: LayerId("family/Limoniidae".to_string()),
                        },
                        name: "Limoniidae".to_string(),
                        description: "Family, 3 occurrences".to_string(),
                        properties: vec![]
                    }),
                    CollectionItem::Layer(LayerListing {
                        r#type: Default::default(),
                        id: ProviderLayerId {
                            provider_id: GBIF_PROVIDER_ID,
                            layer_id: LayerId("genus/Rhipidia".to_string()),
                        },
                        name: "Rhipidia".to_string(),
                        description: "Genus, 3 occurrences".to_string(),
                        properties: vec![]
                    }),
                    CollectionItem::Layer(LayerListing {
                        r#type: Default::default(),
                        id: ProviderLayerId {
                            provider_id: GBIF_PROVIDER_ID,
                            layer_id: LayerId("species/Rhipidia willistoniana".to_string()),
                        },
                        name: "Rhipidia willistoniana".to_string(),
                        description: "Species, 3 occurrences".to_string(),
                        properties: vec![]
                    }),
                ],
                entry_label: None,
                properties: vec![],
            }
        );
    }

    #[ge_context::test]
    async fn it_performs_global_search_autocomplete_on_select_items(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        add_test_data(&db_config).await;

        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let root_id = provider.get_root_layer_collection_id().await.unwrap();

        let autocomplete_items = provider
            .autocomplete_search(
                &root_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "x".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(autocomplete_items, Vec::<String>::new());

        let autocomplete_items = provider
            .autocomplete_search(
                &root_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "i".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            autocomplete_items,
            vec![
                "Limoniidae".to_string(),
                "Rhipidia".to_string(),
                "Rhipidia willistoniana".to_string()
            ]
        );
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_searches_on_filter_items(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        add_test_data(&db_config).await;

        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let layer_collection_id = LayerCollectionId("filter/".to_string());

        let collection = provider
            .search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "x".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: layer_collection_id.clone(),
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![],
                entry_label: None,
                properties: vec![],
            }
        );

        let collection = provider
            .search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "n".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: layer_collection_id.clone(),
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![CollectionItem::Collection(LayerCollectionListing {
                    r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: GBIF_PROVIDER_ID,
                        collection_id: LayerCollectionId("select/Animalia".to_string()),
                    },
                    name: "Animalia".to_string(),
                    description: String::new(),
                    properties: Default::default(),
                })],
                entry_label: None,
                properties: vec![],
            }
        );

        let collection = provider
            .search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "n".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: layer_collection_id.clone(),
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![],
                entry_label: None,
                properties: vec![],
            }
        );

        let collection = provider
            .search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "An".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: layer_collection_id.clone(),
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![CollectionItem::Collection(LayerCollectionListing {
                    r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: GBIF_PROVIDER_ID,
                        collection_id: LayerCollectionId("select/Animalia".to_string()),
                    },
                    name: "Animalia".to_string(),
                    description: String::new(),
                    properties: Default::default(),
                })],
                entry_label: None,
                properties: vec![],
            }
        );
    }

    #[ge_context::test]
    async fn it_autocompletes_search_on_filter_items(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        add_test_data(&db_config).await;

        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let layer_collection_id = LayerCollectionId("filter/".to_string());

        let autocomplete_items = provider
            .autocomplete_search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "x".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(autocomplete_items, Vec::<String>::new());

        let autocomplete_items = provider
            .autocomplete_search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "n".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(autocomplete_items, vec!["Animalia".to_string()]);

        let autocomplete_items = provider
            .autocomplete_search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "n".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(autocomplete_items, Vec::<String>::new());

        let autocomplete_items = provider
            .autocomplete_search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "An".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(autocomplete_items, vec!["Animalia".to_string()]);
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_searches_on_filtered_filter_items(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        add_test_data(&db_config).await;

        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let layer_collection_id = LayerCollectionId("filter/Animalia".to_string());

        let collection = provider
            .search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "x".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: layer_collection_id.clone(),
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![],
                entry_label: None,
                properties: vec![],
            }
        );

        let collection = provider
            .search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "r".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: layer_collection_id.clone(),
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![CollectionItem::Collection(LayerCollectionListing {
                    r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: GBIF_PROVIDER_ID,
                        collection_id: LayerCollectionId("select/Animalia/Arthropoda".to_string()),
                    },
                    name: "Arthropoda".to_string(),
                    description: String::new(),
                    properties: Default::default(),
                })],
                entry_label: None,
                properties: vec![],
            }
        );

        let collection = provider
            .search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "r".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: layer_collection_id.clone(),
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![],
                entry_label: None,
                properties: vec![],
            }
        );

        let collection = provider
            .search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "Ar".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: layer_collection_id.clone(),
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![CollectionItem::Collection(LayerCollectionListing {
                    r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: GBIF_PROVIDER_ID,
                        collection_id: LayerCollectionId("select/Animalia/Arthropoda".to_string()),
                    },
                    name: "Arthropoda".to_string(),
                    description: String::new(),
                    properties: Default::default(),
                })],
                entry_label: None,
                properties: vec![],
            }
        );
    }

    #[ge_context::test]
    async fn it_autocompletes_search_on_filtered_filter_items(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        add_test_data(&db_config).await;

        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let layer_collection_id = LayerCollectionId("filter/Animalia".to_string());

        let autocomplete_items = provider
            .autocomplete_search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "x".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(autocomplete_items, Vec::<String>::new());

        let autocomplete_items = provider
            .autocomplete_search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "r".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(autocomplete_items, vec!["Arthropoda".to_string()]);

        let autocomplete_items = provider
            .autocomplete_search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "r".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(autocomplete_items, Vec::<String>::new());

        let autocomplete_items = provider
            .autocomplete_search(
                &layer_collection_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "Ar".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(autocomplete_items, vec!["Arthropoda".to_string()]);
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_searches_result_items(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        add_test_data(&db_config).await;

        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let root_id = LayerCollectionId("datasets/family/".to_string());

        let collection = provider
            .search(
                &root_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "x".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: root_id.clone(),
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![],
                entry_label: None,
                properties: vec![],
            }
        );

        let collection = provider
            .search(
                &root_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "m".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: root_id.clone(),
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![CollectionItem::Layer(LayerListing {
                    r#type: Default::default(),
                    id: ProviderLayerId {
                        provider_id: GBIF_PROVIDER_ID,
                        layer_id: LayerId("family/Limoniidae".to_string()),
                    },
                    name: "Limoniidae".to_string(),
                    description: "Family, 3 occurrences".to_string(),
                    properties: vec![]
                }),],
                entry_label: None,
                properties: vec![],
            }
        );

        let collection = provider
            .search(
                &root_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "m".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: root_id.clone(),
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![],
                entry_label: None,
                properties: vec![],
            }
        );

        let collection = provider
            .search(
                &root_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "Lim".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: GBIF_PROVIDER_ID,
                    collection_id: root_id.clone(),
                },
                name: "GBIF".to_string(),
                description: "GBIF occurrence datasets".to_string(),
                items: vec![CollectionItem::Layer(LayerListing {
                    r#type: Default::default(),
                    id: ProviderLayerId {
                        provider_id: GBIF_PROVIDER_ID,
                        layer_id: LayerId("family/Limoniidae".to_string()),
                    },
                    name: "Limoniidae".to_string(),
                    description: "Family, 3 occurrences".to_string(),
                    properties: vec![]
                }),],
                entry_label: None,
                properties: vec![],
            }
        );
    }

    #[ge_context::test]
    async fn it_autocompletes_search_on_result_items(
        db_config: DatabaseConnectionConfig,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        add_test_data(&db_config).await;

        let provider = Box::new(GbifDataProviderDefinition {
            name: "GBIF".to_string(),
            description: "GBIF occurrence datasets".to_string(),
            priority: Some(12),
            db_config,
            cache_ttl: Default::default(),
            autocomplete_timeout: 5,
            columns: GbifDataProvider::all_columns(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let root_id = LayerCollectionId("datasets/family/".to_string());

        let autocomplete_items = provider
            .autocomplete_search(
                &root_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "x".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(autocomplete_items, Vec::<String>::new());

        let autocomplete_items = provider
            .autocomplete_search(
                &root_id,
                SearchParameters {
                    search_type: SearchType::Fulltext,
                    search_string: "m".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(autocomplete_items, vec!["Limoniidae".to_string()]);

        let autocomplete_items = provider
            .autocomplete_search(
                &root_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "m".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(autocomplete_items, Vec::<String>::new());

        let autocomplete_items = provider
            .autocomplete_search(
                &root_id,
                SearchParameters {
                    search_type: SearchType::Prefix,
                    search_string: "Lim".to_string(),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .unwrap();

        assert_eq!(autocomplete_items, vec!["Limoniidae".to_string()]);
    }
}
