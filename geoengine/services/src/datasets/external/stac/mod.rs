use crate::api::model::services::SECRET_REPLACEMENT;
use crate::contexts::GeoEngineDb;
use crate::datasets::listing::ProvenanceOutput;
use crate::layers::external::{DataProvider, DataProviderDefinition, TypedDataProviderDefinition};
use async_trait::async_trait;
use cache::StacQueryCache;
use geoengine_datatypes::dataset::DataProviderId;
use geoengine_datatypes::primitives::{SpatialResolution, TimeDimension};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::SpatialGridDescriptor;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

mod cache;
mod listing;
mod loading_info;

const DEFAULT_QUERY_TIMEOUT_SECS: i64 = 60;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacDataProviderDefinition")]
#[serde(rename_all = "camelCase")]
pub struct StacDataProviderDefinition {
    pub name: String,
    pub id: DataProviderId,
    pub description: String,
    pub priority: Option<i16>,
    pub api_url: String,
    pub collection_name: String,
    pub s3_config: Option<StacProviderS3Config>,
    pub time_dimension: TimeDimension, // TODO: should this be on dataset level?
    pub datasets: Vec<StacProviderDataset>,
    // TODO: page limit(?)
    /// Timeout in seconds for outgoing STAC API HTTP requests.
    #[serde(default = "default_query_timeout")]
    pub query_timeout_secs: i64,
}

fn default_query_timeout() -> i64 {
    DEFAULT_QUERY_TIMEOUT_SECS
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacProviderS3Config")]
pub struct StacProviderS3Config {
    pub endpoint: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

/// A geo engine dataset derived from a STAC collection.
/// As all bands and tiles of a geo engine data set must have the same data type, resolution and projection,
/// a stac collection will be split into multiple geo engine datasets if it contains bands with different data types, resolutions or projections.
/// In order to make them browsable they are defined as part of the stac provider definition.
///
/// TODO: different approach would be to just provide data type, resolution and projection + bands and compute all combinations as possible datasets,
/// but not all combinations actually exist and would lead to empty collection.
///
/// TODO: could also be gathered from collection api and probed from items
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacProviderDataset")]
pub struct StacProviderDataset {
    pub name: String, // TODO: derive from collection name + data type + resolution + projection?
    pub description: String,
    pub data_type: RasterDataType,
    pub resolution: SpatialResolution,
    pub projection: SpatialReference,
    pub spatial_grid: SpatialGridDescriptor, // TODO: this could be fetched from STAC, however it is dependent on the projection and the STAC collection API does not include this information for all projections but only the first one. so we would have to probe the items API...
    pub bands: Vec<StacProviderDatasetBand>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacProviderDatasetBand")]
pub struct StacProviderDatasetBand {
    pub asset_title: String,
    pub band_name: Option<String>,
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for StacDataProviderDefinition {
    async fn initialize(self: Box<Self>, _db: D) -> crate::error::Result<Box<dyn DataProvider>> {
        if self.time_dimension == TimeDimension::Irregular {
            return Err(crate::error::Error::StacIrregularTimeDimensionNotSupported);
        }
        Ok(Box::new(StacDataProvider::new(
            self.id,
            self.name,
            self.description,
            self.api_url,
            self.collection_name,
            self.s3_config,
            self.time_dimension,
            self.datasets,
            self.query_timeout_secs,
        )))
    }

    fn type_name(&self) -> &'static str {
        "Stac"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        self.id
    }

    fn priority(&self) -> i16 {
        self.priority.unwrap_or(0)
    }

    async fn update(
        &self,
        new: TypedDataProviderDefinition,
    ) -> crate::error::Result<TypedDataProviderDefinition>
    where
        Self: Sized,
    {
        Ok(match new {
            TypedDataProviderDefinition::StacDataProviderDefinition(mut new) => {
                if let (Some(current_s3), Some(new_s3)) = (&self.s3_config, &mut new.s3_config) {
                    if new_s3.access_key.as_deref() == Some(SECRET_REPLACEMENT) {
                        new_s3.access_key.clone_from(&current_s3.access_key);
                    }

                    if new_s3.secret_key.as_deref() == Some(SECRET_REPLACEMENT) {
                        new_s3.secret_key.clone_from(&current_s3.secret_key);
                    }
                }

                TypedDataProviderDefinition::StacDataProviderDefinition(new)
            }
            _ => new,
        })
    }
}

#[derive(Debug, Clone)]
pub struct StacDataProvider {
    id: DataProviderId,
    name: String,
    description: String,
    api_url: String,
    collection_name: String,
    s3_config: Option<StacProviderS3Config>,
    time_dimension: TimeDimension,
    datasets: Vec<StacProviderDataset>,
    /// Shared HTTP client, reused across all requests for this provider.
    client: reqwest::Client,
    /// In-memory cache for STAC query results (tile files), keyed by dataset
    /// name and spatial/temporal query bounds.
    query_cache: Arc<StacQueryCache>,
}

impl StacDataProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: DataProviderId,
        name: String,
        description: String,
        api_url: String,
        collection_name: String,
        s3_config: Option<StacProviderS3Config>,
        time_dimension: TimeDimension,
        datasets: Vec<StacProviderDataset>,
        query_timeout_secs: i64,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(query_timeout_secs as u64))
            .build()
            .unwrap_or_default();
        Self {
            id,
            name,
            description,
            api_url,
            collection_name,
            s3_config,
            time_dimension,
            datasets,
            client,
            query_cache: Arc::new(StacQueryCache::default()),
        }
    }
}

#[async_trait]
impl DataProvider for StacDataProvider {
    async fn provenance(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> crate::error::Result<ProvenanceOutput> {
        Err(crate::error::Error::NotImplemented {
            message: "STAC provenance is not yet implemented".to_owned(),
        })
    }
}
