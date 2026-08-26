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
use geoengine_operators::engine::{RasterBandDescriptor, SpatialGridDescriptor};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

mod auth;
mod cache;
pub(crate) mod common;
mod listing;
mod loading_info;

const DEFAULT_QUERY_TIMEOUT_SECS: i64 = 60;
const DEFAULT_PAGE_LIMIT: i64 = 100;

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
    pub authentication: Option<StacProviderAuthentication>,
    pub time_dimension: TimeDimension, // TODO: should this be on dataset level?
    pub datasets: Vec<StacProviderDataset>,
    /// Timeout in seconds for outgoing STAC API HTTP requests.
    #[serde(default = "default_query_timeout")]
    pub query_timeout_secs: i64,
    #[serde(default = "default_page_limit")]
    pub page_limit: i64,
}

fn default_query_timeout() -> i64 {
    DEFAULT_QUERY_TIMEOUT_SECS
}

fn default_page_limit() -> i64 {
    DEFAULT_PAGE_LIMIT
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacProviderS3Config")]
pub struct StacProviderS3Config {
    pub endpoint: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacProviderAuthentication")]
pub struct StacProviderAuthentication {
    pub endpoint: String,
    pub username: String,
    pub password: String,
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

/// A band inside a STAC asset.
///
/// *Addresses* the band in the asset files of a STAC collection:
/// [`asset_title`](StacAssetBand::asset_title) selects the asset file,
/// [`band_name`](StacAssetBand::band_name) selects the raster channel within
/// it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacAssetBand")]
pub struct StacAssetBand {
    /// The title of the STAC asset in the collection. Used to *address* the
    /// asset file that contains this band (matched against the STAC asset
    /// `title`).
    pub asset_title: String,
    /// The name of the band *within* the asset file.
    ///
    /// Matches the STAC `bands[].name` metadata (e.g. `B04`) to select the
    /// raster channel inside the asset. `None` for single-band assets.
    pub band_name: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[postgres(name = "StacProviderDatasetBand")]
pub struct StacProviderDatasetBand {
    /// The band inside the STAC asset that this dataset band reads from.
    ///
    /// This is the *addressing* information: which asset file
    /// ([`StacAssetBand::asset_title`]) and which raster channel within it
    /// ([`StacAssetBand::band_name`]).
    pub asset_band: StacAssetBand,
    /// The band descriptor of the resulting geo engine dataset layer.
    ///
    /// This is independent of [`Self::asset_band`], which *addresses* the band
    /// inside the asset files. During discovery it is populated with the same
    /// naming fallback (`asset_band.band_name`, then
    /// `asset_band.asset_title`) and a unitless measurement.
    pub band_descriptor: RasterBandDescriptor,
}

impl StacProviderDatasetBand {
    /// Create a dataset band whose resulting result-descriptor band is
    /// unitless and named after the asset band using the discovery fallback
    /// ([`StacAssetBand::band_name`], else [`StacAssetBand::asset_title`]).
    pub fn new_unitless(asset_band: StacAssetBand) -> Self {
        let band_descriptor = RasterBandDescriptor::new_unitless(
            asset_band
                .band_name
                .clone()
                .unwrap_or_else(|| asset_band.asset_title.clone()),
        );
        Self {
            asset_band,
            band_descriptor,
        }
    }
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for StacDataProviderDefinition {
    async fn initialize(self: Box<Self>, _db: D) -> crate::error::Result<Box<dyn DataProvider>> {
        if self.time_dimension == TimeDimension::Irregular {
            return Err(crate::error::Error::StacIrregularTimeDimensionNotSupported);
        }
        let mut provider = StacDataProvider::new(
            self.id,
            self.name,
            self.description,
            self.api_url,
            self.collection_name,
            self.s3_config,
            self.time_dimension,
            self.datasets,
            self.page_limit,
            self.query_timeout_secs,
        );

        if let Some(authentication) = self.authentication {
            provider.authentication = Some(
                auth::StacAuthentication::initialize(provider.client.clone(), authentication)
                    .await?,
            );
        }

        Ok(Box::new(provider))
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

                if let (Some(current_authentication), Some(new_authentication)) =
                    (&self.authentication, &mut new.authentication)
                    && new_authentication.password == SECRET_REPLACEMENT
                {
                    new_authentication
                        .password
                        .clone_from(&current_authentication.password);
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
    page_limit: i64,
    /// Shared HTTP client, reused across all requests for this provider.
    client: reqwest::Client,
    authentication: Option<auth::StacAuthentication>,
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
        page_limit: i64,
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
            page_limit,
            client,
            authentication: None,
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
