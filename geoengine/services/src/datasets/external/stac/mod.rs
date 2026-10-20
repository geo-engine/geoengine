use crate::api::model::services::SECRET_REPLACEMENT;
use crate::contexts::GeoEngineDb;
use crate::datasets::listing::ProvenanceOutput;
use crate::layers::external::{DataProvider, DataProviderDefinition, TypedDataProviderDefinition};
use async_trait::async_trait;
use geoengine_datatypes::dataset::DataProviderId;
use geoengine_datatypes::primitives::{CacheTtlSeconds, SpatialResolution, TimeDimension};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{RasterBandDescriptor, SpatialGridDescriptor};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

mod auth;
mod cache;
pub(crate) mod common;
mod coordinator;
mod grid;
mod listing;
mod loading_info;
mod storage;
use cache::StacQueryCache;
pub use grid::{ProjectedStacGrid, StacGrid, StacGridCell, StacGridCells};
pub use loading_info::StacMultiBandMetaData;

const DEFAULT_QUERY_TIMEOUT_SECS: i64 = 60;
const DEFAULT_PAGE_LIMIT: i64 = 100;

fn validate_authentication(
    authentication: Option<&StacProviderAuthentication>,
) -> crate::error::Result<()> {
    if authentication.is_some_and(|authentication| authentication.password == SECRET_REPLACEMENT) {
        return Err(crate::error::Error::InvalidConfig {
            reason: "STAC authentication password must not be the secret replacement placeholder"
                .to_owned(),
        });
    }
    Ok(())
}

fn validate_s3_config(s3_config: Option<&StacProviderS3Config>) -> crate::error::Result<()> {
    if s3_config.is_some_and(|s3_config| {
        s3_config.access_key.as_deref() == Some(SECRET_REPLACEMENT)
            || s3_config.secret_key.as_deref() == Some(SECRET_REPLACEMENT)
    }) {
        return Err(crate::error::Error::InvalidConfig {
            reason: "STAC S3 credentials must not use the secret replacement placeholder"
                .to_owned(),
        });
    }
    Ok(())
}

fn validate_time_dimension(time_dimension: TimeDimension) -> crate::error::Result<()> {
    match time_dimension {
        TimeDimension::Regular(regular) if regular.step.step > 0 => Ok(()),
        TimeDimension::Regular(_) => Err(crate::error::Error::InvalidConfig {
            reason: "STAC regular time dimension step must be positive".to_owned(),
        }),
        TimeDimension::Irregular => {
            Err(crate::error::Error::StacIrregularTimeDimensionNotSupported)
        }
    }
}

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
    /// Optional output cache lifetime; omitted values use the global cache default.
    pub cache_ttl_secs: Option<CacheTtlSeconds>,
    /// Target square cell count per dataset CRS projected area of use for STAC metadata searches.
    #[serde(default)]
    pub stac_grid: Option<StacGrid>,
}

fn default_query_timeout() -> i64 {
    DEFAULT_QUERY_TIMEOUT_SECS
}

fn default_page_limit() -> i64 {
    DEFAULT_PAGE_LIMIT
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct StacProviderS3Config {
    pub endpoint: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

impl fmt::Debug for StacProviderS3Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StacProviderS3Config")
            .field("endpoint", &self.endpoint)
            .field(
                "access_key",
                &self.access_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "secret_key",
                &self.secret_key.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct StacProviderAuthentication {
    pub endpoint: String,
    pub client_id: String,
    pub username: String,
    pub password: String,
}

impl fmt::Debug for StacProviderAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StacProviderAuthentication")
            .field("endpoint", &self.endpoint)
            .field("client_id", &self.client_id)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
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
        let mut definition = *self;
        let authentication = definition.authentication.take();
        validate_authentication(authentication.as_ref())?;
        let mut provider = StacDataProvider::from_definition(definition)?;

        provider.client = provider.client.with_authentication(authentication).await?;

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

                // Validate after replacing redacted values. This also covers
                // provider creation, where the generic insert path calls
                // `update` with the new provider as both `self` and `new`.
                validate_authentication(new.authentication.as_ref())?;
                validate_s3_config(new.s3_config.as_ref())?;
                new.stac_grid.unwrap_or_default().validate()?;
                validate_time_dimension(new.time_dimension)?;

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
    cache_ttl_secs: Option<CacheTtlSeconds>,
    /// Shared HTTP client, reused across all requests for this provider.
    client: StacClient,
    /// In-memory cache for complete STAC cell results, keyed by dataset, integer
    /// projected-grid index, and regular layer time step.
    query_cache: Arc<StacQueryCache>,
    grid: StacGrid,
}

impl StacDataProvider {
    /// Builds a provider from its persisted definition after validating its configuration.
    ///
    /// Authentication is installed asynchronously by [`StacDataProviderDefinition::initialize`].
    pub fn from_definition(definition: StacDataProviderDefinition) -> crate::error::Result<Self> {
        validate_s3_config(definition.s3_config.as_ref())?;
        validate_time_dimension(definition.time_dimension)?;
        let grid = definition.stac_grid.unwrap_or_default().validate()?;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(
                definition.query_timeout_secs as u64,
            ))
            .build()
            .unwrap_or_default();
        let client = StacClient::new(client, &definition.api_url)?;

        Ok(Self {
            id: definition.id,
            name: definition.name,
            description: definition.description,
            api_url: definition.api_url,
            collection_name: definition.collection_name,
            s3_config: definition.s3_config,
            time_dimension: definition.time_dimension,
            datasets: definition.datasets,
            page_limit: definition.page_limit,
            client,
            cache_ttl_secs: definition.cache_ttl_secs,
            query_cache: Arc::new(StacQueryCache::default()),
            grid,
        })
    }

    /// Replace the provider's shared metadata cache with the specified capacity and lifetime.
    /// A zero capacity retains in-flight sharing without caching completed results.
    #[must_use]
    pub fn with_query_cache_capacity(mut self, max_bytes: usize, ttl: std::time::Duration) -> Self {
        self.query_cache = Arc::new(StacQueryCache::new(max_bytes, ttl));
        self
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

/// STAC HTTP transport with a fixed provider origin and coordinator.
/// Clones share the connection pool, token renewal state and coordinator.
#[derive(Clone, Debug)]
pub(crate) struct StacClient {
    client: reqwest::Client,
    origin: url::Url,
    coordinator: Arc<coordinator::StacRequestCoordinator>,
    authentication: Option<auth::StacAuthentication>,
}

impl StacClient {
    pub(crate) fn new(client: reqwest::Client, stac_api_url: &str) -> crate::error::Result<Self> {
        let origin = url::Url::parse(stac_api_url)?;
        let coordinator = coordinator::StacRequestCoordinator::shared(&origin, None);
        Ok(Self {
            client,
            origin,
            coordinator,
            authentication: None,
        })
    }

    pub(crate) async fn with_authentication(
        mut self,
        config: Option<StacProviderAuthentication>,
    ) -> crate::error::Result<Self> {
        self.coordinator =
            coordinator::StacRequestCoordinator::shared(&self.origin, config.as_ref());
        self.authentication = match config {
            Some(config) => {
                Some(auth::StacAuthentication::initialize(self.client.clone(), config).await?)
            }
            None => None,
        };
        Ok(self)
    }

    pub(crate) fn get(&self, url: impl reqwest::IntoUrl) -> reqwest::RequestBuilder {
        self.client.get(url)
    }

    /// Executes a request through the provider's coordinator and decodes the JSON response.
    /// The coordinator handles authentication, rate limits and retries.
    pub(crate) async fn execute_json<T: serde::de::DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
        policy: &crate::util::retry::RetryPolicy,
    ) -> reqwest::Result<T> {
        let request = request.build()?;
        let authentication = if same_origin(&self.origin, request.url()) {
            self.authentication.as_ref()
        } else {
            None
        };
        self.coordinator
            .execute(&self.client, request, authentication, policy)
            .await
    }
}

fn same_origin(expected: &url::Url, actual: &url::Url) -> bool {
    expected.scheme() == actual.scheme()
        && expected.host() == actual.host()
        && expected.port_or_known_default() == actual.port_or_known_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    type AuthenticationBucketArrivals =
        Arc<std::sync::Mutex<Vec<(&'static str, std::time::Instant)>>>;

    fn authentication_bucket_server(
        authenticated: bool,
    ) -> (httptest::Server, AuthenticationBucketArrivals) {
        use httptest::{
            Expectation, Server, all_of,
            matchers::{any, contains, not, request},
            responders,
        };
        use serde_json::json;
        use std::{sync::Mutex, time::Instant};

        let server = Server::run();
        // Separate token grants for matching credentials deliberately issue
        // different tokens. Renewed/independently issued tokens keep one bucket.
        let tokens = if authenticated {
            vec!["shared-first", "shared-second", "independent"]
        } else {
            vec!["independent"]
        };
        let token_count = tokens.len();
        server.expect(
            Expectation::matching(request::method_path("POST", "/token"))
                .times(token_count)
                .respond_with(responders::cycle(
                    tokens
                        .into_iter()
                        .map(|token| {
                            Box::new(responders::json_encoded(json!({
                                "access_token": token, "expires_in": 3600
                            }))) as Box<dyn responders::Responder>
                        })
                        .collect(),
                )),
        );
        let arrivals = Arc::new(Mutex::new(Vec::new()));
        for (path, token) in [
            ("/probe", authenticated.then_some("shared-first")),
            ("/shared", authenticated.then_some("shared-second")),
            ("/independent", Some("independent")),
            ("/anonymous", None),
        ] {
            if path == "/anonymous" && !authenticated {
                continue;
            }
            let expectation = if let Some(token) = token {
                Expectation::matching(all_of![
                    request::method_path("GET", path),
                    request::headers(contains(("authorization", format!("Bearer {token}"))))
                ])
            } else {
                Expectation::matching(all_of![
                    request::method_path("GET", path),
                    request::headers(not(contains(("authorization", any()))))
                ])
            };
            let arrivals = arrivals.clone();
            server.expect(expectation.times(1).respond_with(move || {
                arrivals.lock().unwrap().push((path, Instant::now()));
                let response = responders::status_code(200).body("null");
                if path == "/probe" {
                    response
                        .append_header("x-ratelimit-limit", "10, 10;w=1, 60;w=60")
                        .append_header("x-ratelimit-remaining", "0")
                        .append_header("x-ratelimit-reset", "1")
                } else {
                    response
                }
            }));
        }
        (server, arrivals)
    }

    async fn authentication_bucket_contract(authenticated: bool) {
        use serde_json::Value;
        use std::time::{Duration, Instant};

        let (server, arrivals) = authentication_bucket_server(authenticated);
        let create_client = |username: Option<&str>| {
            let config = username.map(|username| StacProviderAuthentication {
                endpoint: server.url_str("/token"),
                client_id: "client".into(),
                username: username.into(),
                password: "password".into(),
            });
            let api_url = server.url_str("/");
            async move {
                StacClient::new(reqwest::Client::new(), &api_url)
                    .unwrap()
                    .with_authentication(config)
                    .await
                    .unwrap()
            }
        };
        let first = create_client(authenticated.then_some("shared-user")).await;
        let same = create_client(authenticated.then_some("shared-user")).await;
        let other = create_client(Some("different-user")).await;
        let anonymous = create_client(None).await;
        let policy = crate::util::retry::RetryPolicy::new().stop_on_status(&[400, 404]);
        let exhausted = Instant::now();
        first
            .execute_json::<Value>(first.get(server.url_str("/probe")), &policy)
            .await
            .unwrap();
        let waiting = same.execute_json::<Value>(same.get(server.url_str("/shared")), &policy);
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut waiting)
                .await
                .is_err(),
            "matching authentication must share the exhausted bucket"
        );
        tokio::time::timeout(
            Duration::from_millis(300),
            other.execute_json::<Value>(other.get(server.url_str("/independent")), &policy),
        )
        .await
        .expect("different authentication has an independent quota")
        .unwrap();
        if authenticated {
            tokio::time::timeout(
                Duration::from_millis(300),
                anonymous
                    .execute_json::<Value>(anonymous.get(server.url_str("/anonymous")), &policy),
            )
            .await
            .expect("anonymous quota is independent of authenticated quota")
            .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(3), &mut waiting)
            .await
            .unwrap()
            .unwrap();
        let arrivals = arrivals.lock().unwrap();
        let shared_start = arrivals
            .iter()
            .find(|(path, _)| *path == "/shared")
            .unwrap()
            .1;
        assert!(shared_start.duration_since(exhausted) >= Duration::from_secs(1));
    }

    #[tokio::test]
    async fn it_shares_anonymous_rate_limits_across_clients() {
        authentication_bucket_contract(false).await;
    }

    #[tokio::test]
    async fn it_reuses_the_initialized_coordinator_across_clones() {
        use httptest::{Expectation, Server, matchers::request, responders};
        use serde_json::Value;
        use std::time::{Duration, Instant};

        let server = Server::run();
        for path in ["/items", "/next"] {
            server.expect(
                Expectation::matching(request::method_path("GET", path))
                    .times(1)
                    .respond_with(
                        responders::status_code(200)
                            .append_header("x-ratelimit-remaining", "0")
                            .append_header("x-ratelimit-reset", "1")
                            .body("null"),
                    ),
            );
        }
        let client = StacClient::new(reqwest::Client::new(), &server.url_str("/")).unwrap();
        let cloned = client.clone();
        let policy = crate::util::retry::RetryPolicy::new().max_retries(1);
        let started = Instant::now();
        client
            .execute_json::<Value>(client.get(server.url_str("/items")), &policy)
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(3),
            cloned.execute_json::<Value>(cloned.get(server.url_str("/next")), &policy),
        )
        .await
        .expect("the cloned client should proceed after the shared quota resets")
        .unwrap();
        assert!(started.elapsed() >= Duration::from_secs(1));
    }

    #[tokio::test]
    async fn it_shares_matching_authentication_and_isolates_other_credentials() {
        authentication_bucket_contract(true).await;
    }

    #[tokio::test]
    async fn it_authorizes_after_queue_wait_and_preserves_request_timeout() {
        use httptest::{
            Expectation, Server, all_of,
            matchers::{contains, request},
            responders,
        };
        use serde_json::json;
        use std::time::Duration;

        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("POST", "/token"))
                .times(2)
                .respond_with(responders::cycle![
                    responders::json_encoded(json!({
                        "access_token": "initial", "refresh_token": "refresh",
                        "expires_in": 1
                    })),
                    responders::json_encoded(json!({
                        "access_token": "renewed", "refresh_token": "next-refresh",
                        "expires_in": 3600
                    })),
                ]),
        );
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/probe"),
                request::headers(contains(("authorization", "Bearer initial"))),
            ])
            .times(1)
            .respond_with(
                responders::status_code(200)
                    .append_header("x-ratelimit-limit", "10, 10;w=1, 60;w=60")
                    .append_header("x-ratelimit-remaining", "0")
                    .append_header("x-ratelimit-reset", "1")
                    .body("null"),
            ),
        );
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/queued"),
                request::headers(contains(("authorization", "Bearer renewed"))),
            ])
            .times(1)
            .respond_with(responders::status_code(200).body("null")),
        );
        let client = StacClient::new(reqwest::Client::new(), &server.url_str("/"))
            .unwrap()
            .with_authentication(Some(StacProviderAuthentication {
                endpoint: server.url_str("/token"),
                client_id: "client".into(),
                username: "user".into(),
                password: "password".into(),
            }))
            .await
            .unwrap();
        let queued = client
            .get(server.url_str("/queued"))
            .timeout(Duration::from_millis(100));
        let policy = crate::util::retry::RetryPolicy::new().stop_on_status(&[400, 404]);
        client
            .execute_json::<serde_json::Value>(client.get(server.url_str("/probe")), &policy)
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(3),
            client.execute_json::<serde_json::Value>(queued, &policy),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn it_reuses_the_provider_coordinator_without_sending_credentials_to_other_origins() {
        use httptest::{
            Expectation, Server, all_of,
            matchers::{any, contains, not, request},
            responders,
        };
        use serde_json::{Value, json};
        use std::time::{Duration, Instant};
        let origin = Server::run();
        let destination = Server::run();
        origin.expect(
            Expectation::matching(request::method_path("POST", "/token"))
                .times(1)
                .respond_with(responders::json_encoded(
                    json!({ "access_token": "origin-only", "expires_in": 3600 }),
                )),
        );
        origin.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/probe"),
                request::headers(contains(("authorization", "Bearer origin-only"))),
            ])
            .times(1)
            .respond_with(
                responders::status_code(200)
                    .append_header("x-ratelimit-remaining", "0")
                    .append_header("x-ratelimit-reset", "1")
                    .body("null"),
            ),
        );
        destination.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/items"),
                request::headers(not(contains(("authorization", any())))),
            ])
            .times(1)
            .respond_with(responders::status_code(200).body("null")),
        );
        let client = StacClient::new(reqwest::Client::new(), &origin.url_str("/"))
            .unwrap()
            .with_authentication(Some(StacProviderAuthentication {
                endpoint: origin.url_str("/token"),
                client_id: "client".into(),
                username: "user".into(),
                password: "password".into(),
            }))
            .await
            .unwrap();
        let policy = crate::util::retry::RetryPolicy::new().max_retries(1);
        let started = Instant::now();
        client
            .execute_json::<Value>(client.get(origin.url_str("/probe")), &policy)
            .await
            .unwrap();
        let cloned = client.clone();
        tokio::time::timeout(
            Duration::from_secs(3),
            cloned.execute_json::<Value>(cloned.get(destination.url_str("/items")), &policy),
        )
        .await
        .expect("cross-origin requests should proceed without provider credentials")
        .unwrap();
        assert!(started.elapsed() >= Duration::from_secs(1));
    }

    #[test]
    fn same_origin_ignores_path_but_checks_scheme_host_and_port() {
        assert!(same_origin(
            &url::Url::parse("https://example.test/api").unwrap(),
            &url::Url::parse("https://example.test/other").unwrap(),
        ));
        assert!(!same_origin(
            &url::Url::parse("https://example.test/api").unwrap(),
            &url::Url::parse("https://other.test/api").unwrap(),
        ));
    }
    #[test]
    fn secret_replacement_is_rejected_without_existing_authentication() {
        let authentication = StacProviderAuthentication {
            endpoint: "https://identity.example/token".into(),
            client_id: "client".into(),
            username: "user".into(),
            password: SECRET_REPLACEMENT.into(),
        };
        assert!(validate_authentication(Some(&authentication)).is_err());
        assert!(validate_authentication(None).is_ok());
    }

    #[tokio::test]
    async fn secret_replacement_is_rejected_during_provider_creation() {
        let mut provider: StacDataProviderDefinition =
            serde_json::from_str::<crate::api::model::services::StacDataProviderDefinition>(
                include_str!("../../../../../test_data/provider_defs_api/stac_sentinel2.json"),
            )
            .unwrap()
            .into();
        provider.authentication = Some(StacProviderAuthentication {
            endpoint: "https://identity.example/token".into(),
            client_id: "client".into(),
            username: "user".into(),
            password: SECRET_REPLACEMENT.into(),
        });
        provider.s3_config = Some(StacProviderS3Config {
            endpoint: "https://s3.example".into(),
            access_key: Some(SECRET_REPLACEMENT.into()),
            secret_key: Some(SECRET_REPLACEMENT.into()),
        });

        let result =
            DataProviderDefinition::<crate::contexts::PostgresDb<tokio_postgres::NoTls>>::update(
                &provider,
                TypedDataProviderDefinition::StacDataProviderDefinition(provider.clone()),
            )
            .await;

        assert!(result.is_err());
    }

    #[crate::ge_context::test]
    async fn grid_configuration_round_trips_through_database_and_api_and_rejects_invalid_updates(
        app_ctx: crate::contexts::PostgresContext<tokio_postgres::NoTls>,
    ) {
        use crate::contexts::{ApplicationContext, SessionContext};
        use crate::layers::storage::LayerProviderDb;
        use crate::users::UserSession;
        let db = app_ctx.session_context(UserSession::admin_session()).db();
        let mut api: crate::api::model::services::StacDataProviderDefinition =
            serde_json::from_str(include_str!(
                "../../../../../test_data/provider_defs_api/stac_sentinel2.json"
            ))
            .unwrap();
        api.s3_config = None;
        api.authentication = None;
        api.stac_grid = Some(
            serde_json::from_value(serde_json::json!({"targetNumberOfCells":259_200})).unwrap(),
        );
        let mut definition = StacDataProviderDefinition::from(api);
        let id = db
            .add_layer_provider(definition.clone().into())
            .await
            .unwrap();
        assert_eq!(
            db.get_layer_provider_definition(id).await.unwrap(),
            definition.clone().into()
        );
        let api = crate::api::model::services::StacDataProviderDefinition::from(definition.clone());
        let api: crate::api::model::services::StacDataProviderDefinition =
            serde_json::from_value(serde_json::to_value(api).unwrap()).unwrap();
        assert_eq!(StacDataProviderDefinition::from(api), definition);
        let mut invalid = definition.clone();
        invalid.stac_grid.as_mut().unwrap().target_number_of_cells = 0;
        assert!(
            db.update_layer_provider_definition(id, invalid.into())
                .await
                .is_err()
        );
        assert_eq!(
            db.get_layer_provider_definition(id).await.unwrap(),
            definition.clone().into()
        );
        definition
            .stac_grid
            .as_mut()
            .unwrap()
            .target_number_of_cells = 1440;
        db.update_layer_provider_definition(id, definition.clone().into())
            .await
            .unwrap();
        assert_eq!(
            db.get_layer_provider_definition(id).await.unwrap(),
            definition.into()
        );
    }
}
