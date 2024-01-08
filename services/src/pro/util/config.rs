use std::net::SocketAddr;

use geoengine_datatypes::util::test::TestDefault;
use serde::Deserialize;

use crate::util::config::ConfigElement;

#[derive(Debug, Deserialize)]
pub struct User {
    pub registration: bool,
    pub admin_email: String,
    pub admin_password: String,
}

impl ConfigElement for User {
    const KEY: &'static str = "user";
}

#[derive(Debug, Deserialize)]
pub struct Quota {
    pub mode: QuotaTrackingMode,
    #[serde(default)]
    pub initial_credits: i64,
    pub increment_quota_buffer_size: usize,
    pub increment_quota_buffer_timeout_seconds: u64,
}

impl ConfigElement for Quota {
    const KEY: &'static str = "quota";
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum QuotaTrackingMode {
    Track,
    Check,
    Disabled,
}

#[derive(Debug, Deserialize)]
pub struct Oidc {
    pub enabled: bool,
    pub issuer: String,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub redirect_uri: String, //TODO: Maybe URL type
    pub scopes: Vec<String>,
}

impl ConfigElement for Oidc {
    const KEY: &'static str = "oidc";
}

#[derive(Debug, Deserialize)]
pub struct OpenTelemetry {
    pub enabled: bool,
    pub endpoint: SocketAddr,
}

impl ConfigElement for OpenTelemetry {
    const KEY: &'static str = "open_telemetry";
}

#[derive(Debug, Deserialize, Clone, Copy)]
pub struct Cache {
    pub enabled: bool,
    pub size_in_mb: usize,
    pub landing_zone_ratio: f64,
}

impl TestDefault for Cache {
    fn test_default() -> Self {
        Self {
            enabled: false,
            size_in_mb: 1_000,       // 1 GB
            landing_zone_ratio: 0.1, // 10% of cache size
        }
    }
}

impl ConfigElement for Cache {
    const KEY: &'static str = "cache";
}
