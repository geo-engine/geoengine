use super::StacProviderAuthentication;
use crate::error::Result;
use reqwest::RequestBuilder;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{error, warn};

const CLIENT_ID: &str = "code-de3-public";
const TOKEN_REFRESH_FACTOR: f64 = 0.8;
const TOKEN_REFRESH_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone, Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: String,
    expires_in: u64,
    refresh_expires_in: u64,
}

impl TokenResponse {
    fn refresh_delay(&self) -> Duration {
        let lifetime = self.expires_in.min(self.refresh_expires_in);
        Duration::from_secs(lifetime)
            .mul_f64(TOKEN_REFRESH_FACTOR)
            .max(Duration::from_millis(100))
    }
}

#[derive(Serialize)]
struct PasswordGrant<'a> {
    grant_type: &'static str,
    username: &'a str,
    password: &'a str,
    client_id: &'static str,
}

#[derive(Serialize)]
struct RefreshTokenGrant<'a> {
    grant_type: &'static str,
    refresh_token: &'a str,
    client_id: &'static str,
}

/// Authentication state shared by the provider and all metadata instances it creates.
#[derive(Clone)]
pub(super) struct StacAuthentication {
    tokens: Arc<RwLock<TokenResponse>>,
}

impl fmt::Debug for StacAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StacAuthentication")
            .finish_non_exhaustive()
    }
}

impl StacAuthentication {
    pub async fn initialize(
        client: reqwest::Client,
        config: StacProviderAuthentication,
    ) -> Result<Self> {
        let tokens = request_password_tokens(&client, &config).await?;
        let tokens = Arc::new(RwLock::new(tokens));

        tokio::spawn(refresh_tokens(client, config, Arc::downgrade(&tokens)));

        Ok(Self { tokens })
    }

    pub(super) async fn access_token(&self) -> String {
        self.tokens.read().await.access_token.clone()
    }

    pub async fn authorize(&self, request: RequestBuilder) -> RequestBuilder {
        let access_token = self.access_token().await;
        request.bearer_auth(access_token)
    }
}

async fn request_password_tokens(
    client: &reqwest::Client,
    config: &StacProviderAuthentication,
) -> Result<TokenResponse> {
    Ok(client
        .post(&config.endpoint)
        .form(&PasswordGrant {
            grant_type: "password",
            username: &config.username,
            password: &config.password,
            client_id: CLIENT_ID,
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn request_refreshed_tokens(
    client: &reqwest::Client,
    config: &StacProviderAuthentication,
    refresh_token: &str,
) -> Result<TokenResponse> {
    Ok(client
        .post(&config.endpoint)
        .form(&RefreshTokenGrant {
            grant_type: "refresh_token",
            refresh_token,
            client_id: CLIENT_ID,
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn refresh_tokens(
    client: reqwest::Client,
    config: StacProviderAuthentication,
    tokens: Weak<RwLock<TokenResponse>>,
) {
    let Some(initial_tokens) = tokens.upgrade() else {
        return;
    };
    let mut refresh_delay = initial_tokens.read().await.refresh_delay();
    drop(initial_tokens);

    loop {
        tokio::time::sleep(refresh_delay).await;

        let Some(current_tokens) = tokens.upgrade() else {
            return;
        };

        let refresh_token = current_tokens.read().await.refresh_token.clone();
        drop(current_tokens);

        let refreshed_tokens =
            match request_refreshed_tokens(&client, &config, &refresh_token).await {
                Ok(tokens) => tokens,
                Err(refresh_error) => {
                    warn!(
                        error = %refresh_error,
                        "refreshing STAC authentication tokens failed; trying password grant"
                    );

                    match request_password_tokens(&client, &config).await {
                        Ok(tokens) => tokens,
                        Err(password_error) => {
                            error!(
                                error = %password_error,
                                "renewing STAC authentication tokens failed"
                            );
                            refresh_delay = TOKEN_REFRESH_RETRY_DELAY;
                            continue;
                        }
                    }
                }
            };

        let Some(current_tokens) = tokens.upgrade() else {
            return;
        };
        refresh_delay = refreshed_tokens.refresh_delay();
        *current_tokens.write().await = refreshed_tokens;
    }
}
