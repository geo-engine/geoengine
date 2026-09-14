use super::StacProviderAuthentication;
use crate::error::Result;
use reqwest::RequestBuilder;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{error, warn};

const TOKEN_REFRESH_FACTOR: f64 = 0.8;
const TOKEN_REFRESH_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone, Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: u64,
    refresh_expires_in: Option<u64>,
}

impl TokenResponse {
    fn refresh_delay(&self) -> Duration {
        let lifetime = self
            .refresh_expires_in
            .filter(|&lifetime| lifetime > 0)
            .map_or(self.expires_in, |lifetime| self.expires_in.min(lifetime));
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
    client_id: &'a str,
}

#[derive(Serialize)]
struct RefreshTokenGrant<'a> {
    grant_type: &'static str,
    refresh_token: &'a str,
    client_id: &'a str,
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
            client_id: &config.client_id,
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
    let mut tokens: TokenResponse = client
        .post(&config.endpoint)
        .form(&RefreshTokenGrant {
            grant_type: "refresh_token",
            refresh_token,
            client_id: &config.client_id,
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if tokens.refresh_token.is_none() {
        tokens.refresh_token = Some(refresh_token.to_owned());
    }
    Ok(tokens)
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

        let refreshed_tokens = match renew_tokens(&client, &config, refresh_token.as_deref()).await
        {
            Ok(tokens) => tokens,
            Err(error) => {
                error!(%error, "renewing STAC authentication tokens failed");
                refresh_delay = TOKEN_REFRESH_RETRY_DELAY;
                continue;
            }
        };

        let Some(current_tokens) = tokens.upgrade() else {
            return;
        };
        refresh_delay = refreshed_tokens.refresh_delay();
        *current_tokens.write().await = refreshed_tokens;
    }
}

async fn renew_tokens(
    client: &reqwest::Client,
    config: &StacProviderAuthentication,
    refresh_token: Option<&str>,
) -> Result<TokenResponse> {
    if let Some(refresh_token) = refresh_token {
        match request_refreshed_tokens(client, config, refresh_token).await {
            Ok(tokens) => return Ok(tokens),
            Err(error) => {
                warn!(%error, "refreshing STAC authentication tokens failed; trying password grant");
            }
        }
    }
    request_password_tokens(client, config).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use httptest::{
        Expectation, Server, all_of,
        matchers::{contains, request, url_decoded},
        responders,
    };

    #[test]
    fn optional_refresh_metadata() {
        for extra in [
            serde_json::json!({}),
            serde_json::json!({"refresh_expires_in": 0}),
        ] {
            let mut response = serde_json::json!({"access_token": "access", "expires_in": 100});
            response
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            let tokens: TokenResponse = serde_json::from_value(response).unwrap();
            assert!(tokens.refresh_token.is_none());
            assert_eq!(tokens.refresh_delay(), Duration::from_secs(80));
        }
    }

    #[tokio::test]
    async fn renew_with_optional_refresh_metadata() {
        let server = Server::run();
        let config = StacProviderAuthentication {
            endpoint: server.url_str("/token"),
            client_id: "client".into(),
            username: "user".into(),
            password: "password".into(),
        };
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/token"),
                request::body(url_decoded(contains(("grant_type", "password")))),
            ])
            .times(2)
            .respond_with(responders::json_encoded(serde_json::json!({
                "access_token": "password-access", "expires_in": 100
            }))),
        );
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/token"),
                request::body(url_decoded(all_of![
                    contains(("grant_type", "refresh_token")),
                    contains(("refresh_token", "refresh"))
                ])),
            ])
            .times(2)
            .respond_with(responders::json_encoded(serde_json::json!({
                "access_token": "refreshed-access", "expires_in": 100
            }))),
        );
        let client = reqwest::Client::new();
        let initial = request_password_tokens(&client, &config).await.unwrap();
        let renewed = renew_tokens(&client, &config, initial.refresh_token.as_deref())
            .await
            .unwrap();
        assert_eq!(renewed.access_token, "password-access");
        let refreshed = renew_tokens(&client, &config, Some("refresh"))
            .await
            .unwrap();
        let refreshed = renew_tokens(&client, &config, refreshed.refresh_token.as_deref())
            .await
            .unwrap();
        assert_eq!(refreshed.access_token, "refreshed-access");
        assert_eq!(refreshed.refresh_token.as_deref(), Some("refresh"));
    }
}
