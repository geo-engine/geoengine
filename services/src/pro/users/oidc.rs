use crate::contexts::Db;
use crate::error::{Error, Result};
use crate::pro::users::UserId;
use crate::pro::util::config::Oidc;
use geoengine_datatypes::error::ErrorSource;
use geoengine_datatypes::primitives::Duration;
use oauth2::basic::{BasicErrorResponseType, BasicRevocationErrorResponse, BasicTokenType};
use oauth2::{Scope, StandardRevocableToken};
use openidconnect::core::{
    CoreAuthDisplay, CoreAuthPrompt, CoreAuthenticationFlow, CoreClaimName, CoreClaimType,
    CoreClient, CoreClientAuthMethod, CoreGenderClaim, CoreGrantType, CoreJsonWebKey,
    CoreJsonWebKeyType, CoreJsonWebKeyUse, CoreJweContentEncryptionAlgorithm,
    CoreJweKeyManagementAlgorithm, CoreJwsSigningAlgorithm, CoreProviderMetadata, CoreResponseMode,
    CoreResponseType, CoreSubjectIdentifierType, CoreTokenIntrospectionResponse, CoreTokenResponse,
};
use openidconnect::reqwest::async_http_client;
use openidconnect::{
    AccessTokenHash, AuthorizationCode, Client, ClientId, ClientSecret, CsrfToken, DiscoveryError,
    EmptyAdditionalClaims, EmptyAdditionalProviderMetadata, IssuerUrl, Nonce,
    OAuth2TokenResponse, PkceCodeChallenge, PkceCodeVerifier, ProviderMetadata, RedirectUrl,
    ResponseTypes, StandardErrorResponse, SubjectIdentifier, TokenResponse,
};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::collections::HashMap;
use std::sync::Arc;
use url::{ParseError, Url};
#[cfg(test)]
use crate::pro::util::tests::mock_oidc::{SINGLE_NONCE, SINGLE_STATE};
#[cfg(test)]
use openidconnect::JsonWebKeySet;

pub type DefaultProviderMetadata = ProviderMetadata<
    EmptyAdditionalProviderMetadata,
    CoreAuthDisplay,
    CoreClientAuthMethod,
    CoreClaimName,
    CoreClaimType,
    CoreGrantType,
    CoreJweContentEncryptionAlgorithm,
    CoreJweKeyManagementAlgorithm,
    CoreJwsSigningAlgorithm,
    CoreJsonWebKeyType,
    CoreJsonWebKeyUse,
    CoreJsonWebKey,
    CoreResponseMode,
    CoreResponseType,
    CoreSubjectIdentifierType,
>;

#[cfg(test)]
pub type DefaultJsonWebKeySet =
    JsonWebKeySet<CoreJwsSigningAlgorithm, CoreJsonWebKeyType, CoreJsonWebKeyUse, CoreJsonWebKey>;

type DefaultClient = Client<
    EmptyAdditionalClaims,
    CoreAuthDisplay,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJwsSigningAlgorithm,
    CoreJsonWebKeyType,
    CoreJsonWebKeyUse,
    CoreJsonWebKey,
    CoreAuthPrompt,
    StandardErrorResponse<BasicErrorResponseType>,
    CoreTokenResponse,
    BasicTokenType,
    CoreTokenIntrospectionResponse,
    StandardRevocableToken,
    BasicRevocationErrorResponse,
>;

pub struct OidcRequestDb {
    issuer: String,
    client_id: String,
    client_secret: String,
    redirect_uri: String,
    scopes: Vec<String>,
    users: Db<HashMap<String, PendingRequest>>,
    state_function: fn() -> CsrfToken,
    nonce_function: fn() -> Nonce,
}

struct PendingRequest {
    nonce: Nonce, //TODO: Is nonce unnecessary in code flow?
    code_verifier: PkceCodeVerifier,
}

#[derive(Serialize, Deserialize)]
pub struct AuthCodeRequestURL {
    url: Url,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AuthCodeResponse {
    #[serde(rename = "sessionState")]
    pub session_state: String,
    pub code: String,
    pub state: String,
}

#[derive(Clone)]
pub struct ExternalUser {
    pub id: UserId,
    pub claims: ExternalUserClaims,
    pub active: bool,
}

#[derive(Clone)]
pub struct ExternalUserClaims {
    pub external_id: SubjectIdentifier,
    pub email: String,
    pub real_name: String,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))]
pub enum OidcError {
    OidcDisabled,
    IllegalProviderConfig {
        source: ParseError,
    },
    ProviderDiscoveryError {
        source: DiscoveryError<oauth2::reqwest::Error<reqwest::Error>>,
    },
    #[snafu(display("Illegal OIDC Provider: {}", reason))]
    IllegalProvider {
        reason: String,
    },
    #[snafu(display("Verification failed: {}", reason))]
    TokenExchangeError {
        reason: String,
        source: Box<dyn ErrorSource>,
    },
    #[snafu(display("Response error regarding field: {}, reason: {}", field, reason))]
    ResponseFieldError {
        field: String,
        reason: String,
    },
    #[snafu(display("Login failed: {}", reason))]
    LoginFailed {
        reason: String,
    },
}

impl From<DiscoveryError<oauth2::reqwest::Error<reqwest::Error>>> for OidcError {
    fn from(source: DiscoveryError<oauth2::reqwest::Error<reqwest::Error>>) -> Self {
        Self::ProviderDiscoveryError { source }
    }
}

impl From<ParseError> for OidcError {
    fn from(source: ParseError) -> Self {
        Self::IllegalProviderConfig { source }
    }
}

impl OidcRequestDb {
    pub async fn get_client(&self) -> Result<DefaultClient, OidcError> {
        let issuer_url = IssuerUrl::new(self.issuer.to_string())?;

        //TODO: Provider meta data could be added as a fixed field in the DB, making discovery a one-time process. This would have implications for server startup.
        let provider_metadata: DefaultProviderMetadata =
            CoreProviderMetadata::discover_async(issuer_url, async_http_client).await?;

        let response_types_supported = provider_metadata.response_types_supported();
        if !response_types_supported.contains(&ResponseTypes::new(vec![CoreResponseType::Code])) {
            return Err(OidcError::IllegalProvider {
                reason: "provider does not support authorization code flow".to_string(),
            });
        }

        let signing_alg = provider_metadata.id_token_signing_alg_values_supported();
        if !signing_alg.contains(&CoreJwsSigningAlgorithm::RsaSsaPssSha256) {
            return Err(OidcError::IllegalProvider {
                reason: "provider does not support RSA signing".to_string(),
            });
        }

        let scopes_supported =
            provider_metadata
                .scopes_supported()
                .ok_or(OidcError::IllegalProvider {
                    reason: "provider does not support any scopes".to_string(),
                })?;
        for scope in &self.scopes {
            if !scopes_supported.contains(&Scope::new(scope.clone())) {
                return Err(OidcError::IllegalProvider {
                    reason: format!("provider does not support requested scope: '{}'", scope),
                });
            }
        }

        //Currently, we expect e-mail and real-name to be present claims to match required fields for internal users.
        let claims_supported =
            provider_metadata
                .claims_supported()
                .ok_or(OidcError::IllegalProvider {
                    reason: "provider does not support any claims".to_string(),
                })?;
        if !claims_supported.contains(&CoreClaimName::new("email".to_string())) {
            return Err(OidcError::IllegalProvider {
                reason: "provider does not support required claim: email".to_string(),
            });
        }
        if !claims_supported.contains(&CoreClaimName::new("name".to_string())) {
            return Err(OidcError::IllegalProvider {
                reason: "provider does not support required claim: name".to_string(),
            });
        }

        let result = CoreClient::from_provider_metadata(
            provider_metadata,
            ClientId::new(self.client_id.to_string()),
            Some(ClientSecret::new(self.client_secret.to_string())),
        ) //TODO: Could be made optional.
        .set_redirect_uri(RedirectUrl::new(self.redirect_uri.to_string())?);

        Ok(result)
    }

    async fn generate_unique_state_and_insert(
        &self,
        client: &DefaultClient,
    ) -> Result<Url, OidcError> {
        let mut auth_request = client.authorize_url(
            CoreAuthenticationFlow::AuthorizationCode,
            self.state_function,
            self.nonce_function,
        );

        for scope in &self.scopes {
            auth_request = auth_request.add_scope(Scope::new(scope.clone()));
        }

        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

        let (auth_url, csrf_token, nonce) = auth_request.set_pkce_challenge(pkce_challenge).url();

        let mut user_db = self.users.write().await;

        if user_db.contains_key(csrf_token.secret()) {
            return Err(OidcError::LoginFailed {
                reason: "Failed to generate unique state".to_string(),
            });
        }

        user_db.insert(
            csrf_token.secret().clone(),
            PendingRequest {
                nonce,
                code_verifier: pkce_verifier,
            },
        );

        Ok(auth_url)
    }

    pub async fn generate_request(
        &self,
        client: DefaultClient,
    ) -> Result<AuthCodeRequestURL, OidcError> {
        let mut result = self.generate_unique_state_and_insert(&client).await;
        let mut max_retries = 5;

        while result.is_err() && max_retries > 0 {
            result = self.generate_unique_state_and_insert(&client).await;
            max_retries -= 1;
        }

        let url = result?;

        Ok(AuthCodeRequestURL { url })
    }

    pub async fn resolve_request(
        &self,
        client: DefaultClient,
        auth_code_response: AuthCodeResponse,
    ) -> Result<(ExternalUserClaims, Duration), OidcError> {
        let mut user_db = self.users.write().await;
        let pending_request =
            user_db
                .remove(&auth_code_response.state)
                .ok_or(OidcError::LoginFailed {
                    reason: "Request unknown".to_string(),
                })?;

        let token_response = client
            .exchange_code(AuthorizationCode::new(auth_code_response.code))
            .set_pkce_verifier(pending_request.code_verifier)
            .request_async(async_http_client)
            .await
            .map_err(|token_error| OidcError::TokenExchangeError {
                reason: "Request for code to token exchange failed".to_string(),
                source: Box::new(token_error),
            })?;

        let id_token = token_response
            .id_token()
            .ok_or_else(|| OidcError::ResponseFieldError {
                field: "id token".to_string(),
                reason: "missing".to_string(),
            })?;

        let claims = id_token
            .claims(&client.id_token_verifier(), &pending_request.nonce)
            .map_err(|claims_error| OidcError::TokenExchangeError {
                reason: "Failed to verify claims".to_string(),
                source: Box::new(claims_error),
            })?;

        if let Some(expected_access_token_hash) = claims.access_token_hash() {
            let actual_access_token_hash = AccessTokenHash::from_token(
                token_response.access_token(),
                &id_token
                    .signing_alg()
                    .map_err(|signing_error| OidcError::TokenExchangeError {
                        reason: "Unsupported Signing Algorithm".to_string(),
                        source: Box::new(signing_error),
                    })?,
            )
            .map_err(|signing_error| OidcError::TokenExchangeError {
                reason: "Unsupported Signing Algorithm".to_string(),
                source: Box::new(signing_error),
            })?;
            if actual_access_token_hash != *expected_access_token_hash {
                return Err(OidcError::ResponseFieldError {
                    field: "access token".to_string(),
                    reason: "wrong hash".to_string(),
                });
            }
        }

        //Currently, we expect e-mail and real-name to be present claims to match required fields for internal users.
        let user = ExternalUserClaims {
            external_id: claims.subject().clone(),
            email: match claims.email() {
                None => Err(OidcError::ResponseFieldError {
                    field: "e-mail".to_string(),
                    reason: "missing".to_string(),
                }),
                Some(x) => Ok(x.to_string()),
            }?,
            real_name: match claims.name() {
                None => Err(OidcError::ResponseFieldError {
                    field: "name".to_string(),
                    reason: "missing".to_string(),
                }),
                Some(x) => Ok(x.get(None).unwrap().to_string()), //TODO: There is no Local logic.
            }?,
        };

        let validity = match token_response.expires_in() {
            None => Err(OidcError::ResponseFieldError {
                field: "duration".to_string(),
                reason: "missing".to_string(),
            }),
            Some(x) => Ok(x),
        }?;

        Ok((user, Duration::milliseconds(validity.as_millis() as i64))) //TODO: Consider allowing u128 for durations to avoid cast.
    }

    #[cfg(test)]
    pub(in crate::pro) fn from_oidc_with_static_tokens(value: Oidc) -> Self {
        OidcRequestDb {
            issuer: value.issuer.to_string(),
            client_id: value.client_id.to_string(),
            client_secret: value.client_secret.to_string(),
            redirect_uri: value.redirect_uri.to_string(),
            scopes: value.scopes,
            users: Arc::new(Default::default()),
            state_function: || CsrfToken::new(SINGLE_STATE.to_string()),
            nonce_function: || Nonce::new(SINGLE_NONCE.to_string()),
        }
    }
}

impl TryFrom<Oidc> for OidcRequestDb {
    type Error = Error;

    fn try_from(value: Oidc) -> Result<Self, Self::Error> {
        if value.enabled {
            let db = OidcRequestDb {
                issuer: value.issuer.to_string(),
                client_id: value.client_id.to_string(),
                client_secret: value.client_secret.to_string(),
                redirect_uri: value.redirect_uri.to_string(),
                scopes: value.scopes,
                users: Arc::new(Default::default()),
                state_function: CsrfToken::new_random,
                nonce_function: Nonce::new_random,
            };
            Ok(db)
        } else {
            Err(Error::OidcError {
                source: OidcError::OidcDisabled,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::{Error, Result};
    use crate::pro::users::oidc::OidcError::{
        IllegalProvider, LoginFailed, ProviderDiscoveryError, ResponseFieldError,
        TokenExchangeError,
    };
    use crate::pro::users::oidc::{
        AuthCodeResponse, DefaultClient, DefaultJsonWebKeySet, DefaultProviderMetadata,
        OidcRequestDb,
    };
    use crate::pro::util::tests::mock_oidc::{
        mock_jwks, mock_provider_metadata, mock_token_response, MockTokenConfig, SINGLE_NONCE,
        SINGLE_STATE,
    };
    use httptest::matchers::request;
    use httptest::responders::status_code;
    use httptest::{Expectation, Server};
    use oauth2::basic::BasicTokenType;
    use oauth2::{
        AccessToken, ClientId, ClientSecret, CsrfToken, EmptyExtraTokenFields, RedirectUrl,
        StandardTokenResponse,
    };
    use openidconnect::core::{CoreClient, CoreIdTokenFields, CoreTokenResponse, CoreTokenType};
    use openidconnect::Nonce;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    const ALTERNATIVE_ACCESS_TOKEN: &str = "DUMMY_ACCESS_TOKEN_2";
    const ISSUER_URL: &str = "https://dummy-issuer.com/";
    const REDIRECT_URI: &str = "https://dummy-redirect.com/";

    fn single_state_nonce_request_db() -> OidcRequestDb {
        OidcRequestDb {
            issuer: ISSUER_URL.to_string(),
            client_id: "DummyClient".to_string(),
            client_secret: "DummySecret".to_string(),
            redirect_uri: REDIRECT_URI.to_string(),
            scopes: vec!["profile".to_string(), "email".to_string()],
            users: Arc::new(Default::default()),
            state_function: || CsrfToken::new(SINGLE_STATE.to_string()),
            nonce_function: || Nonce::new(SINGLE_NONCE.to_string()),
        }
    }

    fn single_state_nonce_mocked_request_db(server_url: String) -> OidcRequestDb {
        OidcRequestDb {
            issuer: server_url,
            client_id: "".to_string(),
            client_secret: "".to_string(),
            redirect_uri: REDIRECT_URI.to_string(),
            scopes: vec!["profile".to_string(), "email".to_string()],
            users: Arc::new(Default::default()),
            state_function: || CsrfToken::new(SINGLE_STATE.to_string()),
            nonce_function: || Nonce::new(SINGLE_NONCE.to_string()),
        }
    }

    fn mock_client(request_db: &OidcRequestDb) -> Result<DefaultClient, Error> {
        let client_id = request_db.client_id.clone();
        let client_secret = request_db.client_secret.clone();
        let redirect_uri = request_db.redirect_uri.clone();

        let provider_metadata =
            mock_provider_metadata(request_db.issuer.as_str()).set_jwks(mock_jwks());

        let result = CoreClient::from_provider_metadata(
            provider_metadata,
            ClientId::new(client_id),
            Some(ClientSecret::new(client_secret)),
        )
        .set_redirect_uri(RedirectUrl::new(redirect_uri)?);

        Ok(result)
    }

    fn mock_provider_discovery(
        server: &Server,
        provider_metadata: &DefaultProviderMetadata,
        jwks: &DefaultJsonWebKeySet,
    ) {
        server.expect(
            Expectation::matching(request::method_path(
                "GET",
                "/.well-known/openid-configuration",
            ))
            .respond_with(
                status_code(200)
                    .insert_header("content-type", "application/json")
                    .body(serde_json::to_string(provider_metadata).unwrap()),
            ),
        );

        server.expect(
            Expectation::matching(request::method_path("GET", "/jwk")).respond_with(
                status_code(200)
                    .insert_header("content-type", "application/json")
                    .body(serde_json::to_string(jwks).unwrap()),
            ),
        );
    }

    fn mock_valid_request(
        server: &Server,
        token_response: &StandardTokenResponse<CoreIdTokenFields, BasicTokenType>,
    ) {
        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                status_code(200)
                    .insert_header("content-type", "application/json")
                    .body(serde_json::to_string(token_response).unwrap()),
            ),
        );
    }

    fn auth_code_response_empty_with_valid_state() -> AuthCodeResponse {
        AuthCodeResponse {
            session_state: "".to_string(),
            code: "".to_string(),
            state: SINGLE_STATE.to_string(),
        }
    }

    #[tokio::test]
    async fn get_client_success() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);

        let provider_metadata = mock_provider_metadata(request_db.issuer.as_str());
        let jwks = mock_jwks();

        mock_provider_discovery(&server, &provider_metadata, &jwks);

        let client = request_db.get_client().await;

        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn get_client_bad_request() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);

        let error_message = serde_json::to_string(&json!({
            "error_description": "Dummy bad request",
            "error": "catch_all_error"
        }))
        .expect("Serde Json unsuccessful");

        server.expect(
            Expectation::matching(request::method_path(
                "GET",
                "/.well-known/openid-configuration",
            ))
            .respond_with(
                status_code(404)
                    .insert_header("content-type", "application/json")
                    .body(error_message),
            ),
        );

        let client = request_db.get_client().await;

        assert!(matches!(client, Err(ProviderDiscoveryError { source: _ })));
    }

    #[tokio::test]
    async fn get_client_auth_code_unsupported() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);

        let provider_metadata =
            mock_provider_metadata(request_db.issuer.as_str()).set_response_types_supported(vec![]);
        let jwks = mock_jwks();

        mock_provider_discovery(&server, &provider_metadata, &jwks);

        let client = request_db.get_client().await;

        assert!(
            matches!(client, Err(IllegalProvider{reason}) if reason == "provider does not support authorization code flow")
        );
    }

    #[tokio::test]
    async fn get_client_id_rsa_signing_unsupported() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);

        let provider_metadata = mock_provider_metadata(request_db.issuer.as_str())
            .set_id_token_signing_alg_values_supported(vec![]);
        let jwks = mock_jwks();

        mock_provider_discovery(&server, &provider_metadata, &jwks);

        let client = request_db.get_client().await;

        assert!(
            matches!(client, Err(IllegalProvider{reason}) if reason == "provider does not support RSA signing")
        );
    }

    #[tokio::test]
    async fn get_client_missing_scopes() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);

        let provider_metadata =
            mock_provider_metadata(request_db.issuer.as_str()).set_scopes_supported(None);
        let jwks = mock_jwks();

        mock_provider_discovery(&server, &provider_metadata, &jwks);

        let client = request_db.get_client().await;

        assert!(
            matches!(client, Err(IllegalProvider{reason}) if reason == "provider does not support any scopes")
        );
    }

    #[tokio::test]
    async fn get_client_missing_claims() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);

        let provider_metadata =
            mock_provider_metadata(request_db.issuer.as_str()).set_claims_supported(None);
        let jwks = mock_jwks();

        mock_provider_discovery(&server, &provider_metadata, &jwks);

        let client = request_db.get_client().await;

        assert!(
            matches!(client, Err(IllegalProvider{reason}) if reason == "provider does not support any claims")
        );
    }

    //TODO: Did not test illegal config (e.g., provider url cannot be parsed).

    #[tokio::test]
    async fn generate_request_success() {
        let request_db = OidcRequestDb {
            issuer: ISSUER_URL.to_owned() + "oidc/test",
            client_id: "DummyClient".to_string(),
            client_secret: "DummySecret".to_string(),
            redirect_uri: REDIRECT_URI.to_string(),
            scopes: vec!["profile".to_string(), "email".to_string()],
            users: Arc::new(Default::default()),
            state_function: || CsrfToken::new(SINGLE_STATE.to_string()),
            nonce_function: || Nonce::new(SINGLE_NONCE.to_string()),
        };

        let client = mock_client(&request_db).unwrap();

        let url = request_db.generate_request(client).await.unwrap().url;

        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), Some("dummy-issuer.com"));
        assert_eq!(url.port(), None);
        assert_eq!(url.path(), "/oidc/test/authorize");
        assert_eq!(url.query_pairs().count(), 8);

        let query_map: HashMap<_, _> = url
            .query_pairs()
            .into_iter()
            .map(|(x, y)| (x.to_string(), y.to_string()))
            .collect();

        assert!(query_map.get("state").is_some());
        assert!(query_map.get("nonce").is_some());
        assert!(query_map.get("code_challenge").is_some());

        assert_eq!(query_map.get("client_id"), Some(&"DummyClient".to_string()));
        assert_eq!(
            query_map.get("redirect_uri"),
            Some(&REDIRECT_URI.to_string())
        );
        assert_eq!(query_map.get("response_type"), Some(&"code".to_string()));
        assert_eq!(
            query_map.get("scope"),
            Some(&"openid profile email".to_string())
        );
        assert_eq!(
            query_map.get("code_challenge_method"),
            Some(&"S256".to_string())
        );
    }

    #[tokio::test]
    async fn generate_request_duplicate_state() {
        let request_db = single_state_nonce_request_db();
        let client = mock_client(&request_db).unwrap();

        let first_request = request_db.generate_request(client.clone()).await;

        assert!(first_request.is_ok());

        let second_request = request_db.generate_request(client).await;

        assert!(
            matches!(second_request, Err(LoginFailed{reason}) if reason == "Failed to generate unique state")
        );
    }

    #[tokio::test]
    async fn resolve_request_success() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mock_token_config = MockTokenConfig::create_from_issuer_and_client(
            request_db.issuer.clone(),
            request_db.client_id.clone(),
        );
        let token_response = mock_token_response(mock_token_config);
        mock_valid_request(&server, &token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn resolve_request_failed_empty_db() {
        let request_db = single_state_nonce_request_db();
        let client = mock_client(&request_db).unwrap();

        let auth_code_response = AuthCodeResponse {
            session_state: "".to_string(),
            code: "".to_string(),
            state: "Illegal Request State".to_string(),
        };

        let response = request_db.resolve_request(client, auth_code_response).await;
        assert!(matches!(response, Err(LoginFailed{reason}) if reason == "Request unknown"));
    }

    #[tokio::test]
    async fn resolve_request_failed_not_found() {
        let request_db = single_state_nonce_request_db();
        let client = mock_client(&request_db).unwrap();

        let request = request_db.generate_request(client.clone()).await;

        assert!(request.is_ok());

        let auth_code_response = AuthCodeResponse {
            session_state: "".to_string(),
            code: "".to_string(),
            state: "Illegal Request State".to_string(),
        };

        let response = request_db.resolve_request(client, auth_code_response).await;
        assert!(matches!(response, Err(LoginFailed{reason}) if reason == "Request unknown"));
    }

    #[tokio::test]
    async fn resolve_request_no_id_token() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);

        let mock_token_config = MockTokenConfig::create_from_issuer_and_client(
            request_db.issuer.clone(),
            request_db.client_id.clone(),
        );
        let mut token_response = CoreTokenResponse::new(
            AccessToken::new(mock_token_config.access),
            CoreTokenType::Bearer,
            CoreIdTokenFields::new(None, EmptyExtraTokenFields {}),
        );
        token_response.set_expires_in(mock_token_config.duration.as_ref());
        mock_valid_request(&server, &token_response);

        let client = mock_client(&request_db).unwrap();

        let request = request_db.generate_request(client.clone()).await;

        assert!(request.is_ok());

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(
            matches!(response, Err(ResponseFieldError{field, reason}) if field == "id token" && reason == "missing")
        );
    }

    #[tokio::test]
    async fn resolve_request_no_nonce() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(
            request_db.issuer.clone(),
            request_db.client_id.clone(),
        );
        mock_token_config.nonce = None;
        let token_response = mock_token_response(mock_token_config);
        mock_valid_request(&server, &token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(
            matches!(response, Err(TokenExchangeError{reason, source: _}) if reason == "Failed to verify claims")
        );
    }

    #[tokio::test]
    async fn resolve_request_wrong_nonce() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(
            request_db.issuer.clone(),
            request_db.client_id.clone(),
        );
        mock_token_config.nonce = Some(Nonce::new("Wrong Nonce".to_string()));
        let token_response = mock_token_response(mock_token_config);
        mock_valid_request(&server, &token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(
            matches!(response, Err(TokenExchangeError{reason, source: _}) if reason == "Failed to verify claims")
        );
    }

    #[tokio::test]
    async fn resolve_request_no_email() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(
            request_db.issuer.clone(),
            request_db.client_id.clone(),
        );
        mock_token_config.email = None;
        let token_response = mock_token_response(mock_token_config);
        mock_valid_request(&server, &token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(
            matches!(response, Err(ResponseFieldError{field, reason}) if field == "e-mail" && reason == "missing")
        );
    }

    #[tokio::test]
    async fn resolve_request_no_name() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(
            request_db.issuer.clone(),
            request_db.client_id.clone(),
        );
        mock_token_config.name = None;
        let token_response = mock_token_response(mock_token_config);
        mock_valid_request(&server, &token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(
            matches!(response, Err(ResponseFieldError{field, reason}) if field == "name" && reason == "missing")
        );
    }

    #[tokio::test]
    async fn resolve_request_no_access_token_duration() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(
            request_db.issuer.clone(),
            request_db.client_id.clone(),
        );
        mock_token_config.duration = None;
        let token_response = mock_token_response(mock_token_config);
        mock_valid_request(&server, &token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(
            matches!(response, Err(ResponseFieldError{field, reason}) if field == "duration" && reason == "missing")
        );
    }

    #[tokio::test]
    async fn resolve_request_access_hashcode_mismatch() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(
            request_db.issuer.clone(),
            request_db.client_id.clone(),
        );
        assert_ne!(mock_token_config.access_for_id, ALTERNATIVE_ACCESS_TOKEN);
        mock_token_config.access_for_id = ALTERNATIVE_ACCESS_TOKEN.to_string();

        let token_response = mock_token_response(mock_token_config);
        mock_valid_request(&server, &token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(
            matches!(response, Err(ResponseFieldError{field, reason}) if field == "access token" && reason == "wrong hash")
        );
    }

    #[tokio::test]
    async fn resolve_request_twice() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mock_token_config = MockTokenConfig::create_from_issuer_and_client(
            request_db.issuer.clone(),
            request_db.client_id.clone(),
        );
        let token_response = mock_token_response(mock_token_config);
        mock_valid_request(&server, &token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db
            .resolve_request(client.clone(), auth_code_response)
            .await;

        assert!(response.is_ok());

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(matches!(response, Err(LoginFailed{reason}) if reason == "Request unknown"));
    }

    #[tokio::test]
    async fn resolve_multiple_requests() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let mut request_db = single_state_nonce_mocked_request_db(server_url);
        let client = mock_client(&request_db).unwrap();

        //TODO: Not sure how to do multiple requests deterministically in a good way.
        let state_functions = [
            || CsrfToken::new("State_1".to_string()),
            || CsrfToken::new("State_2".to_string()),
            || CsrfToken::new("State_3".to_string()),
            || CsrfToken::new("State_4".to_string()),
            || CsrfToken::new("State_5".to_string()),
            || CsrfToken::new("State_6".to_string()),
            || CsrfToken::new("State_7".to_string()),
            || CsrfToken::new("State_8".to_string()),
            || CsrfToken::new("State_9".to_string()),
            || CsrfToken::new("State_10".to_string()),
        ];

        let nonce_functions = [
            || Nonce::new("Nonce_1".to_string()),
            || Nonce::new("Nonce_2".to_string()),
            || Nonce::new("Nonce_3".to_string()),
            || Nonce::new("Nonce_4".to_string()),
            || Nonce::new("Nonce_5".to_string()),
            || Nonce::new("Nonce_6".to_string()),
            || Nonce::new("Nonce_7".to_string()),
            || Nonce::new("Nonce_8".to_string()),
            || Nonce::new("Nonce_9".to_string()),
            || Nonce::new("Nonce_10".to_string()),
        ];

        let query_qualifiers = ["7", "3", "2", "4", "8", "9", "10", "1", "5", "6"];

        for i in 0..10 {
            request_db.state_function = state_functions[i];
            request_db.nonce_function = nonce_functions[i];

            let request_result = request_db.generate_request(client.clone()).await;

            assert!(request_result.is_ok());
        }

        for query_qualifier in query_qualifiers {
            let state = "State_".to_owned() + query_qualifier;
            let nonce = Some(Nonce::new("Nonce_".to_owned() + query_qualifier));

            let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(
                request_db.issuer.clone(),
                request_db.client_id.clone(),
            );
            mock_token_config.nonce = nonce;
            let token_response = mock_token_response(mock_token_config);
            mock_valid_request(&server, &token_response);

            let mut auth_code_response = auth_code_response_empty_with_valid_state();
            auth_code_response.state = state;
            let response = request_db
                .resolve_request(client.clone(), auth_code_response)
                .await;

            assert!(response.is_ok());
        }
    }

    #[tokio::test]
    async fn resolve_bad_request() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        //TODO: Maybe search for more detailed error types and test/display them more gracefully.
        let error_message = serde_json::to_string(&json!({
            "error_description": "Dummy bad request",
            "error": "catch_all_error"
        }))
        .expect("Serde Json unsuccessful");

        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                status_code(404)
                    .insert_header("content-type", "application/json")
                    .body(error_message),
            ),
        );

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(
            matches!(response, Err(TokenExchangeError{reason, source: _}) if reason == "Request for code to token exchange failed")
        );
    }

    #[tokio::test]
    async fn resolve_after_bad_request() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_mocked_request_db(server_url);
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let error_message = serde_json::to_string(&json!({
            "error_description": "Dummy bad request",
            "error": "catch_all_error"
        }))
        .expect("Serde Json unsuccessful");

        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                status_code(404)
                    .insert_header("content-type", "application/json")
                    .body(error_message),
            ),
        );

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response_bad = request_db
            .resolve_request(client.clone(), auth_code_response)
            .await;

        assert!(
            matches!(response_bad, Err(TokenExchangeError{reason, source: _}) if reason == "Request for code to token exchange failed")
        );

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(matches!(response, Err(LoginFailed{reason}) if reason == "Request unknown"));
    }

    //TODO: Did not test code and PKCE verifier.
}
