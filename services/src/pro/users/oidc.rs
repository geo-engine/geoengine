use std::collections::HashMap;
use std::sync::Arc;
use oauth2::basic::{BasicErrorResponseType, BasicRevocationErrorResponse, BasicTokenType};
use oauth2::{Scope, StandardRevocableToken};
use openidconnect::reqwest::async_http_client;
use openidconnect::{AccessTokenHash, AuthorizationCode, Client, ClientId, ClientSecret, CsrfToken, EmptyAdditionalClaims, EmptyAdditionalProviderMetadata, IssuerUrl, JsonWebKeySet, Nonce, OAuth2TokenResponse, PkceCodeChallenge, PkceCodeVerifier, ProviderMetadata, RedirectUrl, ResponseTypes, StandardErrorResponse, SubjectIdentifier, TokenResponse};
use openidconnect::core::{CoreAuthDisplay, CoreAuthenticationFlow, CoreAuthPrompt, CoreClaimName, CoreClaimType, CoreClient, CoreClientAuthMethod, CoreGenderClaim, CoreGrantType, CoreJsonWebKey, CoreJsonWebKeyType, CoreJsonWebKeyUse, CoreJweContentEncryptionAlgorithm, CoreJweKeyManagementAlgorithm, CoreJwsSigningAlgorithm, CoreProviderMetadata, CoreResponseMode, CoreResponseType, CoreSubjectIdentifierType, CoreTokenIntrospectionResponse, CoreTokenResponse};
use serde::{Serialize, Deserialize};
use url::Url;
use geoengine_datatypes::primitives::Duration;
use crate::contexts::Db;
use crate::error::Error::{IllegalOIDCLoginRequest, OIDCLoginFailed, IllegalOIDCProviderConfig};
use crate::pro::users::UserId;
use crate::error::Result;
use crate::error::Error;
use crate::pro::util::config::Oidc;
use crate::pro::util::tests::{SINGLE_NONCE, SINGLE_STATE};

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
    CoreSubjectIdentifierType
>;

pub type DefaultJsonWebKeySet = JsonWebKeySet<
    CoreJwsSigningAlgorithm,
    CoreJsonWebKeyType,
    CoreJsonWebKeyUse,
    CoreJsonWebKey
>;

pub type DefaultClient = Client<
    EmptyAdditionalClaims,
    CoreAuthDisplay,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJwsSigningAlgorithm, CoreJsonWebKeyType,
    CoreJsonWebKeyUse,
    CoreJsonWebKey,
    CoreAuthPrompt,
    StandardErrorResponse<BasicErrorResponseType>,
    CoreTokenResponse,
    BasicTokenType,
    CoreTokenIntrospectionResponse,
    StandardRevocableToken,
    BasicRevocationErrorResponse
>;


pub struct OIDCRequestsDB {
    issuer : String,
    client_id : String,
    client_secret : String,
    redirect_uri : String,
    scopes : Vec<String>,
    users: Db<HashMap<String, PendingRequest>>,
    state_function : fn() -> CsrfToken,
    nonce_function : fn() -> Nonce,
}

pub struct PendingRequest {
    nonce : Nonce, //TODO: Is nonce unnecessary in code flow?
    code_verifier : PkceCodeVerifier,
}

#[derive(Serialize, Deserialize)]
pub struct AuthCodeRequestURL{
    url : Url,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AuthCodeResponse {
    pub session_state : String,
    pub code : String,
    pub state : String,
}

#[derive(Clone)]
pub struct ExternalUser {
    pub id: UserId,
    pub claims : ExternalUserClaims,
    pub active: bool,
}

#[derive(Clone)]
pub struct ExternalUserClaims {
    pub external_id: SubjectIdentifier,
    pub email: String,
    pub real_name: String,
}

impl OIDCRequestsDB {

    pub async fn get_client(&self) -> Result<DefaultClient, Error>
    {
        let issuer_url = IssuerUrl::new(self.issuer.to_string()).unwrap();

        //TODO: Provider meta data could be added as a fixed field in the DB, making discovery a one-time process. This would have implications for server startup.
        let provider_metadata : DefaultProviderMetadata = CoreProviderMetadata::discover_async(issuer_url, async_http_client)
                .await
                .map_err(|_discovery_error| IllegalOIDCProviderConfig)?;

        let response_types_supported = provider_metadata.response_types_supported();
        if !response_types_supported.contains(&ResponseTypes::new(vec![CoreResponseType::Code])) {
            return Err(IllegalOIDCProviderConfig);
        }

        let signing_alg = provider_metadata.id_token_signing_alg_values_supported();
        if !signing_alg.contains(&CoreJwsSigningAlgorithm::RsaSsaPssSha256) {
            return Err(IllegalOIDCProviderConfig);
        }

        let scopes_supported = provider_metadata.scopes_supported().ok_or(IllegalOIDCProviderConfig)?;
        for scope in &self.scopes {
            if !scopes_supported.contains(&Scope::new(scope.clone())) {
                return Err(IllegalOIDCProviderConfig);
            }
        };

        let claims_supported = provider_metadata.claims_supported().ok_or(IllegalOIDCProviderConfig)?;
        if !claims_supported.contains(&CoreClaimName::new("email".to_string())) ||
            !claims_supported.contains(&CoreClaimName::new("name".to_string())) {
            return Err(IllegalOIDCProviderConfig);
        }

        let result = CoreClient::from_provider_metadata(
            provider_metadata,
            ClientId::new(self.client_id.to_string()),
            Some(ClientSecret::new(self.client_secret.to_string()))) //TODO: Think about client secret
            .set_redirect_uri(RedirectUrl::new(self.redirect_uri.to_string()).map_err(|_parser_error| IllegalOIDCProviderConfig)?);

        Ok(result)
    }

    pub async fn generate_request(&self, client : DefaultClient) -> Result<AuthCodeRequestURL, Error> {
        let mut user_db = self.users.write().await;

        let mut auth_request = client
            .authorize_url(
                CoreAuthenticationFlow::AuthorizationCode,
                self.state_function,
                self.nonce_function,
            );

        for scope in &self.scopes {
            auth_request = auth_request.add_scope(Scope::new(scope.clone()));
        };

        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

        let (auth_url, csrf_token, nonce) = auth_request
            .set_pkce_challenge(pkce_challenge)
            .url();

        //TODO: Alternative for error would be a loop, but that would make a theoretical endless loop possible.
        //TODO: There might be the possibility of guessing states here, which might be a security flaw, but I don't see it.
        if user_db.contains_key(csrf_token.secret()) {
            return Err(OIDCLoginFailed {
                reason: "Generated duplicate state".to_string()
            });
        }

        user_db.insert(csrf_token.secret().clone(), PendingRequest{
            nonce,
            code_verifier: pkce_verifier,
        });

        Ok(AuthCodeRequestURL{url : auth_url})
    }

    pub async fn resolve_request(&self, client: DefaultClient, auth_code_response: AuthCodeResponse) -> Result<(ExternalUserClaims, Duration), Error> {
        let mut user_db = self.users.write().await;
        let pending_request = user_db.remove(&auth_code_response.state).ok_or(IllegalOIDCLoginRequest)?;

        let token_response = client.exchange_code(AuthorizationCode::new(auth_code_response.code))
            .set_pkce_verifier(pending_request.code_verifier)
            .request_async(async_http_client)
            .await
            .map_err(|_token_error| OIDCLoginFailed {
                reason: "Failed code to token exchange".to_string()
            })?;

        let id_token = token_response
            .id_token()
            .ok_or_else(|| OIDCLoginFailed {
                reason: "No id token returned".to_string()
            })?;

        let claims = id_token.claims(&client.id_token_verifier(), &pending_request.nonce)
            .map_err(|_claims_error| OIDCLoginFailed {
                reason: "Failed to verify claims".to_string()
            })?;

        if let Some(expected_access_token_hash) = claims.access_token_hash() {
            let actual_access_token_hash = AccessTokenHash::from_token(
                token_response.access_token(),
                &id_token.signing_alg()
                    .map_err(|_signing_error| OIDCLoginFailed { reason: "Unsupported Signing Algorithm".to_string()})?
            ).map_err(|_signing_error| OIDCLoginFailed {
                reason: "Unsupported Signing Algorithm".to_string()
            })?;
            if actual_access_token_hash != *expected_access_token_hash {
                return Err(OIDCLoginFailed {
                    reason: "Illegal access token".to_string()
                });
            }
        }

        //TODO: Failing at missing claims seems unnecessary for external users, but UI expects email and real name.
        let user = ExternalUserClaims {
            external_id: claims.subject().clone(),
            email: match claims.email() {
                None => {Err(OIDCLoginFailed { reason: "Missing e-mail claim".to_string()})}
                Some(x) => {Ok(x.to_string())}
            }?,
            real_name: match claims.name() {
                None => {Err(OIDCLoginFailed { reason: "Missing name claim".to_string()})}
                Some(x) => {Ok(x.get(None).unwrap().to_string())} //TODO: There is no Local logic.
            }?
        };

        let validity = match token_response.expires_in(){
            None => {Err(OIDCLoginFailed { reason: "No Duration in token".to_string() })}
            Some(x) => {Ok(x)}
        }?;

        Ok((user, Duration::milliseconds(validity.as_millis() as i64))) //TODO: Is that cast ok, how does Geo Engine handle conversions between durations?
    }

    //TODO: Is there a better way to do that for testing? Seems hacky.
    pub fn from_oidc_with_static_tokens(value: Oidc) -> Self {
        OIDCRequestsDB {
            issuer: value.issuer.to_string(),
            client_id: value.client_id.to_string(),
            client_secret: value.client_secret.to_string(),
            redirect_uri: value.redirect_uri.to_string(),
            scopes: value.scopes,
            users: Arc::new(Default::default()),
            state_function: || CsrfToken::new(SINGLE_STATE.to_string()),
            nonce_function: || Nonce::new(SINGLE_NONCE.to_string())
        }
    }
}

impl Default for OIDCRequestsDB {

    fn default() -> Self {
        OIDCRequestsDB {
            issuer: "".to_string(),
            client_id: "".to_string(),
            client_secret: "".to_string(),
            redirect_uri: "".to_string(),
            scopes: vec![],
            users: Arc::new(Default::default()),
            state_function: CsrfToken::new_random,
            nonce_function: Nonce::new_random
        }
    }

}

impl From<Oidc> for OIDCRequestsDB {
    fn from(value: Oidc) -> Self {
        OIDCRequestsDB {
            issuer: value.issuer.to_string(),
            client_id: value.client_id.to_string(),
            client_secret: value.client_secret.to_string(),
            redirect_uri: value.redirect_uri.to_string(),
            scopes: value.scopes,
            users: Arc::new(Default::default()),
            state_function: CsrfToken::new_random,
            nonce_function: Nonce::new_random,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use mockito::{mock, Mock};
    use oauth2::{AccessToken, ClientId, ClientSecret, CsrfToken, EmptyExtraTokenFields, RedirectUrl, StandardTokenResponse};
    use oauth2::basic::BasicTokenType;
    use openidconnect::core::{CoreClient, CoreIdTokenFields, CoreTokenResponse, CoreTokenType};
    use openidconnect::Nonce;
    use crate::error::{Result, Error};
    use crate::pro::users::oidc::{AuthCodeResponse, DefaultClient, ExternalUserClaims, OIDCRequestsDB};
    use crate::pro::util::tests::{mock_jwks, mock_provider_metadata, mock_token_response, MockTokenConfig, SINGLE_NONCE, SINGLE_STATE};

    const ALTERNATIVE_ACCESS_TOKEN: &str = "DUMMY_ACCESS_TOKEN_2";
    const ISSUER_URL : &str = "https://dummy-issuer.com/";
    const REDIRECT_URI : &str = "https://dummy-redirect.com/";

    fn single_state_nonce_request_db() -> OIDCRequestsDB{
        OIDCRequestsDB {
            issuer: ISSUER_URL.to_string(),
            client_id: "DummyClient".to_string(),
            client_secret: "DummySecret".to_string(),
            redirect_uri: REDIRECT_URI.to_string(),
            scopes: vec!["profile".to_string(), "email".to_string()],
            users: Arc::new(Default::default()),
            state_function: || CsrfToken::new(SINGLE_STATE.to_string()),
            nonce_function: || Nonce::new(SINGLE_NONCE.to_string())
        }
    }

    fn single_state_nonce_mockito_request_db() -> OIDCRequestsDB{
        OIDCRequestsDB {
            issuer: mockito::server_url(),
            client_id: "".to_string(),
            client_secret: "".to_string(),
            redirect_uri: REDIRECT_URI.to_string(),
            scopes: vec!["profile".to_string(), "email".to_string()],
            users: Arc::new(Default::default()),
            state_function: || CsrfToken::new(SINGLE_STATE.to_string()),
            nonce_function: || Nonce::new(SINGLE_NONCE.to_string())
        }
    }

    fn mock_client(request_db: &OIDCRequestsDB) -> Result<DefaultClient, Error> {
        let client_id = request_db.client_id.clone();
        let client_secret = request_db.client_secret.clone();
        let redirect_uri = request_db.redirect_uri.clone();

        let provider_metadata = mock_provider_metadata(request_db.issuer.as_str())
            .unwrap()
            .set_jwks(mock_jwks().unwrap());

        let result = CoreClient::from_provider_metadata(
            provider_metadata,
            ClientId::new(client_id),
            Some(ClientSecret::new(client_secret)))
            .set_redirect_uri(RedirectUrl::new(redirect_uri)?
            );

        Ok(result)
    }

    fn mock_valid_request(token_response: &StandardTokenResponse<CoreIdTokenFields, BasicTokenType>) -> Mock {
        mock("POST", "/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(token_response).unwrap())
            .create()
    }

    fn auth_code_response_empty_with_valid_state() -> AuthCodeResponse {
        AuthCodeResponse {
            session_state: "".to_string(),
            code: "".to_string(),
            state: SINGLE_STATE.to_string()
        }
    }

    //TODO: What is the Rust way for expected Exceptions/Errors?
    fn match_login_failed_reason(response: Result<(ExternalUserClaims, geoengine_datatypes::primitives::Duration), Error>, expected_reason: &str) {
        match response {
            Err(Error::OIDCLoginFailed { reason } ) => {assert_eq!(reason, expected_reason)}
            _ => {panic!("Wrong Error Type")}
        }
    }

    #[tokio::test]
    async fn get_client_success() {
        let request_db = single_state_nonce_mockito_request_db();

        let provider_metadata = mock_provider_metadata(request_db.issuer.as_str()).unwrap();
        let jwks = mock_jwks().unwrap();

        let _mock_provider = mock("GET", "/.well-known/openid-configuration")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&provider_metadata).unwrap())
            .create();

        let _mock_jwk = mock("GET", "/jwk")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&jwks).unwrap())
            .create();

        let client = request_db.get_client().await;

        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn get_client_bad_request() {
        let request_db = single_state_nonce_mockito_request_db();

        let error_message = "{\
            \"error_description\": \"Dummy bad request\",
            \"error\": \"catch_all_error\",
        }";

        let _mock_provider = mock("GET", "/.well-known/openid-configuration")
            .with_status(404)
            .with_header("content-type", "application/json")
            .with_body(error_message)
            .create();

        let client = request_db.get_client().await;

        assert!(client.is_err());
    }

    #[tokio::test]
    async fn get_client_auth_code_unsupported() {
        let request_db = single_state_nonce_mockito_request_db();

        let provider_metadata = mock_provider_metadata(request_db.issuer.as_str())
            .unwrap()
            .set_response_types_supported(vec![]);
        let jwks = mock_jwks().unwrap();

        let _mock_provider = mock("GET", "/.well-known/openid-configuration")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&provider_metadata).unwrap())
            .create();

        let _mock_jwk = mock("GET", "/jwk")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&jwks).unwrap())
            .create();

        let client = request_db.get_client().await;

        assert!(client.is_err());
    }

    #[tokio::test]
    async fn get_client_id_rsa_signing_unsupported() {
        let request_db = single_state_nonce_mockito_request_db();

        let provider_metadata = mock_provider_metadata(request_db.issuer.as_str())
            .unwrap()
            .set_id_token_signing_alg_values_supported(vec![]);
        let jwks = mock_jwks().unwrap();

        let _mock_provider = mock("GET", "/.well-known/openid-configuration")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&provider_metadata).unwrap())
            .create();

        let _mock_jwk = mock("GET", "/jwk")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&jwks).unwrap())
            .create();

        let client = request_db.get_client().await;

        assert!(client.is_err());
    }

    #[tokio::test]
    async fn get_client_missing_scopes() {
        let request_db = single_state_nonce_mockito_request_db();

        let provider_metadata = mock_provider_metadata(request_db.issuer.as_str())
            .unwrap()
            .set_scopes_supported(None);
        let jwks = mock_jwks().unwrap();

        let _mock_provider = mock("GET", "/.well-known/openid-configuration")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&provider_metadata).unwrap())
            .create();

        let _mock_jwk = mock("GET", "/jwk")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&jwks).unwrap())
            .create();

        let client = request_db.get_client().await;

        assert!(client.is_err());
    }

    #[tokio::test]
    async fn get_client_missing_claims() {
        let request_db = single_state_nonce_mockito_request_db();

        let provider_metadata = mock_provider_metadata(request_db.issuer.as_str())
            .unwrap()
            .set_claims_supported(None);
        let jwks = mock_jwks().unwrap();

        let _mock_provider = mock("GET", "/.well-known/openid-configuration")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&provider_metadata).unwrap())
            .create();

        let _mock_jwk = mock("GET", "/jwk")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&jwks).unwrap())
            .create();

        let client = request_db.get_client().await;

        assert!(client.is_err());
    }

    #[tokio::test]
    async fn generate_request_success() {
        let request_db = OIDCRequestsDB {
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

        let query_map : HashMap<_, _> = url.query_pairs()
            .into_iter()
            .map(|(x, y)| (x.to_string(), y.to_string()))
            .collect();

        assert!(query_map.get("state").is_some());
        assert!(query_map.get("nonce").is_some());
        assert!(query_map.get("code_challenge").is_some());

        assert_eq!(query_map.get("client_id"), Some(&"DummyClient".to_string()));
        assert_eq!(query_map.get("redirect_uri"), Some(&REDIRECT_URI.to_string()));
        assert_eq!(query_map.get("response_type"), Some(&"code".to_string()));
        assert_eq!(query_map.get("scope"), Some(&"openid profile email".to_string()));
        assert_eq!(query_map.get("code_challenge_method"), Some(&"S256".to_string()));
    }

    #[tokio::test]
    async fn generate_request_duplicate_state() {
        let request_db = single_state_nonce_request_db();
        let client = mock_client(&request_db).unwrap();

        let first_request = request_db.generate_request(client.clone()).await;

        assert!(first_request.is_ok());

        let second_request = request_db.generate_request(client).await;

        assert!(second_request.is_err());
    }

    #[tokio::test]
    async fn resolve_request_success() {
        let request_db = single_state_nonce_mockito_request_db();
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mock_token_config = MockTokenConfig::create_from_issuer_and_client(request_db.issuer.clone(), request_db.client_id.clone());
        let token_response = mock_token_response(mock_token_config).unwrap();
        let _mock = mock_valid_request(&token_response);

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
            state: "Illegal Request State".to_string()
        };

        let response = request_db.resolve_request(client, auth_code_response).await;
        assert!(response.is_err());
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
            state: "Illegal Request State".to_string()
        };
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(response.is_err());
    }


    #[tokio::test]
    async fn resolve_request_no_id_token() {
        let request_db = single_state_nonce_mockito_request_db();

        let mock_token_config = MockTokenConfig::create_from_issuer_and_client(request_db.issuer.clone(), request_db.client_id.clone());

        let mut token_response = CoreTokenResponse::new(
            AccessToken::new(mock_token_config.access),
            CoreTokenType::Bearer,
            CoreIdTokenFields::new(None, EmptyExtraTokenFields {}));
        token_response.set_expires_in(mock_token_config.duration.as_ref());

        let _m = mock("POST", "/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&token_response).unwrap())
            .create();

        let client = mock_client(&request_db).unwrap();

        let request = request_db.generate_request(client.clone()).await;

        assert!(request.is_ok());

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(response.is_err());
        match_login_failed_reason(response, "No id token returned");
    }


    #[tokio::test]
    async fn resolve_request_no_nonce() {
        let request_db = single_state_nonce_mockito_request_db();
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(request_db.issuer.clone(), request_db.client_id.clone());
        mock_token_config.nonce = None;
        let token_response = mock_token_response(mock_token_config).unwrap();
        let _mock = mock_valid_request(&token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(response.is_err());
        match_login_failed_reason(response, "Failed to verify claims");
    }

    #[tokio::test]
    async fn resolve_request_wrong_nonce() {
        let request_db = single_state_nonce_mockito_request_db();
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(request_db.issuer.clone(), request_db.client_id.clone());
        mock_token_config.nonce = Some(Nonce::new("Wrong Nonce".to_string()));
        let token_response = mock_token_response(mock_token_config).unwrap();
        let _mock = mock_valid_request(&token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(response.is_err());
        match_login_failed_reason(response, "Failed to verify claims");
    }


    #[tokio::test]
    async fn resolve_request_no_email() {
        let request_db = single_state_nonce_mockito_request_db();
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(request_db.issuer.clone(), request_db.client_id.clone());
        mock_token_config.email = None;
        let token_response = mock_token_response(mock_token_config).unwrap();
        let _mock = mock_valid_request(&token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(response.is_err());
        match_login_failed_reason(response, "Missing e-mail claim");
    }


    #[tokio::test]
    async fn resolve_request_no_name() {
        let request_db = single_state_nonce_mockito_request_db();
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(request_db.issuer.clone(), request_db.client_id.clone());
        mock_token_config.name = None;
        let token_response = mock_token_response(mock_token_config).unwrap();
        let _mock = mock_valid_request(&token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(response.is_err());
        match_login_failed_reason(response, "Missing name claim");
    }

    #[tokio::test]
    async fn resolve_request_no_access_token_duration() {
        let request_db = single_state_nonce_mockito_request_db();
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(request_db.issuer.clone(), request_db.client_id.clone());
        mock_token_config.duration = None;
        let token_response = mock_token_response(mock_token_config).unwrap();
        let _mock = mock_valid_request(&token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(response.is_err());
        match_login_failed_reason(response, "No Duration in token");
    }

    #[tokio::test]
    async fn resolve_request_access_hashcode_mismatch() {

        let request_db = single_state_nonce_mockito_request_db();
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(request_db.issuer.clone(), request_db.client_id.clone());
        assert_ne!(mock_token_config.access_for_id, ALTERNATIVE_ACCESS_TOKEN);
        mock_token_config.access_for_id = ALTERNATIVE_ACCESS_TOKEN.to_string();

        let token_response = mock_token_response(mock_token_config).unwrap();
        let _mock = mock_valid_request(&token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(response.is_err());
        match_login_failed_reason(response, "Illegal access token");
    }

    #[tokio::test]
    async fn resolve_request_twice() {
        let request_db = single_state_nonce_mockito_request_db();
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let mock_token_config = MockTokenConfig::create_from_issuer_and_client(request_db.issuer.clone(), request_db.client_id.clone());
        let token_response = mock_token_response(mock_token_config).unwrap();
        let _mock = mock_valid_request(&token_response);

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client.clone(), auth_code_response).await;

        assert!(response.is_ok());

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(response.is_err());
    }

    #[tokio::test]
    async fn resolve_multiple_requests() {
        let mut request_db = single_state_nonce_mockito_request_db();
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
            || CsrfToken::new("State_10".to_string())
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
            || Nonce::new("Nonce_10".to_string())
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

            let mut mock_token_config = MockTokenConfig::create_from_issuer_and_client(request_db.issuer.clone(), request_db.client_id.clone());
            mock_token_config.nonce = nonce;
            let token_response = mock_token_response(mock_token_config).unwrap();
            let _mock = mock_valid_request(&token_response);

            let mut auth_code_response = auth_code_response_empty_with_valid_state();
            auth_code_response.state = state;
            let response = request_db.resolve_request(client.clone(), auth_code_response).await;

            assert!(response.is_ok());
        }
    }

    #[tokio::test]
    async fn resolve_bad_request() {
        let request_db = single_state_nonce_mockito_request_db();
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        //TODO: Maybe search for more detailed error types and test/display them more gracefully.
        let error_message = "{\
            \"error_description\": \"Dummy bad request\",
            \"error\": \"catch_all_error\",
        }";

        let _mock = mock("POST", "/token")
                .with_status(400)
                .with_header("content-type", "application/json")
                .with_body(error_message)
                .create();

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(response.is_err());
        match_login_failed_reason(response, "Failed code to token exchange");
    }

    #[tokio::test]
    async fn resolve_after_bad_request() {
        let request_db = single_state_nonce_mockito_request_db();
        let client = mock_client(&request_db).unwrap();

        request_db.generate_request(client.clone()).await.unwrap();

        let error_message = "{\
            \"error_description\": \"Dummy bad request\",
            \"error\": \"catch_all_error\",
        }";

        let _mock = mock("POST", "/token")
            .with_status(400)
            .with_header("content-type", "application/json")
            .with_body(error_message)
            .create();

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response_bad = request_db.resolve_request(client.clone(), auth_code_response).await;

        assert!(response_bad.is_err());
        match_login_failed_reason(response_bad, "Failed code to token exchange");

        let auth_code_response = auth_code_response_empty_with_valid_state();
        let response = request_db.resolve_request(client, auth_code_response).await;

        assert!(response.is_err());
    }

    //TODO: Did not test code and PKCE verifier.

}