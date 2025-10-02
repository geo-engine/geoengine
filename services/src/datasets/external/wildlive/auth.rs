use std::borrow::Cow;

use crate::util::join_base_url_and_path;

use super::{Result, WildliveError, error};
use geoengine_datatypes::error::BoxedResultExt;
use oauth2::{
    AccessToken, AuthorizationCode, ClientId, ClientSecret, EndpointNotSet, ExtraTokenFields,
    PkceCodeVerifier, RedirectUrl, RefreshToken, StandardErrorResponse, StandardTokenResponse,
    TokenResponse as _, TokenUrl,
};
use openidconnect::{
    Client, EmptyAdditionalClaims, IdTokenFields, IdTokenVerifier, IssuerUrl, JsonWebKeySet,
    JsonWebKeySetUrl, Nonce, TokenResponse as _,
    core::{
        CoreAuthDisplay, CoreAuthPrompt, CoreErrorResponseType, CoreGenderClaim, CoreJsonWebKey,
        CoreJsonWebKeySet, CoreJweContentEncryptionAlgorithm, CoreJwsSigningAlgorithm,
        CoreRevocableToken, CoreRevocationErrorResponse, CoreTokenIntrospectionResponse,
        CoreTokenType,
    },
};
use url::Url;

#[derive(Debug, serde::Deserialize)]
pub(super) struct TokenResponse {
    pub(super) user: Option<String>,
    pub(super) access_token: AccessToken,
    #[allow(dead_code)]
    pub(super) expires_in: u64,
    pub(super) refresh_token: RefreshToken,
    pub(super) refresh_expires_in: u64,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct RefreshExpiresInField {
    refresh_expires_in: u64,
}

impl ExtraTokenFields for RefreshExpiresInField {}

impl TokenResponse {
    fn from_response(
        response: &WildliveTokenResponse,
        id_token_verifier: &IdTokenVerifier<'_, CoreJsonWebKey>,
    ) -> Result<Self> {
        let refresh_token = response
            .refresh_token()
            .cloned()
            .unwrap_or_else(|| RefreshToken::new(String::new()));
        debug_assert!(
            !refresh_token.secret().is_empty(),
            "refresh token should be present"
        );

        let user = response
            .id_token()
            .ok_or(WildliveError::Oidc {
                info: "missing ID token in response",
                source: Box::new(openidconnect::ClaimsVerificationError::Unsupported(
                    "ID token is missing".to_string(),
                )),
            })?
            .claims(id_token_verifier, |_n: Option<&Nonce>| {
                // we just want the username for display purposes, so we skip nonce verification
                Ok(())
            })
            .boxed_context(error::Oidc {
                info: "failed to extract user from ID token",
            })?
            .preferred_username()
            .map(|e| e.to_string());

        Ok(Self {
            user,
            access_token: response.access_token().clone(),
            expires_in: response.expires_in().map_or(0, |d| d.as_secs()),
            refresh_token,
            refresh_expires_in: response.extra_fields().extra_fields().refresh_expires_in,
        })
    }
}

type WildliveIdTokenFields = IdTokenFields<
    EmptyAdditionalClaims,
    RefreshExpiresInField,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJwsSigningAlgorithm,
>;
type WildliveTokenResponse = StandardTokenResponse<WildliveIdTokenFields, CoreTokenType>;
type WildliveClient<
    HasAuthUrl = EndpointNotSet,
    HasDeviceAuthUrl = EndpointNotSet,
    HasIntrospectionUrl = EndpointNotSet,
    HasRevocationUrl = EndpointNotSet,
    HasTokenUrl = EndpointNotSet,
    HasUserInfoUrl = EndpointNotSet,
> = Client<
    EmptyAdditionalClaims,
    CoreAuthDisplay,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJsonWebKey,
    CoreAuthPrompt,
    StandardErrorResponse<CoreErrorResponseType>,
    WildliveTokenResponse,
    CoreTokenIntrospectionResponse,
    CoreRevocableToken,
    CoreRevocationErrorResponse,
    HasAuthUrl,
    HasDeviceAuthUrl,
    HasIntrospectionUrl,
    HasRevocationUrl,
    HasTokenUrl,
    HasUserInfoUrl,
>;

pub async fn retrieve_access_and_refresh_token(
    http_client: &reqwest::Client,
    wildlive_config: &crate::config::WildliveOidc,
    refresh_token: &RefreshToken,
) -> Result<TokenResponse> {
    let client_id = ClientId::new(wildlive_config.client_id.clone());

    let token_url = TokenUrl::from_url(
        join_base_url_and_path(&wildlive_config.issuer, "protocol/openid-connect/token")
            .map_err(|source| WildliveError::InvalidUrl { source })?,
    );

    // TODO: cache the jwks to not fetch it every time
    let jwks = retrieve_jwks(http_client, &wildlive_config.issuer).await?;

    let mut client = WildliveClient::new(
        client_id,
        IssuerUrl::from_url(wildlive_config.issuer.clone()),
        jwks,
    )
    .set_token_uri(token_url);

    if let Some(client_secret) = wildlive_config.client_secret.as_ref() {
        client = client.set_client_secret(ClientSecret::new(client_secret.into()));
    }

    let id_token_verifier = client.id_token_verifier();

    let refresh_token_request = client.exchange_refresh_token(refresh_token);

    TokenResponse::from_response(
        &refresh_token_request
            .request_async(http_client)
            .await
            .boxed_context(error::AuthKeyInvalid)?,
        &id_token_verifier,
    )
}

pub async fn retrieve_jwks(
    http_client: &reqwest::Client,
    issuer_url: &Url,
) -> Result<CoreJsonWebKeySet> {
    let jwks_url = join_base_url_and_path(issuer_url, "protocol/openid-connect/certs")
        .map_err(|source| WildliveError::InvalidUrl { source })?;

    // let provider_metadata = CoreProviderMetadata::discover_async(issuer_url, &http_client).await?;
    // Ok(provider_metadata.jwks())

    let jwks_url = JsonWebKeySetUrl::from_url(jwks_url);

    JsonWebKeySet::fetch_async(&jwks_url, http_client)
        .await
        .boxed_context(error::Oidc {
            info: "fetching JWKS failed",
        })
}

pub async fn retrieve_wildlive_tokens(
    http_client: &reqwest::Client,
    local_config: &crate::config::Oidc,
    code: &str,
    pkce_verifier: &str,
    redirect_uri: Url,
    broker_provider: &str,
) -> Result<TokenResponse> {
    let client_id = ClientId::new(local_config.client_id.clone());
    let token_url = TokenUrl::from_url(
        join_base_url_and_path(&local_config.issuer, "protocol/openid-connect/token")
            .map_err(|source| WildliveError::InvalidUrl { source })?,
    );
    let broker_url = join_base_url_and_path(
        &local_config.issuer,
        &format!("broker/{broker_provider}/token"),
    )
    .map_err(|source| WildliveError::InvalidUrl { source })?;

    // Step 1: Exchange code for tokens at local OIDC provider

    let jwks = retrieve_jwks(http_client, &local_config.issuer).await?;

    let mut client = WildliveClient::new(
        client_id,
        IssuerUrl::from_url(local_config.issuer.clone()),
        jwks,
    )
    .set_token_uri(token_url);

    if let Some(client_secret) = local_config.client_secret.as_ref() {
        client = client.set_client_secret(ClientSecret::new(client_secret.into()));
    }

    let code = AuthorizationCode::new(code.to_string());
    let pkce_verifier = PkceCodeVerifier::new(pkce_verifier.to_string());

    let token_request = client
        .exchange_code(code)
        .set_pkce_verifier(pkce_verifier)
        .set_redirect_uri(Cow::Owned(RedirectUrl::from_url(redirect_uri)));

    let token_response = token_request
        .request_async(http_client)
        .await
        .boxed_context(error::AuthKeyInvalid)?;

    // Step 2: Access tokens for wildlive OIDC provider using the access token from step 1

    let broker_request = http_client
        .get(broker_url)
        .bearer_auth(token_response.access_token().secret())
        .header("Accept", "application/json");

    broker_request
        .send()
        .await
        .boxed_context(error::Oidc {
            info: "request to broker failed",
        })?
        .json::<TokenResponse>()
        .await
        .boxed_context(error::AuthKeyInvalid)
}

pub async fn retrieve_refresh_token_from_local_code(
    local_config: crate::config::Oidc,
    wildlive_config: crate::config::WildliveOidc,
    code: &str,
    pkce_verifier: &str,
    redirect_uri: Url,
) -> Result<TokenResponse> {
    let http_client = reqwest::ClientBuilder::new()
        // Following redirects opens the client up to SSRF vulnerabilities.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .boxed_context(error::UnexpectedExecution)?;

    let tokens = retrieve_wildlive_tokens(
        &http_client,
        &local_config,
        code,
        pkce_verifier,
        redirect_uri,
        &wildlive_config.broker_provider,
    )
    .await?;

    retrieve_access_and_refresh_token(&http_client, &wildlive_config, &tokens.refresh_token).await
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::util::tests::mock_oidc::{MockTokenConfig, mock_jwks, mock_token_response};
    use httptest::{Expectation, all_of, matchers, responders::status_code};

    #[tokio::test]
    async fn it_retrieves_a_new_set_of_tokens() {
        let mock_server = httptest::Server::run();
        let server_url = Url::parse(&mock_server.url_str("/"))
            .unwrap()
            .join("realms/AI4WildLIVE")
            .unwrap();

        mock_server.expect(
            Expectation::matching(all_of![
                matchers::request::method("GET"),
                matchers::request::path("/realms/AI4WildLIVE/protocol/openid-connect/certs"),
                matchers::request::headers(matchers::contains(("accept", "application/json"))),
            ])
            .respond_with(
                status_code(200)
                    .insert_header("content-type", "application/json")
                    .body(serde_json::to_string(&mock_jwks()).unwrap()),
            ),
        );

        let mut mock_token_config = MockTokenConfig::create_from_tokens(
            server_url.clone(),
            "geoengine".into(),
            std::time::Duration::from_secs(300),
            "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJsbkF4V0NqY2lhX3I3cFBySUtTSGd6OVhyRGxzcGY4MHUxMDJpdENoelE4In0".into(),
            "eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI0MWFlNzYxMC1jNzIyLTRmOWQtOGJhNi02ZTc2MmRkNDIxOWIifQ.eyJleHAiOjE3NTYxMzI5NTcsImlhdCI6MTc1NjEzMTE1NywianRpIjoiOTIwYTljM2EtMjZkMC00Y2NiLWIxYWQtNDRlNWU2NzZmZWU4IiwiaXNzIjoiaHR0cHM6Ly93ZWJhcHAuc2VuY2tlbmJlcmcuZGUvYXV0aC9yZWFsbXMvd2lsZGxpdmUtcG9ydGFsIiwiYXVkIjoiaHR0cHM6Ly93ZWJhcHAuc2VuY2tlbmJlcmcuZGUvYXV0aC9yZWFsbXMvd2lsZGxpdmUtcG9ydGFsIiwic3ViIjoiMmRhZDgxZGYtOTVhZS00Y2E4LWE4NTktZWQyZjM0OWRlOWY2IiwidHlwIjoiUmVmcmVzaCIsImF6cCI6IndpbGRsaXZlLWZyb250ZW5kIiwic2Vzc2lvbl9zdGF0ZSI6ImFhNjljMDkzLTQ4YTUtNDg1Zi1iMWZkLTQ4MTE4YmY2YmI1NSIsInNjb3BlIjoiZW1haWwgcHJvZmlsZSIsInNpZCI6ImFhNjljMDkzLTQ4YTUtNDg1Zi1iMWZkLTQ4MTE4YmY2YmI1NSJ9.COoMWxp6IZ_IKTQ-GGAb22CIcybY32II5wn9beaSoyw".into(),
        );
        mock_token_config.signing_alg = Some(CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256);
        mock_token_config.preferred_username = Some("testuser".into());
        let token_response = mock_token_response(mock_token_config);

        let mut token_response_json = serde_json::to_value(&token_response).unwrap();
        token_response_json["refresh_expires_in"] = serde_json::json!(300);

        mock_server.expect(
            Expectation::matching(all_of![
                matchers::request::method("POST"),
                matchers::request::path("/realms/AI4WildLIVE/protocol/openid-connect/token"),
                matchers::request::headers(matchers::contains(("accept", "application/json"))),
            ])
            .respond_with(
                status_code(200)
                    .insert_header("content-type", "application/json")
                    .body(token_response_json.to_string()),
            ),
        );
        let http_client = reqwest::ClientBuilder::new()
            // Following redirects opens the client up to SSRF vulnerabilities.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        let refresh_token = RefreshToken::new("eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI0MWFlNzYxMC1jNzIyLTRmOWQtOGJhNi02ZTc2MmRkNDIxOWIifQ.eyJleHAiOjE3NTYxMzI5NTcsImlhdCI6MTc1NjEzMTE1NywianRpIjoiOTIwYTljM2EtMjZkMC00Y2NiLWIxYWQtNDRlNWU2NzZmZWU4IiwiaXNzIjoiaHR0cHM6Ly93ZWJhcHAuc2VuY2tlbmJlcmcuZGUvYXV0aC9yZWFsbXMvd2lsZGxpdmUtcG9ydGFsIiwiYXVkIjoiaHR0cHM6Ly93ZWJhcHAuc2VuY2tlbmJlcmcuZGUvYXV0aC9yZWFsbXMvd2lsZGxpdmUtcG9ydGFsIiwic3ViIjoiMmRhZDgxZGYtOTVhZS00Y2E4LWE4NTktZWQyZjM0OWRlOWY2IiwidHlwIjoiUmVmcmVzaCIsImF6cCI6IndpbGRsaXZlLWZyb250ZW5kIiwic2Vzc2lvbl9zdGF0ZSI6ImFhNjljMDkzLTQ4YTUtNDg1Zi1iMWZkLTQ4MTE4YmY2YmI1NSIsInNjb3BlIjoiZW1haWwgcHJvZmlsZSIsInNpZCI6ImFhNjljMDkzLTQ4YTUtNDg1Zi1iMWZkLTQ4MTE4YmY2YmI1NSJ9.COoMWxp6IZ_IKTQ-GGAb22CIcybY32II5wn9beaSoyw".into());
        let response = retrieve_access_and_refresh_token(
            &http_client,
            &crate::config::WildliveOidc {
                issuer: server_url.clone(),
                client_id: "geoengine".into(),
                client_secret: None,
                broker_provider: "wildlive-portal".into(),
            },
            &refresh_token,
        )
        .await
        .unwrap();

        assert!(!response.refresh_token.secret().is_empty());
        assert!(response.user.is_some());
    }
}
