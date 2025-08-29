use super::{Result, WildliveError, error};
use geoengine_datatypes::error::BoxedResultExt;
use oauth2::{
    ClientId, EndpointNotSet, ExtraTokenFields, RefreshToken, StandardErrorResponse,
    StandardTokenResponse, TokenResponse as _, TokenUrl,
};
use openidconnect::{
    Client, EmptyAdditionalClaims, IdTokenFields, IssuerUrl, JsonWebKeySet, JsonWebKeySetUrl,
    core::{
        CoreAuthDisplay, CoreAuthPrompt, CoreErrorResponseType, CoreGenderClaim, CoreJsonWebKey,
        CoreJsonWebKeySet, CoreJweContentEncryptionAlgorithm, CoreJwsSigningAlgorithm,
        CoreRevocableToken, CoreRevocationErrorResponse, CoreTokenIntrospectionResponse,
        CoreTokenType,
    },
};
use url::Url;

const CLIENT_ID: &str = "wildlive-frontend";
const ISSUER_URL: &str = "https://webapp.senckenberg.de/auth/realms/wildlive-portal/";

#[derive(Debug)]
struct TokenResponse {
    pub(super) access_token: String,
    pub(super) expires_in: u64,
    pub(super) refresh_token: String,
    pub(super) refresh_expires_in: u64,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct RefreshExpiresInField {
    refresh_expires_in: u64,
}

impl ExtraTokenFields for RefreshExpiresInField {}

impl From<WildliveTokenResponse> for TokenResponse {
    fn from(response: WildliveTokenResponse) -> Self {
        Self {
            access_token: response.access_token().secret().to_string(),
            expires_in: response.expires_in().map_or(0, |d| d.as_secs()),
            refresh_token: response
                .refresh_token()
                .map_or_else(|| String::new(), |rt| rt.secret().to_string()),
            refresh_expires_in: response.extra_fields().extra_fields().refresh_expires_in,
        }
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
    issuer_url: &str,
    refresh_token: String,
) -> Result<TokenResponse> {
    let client_id = ClientId::new(CLIENT_ID.to_string());

    let issuer_url =
        Url::parse(issuer_url).map_err(|source| WildliveError::InvalidUrl { source })?;
    let token_url = TokenUrl::from_url(
        issuer_url
            .join("protocol/openid-connect/token")
            .map_err(|source| WildliveError::InvalidUrl { source })?,
    );

    // TODO: cache the jwks to not fetch it every time
    let jwks = retrieve_jwks(issuer_url.as_str()).await?;

    let client = WildliveClient::new(client_id, IssuerUrl::from_url(issuer_url), jwks);
    let client = client.set_token_uri(token_url);

    let refresh_token = RefreshToken::new(refresh_token);
    let refresh_token_request = client.exchange_refresh_token(&refresh_token);

    // TODO: re-use the client
    let http_client = reqwest::ClientBuilder::new()
        // Following redirects opens the client up to SSRF vulnerabilities.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .boxed_context(error::UnexpectedExecution)?;

    refresh_token_request
        .request_async(&http_client)
        .await
        .boxed_context(error::AuthKeyInvalid)
        .map(TokenResponse::from)
}

pub async fn retrieve_jwks(issuer_url: &str) -> Result<CoreJsonWebKeySet> {
    let issuer_url =
        Url::parse(issuer_url).map_err(|source| WildliveError::InvalidUrl { source })?;

    // TODO: re-use the client
    let http_client = reqwest::ClientBuilder::new()
        // Following redirects opens the client up to SSRF vulnerabilities.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .boxed_context(error::UnexpectedExecution)?;

    // let provider_metadata = CoreProviderMetadata::discover_async(issuer_url, &http_client).await?;
    // Ok(provider_metadata.jwks())

    let jwks_url = JsonWebKeySetUrl::from_url(
        issuer_url
            .join("protocol/openid-connect/certs")
            .map_err(|source| WildliveError::InvalidUrl { source })?,
    );

    JsonWebKeySet::fetch_async(&jwks_url, &http_client)
        .await
        .boxed_context(error::UnexpectedExecution)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::tests::json_file_responder;
    use geoengine_datatypes::test_data;
    use httptest::{Expectation, all_of, matchers};

    #[tokio::test]
    async fn test_name() {
        let mock_server = httptest::Server::run();

        mock_server.expect(
            Expectation::matching(all_of![
                matchers::request::method("GET"),
                matchers::request::path("/protocol/openid-connect/certs"),
                matchers::request::headers(matchers::contains(("accept", "application/json"))),
            ])
            .respond_with(json_file_responder(test_data!(
                "wildlive/responses/certs.json"
            ))),
        );

        mock_server.expect(
            Expectation::matching(all_of![
                matchers::request::method("POST"),
                matchers::request::path("/protocol/openid-connect/token"),
                matchers::request::headers(matchers::contains(("accept", "application/json"))),
            ])
            .respond_with(json_file_responder(test_data!(
                "wildlive/responses/id-token.json"
            ))),
        );

        assert!(retrieve_access_and_refresh_token(&mock_server.url_str("/"), "eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI0MWFlNzYxMC1jNzIyLTRmOWQtOGJhNi02ZTc2MmRkNDIxOWIifQ.eyJleHAiOjE3NTYxMzI5NTcsImlhdCI6MTc1NjEzMTE1NywianRpIjoiOTIwYTljM2EtMjZkMC00Y2NiLWIxYWQtNDRlNWU2NzZmZWU4IiwiaXNzIjoiaHR0cHM6Ly93ZWJhcHAuc2VuY2tlbmJlcmcuZGUvYXV0aC9yZWFsbXMvd2lsZGxpdmUtcG9ydGFsIiwiYXVkIjoiaHR0cHM6Ly93ZWJhcHAuc2VuY2tlbmJlcmcuZGUvYXV0aC9yZWFsbXMvd2lsZGxpdmUtcG9ydGFsIiwic3ViIjoiMmRhZDgxZGYtOTVhZS00Y2E4LWE4NTktZWQyZjM0OWRlOWY2IiwidHlwIjoiUmVmcmVzaCIsImF6cCI6IndpbGRsaXZlLWZyb250ZW5kIiwic2Vzc2lvbl9zdGF0ZSI6ImFhNjljMDkzLTQ4YTUtNDg1Zi1iMWZkLTQ4MTE4YmY2YmI1NSIsInNjb3BlIjoiZW1haWwgcHJvZmlsZSIsInNpZCI6ImFhNjljMDkzLTQ4YTUtNDg1Zi1iMWZkLTQ4MTE4YmY2YmI1NSJ9.COoMWxp6IZ_IKTQ-GGAb22CIcybY32II5wn9beaSoyw".to_string()).await.is_ok());
    }
}
