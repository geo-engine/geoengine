use crate::{
    api::model::{datatypes::DatasetId, services::AddDataset},
    contexts::{ApplicationContext, MockableSession, SessionContext, SessionId},
    datasets::{
        listing::Provenance,
        storage::{DatasetDefinition, DatasetStore, MetaDataDefinition},
    },
    handlers, pro,
    pro::{
        contexts::{OidcRequestDbProvider, ProGeoEngineDb, ProInMemoryContext},
        permissions::{Permission, PermissionDb, Role},
        users::{UserAuth, UserCredentials, UserId, UserInfo, UserRegistration, UserSession},
    },
    projects::{CreateProject, ProjectDb, ProjectId, STRectangle},
    util::server::{configure_extractors, render_404, render_405},
    util::{config::get_config_element, user_input::UserInput},
    workflows::{
        registry::WorkflowRegistry,
        workflow::{Workflow, WorkflowId},
    },
};
use actix_web::{dev::ServiceResponse, FromRequest};
use actix_web::{http, middleware, test, web, App};
use geoengine_datatypes::{
    primitives::DateTime, spatial_reference::SpatialReferenceOption, util::Identifier,
};
use geoengine_operators::{
    engine::{RasterOperator, TypedOperator},
    source::{GdalSource, GdalSourceParameters},
    util::gdal::create_ndvi_meta_data,
};

#[allow(clippy::missing_panics_doc)]
pub async fn create_session_helper<C: UserAuth>(app_ctx: &C) -> UserSession {
    app_ctx
        .register_user(
            UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
                real_name: "Foo Bar".to_string(),
            }
            .validated()
            .unwrap(),
        )
        .await
        .unwrap();

    app_ctx
        .login(UserCredentials {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
        })
        .await
        .unwrap()
}

pub fn create_random_user_session_helper() -> UserSession {
    let user_id = UserId::new();

    UserSession {
        id: SessionId::new(),
        user: UserInfo {
            id: user_id,
            email: Some(user_id.to_string()),
            real_name: Some(user_id.to_string()),
        },
        created: DateTime::MIN,
        valid_until: DateTime::MAX,
        project: None,
        view: None,
        roles: vec![user_id.into(), Role::registered_user_role_id()],
    }
}

#[allow(clippy::missing_panics_doc)]
pub async fn create_project_helper<C: ApplicationContext<Session = UserSession> + UserAuth>(
    app_ctx: &C,
) -> (UserSession, ProjectId)
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let session = create_session_helper(app_ctx).await;

    let project = app_ctx
        .session_context(session.clone())
        .db()
        .create_project(
            CreateProject {
                name: "Test".to_string(),
                description: "Foo".to_string(),
                bounds: STRectangle::new(
                    SpatialReferenceOption::Unreferenced,
                    0.,
                    0.,
                    1.,
                    1.,
                    0,
                    1,
                )
                .unwrap(),
                time_step: None,
            }
            .validated()
            .unwrap(),
        )
        .await
        .unwrap();

    (session, project)
}

pub async fn send_pro_test_request<C>(req: test::TestRequest, app_ctx: C) -> ServiceResponse
where
    C: ApplicationContext<Session = UserSession> + UserAuth + OidcRequestDbProvider,
    C::Session: FromRequest,
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    #[allow(unused_mut)]
    let mut app = App::new()
        .app_data(web::Data::new(app_ctx))
        .wrap(
            middleware::ErrorHandlers::default()
                .handler(http::StatusCode::NOT_FOUND, render_404)
                .handler(http::StatusCode::METHOD_NOT_ALLOWED, render_405),
        )
        .wrap(middleware::NormalizePath::trim())
        .configure(configure_extractors)
        .configure(pro::handlers::datasets::init_dataset_routes::<C>)
        .configure(handlers::layers::init_layer_routes::<C>)
        .configure(pro::handlers::permissions::init_permissions_routes::<C>)
        .configure(handlers::plots::init_plot_routes::<C>)
        .configure(pro::handlers::projects::init_project_routes::<C>)
        .configure(pro::handlers::users::init_user_routes::<C>)
        .configure(handlers::spatial_references::init_spatial_reference_routes::<C>)
        .configure(handlers::upload::init_upload_routes::<C>)
        .configure(handlers::wcs::init_wcs_routes::<C>)
        .configure(handlers::wfs::init_wfs_routes::<C>)
        .configure(handlers::wms::init_wms_routes::<C>)
        .configure(handlers::workflows::init_workflow_routes::<C>);
    #[cfg(feature = "odm")]
    {
        app = app.configure(pro::handlers::drone_mapping::init_drone_mapping_routes::<C>);
    }
    let app = test::init_service(app).await;
    test::call_service(&app, req.to_request())
        .await
        .map_into_boxed_body()
}

#[cfg(test)]
pub(in crate::pro) mod mock_oidc {
    use crate::pro::users::{DefaultJsonWebKeySet, DefaultProviderMetadata};
    use chrono::{Duration, Utc};
    use oauth2::basic::BasicTokenType;
    use oauth2::{
        AccessToken, AuthUrl, EmptyExtraTokenFields, Scope, StandardTokenResponse, TokenUrl,
    };
    use openidconnect::core::{
        CoreClaimName, CoreIdToken, CoreIdTokenClaims, CoreIdTokenFields, CoreJsonWebKey,
        CoreJwsSigningAlgorithm, CoreProviderMetadata, CoreResponseType, CoreRsaPrivateSigningKey,
        CoreTokenResponse, CoreTokenType,
    };
    use openidconnect::{
        Audience, EmptyAdditionalClaims, EmptyAdditionalProviderMetadata, EndUserEmail,
        EndUserName, IssuerUrl, JsonWebKeySet, JsonWebKeySetUrl, LocalizedClaim, Nonce,
        ResponseTypes, StandardClaims, SubjectIdentifier,
    };

    const TEST_PRIVATE_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----\n\
	    MIIEogIBAAKCAQEAxIm5pngAgY4V+6XJPtlATkU6Gbcen22M3Tf16Gwl4uuFagEp\n\
	    SQ4u/HXvcyAYvdNfAwR34nsAyS1qFQasWYtcU4HwmFvo5ADfdJpfo6myRiGN3ocA\n\
	    4+/S1tH8HqLH+w7U/9SopwUP0n0+N0UaaFA1htkRY4zNWEDnJ2AVN2Vi0dUtS62D\n\
	    jOfvz+QMd04mAZaLkLdSxlHCYKjx6jmTQEbVFwSt/Pm1MryF7gkXg6YeiNG6Ehgm\n\
	    LUHv50Jwt1salVH9/FQVNkqiVivHNAW4cEVbuTZJl8TjtQn6MnOZSP7n8TkonrUd\n\
	    ULoIxIl3L+kneJABBaQ6zg52w00W1MXwlu+C8wIDAQABAoIBACW+dWLc5Ov8h4g+\n\
	    fHmPa2Qcs13A5yai+Ux6tMUgD96WcJa9Blq7WJavZ37qiRXbhAGmWAesq6f3Cspi\n\
	    77J6qw52g+gerokrCb7w7rEVo+EIDKDRuIANzKXoycxwYot6e7lt872voSxBVTN0\n\
	    F/A0hzMQeOBvZ/gs7reHIkvzMpktSyKVJOt9ie1cZ1jp7r1bazbFs2qIyDc5Z521\n\
	    BQ6GgRyNJ5toTttmF5ZxpSQXWyvumldWL5Ue9wNEIPjRgsL9UatqagxgmouGxEOL\n\
	    0F9bFWUFlrsqTArTWNxg5R0zFwfzFqidx0HwyF9SyidVq9Bz8/FtgVe2ed4u7snm\n\
	    vYOUbsECgYEA7yg6gyhlQvA0j5MAe6rhoMD0sYRG07ZR0vNzzZRoud9DSdE749f+\n\
	    ZvqUqv3Wuv5p97dd4sGuMkzihXdGqcpWO4CAbalvB2CB5HKVMIKR5cjMIzeVE17v\n\
	    0Hcdd2Spx6yMahFX3eePLl3wDDLSP2ITYi6m4SGckGwd5BeFkn4gNyMCgYEA0mEd\n\
	    Vt2bGF9+5sFfsZgd+3yNAnqLGZ+bxZuYcF/YayH8dKKHdrmhTJ+1w78JdFC5uV2G\n\
	    F75ubyrEEY09ftE/HNG90fanUAYxmVJXMFxxgMIE8VqsjiB/i1Q3ofN2HOlOB1W+\n\
	    4e8BEXrAxCgsXMGCwU73b52474/BDq4Bh1cNKfECgYB4cfw1/ewxsCPogxJlNgR4\n\
	    H3WcyY+aJGJFKZMS4EF2CvkqfhP5hdh8KIsjKsAwYN0hgtnnz79ZWdtjeFTAQkT3\n\
	    ppoHoKNoRbRlR0fXrIqp/VzCB8YugUup47OVY78V7tKwwJdODMbRhUHWAupcPZqh\n\
	    gflNvM3K9oh/TVFaG+dBnQKBgHE2mddZQlGHcn8zqQ+lUN05VZjz4U9UuTtKVGqE\n\
	    6a4diAIsRMH7e3YErIg+khPqLUg3sCWu8TcZyJG5dFJ+wHv90yzek4NZEe/0g78e\n\
	    wGYOAyLvLNT/YCPWmmmo3vMIClmgJyzmtah2aq4lAFqaOIdWu4lxU0h4D+iac3Al\n\
	    xIvBAoGAZtOeVlJCzmdfP8/J1IMHqFX3/unZEundqL1tiy5UCTK/RJTftr6aLkGL\n\
	    xN3QxN+Kuc5zMyHeQWY9jKO8SUwyuzrCuwduzzqC1OXEWinfcvCPg1yotRxgPGsV\n\
	    Wj4iz6nkuRK0fTLfTu6Nglx6mjX8Q3rz0UUFVjOL/gpgEWxzoHk=\n\
	    -----END RSA PRIVATE KEY-----";

    const TEST_JWK: &str = "{\
        \"kty\":\"RSA\",
        \"use\":\"sig\",
        \"n\":\"xIm5pngAgY4V-6XJPtlATkU6Gbcen22M3Tf16Gwl4uuFagEpSQ4u_HXvcyAYv\
            dNfAwR34nsAyS1qFQasWYtcU4HwmFvo5ADfdJpfo6myRiGN3ocA4-_S1tH8HqLH-w\
            7U_9SopwUP0n0-N0UaaFA1htkRY4zNWEDnJ2AVN2Vi0dUtS62DjOfvz-QMd04mAZa\
            LkLdSxlHCYKjx6jmTQEbVFwSt_Pm1MryF7gkXg6YeiNG6EhgmLUHv50Jwt1salVH9\
            _FQVNkqiVivHNAW4cEVbuTZJl8TjtQn6MnOZSP7n8TkonrUdULoIxIl3L-kneJABB\
            aQ6zg52w00W1MXwlu-C8w\",
        \"e\":\"AQAB\",
        \"d\":\"Jb51Ytzk6_yHiD58eY9rZByzXcDnJqL5THq0xSAP3pZwlr0GWrtYlq9nfuqJF\
            duEAaZYB6yrp_cKymLvsnqrDnaD6B6uiSsJvvDusRWj4QgMoNG4gA3MpejJzHBii3\
            p7uW3zva-hLEFVM3QX8DSHMxB44G9n-Czut4ciS_MymS1LIpUk632J7VxnWOnuvVt\
            rNsWzaojINzlnnbUFDoaBHI0nm2hO22YXlnGlJBdbK-6aV1YvlR73A0Qg-NGCwv1R\
            q2pqDGCai4bEQ4vQX1sVZQWWuypMCtNY3GDlHTMXB_MWqJ3HQfDIX1LKJ1Wr0HPz8\
            W2BV7Z53i7uyea9g5RuwQ\"
        }";

    const ACCESS_TOKEN: &str = "DUMMY_ACCESS_TOKEN_1";

    pub const SINGLE_STATE: &str = "State_1";
    pub const SINGLE_NONCE: &str = "Nonce_1";

    pub struct MockTokenConfig {
        issuer: String,
        client_id: String,
        pub email: Option<EndUserEmail>,
        pub name: Option<LocalizedClaim<EndUserName>>,
        pub nonce: Option<Nonce>,
        pub duration: Option<core::time::Duration>,
        pub access: String,
        pub access_for_id: String,
    }

    impl MockTokenConfig {
        pub fn create_from_issuer_and_client(issuer: String, client_id: String) -> Self {
            let mut name = LocalizedClaim::new();
            name.insert(None, EndUserName::new("Robin".to_string()));
            let name = Some(name);

            MockTokenConfig {
                issuer,
                client_id,
                email: Some(EndUserEmail::new("robin@dummy_db.com".to_string())),
                name,
                nonce: Some(Nonce::new(SINGLE_NONCE.to_string())),
                duration: Some(core::time::Duration::from_secs(1800)),
                access: ACCESS_TOKEN.to_string(),
                access_for_id: ACCESS_TOKEN.to_string(),
            }
        }
    }

    pub fn mock_provider_metadata(provider_base_url: &str) -> DefaultProviderMetadata {
        CoreProviderMetadata::new(
            IssuerUrl::new(provider_base_url.to_string())
                .expect("Parsing mock issuer should not fail"),
            AuthUrl::new(provider_base_url.to_owned() + "/authorize")
                .expect("Parsing mock auth url should not fail"),
            JsonWebKeySetUrl::new(provider_base_url.to_owned() + "/jwk")
                .expect("Parsing mock jwk url should not fail"),
            vec![ResponseTypes::new(vec![CoreResponseType::Code])],
            vec![],
            vec![CoreJwsSigningAlgorithm::RsaSsaPssSha256],
            EmptyAdditionalProviderMetadata {},
        )
        .set_token_endpoint(Some(
            TokenUrl::new(provider_base_url.to_owned() + "/token")
                .expect("Parsing mock token url should not fail"),
        ))
        .set_scopes_supported(Some(vec![
            Scope::new("openid".to_string()),
            Scope::new("email".to_string()),
            Scope::new("profile".to_string()),
        ]))
        .set_claims_supported(Some(vec![
            CoreClaimName::new("sub".to_string()),
            CoreClaimName::new("email".to_string()),
            CoreClaimName::new("name".to_string()),
        ]))
    }

    pub fn mock_jwks() -> DefaultJsonWebKeySet {
        let jwk: CoreJsonWebKey =
            serde_json::from_str(TEST_JWK).expect("Parsing mock jwk should not fail");
        JsonWebKeySet::new(vec![jwk])
    }

    pub fn mock_token_response(
        mock_token_config: MockTokenConfig,
    ) -> StandardTokenResponse<CoreIdTokenFields, BasicTokenType> {
        let id_token = CoreIdToken::new(
            CoreIdTokenClaims::new(
                IssuerUrl::new(mock_token_config.issuer)
                    .expect("Parsing mock issuer should not fail"),
                vec![Audience::new(mock_token_config.client_id)],
                Utc::now() + Duration::seconds(300),
                Utc::now(),
                StandardClaims::new(SubjectIdentifier::new("DUMMY_SUBJECT_ID".to_string()))
                    .set_email(mock_token_config.email)
                    .set_name(mock_token_config.name),
                EmptyAdditionalClaims {},
            )
            .set_nonce(mock_token_config.nonce),
            &CoreRsaPrivateSigningKey::from_pem(TEST_PRIVATE_KEY, None)
                .expect("Cannot create mock of RSA private key"),
            CoreJwsSigningAlgorithm::RsaSsaPssSha256,
            Some(&AccessToken::new(
                mock_token_config.access_for_id.to_string(),
            )),
            None,
        )
        .expect("Cannot create mock of ID Token");

        let mut result = CoreTokenResponse::new(
            AccessToken::new(mock_token_config.access.to_string()),
            CoreTokenType::Bearer,
            CoreIdTokenFields::new(Some(id_token), EmptyExtraTokenFields {}),
        );

        result.set_expires_in(mock_token_config.duration.as_ref());

        result
    }
}

#[allow(clippy::missing_panics_doc)]
pub async fn register_ndvi_workflow_helper(app_ctx: &ProInMemoryContext) -> (Workflow, WorkflowId) {
    let dataset = add_ndvi_to_datasets(app_ctx).await;

    let workflow = Workflow {
        operator: TypedOperator::Raster(
            GdalSource {
                params: GdalSourceParameters {
                    data: dataset.into(),
                },
            }
            .boxed(),
        ),
    };

    let session = UserSession::mock();

    let id = app_ctx
        .session_context(session)
        .db()
        .register_workflow(workflow.clone())
        .await
        .unwrap();

    (workflow, id)
}

#[allow(clippy::missing_panics_doc)]
pub async fn add_ndvi_to_datasets(app_ctx: &ProInMemoryContext) -> DatasetId {
    let ndvi = DatasetDefinition {
        properties: AddDataset {
            id: None,
            name: "NDVI".to_string(),
            description: "NDVI data from MODIS".to_string(),
            source_operator: "GdalSource".to_string(),
            symbology: None,
            provenance: Some(vec![Provenance {
                citation: "Sample Citation".to_owned(),
                license: "Sample License".to_owned(),
                uri: "http://example.org/".to_owned(),
            }]),
        },
        meta_data: MetaDataDefinition::GdalMetaDataRegular(create_ndvi_meta_data()),
    };

    let system_session = UserSession::admin_session();

    let db = app_ctx.session_context(system_session).db();

    let dataset_id = db
        .add_dataset(
            ndvi.properties
                .validated()
                .expect("valid dataset description"),
            Box::new(ndvi.meta_data),
        )
        .await
        .expect("dataset db access");

    db.add_permission(
        Role::registered_user_role_id(),
        dataset_id,
        Permission::Read,
    )
    .await
    .unwrap();

    db.add_permission(Role::anonymous_role_id(), dataset_id, Permission::Read)
        .await
        .unwrap();

    dataset_id
}

#[allow(clippy::missing_panics_doc)]
pub async fn admin_login<
    C: ApplicationContext<Session = UserSession> + UserAuth + OidcRequestDbProvider,
>(
    ctx: &C,
) -> UserSession
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let user_config = get_config_element::<super::config::User>().unwrap();

    ctx.login(UserCredentials {
        email: user_config.admin_email.clone(),
        password: user_config.admin_password.clone(),
    })
    .await
    .unwrap()
}
