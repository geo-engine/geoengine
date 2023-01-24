use openid::{
    uma2::{
        DiscoveredUma2, Uma2AuthenticationMethod, Uma2Owner, Uma2PermissionDecisionStrategy,
        Uma2PermissionLogic, Uma2PermissionTicketRequest,
    },
    Client, StandardClaims,
};
use uuid::Uuid;

use crate::{error::Result, util::config::get_config_element};

use super::util::config::Oidc;

type UmaClient = Client<DiscoveredUma2, StandardClaims>;

pub enum ResourceType {
    Dataset,
    RasterDataset,
}

impl ResourceType {
    fn uma_resource_type_name(&self) -> &'static str {
        match self {
            ResourceType::Dataset => "dataset", // TODO: name space?
            ResourceType::RasterDataset => "raster_dataset", // TODO: name space?
        }
    }
}

pub enum ResourceScope {
    Read,
    Write,
}

impl ResourceScope {
    fn uma_scope_name(&self) -> &'static str {
        // TODO (idea): additional type ReadWrite and make a method to create all the scopes from a variant as an array
        match self {
            ResourceScope::Read => "read",
            ResourceScope::Write => "write",
        }
    }
}

pub async fn uma_client() -> Result<UmaClient> {
    let config = get_config_element::<Oidc>()?;

    UmaClient::discover_uma2(
        config.client_id.clone(),
        config
            .client_secret
            .clone()
            .ok_or(crate::error::Error::OidcMissingClientSecret)?,
        Some(config.redirect_uri.clone()),
        config.issuer.parse()?,
    )
    .await
    .map_err(Into::into)
}

// TODO: resource name should be unique, so use UUID? but also maybe include datasert name for filtering?
pub async fn register_resource(
    user_access_token: String,
    resource_name: String,
    resource_type: ResourceType,
    owner: Option<Uma2Owner>,
) -> Result<String> {
    // TODO: reuse client
    let client = uma_client().await?;

    let resource = client
        .create_uma2_resource(
            user_access_token,
            resource_name,
            Some(resource_type.uma_resource_type_name().into()),
            None,
            Some(vec![
                ResourceScope::Read.uma_scope_name().into(),
                ResourceScope::Write.uma_scope_name().into(),
            ]),
            None, // TODO: dataset name? has to be kept in sync though when dataset name is updated in Geo Engine
            owner,
            Some(true),
        )
        .await?;

    resource
        .id
        .ok_or(crate::error::Error::Uma2MissingResourceId)
}

pub async fn grant_permission(
    user_access_token: String,
    resource_id: String,
    scopes: Vec<ResourceScope>,
    roles: Option<Vec<String>>,
    groups: Option<Vec<String>>,
) -> Result<()> {
    // TODO: ensure at least one of roles or groups is given

    // TODO: reuse client
    let client = uma_client().await?;

    let _permission = client
        .associate_uma2_resource_with_a_permission(
            user_access_token,
            resource_id,
            Uuid::new_v4().to_string(), // TODO: embed resource, target (roles+groups), scopes into the name?
            String::new(),
            scopes
                .into_iter()
                .map(|s| s.uma_scope_name().into())
                .collect(),
            roles,
            groups,
            None,
            None,
            Some(Uma2PermissionLogic::Positive),
            Some(Uma2PermissionDecisionStrategy::Affirmative),
        )
        .await?;

    Ok(())
}

/// Check whether the owner of the suppliad token has the permission to access the resource on the given scope
pub async fn has_permission(
    user_access_token: String,
    resource_id: String,
    scope: ResourceScope,
) -> Result<bool> {
    // TODO: reuse client
    let client = uma_client().await?;

    let request = Uma2PermissionTicketRequest {
        resource_id: resource_id.clone(),
        resource_scopes: Some(vec![scope.uma_scope_name().into()]),
        claims: None,
    };

    let ticket = client
        .create_uma2_permission_ticket(user_access_token.clone(), vec![request])
        .await?;

    let permission = None; // TODO ??
    let response = client
        .obtain_requesting_party_token(
            user_access_token,
            Uma2AuthenticationMethod::Bearer,
            Some(ticket.ticket),
            None,
            None,
            None,
            permission,
            Some("test-client".into()),
            Some(true),
            None,
            Some(true),
        )
        .await;

    match response {
        Ok(_token) => Ok(true),
        Err(_) => Ok(false), // TODO: only access denied error should return Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use actix_http::header::{AUTHORIZATION, CONTENT_TYPE};
    use futures::{stream, StreamExt};
    use keycloak::{
        types::{
            ClientRepresentation, CredentialRepresentation, RealmRepresentation,
            ResourceServerRepresentation, RoleRepresentation, ScopeRepresentation,
            UserRepresentation,
        },
        KeycloakAdmin, KeycloakAdminToken, KeycloakError,
    };
    use openid::uma2::{Uma2Owner, Uma2Provider};
    use serde_json::Value;

    use super::*;

    // set up a keycloak realm using the keycloak admin api
    #[allow(clippy::too_many_lines)]
    async fn setup_keycloak_test_realm() -> Result<KeycloakAdmin> {
        // TODO: use groups?

        // TODO: load from config(?)
        let keycloak_url = "http://localhost:8080";
        let user = "admin";
        let password = "admin";

        let realm_name = "TestRealm";

        let client_id = "test-client";
        let client_secret = "ZXlV2S6aZcfDfe1ILa4dRuk4KlI8k7qg";
        let client_redirect_url = "*";

        let users = [("test-user", "test"), ("test-user2", "test")];

        let roles = ["test-role"];

        let client = reqwest::Client::new();
        let admin_token =
            KeycloakAdminToken::acquire(keycloak_url, user, password, &client).await?;

        let admin = KeycloakAdmin::new(keycloak_url, admin_token, client);

        // remove the test realm if it already exist
        let _delete = admin.realm_delete("TestRealm").await;

        // (re-)create test realm
        admin
            .post(RealmRepresentation {
                realm: Some(realm_name.into()),
                enabled: Some(true),
                ..Default::default()
            })
            .await?;

        // create client
        admin
            .realm_clients_post(
                realm_name,
                ClientRepresentation {
                    id: Some(client_id.into()),
                    enabled: Some(true),
                    secret: Some(client_secret.into()),
                    service_accounts_enabled: Some(true),
                    redirect_uris: Some(vec![client_redirect_url.into()]),
                    direct_access_grants_enabled: Some(true),
                    authorization_services_enabled: Some(true),
                    authorization_settings: Some(ResourceServerRepresentation {
                        allow_remote_resource_management: Some(true),
                        scopes: Some(vec![
                            ScopeRepresentation {
                                name: Some("read".into()),
                                ..Default::default()
                            },
                            ScopeRepresentation {
                                name: Some("write".into()),
                                ..Default::default()
                            },
                        ]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await?;

        // create roles
        for name in roles {
            admin
                .realm_roles_post(
                    realm_name,
                    RoleRepresentation {
                        // id: Some(id.into()), note: id is ignored unfortunately
                        name: Some(name.into()),
                        container_id: Some(realm_name.into()),
                        ..Default::default()
                    },
                )
                .await?;

            // TODO: add roles as (realm) default roles(?), how?
        }

        // resolve the role ids because we cannot get them directly
        let roles_with_id: Vec<Result<RoleRepresentation, KeycloakError>> = stream::iter(roles)
            .then(|name| async { admin.realm_roles_with_role_name_get(realm_name, name).await })
            .collect()
            .await;
        let roles_with_id = roles_with_id
            .into_iter()
            .collect::<Result<Vec<_>, KeycloakError>>()?;

        // create users
        for (user, password) in users {
            admin
                .realm_users_post(
                    realm_name,
                    UserRepresentation {
                        // id: Some(id.into()), note: id is ignored unfortunately
                        username: Some(user.into()),
                        enabled: Some(true),
                        credentials: Some(vec![CredentialRepresentation {
                            value: Some(password.into()),
                            temporary: Some(false),
                            ..Default::default()
                        }]),
                        // setting realm roles here somehow has no effect, maybe needs more fields?
                        // realm_roles: Some(
                        //     roles_with_id
                        //         .iter()
                        //         .map(|role| role.id.as_ref().unwrap().clone()) // TODO: when is id not present?
                        //         .collect(),
                        // ),
                        ..Default::default()
                    },
                )
                .await?;

            // note: user id is returned in the location header of the response, but not accessible in the keycloak crate
            // thus we iterate manually over the users to add them to the roles
        }

        // assing roles to users
        let users = admin
            .realm_users_get(
                realm_name,
                Some(true),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;

        let uma_protection_role = admin
            .realm_clients_with_id_roles_with_role_name_get(realm_name, client_id, "uma_protection")
            .await?;

        for user in users {
            // as permissions can only be granted to roles and groups and not user(?), we need to create a role for each user
            // to allow granting permissions to single users

            // TODO: extract a method for building a role name from a user id
            let user_role_name = format!("user_{}", user.id.as_ref().unwrap());

            admin
                .realm_roles_post(
                    realm_name,
                    RoleRepresentation {
                        name: Some(user_role_name.clone()),
                        container_id: Some(realm_name.into()),
                        ..Default::default()
                    },
                )
                .await?;

            let user_role = admin
                .realm_roles_with_role_name_get(realm_name, &user_role_name)
                .await?;

            // add user to user_role and the other custom roles
            admin
                .realm_users_with_id_role_mappings_realm_post(
                    realm_name,
                    user.id.as_ref().unwrap(), // TODO: when is id None?
                    roles_with_id
                        .iter()
                        .chain([user_role].iter())
                        .map(|role| RoleRepresentation {
                            id: Some(role.id.as_ref().unwrap().into()),
                            name: Some(role.name.as_ref().unwrap().into()),
                            ..Default::default()
                        })
                        .collect(),
                )
                .await?;

            // add user to uma_protection role, s.t. they can access resources
            // TODO: instead of adding each user to uma_protections, we could have a common group with role uma_protection
            admin
                .realm_users_with_id_role_mappings_clients_with_client_post(
                    realm_name,
                    user.id.as_ref().unwrap(),
                    client_id,
                    vec![RoleRepresentation {
                        id: Some(uma_protection_role.id.as_ref().unwrap().into()),
                        name: Some(uma_protection_role.name.as_ref().unwrap().into()),
                        ..Default::default()
                    }],
                )
                .await?;
        }

        Ok(admin)
    }

    // ignore test for now because it requires a running keycloak instance
    #[ignore]
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test() -> Result<()> {
        // make sure keycloak runs with:
        // `podman run --name keycloak -dt -p 8080:8080 -e KEYCLOAK_ADMIN=admin -e KEYCLOAK_ADMIN_PASSWORD=admin quay.io/keycloak/keycloak:20.0 start-dev`
        let admin = setup_keycloak_test_realm().await?;

        let client = uma_client().await?;

        let client_token = client
            .request_token_using_client_credentials()
            .await?
            .access_token;

        let user_access_token = client
            .request_token_using_password_credentials("test-user", "test", None)
            .await?
            .access_token;

        let user2_access_token = client
            .request_token_using_password_credentials("test-user2", "test", None)
            .await?
            .access_token;

        // Create a new resource, owned by the client
        let resource_id = register_resource(
            client_token.clone(),
            "new_resource".to_string(),
            ResourceType::Dataset,
            None,
        )
        .await
        .unwrap();

        // ensure user doesn't have access yets
        assert!(
            !has_permission(
                user_access_token.clone(),
                resource_id.clone(),
                ResourceScope::Read
            )
            .await?
        );

        // give read permision to the test-role
        // note: we use the client token here, because the client owns the resource. In practice, we need to check whether the entity that wants to give the permission is allowed to do so.
        grant_permission(
            client_token.clone(),
            resource_id.clone(),
            vec![ResourceScope::Read],
            Some(vec!["test-role".to_string()]),
            None,
        )
        .await?;

        // test-user can read
        assert!(
            has_permission(
                user_access_token.clone(),
                resource_id.clone(),
                ResourceScope::Read
            )
            .await?
        );

        // but not write
        assert!(
            !has_permission(
                user_access_token.clone(),
                resource_id.clone(),
                ResourceScope::Write
            )
            .await?
        );

        // test-user2 can read too
        assert!(
            has_permission(
                user2_access_token.clone(),
                resource_id.clone(),
                ResourceScope::Read
            )
            .await?
        );

        // create another resource
        let resource2_id = register_resource(
            client_token.clone(),
            "new-resource2".to_string(),
            ResourceType::Dataset,
            None,
        )
        .await?;

        // share only with test-user
        // note: there is no way to get a user by name, so we need to get all users and filter to get the user id
        let users = admin
            .realm_users_get(
                "TestRealm",
                Some(true),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;
        let test_user_id = users
            .iter()
            .find(|user| user.username == Some("test-user".to_string()))
            .unwrap()
            .id
            .clone()
            .unwrap();

        grant_permission(
            client_token.clone(),
            resource2_id.clone(),
            vec![ResourceScope::Read],
            Some(vec![format!("user_{}", test_user_id)]),
            None,
        )
        .await?;

        // test-user can read
        assert!(
            has_permission(
                user_access_token.clone(),
                resource2_id.clone(),
                ResourceScope::Read
            )
            .await?
        );

        // but test-user2 can not
        assert!(
            !has_permission(
                user2_access_token.clone(),
                resource2_id.clone(),
                ResourceScope::Read
            )
            .await?
        );

        // check that test-user can read all the resources
        // note: there is no pagination here... maybe we have to use another endpoint?
        let mut search = client
            .search_for_uma2_resources(
                user_access_token.clone(),
                None,
                None,
                None,
                None,
                Some(ResourceScope::Read.uma_scope_name().to_string()),
                Some(0),
                Some(1),
            )
            .await?;
        search.sort();

        dbg!(&search);

        let mut expected = vec![resource_id.clone(), resource2_id.clone()];
        expected.sort();

        assert_eq!(search, expected);

        Ok(())
    }

    // ignore test for now because it requires a running keycloak instance
    #[ignore]
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test2() -> Result<()> {
        // make sure keycloak runs with:
        // `podman run --name keycloak -dt -p 8080:8080 -e KEYCLOAK_ADMIN=admin -e KEYCLOAK_ADMIN_PASSWORD=admin quay.io/keycloak/keycloak:20.0 start-dev`
        let admin = setup_keycloak_test_realm().await?;

        let client = uma_client().await?;

        let client_token = client
            .request_token_using_client_credentials()
            .await?
            .access_token;

        let user_access_token = client
            .request_token_using_password_credentials("test-user", "test", None)
            .await?
            .access_token;

        let user2_access_token = client
            .request_token_using_password_credentials("test-user2", "test", None)
            .await?
            .access_token;

        let resource_name = "new_resource".to_string();

        let resource_type = ResourceType::Dataset;

        let resource = client
            .create_uma2_resource(
                user_access_token.clone(),
                resource_name,
                Some(resource_type.uma_resource_type_name().into()),
                None,
                Some(vec![
                    ResourceScope::Read.uma_scope_name().into(),
                    ResourceScope::Write.uma_scope_name().into(),
                ]),
                None, // TODO: dataset name? has to be kept in sync though when dataset name is updated in Geo Engine
                Some(Uma2Owner {
                    id: None,
                    name: Some("test-user".to_string()),
                }),
                Some(true),
            )
            .await?;

        let resource_id = resource
            .id
            .ok_or(crate::error::Error::Uma2MissingResourceId)?;

        let users = admin
            .realm_users_get(
                "TestRealm",
                Some(true),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;

        let test_user_id = users
            .iter()
            .find(|user| user.username == Some("test-user".to_string()))
            .unwrap()
            .id
            .clone()
            .unwrap();

        grant_permission(
            user_access_token.clone(),
            resource_id.clone(),
            vec![ResourceScope::Read],
            Some(vec![format!("user_{}", test_user_id)]),
            None,
        )
        .await?;

        let permission_search = client
            .search_for_uma2_resource_permission(
                user_access_token.clone(),
                None,                                                   // resource
                None,                                                   // name
                Some(ResourceScope::Read.uma_scope_name().to_string()), // scope
                Some(0),
                Some(10),
            )
            .await?;

        dbg!(permission_search);

        Ok(())
    }

    // ignore test for now because it requires a running keycloak instance
    #[ignore]
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_order() -> Result<()> {
        // make sure keycloak runs with:
        // `podman run --name keycloak -dt -p 8080:8080 -e KEYCLOAK_ADMIN=admin -e KEYCLOAK_ADMIN_PASSWORD=admin quay.io/keycloak/keycloak:20.0 start-dev`
        let admin = setup_keycloak_test_realm().await?;

        let client = uma_client().await?;

        let client_token = client
            .request_token_using_client_credentials()
            .await?
            .access_token;

        let user_access_token = client
            .request_token_using_password_credentials("test-user", "test", None)
            .await?
            .access_token;

        let user2_access_token = client
            .request_token_using_password_credentials("test-user2", "test", None)
            .await?
            .access_token;

        // Create a new resource, owned by the client
        let resource_id = register_resource(
            client_token.clone(),
            "AResource1".to_string(),
            ResourceType::Dataset,
            None,
        )
        .await
        .unwrap();

        // create another resource
        let resource2_id = register_resource(
            client_token.clone(),
            "AResource2".to_string(),
            ResourceType::RasterDataset,
            None,
        )
        .await?;

        // create another resource
        let resource2_id = register_resource(
            user2_access_token.clone(),
            "BRescource (shared)".to_string(),
            ResourceType::Dataset,
            None,
            // Some(Uma2Owner {
            //     id: None,
            //     name: Some("test-user2".to_string()),
            // }),
        )
        .await?;

        let users = admin
            .realm_users_get(
                "TestRealm",
                Some(true),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;

        let test_user_id = users
            .iter()
            .find(|user| user.username == Some("test-user".to_string()))
            .unwrap()
            .id
            .clone()
            .unwrap();

        // grant_permission(
        //     user2_access_token.clone(),
        //     resource2_id.clone(),
        //     vec![ResourceScope::Read],
        //     Some(vec![format!("user_{}", test_user_id)]),
        //     None,
        // )
        // .await?;

        // check that test-user can read all the resources
        // note: there is no pagination here... maybe we have to use another endpoint?
        let mut search = client
            .search_for_uma2_resources(
                user2_access_token.clone(),
                None,
                None,
                None,
                None,
                // Some(
                //     ResourceType::RasterDataset
                //         .uma_resource_type_name()
                //         .to_string(),
                // ),
                Some(ResourceScope::Read.uma_scope_name().to_string()),
                Some(0),
                Some(10),
            )
            .await?;

        for resource_id in search {
            let resource = client
                .get_uma2_resource_by_id(user_access_token.clone(), resource_id)
                .await?;

            dbg!(&resource.name);
        }

        Ok(())
    }
}
