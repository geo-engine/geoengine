use actix_web::{web, FromRequest, HttpResponse};
use serde::Deserialize;
use utoipa::ToSchema;

use crate::contexts::{ApplicationContext, SessionContext};
use crate::error::Result;

use crate::pro::contexts::{OidcRequestDbProvider, ProGeoEngineDb};
use crate::pro::permissions::Permission;
use crate::pro::permissions::{PermissionDb, ResourceId, RoleId};
use crate::pro::users::{UserAuth, UserSession};

pub(crate) fn init_permissions_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext<Session = UserSession> + UserAuth + OidcRequestDbProvider,
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
    C::Session: FromRequest,
{
    cfg.service(
        web::scope("/permissions").service(
            web::resource("")
                .route(web::put().to(add_permission_handler::<C>))
                .route(web::delete().to(remove_permission_handler::<C>)),
        ),
    );
}

/// Request for adding a new permission to the given role on the given resource
#[derive(Debug, PartialEq, Eq, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PermissionRequest {
    resource_id: ResourceId,
    role_id: RoleId,
    permission: Permission,
}

/// Adds a new permission.
#[utoipa::path(
    tag = "Permissions",
    put,
    path = "/permissions",
    request_body(content = PermissionRequest, example =
        json!({
            "resourceId": {
                "type": "Layer",
                "id": "00000000-0000-0000-0000-000000000000",
            },
            "roleId": "00000000-0000-0000-0000-000000000000",
            "permission": "Read"
        })
    ),
    responses(
        (status = 200, description = "OK"),        
    ),
    security(
        ("session_token" = [])
    )
)]
async fn add_permission_handler<
    C: ApplicationContext<Session = UserSession> + UserAuth + OidcRequestDbProvider,
>(
    session: C::Session,
    app_ctx: web::Data<C>,
    permission: web::Json<PermissionRequest>,
) -> Result<HttpResponse>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let permission = permission.into_inner();

    app_ctx
        .session_context(session)
        .db()
        .add_permission(
            permission.role_id,
            permission.resource_id,
            permission.permission,
        )
        .await?;

    Ok(HttpResponse::Ok().finish())
}

/// Removes an existing permission.
#[utoipa::path(
    tag = "Permissions",
    delete,
    path = "/permissions",
    request_body(content = PermissionRequest, example =
        json!({
            "resourceId": {
                "type": "Layer",
                "id": "00000000-0000-0000-0000-000000000000",
            },
            "roleId": "00000000-0000-0000-0000-000000000000",
            "permission": "Read"
        })
    ),
    responses(
        (status = 200, description = "OK"),        
    ),
    security(
        ("session_token" = [])
    )
)]
async fn remove_permission_handler<
    C: ApplicationContext<Session = UserSession> + UserAuth + OidcRequestDbProvider,
>(
    session: C::Session,
    app_ctx: web::Data<C>,
    permission: web::Json<PermissionRequest>,
) -> Result<HttpResponse>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let permission = permission.into_inner();

    app_ctx
        .session_context(session)
        .db()
        .remove_permission(
            permission.role_id,
            permission.resource_id,
            permission.permission,
        )
        .await?;

    Ok(HttpResponse::Ok().finish())
}
