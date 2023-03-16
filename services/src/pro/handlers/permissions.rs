use actix_web::{web, FromRequest, HttpResponse};
use serde::Deserialize;
use utoipa::ToSchema;

use crate::error::Result;

use crate::pro::contexts::{ProContext, ProGeoEngineDb};
use crate::pro::permissions::Permission;
use crate::pro::permissions::{PermissionDb, ResourceId, RoleId};

pub(crate) fn init_permissions_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProContext,
    C::GeoEngineDB: ProGeoEngineDb,
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

/// Enum used for dispatching the permissions to the correct Dbs
#[derive(Debug, PartialEq, Eq, Deserialize, Clone, Hash, ToSchema)]
pub enum ResourceType {
    // Dataset,
    Layer,
    LayerCollection,
    // Workflow, Project, Upload, Task, ...
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
async fn add_permission_handler<C: ProContext>(
    session: C::Session,
    ctx: web::Data<C>,
    permission: web::Json<PermissionRequest>,
) -> Result<HttpResponse>
where
    C::GeoEngineDB: ProGeoEngineDb,
{
    let permission = permission.into_inner();

    ctx.db(session)
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
async fn remove_permission_handler<C: ProContext>(
    session: C::Session,
    ctx: web::Data<C>,
    permission: web::Json<PermissionRequest>,
) -> Result<HttpResponse>
where
    C::GeoEngineDB: ProGeoEngineDb,
{
    let permission = permission.into_inner();

    ctx.db(session)
        .remove_permission(
            permission.role_id,
            permission.resource_id,
            permission.permission,
        )
        .await?;

    Ok(HttpResponse::Ok().finish())
}
