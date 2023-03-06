use actix_web::{web, FromRequest, HttpResponse};
use serde::Deserialize;
use utoipa::ToSchema;



use crate::error::Result;

use crate::pro::contexts::ProContext;
use crate::pro::permissions::{PermissionDb, RoleId, ResourceId};
use crate::{pro::permissions::Permission};

pub(crate) fn init_permissions_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProContext,
    C::Session: FromRequest,
{
    cfg.service(
        web::scope("/permissions")
            .service(web::resource("").route(web::put().to(add_permissions_handler::<C>))), // .service(web::resource("").route(web::delete().to(remove_permission_handler::<C>))),
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
pub struct PermissionRequest {
    resource_id: ResourceId,
    role: RoleId,
    permission: Permission,
}

/// Registers a new Workflow.
#[utoipa::path(
    tag = "Permissions",
    put,
    path = "/permissions",
    request_body(content = PermissionRequest, example =
        json!({ 
            "resource_type": "Layer",
            "resource_id": "00000000-0000-0000-0000-000000000000",
            "role": "00000000-0000-0000-0000-000000000000",
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
async fn add_permissions_handler<C: ProContext>(
    session: C::Session,
    ctx: web::Data<C>,
    permission: web::Json<PermissionRequest>,
) -> Result<HttpResponse> {
    let permission = permission.into_inner();

    ctx.pro_db(session)
        .add_permission(permission.role, permission.resource_id, permission.permission)
        .await?;

    Ok(HttpResponse::Ok().finish())
}
