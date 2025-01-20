use crate::api::model::datatypes::{DatasetId, LayerId};
use crate::contexts::{ApplicationContext, SessionContext};
use crate::error::Result;
use crate::layers::listing::LayerCollectionId;
use crate::permissions::{Permission, PermissionListing};
use crate::permissions::{PermissionDb, ResourceId, RoleId};
use crate::pro::contexts::{ProApplicationContext, ProGeoEngineDb};
use crate::projects::ProjectId;
use actix_web::{web, FromRequest, HttpResponse};
use geoengine_datatypes::error::BoxedResultExt;
use serde::Deserialize;
use utoipa::{IntoParams, ToSchema};

pub(crate) fn init_permissions_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProApplicationContext,
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
    C::Session: FromRequest,
{
    cfg.service(
        web::scope("/permissions")
            .service(
                web::resource("")
                    .route(web::put().to(add_permission_handler::<C>))
                    .route(web::delete().to(remove_permission_handler::<C>)),
            )
            .service(
                web::resource("/resources/{resource_type}/{resource_id}")
                    .route(web::get().to(get_resource_permissions_handler::<C>)),
            ),
    );
}

/// Request for adding a new permission to the given role on the given resource
#[derive(Debug, PartialEq, Eq, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PermissionRequest {
    resource: Resource,
    role_id: RoleId,
    permission: Permission,
}

/// A resource that is affected by a permission.
#[derive(Debug, PartialEq, Eq, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type", content = "id")]
pub enum Resource {
    #[schema(title = "LayerResource")]
    Layer(LayerId),
    #[schema(title = "LayerCollectionResource")]
    LayerCollection(LayerCollectionId),
    #[schema(title = "ProjectResource")]
    Project(ProjectId),
    #[schema(title = "DatasetResource")]
    Dataset(DatasetId),
}

impl From<Resource> for ResourceId {
    fn from(resource: Resource) -> Self {
        match resource {
            Resource::Layer(layer_id) => ResourceId::Layer(layer_id.into()),
            Resource::LayerCollection(layer_collection_id) => {
                ResourceId::LayerCollection(layer_collection_id)
            }
            Resource::Project(project_id) => ResourceId::Project(project_id),
            Resource::Dataset(dataset_id) => ResourceId::DatasetId(dataset_id.into()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize, Clone, IntoParams, ToSchema)]
pub struct PermissionListOptions {
    pub limit: u32,
    pub offset: u32,
}

/// Lists permission for a given resource.
#[utoipa::path(
    tag = "Permissions",
    get,
    path = "/permissions/resources/{resource_type}/{resource_id}",
    responses(
        (status = 200, description = "List of permission", body = Vec<PermissionListing>),        
    ),
    params(
        ("resource_type" = String, description = "Resource Type"),
        ("resource_id" = String, description = "Resource Id"),
        PermissionListOptions,
    ),
    security(
        ("session_token" = [])
    )
)]
async fn get_resource_permissions_handler<C: ProApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    resource_id: web::Path<(String, String)>,
    options: web::Query<PermissionListOptions>,
) -> Result<web::Json<Vec<PermissionListing>>>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let resource_id = ResourceId::try_from(resource_id.into_inner())?;
    let options = options.into_inner();

    let db = app_ctx.session_context(session).db();
    let permissions = db
        .list_permissions(resource_id, options.offset, options.limit)
        .await
        .boxed_context(crate::error::PermissionDb)?;

    Ok(web::Json(permissions))
}

/// Adds a new permission.
#[utoipa::path(
    tag = "Permissions",
    put,
    path = "/permissions",
    request_body(content = PermissionRequest, example =
        json!({
            "resource": {
                "type": "layer",
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
async fn add_permission_handler<C: ProApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    permission: web::Json<PermissionRequest>,
) -> Result<HttpResponse>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let permission = permission.into_inner();

    let db = app_ctx.session_context(session).db();
    db.add_permission::<ResourceId>(
        permission.role_id,
        permission.resource.into(),
        permission.permission,
    )
    .await
    .boxed_context(crate::error::PermissionDb)?;

    Ok(HttpResponse::Ok().finish())
}

/// Removes an existing permission.
#[utoipa::path(
    tag = "Permissions",
    delete,
    path = "/permissions",
    request_body(content = PermissionRequest, example =
        json!({
            "resource": {
                "type": "layer",
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
async fn remove_permission_handler<C: ProApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    permission: web::Json<PermissionRequest>,
) -> Result<HttpResponse>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let permission = permission.into_inner();

    let db = app_ctx.session_context(session).db();
    db.remove_permission::<ResourceId>(
        permission.role_id,
        permission.resource.into(),
        permission.permission,
    )
    .await
    .boxed_context(crate::error::PermissionDb)?;

    Ok(HttpResponse::Ok().finish())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        ge_context,
        pro::contexts::PostgresContext,
        users::{UserAuth, UserCredentials, UserRegistration},
        util::tests::{
            add_ndvi_to_datasets2, add_ports_to_datasets, admin_login, read_body_string,
            send_test_request,
        },
    };
    use actix_http::header;
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_operators::{
        engine::{RasterOperator, VectorOperator, WorkflowOperatorPath},
        source::{GdalSource, GdalSourceParameters, OgrSource, OgrSourceParameters},
    };
    use serde_json::{json, Value};
    use tokio_postgres::NoTls;

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_checks_permission_during_intialization(app_ctx: PostgresContext<NoTls>) {
        // create user and sessions

        let user_id = app_ctx
            .register_user(UserRegistration {
                email: "test@localhost".to_string(),
                real_name: "Foo Bar".to_string(),
                password: "test".to_string(),
            })
            .await
            .unwrap();

        let user_session = app_ctx
            .login(UserCredentials {
                email: "test@localhost".to_string(),
                password: "test".to_string(),
            })
            .await
            .unwrap();

        let admin_session = admin_login(&app_ctx).await;
        let admin_ctx = app_ctx.session_context(admin_session.clone());

        // setup data and operators

        let (gdal_dataset_id, gdal_dataset_name) =
            add_ndvi_to_datasets2(&app_ctx, false, false).await;
        let gdal = GdalSource {
            params: GdalSourceParameters {
                data: gdal_dataset_name,
            },
        }
        .boxed();

        let (ogr_dataset_id, ogr_dataset_name) =
            add_ports_to_datasets(&app_ctx, false, false).await;
        let ogr = OgrSource {
            params: OgrSourceParameters {
                data: ogr_dataset_name,
                attribute_filters: None,
                attribute_projection: None,
            },
        }
        .boxed();

        let user_ctx = app_ctx.session_context(user_session.clone());

        let exe_ctx = user_ctx.execution_context().unwrap();

        // check that workflow can only be intitialized after adding permissions

        assert!(gdal
            .clone()
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .is_err());

        admin_ctx
            .db()
            .add_permission(
                user_id.into(),
                ResourceId::from(gdal_dataset_id),
                Permission::Read,
            )
            .await
            .unwrap();

        assert!(gdal
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .is_ok());

        assert!(ogr
            .clone()
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .is_err());

        admin_ctx
            .db()
            .add_permission(
                user_id.into(),
                ResourceId::from(ogr_dataset_id),
                Permission::Read,
            )
            .await
            .unwrap();

        assert!(ogr
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .is_ok());
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_lists_permissions(app_ctx: PostgresContext<NoTls>) {
        let admin_session = admin_login(&app_ctx).await;

        let (gdal_dataset_id, _) = add_ndvi_to_datasets2(&app_ctx, true, true).await;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/permissions/resources/dataset/{gdal_dataset_id}?offset=0&limit=10",
            ))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session.id.to_string()),
            ));
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        let res_body = serde_json::from_str::<Value>(&read_body_string(res).await).unwrap();
        assert_eq!(res_status, 200, "{res_body}");

        assert_eq!(
            res_body,
            json!([{
                   "permission":"Owner",
                   "resourceId":  {
                       "id": gdal_dataset_id.to_string(),
                       "type": "DatasetId"
                   },
                   "role": {
                       "id": "d5328854-6190-4af9-ad69-4e74b0961ac9",
                       "name":
                       "admin"
                   }
               }, {
                   "permission": "Read",
                   "resourceId": {
                       "id": gdal_dataset_id.to_string(),
                       "type": "DatasetId"
                   },
                   "role": {
                       "id": "fd8e87bf-515c-4f36-8da6-1a53702ff102",
                       "name": "anonymous"
                   }
               }, {
                   "permission": "Read",
                   "resourceId": {
                       "id": gdal_dataset_id.to_string(),
                       "type": "DatasetId"
                   },
                   "role": {
                       "id": "4e8081b6-8aa6-4275-af0c-2fa2da557d28",
                       "name":
                       "user"
                   }
               }]
            )
        );
    }
}
