use crate::api::model::datatypes::LayerId;
use crate::contexts::{ApplicationContext, SessionContext};
use crate::datasets::storage::DatasetDb;
use crate::datasets::DatasetName;
use crate::error::Result;
use crate::layers::listing::LayerCollectionId;
use crate::pro::contexts::{ProApplicationContext, ProGeoEngineDb};
use crate::pro::permissions::Permission;
use crate::pro::permissions::{PermissionDb, ResourceId, RoleId};
use crate::projects::ProjectId;
use actix_web::{web, FromRequest, HttpResponse};
use serde::Deserialize;
use utoipa::ToSchema;

pub(crate) fn init_permissions_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProApplicationContext,
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
    resource: Resource,
    role_id: RoleId,
    permission: Permission,
}

/// A resource that is affected by a permission.
#[derive(Debug, PartialEq, Eq, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type", content = "id")]
pub enum Resource {
    Layer(LayerId),
    LayerCollection(LayerCollectionId),
    Project(ProjectId),
    Dataset(DatasetName),
}

impl Resource {
    pub async fn into_resource_id<D: DatasetDb>(self, db: &D) -> Result<ResourceId> {
        Ok(match self {
            Resource::Layer(layer_id) => ResourceId::Layer(layer_id.into()),
            Resource::LayerCollection(layer_collection_id) => {
                ResourceId::LayerCollection(layer_collection_id)
            }
            Resource::Project(project_id) => ResourceId::Project(project_id),
            Resource::Dataset(dataset_name) => {
                ResourceId::DatasetId(db.resolve_dataset_name_to_id(&dataset_name).await?)
            }
        })
    }
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
    db.add_permission(
        permission.role_id,
        permission.resource.into_resource_id(&db).await?,
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
    db.remove_permission(
        permission.role_id,
        permission.resource.into_resource_id(&db).await?,
        permission.permission,
    )
    .await?;

    Ok(HttpResponse::Ok().finish())
}

#[cfg(test)]
mod tests {

    use geoengine_operators::{
        engine::{RasterOperator, VectorOperator, WorkflowOperatorPath},
        source::{GdalSource, GdalSourceParameters, OgrSource, OgrSourceParameters},
    };

    use crate::pro::util::tests::with_pro_temp_context;
    use crate::pro::{
        users::{UserAuth, UserCredentials, UserRegistration},
        util::tests::{add_ndvi_to_datasets, add_ports_to_datasets, admin_login},
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn it_checks_permission_during_intialization() {
        with_pro_temp_context(|app_ctx, _| async move {
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
                add_ndvi_to_datasets(&app_ctx, false, false).await;
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
        })
        .await;
    }
}
