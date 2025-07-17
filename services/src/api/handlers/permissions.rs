use crate::api::model::datatypes::LayerId;
use crate::contexts::{ApplicationContext, GeoEngineDb, SessionContext};
use crate::datasets::DatasetName;
use crate::datasets::storage::DatasetDb;
use crate::error::{self, Error, Result};
use crate::layers::listing::LayerCollectionId;
use crate::machine_learning::MlModelDb;
use crate::permissions::{
    Permission, PermissionDb, PermissionListing as DbPermissionListing, ResourceId, Role, RoleId,
};
use crate::projects::ProjectId;
use actix_web::{FromRequest, HttpResponse, web};
use geoengine_datatypes::error::BoxedResultExt;
use geoengine_datatypes::machine_learning::MlModelName;
use geoengine_macros::type_tag;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::str::FromStr;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

pub(crate) fn init_permissions_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PermissionListing {
    resource: Resource,
    role: Role,
    permission: Permission,
}

impl PermissionListing {
    fn wrap_permission_listing_and_resource(
        resource: Resource,
        db_permission_listing: DbPermissionListing,
    ) -> PermissionListing {
        Self {
            resource,
            role: db_permission_listing.role,
            permission: db_permission_listing.permission,
        }
    }
}

/// A resource that is affected by a permission.
#[derive(Debug, PartialEq, Eq, Deserialize, Clone, ToSchema, Serialize)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum Resource {
    Layer(LayerResource),
    LayerCollection(LayerCollectionResource),
    Project(ProjectResource),
    Dataset(DatasetResource),
    MlModel(MlModelResource),
}

#[type_tag(value = "layer")]
#[derive(Debug, PartialEq, Eq, Deserialize, Clone, ToSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LayerResource {
    pub id: LayerId,
}

#[type_tag(value = "layerCollection")]
#[derive(Debug, PartialEq, Eq, Deserialize, Clone, ToSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LayerCollectionResource {
    pub id: LayerCollectionId,
}

#[type_tag(value = "project")]
#[derive(Debug, PartialEq, Eq, Deserialize, Clone, ToSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectResource {
    pub id: ProjectId,
}

#[type_tag(value = "dataset")]
#[derive(Debug, PartialEq, Eq, Deserialize, Clone, ToSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetResource {
    pub id: DatasetName,
}

#[type_tag(value = "mlModel")]
#[derive(Debug, PartialEq, Eq, Deserialize, Clone, ToSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MlModelResource {
    #[schema(value_type = String)]
    pub id: MlModelName,
    // TODO: add a DatasetName to model
    // TODO: check model
}

impl Resource {
    pub async fn resolve_resource_id<D: DatasetDb + MlModelDb>(
        &self,
        db: &D,
    ) -> Result<ResourceId> {
        match self {
            Resource::Layer(layer) => Ok(ResourceId::Layer(layer.id.clone().into())),
            Resource::LayerCollection(layer_collection) => {
                Ok(ResourceId::LayerCollection(layer_collection.id.clone()))
            }
            Resource::Project(project_id) => Ok(ResourceId::Project(project_id.id)),
            Resource::Dataset(dataset_name) => {
                let dataset_id_option = db.resolve_dataset_name_to_id(&dataset_name.id).await?;
                dataset_id_option
                    .ok_or(error::Error::UnknownResource {
                        kind: "Dataset".to_owned(),
                        name: dataset_name.id.to_string(),
                    })
                    .map(ResourceId::DatasetId)
            }
            Resource::MlModel(model_name) => {
                let actual_name = model_name.id.clone().into();
                let model_id_option =
                    db.resolve_model_name_to_id(&actual_name)
                        .await
                        .map_err(|e| error::Error::MachineLearning {
                            source: Box::new(e),
                        })?; // should prob. also map to UnknownResource oder something like that
                model_id_option
                    .ok_or(error::Error::UnknownResource {
                        kind: "MlModel".to_owned(),
                        name: actual_name.to_string(),
                    })
                    .map(ResourceId::MlModel)
            }
        }
    }
}

impl TryFrom<(String, String)> for Resource {
    type Error = Error;

    /// Transform a tuple of `String` into a `Resource`. The first element is used as type and the second element as the id / name.
    fn try_from(value: (String, String)) -> Result<Self> {
        Ok(match value.0.as_str() {
            "layer" => Resource::Layer(LayerResource {
                r#type: Default::default(),
                id: LayerId(value.1),
            }),
            "layerCollection" => Resource::LayerCollection(LayerCollectionResource {
                r#type: Default::default(),
                id: LayerCollectionId(value.1),
            }),
            "project" => Resource::Project(ProjectResource {
                r#type: Default::default(),
                id: ProjectId(Uuid::from_str(&value.1).context(error::Uuid)?),
            }),
            "dataset" => Resource::Dataset(DatasetResource {
                r#type: Default::default(),
                id: DatasetName::from_str(&value.1)?,
            }),
            "mlModel" => Resource::MlModel(MlModelResource {
                r#type: Default::default(),
                id: MlModelName::from_str(&value.1)?,
            }),
            _ => {
                return Err(Error::InvalidResourceId {
                    resource_type: value.0,
                    resource_id: value.1,
                });
            }
        })
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
async fn get_resource_permissions_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    resource_id: web::Path<(String, String)>,
    options: web::Query<PermissionListOptions>,
) -> Result<web::Json<Vec<PermissionListing>>>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: GeoEngineDb,
{
    let resource = Resource::try_from(resource_id.into_inner())?;
    let db = app_ctx.session_context(session).db();
    let resource_id = resource.resolve_resource_id(&db).await?;
    let options = options.into_inner();

    let permissions = db
        .list_permissions(resource_id, options.offset, options.limit)
        .await
        .boxed_context(crate::error::PermissionDb)?;

    let permissions = permissions
        .into_iter()
        .map(|p| PermissionListing::wrap_permission_listing_and_resource(resource.clone(), p))
        .collect();

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
async fn add_permission_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    permission: web::Json<PermissionRequest>,
) -> Result<HttpResponse> {
    let permission = permission.into_inner();

    let db = app_ctx.session_context(session).db();
    let permission_id = permission.resource.resolve_resource_id(&db).await?;

    db.add_permission::<ResourceId>(permission.role_id, permission_id, permission.permission)
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
async fn remove_permission_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    permission: web::Json<PermissionRequest>,
) -> Result<HttpResponse> {
    let permission = permission.into_inner();

    let db = app_ctx.session_context(session).db();
    let permission_id = permission.resource.resolve_resource_id(&db).await?;

    db.remove_permission::<ResourceId>(permission.role_id, permission_id, permission.permission)
        .await
        .boxed_context(crate::error::PermissionDb)?;

    Ok(HttpResponse::Ok().finish())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        contexts::PostgresContext,
        datasets::upload::{Upload, UploadDb, UploadId},
        ge_context,
        layers::{layer::AddLayer, listing::LayerCollectionProvider, storage::LayerDb},
        machine_learning::{MlModel, MlModelIdAndName },
        users::{UserAuth, UserCredentials, UserRegistration},
        util::tests::{
            add_ndvi_to_datasets2, add_ports_to_datasets, admin_login, read_body_string,
            send_test_request,
        },
        workflows::workflow::Workflow,
    };
    use actix_http::header;
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::{machine_learning::MlTensorShape3D, primitives::Coordinate2D, raster::RasterDataType, util::Identifier};
    use geoengine_operators::{
        engine::{RasterOperator, TypedOperator, VectorOperator, WorkflowOperatorPath}, machine_learning::{MlModelInputNoDataHandling, MlModelMetadata, MlModelOutputNoDataHandling}, mock::{MockPointSource, MockPointSourceParams}, source::{GdalSource, GdalSourceParameters, OgrSource, OgrSourceParameters}
    };
    use serde_json::{Value, json};
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

        assert!(
            gdal.clone()
                .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
                .await
                .is_err()
        );

        admin_ctx
            .db()
            .add_permission(
                user_id.into(),
                ResourceId::from(gdal_dataset_id),
                Permission::Read,
            )
            .await
            .unwrap();

        assert!(
            gdal.initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
                .await
                .is_ok()
        );

        assert!(
            ogr.clone()
                .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
                .await
                .is_err()
        );

        admin_ctx
            .db()
            .add_permission(
                user_id.into(),
                ResourceId::from(ogr_dataset_id),
                Permission::Read,
            )
            .await
            .unwrap();

        assert!(
            ogr.initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
                .await
                .is_ok()
        );
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_lists_dataset_permissions(app_ctx: PostgresContext<NoTls>) {
        let admin_session = admin_login(&app_ctx).await;

        let (_dataset_id, dataset_name) = add_ndvi_to_datasets2(&app_ctx, true, true).await;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/permissions/resources/dataset/{dataset_name}?offset=0&limit=10",
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

        let expected_result = json!([{
               "permission":"Owner",
               "resource":  {
                   "id": dataset_name.to_string(),
                   "type": "dataset"
               },
               "role": {
                   "id": "d5328854-6190-4af9-ad69-4e74b0961ac9",
                   "name":
                   "admin"
               }
           }, {
               "permission": "Read",
               "resource": {
                   "id": dataset_name.to_string(),
                   "type": "dataset"
               },
               "role": {
                   "id": "fd8e87bf-515c-4f36-8da6-1a53702ff102",
                   "name": "anonymous"
               }
           }, {
               "permission": "Read",
               "resource": {
                   "id": dataset_name.to_string(),
                   "type": "dataset",
               },
               "role": {
                   "id": "4e8081b6-8aa6-4275-af0c-2fa2da557d28",
                   "name": "user"
               }
           }]
        );

        assert_eq!(res_body, expected_result, "{res_body} != {expected_result}");
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_lists_ml_model_permissions(app_ctx: PostgresContext<NoTls>) {
        let admin_session = admin_login(&app_ctx).await;

        let db = app_ctx.session_context(admin_session.clone()).db();

        let upload_id = UploadId::new();
        let upload = Upload {
            id: upload_id,
            files: vec![],
        };
        db.create_upload(upload).await.unwrap();

        let model = MlModel {
            description: "No real model here".to_owned(),
            display_name: "my unreal model".to_owned(),
            file_name: "myUnrealmodel.onnx".to_owned(),
            metadata: MlModelMetadata {                
                input_type: RasterDataType::F32,
                input_shape: MlTensorShape3D::new_single_pixel_bands(17),
                output_shape: MlTensorShape3D::new_single_pixel_single_band(),
                output_type: RasterDataType::F64,
                input_no_data_handling: MlModelInputNoDataHandling::SkipIfNoData,
                output_no_data_handling: MlModelOutputNoDataHandling::NanIsNoData,
            },
            name: MlModelName::new(None, "myUnrealModel").into(),
            upload: upload_id,
        };

        let MlModelIdAndName {
            id: _model_id,
            name: model_name,
        } = db.add_model(model).await.unwrap();

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/permissions/resources/mlModel/{model_name}?offset=0&limit=10",
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
                   "resource":  {
                       "id": model_name.to_string(),
                       "type": "mlModel"
                   },
                   "role": {
                       "id": "d5328854-6190-4af9-ad69-4e74b0961ac9",
                       "name": "admin"
                   }
               }]
            )
        );
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_lists_layer_collection_permissions(app_ctx: PostgresContext<NoTls>) {
        let admin_session = admin_login(&app_ctx).await;

        let db = app_ctx.session_context(admin_session.clone()).db();

        let root_collection = &db.get_root_layer_collection_id().await.unwrap();

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/permissions/resources/layerCollection/{root_collection}?offset=0&limit=10",
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
                   "resource":  {
                       "id": root_collection.to_string(),
                       "type": "layerCollection"
                   },
                   "role": {
                       "id": "d5328854-6190-4af9-ad69-4e74b0961ac9",
                       "name":
                       "admin"
                   }
               }, {
                   "permission": "Read",
                   "resource": {
                       "id": root_collection.to_string(),
                       "type": "layerCollection"
                   },
                   "role": {
                       "id": "fd8e87bf-515c-4f36-8da6-1a53702ff102",
                       "name": "anonymous"
                   }
               }, {
                   "permission": "Read",
                   "resource": {
                       "id": root_collection.to_string(),
                       "type": "layerCollection",
                   },
                   "role": {
                       "id": "4e8081b6-8aa6-4275-af0c-2fa2da557d28",
                       "name": "user"
                   }
               }]
            )
        );
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_lists_layer_permissions(app_ctx: PostgresContext<NoTls>) {
        let admin_session = admin_login(&app_ctx).await;

        let db = app_ctx.session_context(admin_session.clone()).db();

        let root_collection = &db.get_root_layer_collection_id().await.unwrap();

        let layer = AddLayer {
            name: "layer".to_string(),
            description: "description".to_string(),
            workflow: Workflow {
                operator: TypedOperator::Vector(
                    MockPointSource {
                        params: MockPointSourceParams {
                            points: vec![Coordinate2D::new(1., 2.); 3],
                        },
                    }
                    .boxed(),
                ),
            },
            symbology: None,
            metadata: Default::default(),
            properties: Default::default(),
        };

        let l_id = db.add_layer(layer, root_collection).await.unwrap();

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/permissions/resources/layer/{l_id}?offset=0&limit=10",
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
                   "resource":  {
                       "id": l_id.to_string(),
                       "type": "layer"
                   },
                   "role": {
                       "id": "d5328854-6190-4af9-ad69-4e74b0961ac9",
                       "name":
                       "admin"
                   }
               } ]
            )
        );
    }

    #[test]
    fn resource_from_str_tuple() {
        let test_uuid = Uuid::new_v4();

        let layer_res = Resource::try_from(("layer".to_owned(), "cats".to_owned())).unwrap();
        assert_eq!(
            layer_res,
            Resource::Layer(LayerResource {
                r#type: Default::default(),
                id: LayerId("cats".to_owned())
            })
        );

        let layer_col_res =
            Resource::try_from(("layerCollection".to_owned(), "cats".to_owned())).unwrap();
        assert_eq!(
            layer_col_res,
            Resource::LayerCollection(LayerCollectionResource {
                r#type: Default::default(),
                id: LayerCollectionId("cats".to_owned())
            })
        );

        let project_res = Resource::try_from(("project".to_owned(), test_uuid.into())).unwrap();
        assert_eq!(
            project_res,
            Resource::Project(ProjectResource {
                r#type: Default::default(),
                id: ProjectId(test_uuid)
            })
        );

        let dataset_res = Resource::try_from(("dataset".to_owned(), "cats".to_owned())).unwrap();
        assert_eq!(
            dataset_res,
            Resource::Dataset(DatasetResource {
                r#type: Default::default(),
                id: DatasetName::new(None, "cats".to_owned())
            })
        );

        let ml_model_res = Resource::try_from(("mlModel".to_owned(), "cats".to_owned())).unwrap();
        assert_eq!(
            ml_model_res,
            Resource::MlModel(MlModelResource {
                r#type: Default::default(),
                id: MlModelName::new(None, "cats".to_owned())
            })
        );
    }
}
