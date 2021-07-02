use crate::error::Result;
use crate::handlers::Context;
use crate::projects::{CreateProject, ProjectDb, ProjectId, ProjectListOptions, UpdateProject};
use crate::util::user_input::UserInput;
use crate::util::IdResponse;
use actix_web::{web, HttpResponse, Responder};

/// Create a new project for the user by providing [`CreateProject`].
///
/// # Example
///
/// ```text
/// POST /project
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
///
/// {
///   "name": "Test",
///   "description": "Foo",
///   "bounds": {
///     "spatialReference": "EPSG:4326",
///     "boundingBox": {
///       "lowerLeftCoordinate": { "x": 0, "y": 0 },
///       "upperRightCoordinate": { "x": 1, "y": 1 }
///     },
///     "timeInterval": {
///       "start": 0,
///       "end": 1
///     }
///   },
///   "timeStep": {
///     "step": 1,
///     "granularity": "Months"
///   }
/// }
/// ```
/// Response:
/// ```text
/// {
///   "id": "df4ad02e-0d61-4e29-90eb-dc1259c1f5b9"
/// }
/// ```
pub(crate) async fn create_project_handler<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    create: web::Json<CreateProject>,
) -> Result<impl Responder> {
    let create = create.into_inner().validated()?;
    let id = ctx
        .project_db_ref_mut()
        .await
        .create(&session, create)
        .await?;
    Ok(web::Json(IdResponse::from(id)))
}

/// List all projects accessible to the user that match the [`ProjectListOptions`].
///
/// # Example
///
/// ```text
/// GET /projects?permissions=%5B%22Read%22,%20%22Write%22,%20%22Owner%22%5Dorder=NameAsc&offset=0&limit=2
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
/// ```
/// Response:
/// ```text
/// [
///   {
///     "id": "df4ad02e-0d61-4e29-90eb-dc1259c1f5b9",
///     "name": "Test",
///     "description": "Foo",
///     "layerNames": [],
///     "plotNames": [],
///     "changed": "2021-04-26T14:03:51.984537900Z"
///   }
/// ]
/// ```
pub(crate) async fn list_projects_handler<C: Context>(
    session: C::Session,
    ctx: web::Data<C>,
    options: web::Query<ProjectListOptions>,
) -> Result<impl Responder> {
    let options = options.into_inner().validated()?;
    let listing = ctx.project_db_ref().await.list(&session, options).await?;
    Ok(web::Json(listing))
}

/// Retrieves details about the latest version of a [project](crate::projects::project::Project).
///
/// # Example
///
/// ```text
/// GET /project/df4ad02e-0d61-4e29-90eb-dc1259c1f5b9
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
/// ```
/// Response:
/// ```text
/// {
///   "id": "df4ad02e-0d61-4e29-90eb-dc1259c1f5b9",
///   "version": {
///     "id": "8f4b8683-f92c-4129-a16f-818aeeee484e",
///     "changed": "2021-04-26T14:05:39.677390600Z",
///     "author": "5b4466d2-8bab-4ed8-a182-722af3c80958"
///   },
///   "name": "Test",
///   "description": "Foo",
///   "layers": [],
///   "plots": [],
///   "bounds": {
///     "spatialReference": "EPSG:4326",
///     "boundingBox": {
///       "lowerLeftCoordinate": {
///         "x": 0.0,
///         "y": 0.0
///       },
///       "upperRightCoordinate": {
///         "x": 1.0,
///         "y": 1.0
///       }
///     },
///     "timeInterval": {
///       "start": 0,
///       "end": 1
///     }
///   },
///   "timeStep": {
///     "granularity": "Months",
///     "step": 1
///   }
/// }
/// ```
pub(crate) async fn load_project_handler<C: Context>(
    project: web::Path<ProjectId>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let id = ctx
        .project_db_ref()
        .await
        .load(&session, project.into_inner())
        .await?;
    Ok(web::Json(id))
}

/// Updates a project.
/// This will create a new version.
///
/// # Example
///
/// ```text
/// PATCH /project/df4ad02e-0d61-4e29-90eb-dc1259c1f5b9
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
///
/// {
///   "id": "df4ad02e-0d61-4e29-90eb-dc1259c1f5b9",
///   "name": "TestUpdate",
///   "layers": [
///     {
///       "workflow": "100ee39c-761c-4218-9d85-ec861a8f3097",
///       "name": "L1",
///       "visibility": {
///         "data": true,
///         "legend": false
///       },
///       "symbology": {
///         "raster": {
///           "opacity": 1.0,
///           "colorizer": "rgba"
///         }
///       }
///     }
///   ]
/// }
/// ```
pub(crate) async fn update_project_handler<C: Context>(
    project: web::Path<ProjectId>,
    session: C::Session,
    ctx: web::Data<C>,
    mut update: web::Json<UpdateProject>,
) -> Result<impl Responder> {
    update.id = project.into_inner(); // TODO: avoid passing project id in path AND body
    let update = update.into_inner().validated()?;
    ctx.project_db_ref_mut()
        .await
        .update(&session, update)
        .await?;
    Ok(HttpResponse::Ok())
}

/// Deletes a project.
///
/// # Example
///
/// ```text
/// DELETE /project/df4ad02e-0d61-4e29-90eb-dc1259c1f5b9
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
/// ```
pub(crate) async fn delete_project_handler<C: Context>(
    project: web::Path<ProjectId>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    ctx.project_db_ref_mut()
        .await
        .delete(&session, *project)
        .await?;
    Ok(HttpResponse::Ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{Session, SimpleContext, SimpleSession};
    use crate::handlers::{handle_rejection, ErrorResponse};
    use crate::projects::{
        LayerUpdate, LayerVisibility, Plot, PlotUpdate, RasterSymbology, Symbology,
    };
    use crate::users::session::Session;
    use crate::users::user::UserRegistration;
    use crate::users::userdb::UserDb;
    use crate::util::tests::{
        check_allowed_http_methods, check_allowed_http_methods2, create_project_helper,
        create_session_helper, update_project_helper,
    };
    use crate::util::Identifier;
    use crate::workflows::workflow::WorkflowId;
    use crate::{
        contexts::InMemoryContext,
        projects::project::{
            Layer, Project, ProjectId, ProjectListing, ProjectPermission, ProjectVersion,
            STRectangle, UpdateProject,
        },
    };
    use geoengine_datatypes::operations::image::Colorizer;
    use geoengine_datatypes::primitives::{TimeGranularity, TimeStep};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use serde_json::json;
    use warp::http::Response;
    use warp::hyper::body::Bytes;

    async fn create_test_helper(method: &str) -> Response<Bytes> {
        let ctx = InMemoryContext::default();

        let session = ctx.default_session_ref().await;

        let create = CreateProject {
            name: "Test".to_string(),
            description: "Foo".to_string(),
            bounds: STRectangle::new(SpatialReference::epsg_4326(), 0., 0., 1., 1., 0, 1).unwrap(),
            time_step: Some(TimeStep {
                step: 1,
                granularity: TimeGranularity::Months,
            }),
        };

        warp::test::request()
            .method(method)
            .path("/project/create")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id().to_string()),
            )
            .json(&create)
            .reply(&create_project_handler(ctx.clone()).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn create() {
        let res = create_test_helper("POST").await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert!(serde_json::from_str::<IdResponse<ProjectId>>(&body).is_ok());
    }

    #[tokio::test]
    async fn create_invalid_method() {
        check_allowed_http_methods(create_test_helper, &["POST"]).await;
    }

    #[tokio::test]
    async fn create_invalid_body() {
        let ctx = InMemoryContext::default();

        let session_id = ctx.default_session_ref().await.id();

        let res = warp::test::request()
            .method("POST")
            .path("/project/create")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session_id.to_string()),
            )
            .body("no json")
            .reply(&create_project_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "expected ident at line 1 column 2",
        );
    }

    #[tokio::test]
    async fn create_missing_fields() {
        let ctx = InMemoryContext::default();

        let session_id = ctx.default_session_ref().await.id();

        let create = json!({
            "description": "Foo".to_string(),
            "bounds": STRectangle::new(SpatialReference::epsg_4326(), 0., 0., 1., 1., 0, 1).unwrap(),
        });

        let res = warp::test::request()
            .method("POST")
            .path("/project/create")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session_id.to_string()),
            )
            .json(&create)
            .reply(&create_project_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "missing field `name` at line 1 column 195",
        );
    }

    #[tokio::test]
    async fn create_missing_header() {
        let ctx = InMemoryContext::default();

        let create = json!({
            "description": "Foo".to_string(),
            "bounds": STRectangle::new(SpatialReference::epsg_4326(), 0., 0., 1., 1., 0, 1).unwrap(),
        });

        let res = warp::test::request()
            .method("POST")
            .path("/project/create")
            .header("Content-Length", "0")
            .json(&create)
            .reply(&create_project_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        );
    }

    async fn list_test_helper(method: &str) -> Response<Bytes> {
        let ctx = InMemoryContext::default();

        let (session, _) = create_project_helper(&ctx).await;

        warp::test::request()
            .method(method)
            .path(&format!(
                "/projects?{}",
                &serde_urlencoded::to_string([
                    (
                        "permissions",
                        serde_json::json! {["Read", "Write", "Owner"]}
                            .to_string()
                            .as_str()
                    ),
                    // omitted ("filter", "None"),
                    ("order", "NameDesc"),
                    ("offset", "0"),
                    ("limit", "2"),
                ])
                .unwrap()
            ))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id().to_string()),
            )
            .reply(&list_projects_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn list() {
        let res = list_test_helper("GET").await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let result = serde_json::from_str::<Vec<ProjectListing>>(&body);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn list_invalid_method() {
        check_allowed_http_methods(list_test_helper, &["GET"]).await;
    }

    #[tokio::test]
    async fn list_missing_header() {
        let ctx = InMemoryContext::default();

        create_project_helper(&ctx).await;

        let res = warp::test::request()
            .method("GET")
            .path(&format!(
                "/projects?{}",
                &serde_urlencoded::to_string([
                    (
                        "permissions",
                        serde_json::json! {["Read", "Write", "Owner"]}
                            .to_string()
                            .as_str()
                    ),
                    // omitted ("filter", "None"),
                    ("order", "NameDesc"),
                    ("offset", "0"),
                    ("limit", "2"),
                ])
                .unwrap()
            ))
            .header("Content-Length", "0")
            .reply(&list_projects_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        );
    }

    async fn load_test_helper(method: &str) -> Response<Bytes> {
        let ctx = InMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        warp::test::request()
            .method(method)
            .path(&format!("/project/{}", project.to_string()))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id().to_string()),
            )
            .reply(&load_project_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn load() {
        let res = load_test_helper("GET").await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert!(serde_json::from_str::<Project>(&body).is_ok());
    }

    #[tokio::test]
    async fn load_invalid_method() {
        check_allowed_http_methods(load_test_helper, &["GET"]).await;
    }

    #[tokio::test]
    async fn load_missing_header() {
        let ctx = InMemoryContext::default();

        let (_, project) = create_project_helper(&ctx).await;

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/project/{}", project.to_string()))
            .header("Content-Length", "0")
            .reply(&load_project_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        );
    }

    #[tokio::test]
    async fn load_not_found() {
        let ctx = InMemoryContext::default();

        let session_id = ctx.default_session_ref().await.id();

        let res = warp::test::request()
            .method("GET")
            .path("/project")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session_id.to_string()),
            )
            .reply(&load_project_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(res, 404, "NotFound", "Not Found");
    }

    async fn update_test_helper(
        method: &str,
    ) -> (InMemoryContext, SimpleSession, ProjectId, Response<Bytes>) {
        let ctx = InMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        let update = update_project_helper(project);

        let res = warp::test::request()
            .method(method)
            .path(&format!("/project/{}", project.to_string()))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id().to_string()),
            )
            .json(&update)
            .reply(&update_project_handler(ctx.clone()).recover(handle_rejection))
            .await;

        (ctx, session, project, res)
    }

    #[tokio::test]
    async fn update() {
        let (ctx, session, project, res) = update_test_helper("PATCH").await;

        assert_eq!(res.status(), 200);

        let loaded = ctx
            .project_db()
            .read()
            .await
            .load_latest(session.user.id, project)
            .await
            .unwrap();
        assert_eq!(loaded.name, "TestUpdate");
        assert_eq!(loaded.layers.len(), 1);
    }

    #[tokio::test]
    async fn update_invalid_method() {
        check_allowed_http_methods2(update_test_helper, &["PATCH"], |(_, _, _, res)| res).await;
    }

    #[tokio::test]
    async fn update_invalid_body() {
        let ctx = InMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        let res = warp::test::request()
            .method("PATCH")
            .path(&format!("/project/{}", project.to_string()))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .body("no json")
            .reply(&update_project_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "expected ident at line 1 column 2",
        );
    }

    #[tokio::test]
    async fn update_missing_fields() {
        let ctx = InMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        let update = json!({
            "name": "TestUpdate",
            "description": None::<String>,
            "layers": vec![LayerUpdate::UpdateOrInsert(Layer {
                workflow: WorkflowId::new(),
                name: "L1".to_string(),
                visibility: Default::default(),
                symbology: Symbology::Raster(RasterSymbology {
                    opacity: 1.0,
                    colorizer: Colorizer::Rgba,
                })
            })],
            "bounds": None::<String>,
            "time_step": None::<String>,
        });

        let res = warp::test::request()
            .method("PATCH")
            .path(&format!("/project/{}", project.to_string()))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id().to_string()),
            )
            .json(&update)
            .reply(&update_project_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "missing field `id` at line 1 column 260",
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn update_layers() {
        async fn update_and_load_latest(
            ctx: &InMemoryContext,
            session: &SimpleSession,
            project_id: ProjectId,
            update: UpdateProject,
        ) -> Vec<Layer> {
            let res = warp::test::request()
                .method("PATCH")
                .path(&format!("/project/{}", project_id.to_string()))
                .header("Content-Length", "0")
                .header(
                    "Authorization",
                    format!("Bearer {}", session.id().to_string()),
                )
                .json(&update)
                .reply(&update_project_handler(ctx.clone()))
                .await;

            assert_eq!(res.status(), 200);

            let loaded = ctx
                .project_db()
                .read()
                .await
                .load(&session, project_id)
                .await
                .unwrap();

            loaded.layers
        }

        let ctx = InMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        let layer_1 = Layer {
            workflow: WorkflowId::new(),
            name: "L1".to_string(),
            visibility: LayerVisibility {
                data: true,
                legend: false,
            },
            symbology: Symbology::Raster(RasterSymbology {
                opacity: 1.0,
                colorizer: Colorizer::Rgba,
            }),
        };

        let layer_2 = Layer {
            workflow: WorkflowId::new(),
            name: "L2".to_string(),
            visibility: LayerVisibility {
                data: false,
                legend: true,
            },
            symbology: Symbology::Raster(RasterSymbology {
                opacity: 1.0,
                colorizer: Colorizer::Rgba,
            }),
        };

        // add first layer
        assert_eq!(
            update_and_load_latest(
                &ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: Some(vec![LayerUpdate::UpdateOrInsert(layer_1.clone())]),
                    plots: None,
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![layer_1.clone()]
        );

        // add second layer
        assert_eq!(
            update_and_load_latest(
                &ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: Some(vec![
                        LayerUpdate::None(Default::default()),
                        LayerUpdate::UpdateOrInsert(layer_2.clone())
                    ]),
                    plots: None,
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![layer_1.clone(), layer_2.clone()]
        );

        // remove first layer
        assert_eq!(
            update_and_load_latest(
                &ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: Some(vec![
                        LayerUpdate::Delete(Default::default()),
                        LayerUpdate::None(Default::default()),
                    ]),
                    plots: None,
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![layer_2.clone()]
        );

        // clear layers
        assert_eq!(
            update_and_load_latest(
                &ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: Some(vec![]),
                    plots: None,
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![]
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn update_plots() {
        async fn update_and_load_latest(
            ctx: &InMemoryContext,
            session: &SimpleSession,
            project_id: ProjectId,
            update: UpdateProject,
        ) -> Vec<Plot> {
            let res = warp::test::request()
                .method("PATCH")
                .path(&format!("/project/{}", project_id.to_string()))
                .header("Content-Length", "0")
                .header(
                    "Authorization",
                    format!("Bearer {}", session.id().to_string()),
                )
                .json(&update)
                .reply(&update_project_handler(ctx.clone()))
                .await;

            assert_eq!(res.status(), 200);

            let loaded = ctx
                .project_db()
                .read()
                .await
                .load(&session, project_id)
                .await
                .unwrap();

            loaded.plots
        }

        let ctx = InMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        let plot_1 = Plot {
            workflow: WorkflowId::new(),
            name: "P1".to_string(),
        };

        let plot_2 = Plot {
            workflow: WorkflowId::new(),
            name: "P2".to_string(),
        };

        // add first plot
        assert_eq!(
            update_and_load_latest(
                &ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: None,
                    plots: Some(vec![PlotUpdate::UpdateOrInsert(plot_1.clone())]),
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![plot_1.clone()]
        );

        // add second plot
        assert_eq!(
            update_and_load_latest(
                &ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: None,
                    plots: Some(vec![
                        PlotUpdate::None(Default::default()),
                        PlotUpdate::UpdateOrInsert(plot_2.clone())
                    ]),
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![plot_1.clone(), plot_2.clone()]
        );

        // remove first plot
        assert_eq!(
            update_and_load_latest(
                &ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: None,
                    plots: Some(vec![
                        PlotUpdate::Delete(Default::default()),
                        PlotUpdate::None(Default::default()),
                    ]),
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![plot_2.clone()]
        );

        // clear plots
        assert_eq!(
            update_and_load_latest(
                &ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: None,
                    plots: Some(vec![]),
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![]
        );
    }

    #[tokio::test]
    async fn delete() {
        let ctx = InMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        let res = warp::test::request()
            .method("DELETE")
            .path(&format!("/project/{}", project.to_string()))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id().to_string()),
            )
            .reply(&delete_project_handler(ctx.clone()))
            .await;

        assert_eq!(res.status(), 200);

        assert!(ctx
            .project_db()
            .read()
            .await
            .load(&session, project)
            .await
            .is_err());

        let res = warp::test::request()
            .method("DELETE")
            .path(&format!("/project/{}", project.to_string()))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id().to_string()),
            )
            .reply(&delete_project_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
            400,
            "ProjectDeleteFailed",
            "Failed to delete the project.",
        );
    }

    async fn versions_test_helper(method: &str) -> Response<Bytes> {
        let ctx = InMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        ctx.project_db()
            .write()
            .await
            .update(
                session.user.id,
                update_project_helper(project).validated().unwrap(),
            )
            .await
            .unwrap();

        warp::test::request()
            .method(method)
            .path("/project/versions")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .json(&project)
            .reply(&project_versions_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn versions() {
        let res = versions_test_helper("GET").await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert!(serde_json::from_str::<Vec<ProjectVersion>>(&body).is_ok());
    }

    #[tokio::test]
    async fn versions_invalid_method() {
        check_allowed_http_methods(versions_test_helper, &["GET"]).await;
    }

    #[tokio::test]
    async fn versions_missing_header() {
        let ctx = InMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        ctx.project_db()
            .write()
            .await
            .update(
                session.user.id,
                update_project_helper(project).validated().unwrap(),
            )
            .await
            .unwrap();

        let res = warp::test::request()
            .method("GET")
            .path("/project/versions")
            .header("Content-Length", "0")
            .json(&project)
            .reply(&project_versions_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        );
    }

    #[tokio::test]
    async fn add_permission() {
        let ctx = InMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        let target_user = ctx
            .user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo2@bar.de".to_string(),
                    password: "secret1234".to_string(),
                    real_name: "Foo2 Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        let res = warp::test::request()
            .method("POST")
            .path("/project/permission/add")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .json(&permission)
            .reply(&add_permission_handler(ctx.clone()))
            .await;

        assert_eq!(res.status(), 200);

        assert!(ctx
            .project_db()
            .write()
            .await
            .load_latest(target_user, project)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn add_permission_missing_header() {
        let ctx = InMemoryContext::default();

        let (_, project) = create_project_helper(&ctx).await;

        let target_user = ctx
            .user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo2@bar.de".to_string(),
                    password: "secret1234".to_string(),
                    real_name: "Foo2 Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        let res = warp::test::request()
            .method("POST")
            .path("/project/permission/add")
            .header("Content-Length", "0")
            .json(&permission)
            .reply(&add_permission_handler(ctx.clone()).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        );
    }

    #[tokio::test]
    async fn remove_permission() {
        let ctx = InMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        let target_user = ctx
            .user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo2@bar.de".to_string(),
                    password: "secret1234".to_string(),
                    real_name: "Foo2 Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        ctx.project_db()
            .write()
            .await
            .add_permission(session.user.id, permission.clone())
            .await
            .unwrap();

        let res = warp::test::request()
            .method("DELETE")
            .path("/project/permission")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .json(&permission)
            .reply(&remove_permission_handler(ctx.clone()))
            .await;

        assert_eq!(res.status(), 200);

        assert!(ctx
            .project_db()
            .write()
            .await
            .load_latest(target_user, project)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn remove_permission_missing_header() {
        let ctx = InMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        let target_user = ctx
            .user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo2@bar.de".to_string(),
                    password: "secret1234".to_string(),
                    real_name: "Foo2 Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        ctx.project_db()
            .write()
            .await
            .add_permission(session.user.id, permission.clone())
            .await
            .unwrap();

        let res = warp::test::request()
            .method("DELETE")
            .path("/project/permission")
            .header("Content-Length", "0")
            .json(&permission)
            .reply(&remove_permission_handler(ctx.clone()).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        );
    }

    #[tokio::test]
    async fn list_permissions() {
        let ctx = InMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        let target_user = ctx
            .user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo2@bar.de".to_string(),
                    password: "secret1234".to_string(),
                    real_name: "Foo2 Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        ctx.project_db()
            .write()
            .await
            .add_permission(session.user.id, permission.clone())
            .await
            .unwrap();

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/project/{}/permissions", project.to_string()))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&list_permissions_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let result = serde_json::from_str::<Vec<UserProjectPermission>>(&body);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn list_permissions_missing_header() {
        let ctx = InMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        let target_user = ctx
            .user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo2@bar.de".to_string(),
                    password: "secret1234".to_string(),
                    real_name: "Foo2 Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        ctx.project_db()
            .write()
            .await
            .add_permission(session.user.id, permission.clone())
            .await
            .unwrap();

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/project/{}/permissions", project.to_string()))
            .header("Content-Length", "0")
            .reply(&list_permissions_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        );
    }
}
