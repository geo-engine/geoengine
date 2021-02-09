use crate::handlers::{authenticate, Context};
use crate::projects::project::{
    CreateProject, LoadVersion, ProjectId, ProjectListOptions, UpdateProject, UserProjectPermission,
};
use crate::projects::projectdb::ProjectDB;
use crate::users::session::Session;
use crate::util::user_input::UserInput;
use crate::util::IdResponse;
use uuid::Uuid;
use warp::Filter;

pub(crate) fn create_project_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path("project"))
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::body::json())
        .and_then(create_project)
}

// TODO: move into handler once async closures are available?
async fn create_project<C: Context>(
    session: Session,
    ctx: C,
    create: CreateProject,
) -> Result<impl warp::Reply, warp::Rejection> {
    let create = create.validated()?;
    let id = ctx
        .project_db_ref_mut()
        .await
        .create(session.user.id, create)
        .await?;
    Ok(warp::reply::json(&IdResponse::from(id)))
}

pub(crate) fn list_projects_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path("projects"))
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::query::<ProjectListOptions>())
        .and_then(list_projects)
}

// TODO: move into handler once async closures are available?
async fn list_projects<C: Context>(
    session: Session,
    ctx: C,
    options: ProjectListOptions,
) -> Result<impl warp::Reply, warp::Rejection> {
    let options = options.validated()?;
    let listing = ctx
        .project_db_ref()
        .await
        .list(session.user.id, options)
        .await?;
    Ok(warp::reply::json(&listing))
}

pub(crate) fn load_project_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::get()
        .and(
            (warp::path!("project" / Uuid / Uuid).map(|project_id: Uuid, version_id: Uuid| {
                (ProjectId(project_id), LoadVersion::from(Some(version_id)))
            }))
            .or(warp::path!("project" / Uuid)
                .map(|project_id| (ProjectId(project_id), LoadVersion::Latest)))
            .unify(),
        )
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and_then(load_project)
}

// TODO: move into handler once async closures are available?
async fn load_project<C: Context>(
    project: (ProjectId, LoadVersion),
    session: Session,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    let id = ctx
        .project_db_ref()
        .await
        .load(session.user.id, project.0, project.1)
        .await?;
    Ok(warp::reply::json(&id))
}

pub(crate) fn update_project_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::patch()
        .and(warp::path!("project" / Uuid).map(ProjectId))
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::body::json())
        .and_then(update_project)
}

// TODO: move into handler once async closures are available?
async fn update_project<C: Context>(
    project: ProjectId,
    session: Session,
    ctx: C,
    mut update: UpdateProject,
) -> Result<impl warp::Reply, warp::Rejection> {
    update.id = project; // TODO: avoid passing project id in path AND body
    let update = update.validated()?;
    ctx.project_db_ref_mut()
        .await
        .update(session.user.id, update)
        .await?;
    Ok(warp::reply())
}

pub(crate) fn delete_project_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::delete()
        .and(warp::path!("project" / Uuid).map(ProjectId))
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and_then(delete_project)
}

// TODO: move into handler once async closures are available?
async fn delete_project<C: Context>(
    project: ProjectId,
    session: Session,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.project_db_ref_mut()
        .await
        .delete(session.user.id, project)
        .await?;
    Ok(warp::reply())
}

pub(crate) fn project_versions_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path!("project" / "versions"))
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::body::json())
        .and_then(project_versions)
}

// TODO: move into handler once async closures are available?
async fn project_versions<C: Context>(
    session: Session,
    ctx: C,
    project: ProjectId,
) -> Result<impl warp::Reply, warp::Rejection> {
    let versions = ctx
        .project_db_ref_mut()
        .await
        .versions(session.user.id, project)
        .await?;
    Ok(warp::reply::json(&versions))
}

pub(crate) fn add_permission_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "permission" / "add"))
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::body::json())
        .and_then(add_permission)
}

// TODO: move into handler once async closures are available?
async fn add_permission<C: Context>(
    session: Session,
    ctx: C,
    permission: UserProjectPermission,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.project_db_ref_mut()
        .await
        .add_permission(session.user.id, permission)
        .await?;
    Ok(warp::reply())
}

pub(crate) fn remove_permission_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::delete()
        .and(warp::path!("project" / "permission"))
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::body::json())
        .and_then(remove_permission)
}

// TODO: move into handler once async closures are available?
async fn remove_permission<C: Context>(
    session: Session,
    ctx: C,
    permission: UserProjectPermission,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.project_db_ref_mut()
        .await
        .remove_permission(session.user.id, permission)
        .await?;
    Ok(warp::reply())
}

pub(crate) fn list_permissions_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path!("project" / Uuid / "permissions").map(ProjectId))
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and_then(list_permissions)
}

// TODO: move into handler once async closures are available?
async fn list_permissions<C: Context>(
    project: ProjectId,
    session: Session,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    let permissions = ctx
        .project_db_ref_mut()
        .await
        .list_permissions(session.user.id, project)
        .await?;
    Ok(warp::reply::json(&permissions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projects::project::{LayerUpdate, LayerVisibility, VectorInfo};
    use crate::users::session::Session;
    use crate::users::user::{UserCredentials, UserRegistration};
    use crate::users::userdb::UserDB;
    use crate::util::Identifier;
    use crate::workflows::workflow::WorkflowId;
    use crate::{
        contexts::InMemoryContext,
        projects::project::{
            Layer, LayerInfo, Project, ProjectId, ProjectListing, ProjectPermission,
            ProjectVersion, RasterInfo, STRectangle, UpdateProject,
        },
    };
    use geoengine_datatypes::operations::image::Colorizer;
    use geoengine_datatypes::primitives::{TimeGranularity, TimeStep};
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};

    #[tokio::test]
    async fn create() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        let create = CreateProject {
            name: "Test".to_string(),
            description: "Foo".to_string(),
            bounds: STRectangle::new(SpatialReference::epsg_4326(), 0., 0., 1., 1., 0, 1).unwrap(),
            time_step: Some(TimeStep {
                step: 1,
                granularity: TimeGranularity::Months,
            }),
        };

        let res = warp::test::request()
            .method("POST")
            .path("/project/create")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .json(&create)
            .reply(&create_project_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert!(serde_json::from_str::<IdResponse<ProjectId>>(&body).is_ok());
    }

    #[tokio::test]
    async fn list() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        for i in 0..10 {
            let create = CreateProject {
                name: format!("Test{}", i),
                description: format!("Test{}", 10 - i),
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
            .unwrap();
            ctx.project_db()
                .write()
                .await
                .create(session.user.id, create)
                .await
                .unwrap();
        }

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
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&list_projects_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let result = serde_json::from_str::<Vec<ProjectListing>>(&body);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn load() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        let project = ctx
            .project_db()
            .write()
            .await
            .create(
                session.user.id,
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

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/project/{}", project.to_string()))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&load_project_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert!(serde_json::from_str::<Project>(&body).is_ok());
    }

    #[tokio::test]
    async fn load_version() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        let project = ctx
            .project_db()
            .write()
            .await
            .create(
                session.user.id,
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

        ctx.project_db()
            .write()
            .await
            .update(
                session.user.id,
                UpdateProject {
                    id: project,
                    name: Some("TestUpdate".to_string()),
                    description: None,
                    layers: None,
                    bounds: None,
                    time_step: None,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/project/{}", project.to_string()))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&load_project_handler(ctx.clone()))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert_eq!(
            serde_json::from_str::<Project>(&body).unwrap().name,
            "TestUpdate"
        );

        let versions = ctx
            .project_db()
            .read()
            .await
            .versions(session.user.id, project)
            .await
            .unwrap();
        let version_id = versions.first().unwrap().id;

        let res = warp::test::request()
            .method("GET")
            .path(&format!(
                "/project/{}/{}",
                project.to_string(),
                version_id.to_string()
            ))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&load_project_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert_eq!(serde_json::from_str::<Project>(&body).unwrap().name, "Test");
    }

    #[tokio::test]
    async fn update() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        let project = ctx
            .project_db()
            .write()
            .await
            .create(
                session.user.id,
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

        let update = UpdateProject {
            id: project,
            name: Some("TestUpdate".to_string()),
            description: None,
            layers: Some(vec![LayerUpdate::UpdateOrInsert(Layer {
                workflow: WorkflowId::new(),
                name: "L1".to_string(),
                info: LayerInfo::Raster(RasterInfo {
                    colorizer: Colorizer::Rgba,
                }),
                visibility: Default::default(),
            })]),
            bounds: None,
            time_step: None,
        };

        let res = warp::test::request()
            .method("PATCH")
            .path(&format!("/project/{}", project.to_string()))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .json(&update)
            .reply(&update_project_handler(ctx.clone()))
            .await;

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
    async fn update_layers() {
        async fn update_and_load_latest(
            ctx: &InMemoryContext,
            session: &Session,
            project_id: ProjectId,
            update: UpdateProject,
        ) -> Vec<Layer> {
            let res = warp::test::request()
                .method("PATCH")
                .path(&format!("/project/{}", project_id.to_string()))
                .header("Content-Length", "0")
                .header(
                    "Authorization",
                    format!("Bearer {}", session.id.to_string()),
                )
                .json(&update)
                .reply(&update_project_handler(ctx.clone()))
                .await;

            assert_eq!(res.status(), 200);

            let loaded = ctx
                .project_db()
                .read()
                .await
                .load_latest(session.user.id, project_id)
                .await
                .unwrap();

            loaded.layers
        }

        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        let project = ctx
            .project_db()
            .write()
            .await
            .create(
                session.user.id,
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

        let layer_1 = Layer {
            workflow: WorkflowId::new(),
            name: "L1".to_string(),
            info: LayerInfo::Raster(RasterInfo {
                colorizer: Colorizer::Rgba,
            }),
            visibility: LayerVisibility {
                data: true,
                legend: false,
            },
        };

        let layer_2 = Layer {
            workflow: WorkflowId::new(),
            name: "L2".to_string(),
            info: LayerInfo::Vector(VectorInfo {}),
            visibility: LayerVisibility {
                data: false,
                legend: true,
            },
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

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        let project = ctx
            .project_db()
            .write()
            .await
            .create(
                session.user.id,
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

        let res = warp::test::request()
            .method("DELETE")
            .path(&format!("/project/{}", project.to_string()))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&delete_project_handler(ctx.clone()))
            .await;

        assert_eq!(res.status(), 200);

        assert!(ctx
            .project_db()
            .read()
            .await
            .load_latest(session.user.id, project)
            .await
            .is_err());

        let res = warp::test::request()
            .method("DELETE")
            .path(&format!("/project/{}", project.to_string()))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&delete_project_handler(ctx))
            .await;

        assert_eq!(res.status(), 500);
    }

    #[tokio::test]
    async fn versions() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        let project = ctx
            .project_db()
            .write()
            .await
            .create(
                session.user.id,
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

        ctx.project_db()
            .write()
            .await
            .update(
                session.user.id,
                UpdateProject {
                    id: project,
                    name: Some("TestUpdate".to_string()),
                    description: None,
                    layers: None,
                    bounds: None,
                    time_step: None,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let res = warp::test::request()
            .method("GET")
            .path("/project/versions")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .json(&project)
            .reply(&project_versions_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert!(serde_json::from_str::<Vec<ProjectVersion>>(&body).is_ok());
    }

    #[tokio::test]
    async fn add_permission() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

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

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        let project = ctx
            .project_db()
            .write()
            .await
            .create(
                session.user.id,
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
    async fn remove_permission() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

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

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        let project = ctx
            .project_db()
            .write()
            .await
            .create(
                session.user.id,
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
    async fn list_permissions() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

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

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        let project = ctx
            .project_db()
            .write()
            .await
            .create(
                session.user.id,
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
}
