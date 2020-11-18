use crate::handlers::{authenticate, Context};
use crate::projects::project::{
    CreateProject, LoadVersion, ProjectId, ProjectListOptions, UpdateProject, UserProjectPermission,
};
use crate::projects::projectdb::ProjectDB;
use crate::util::identifiers::IdResponse;
use crate::util::user_input::UserInput;
use uuid::Uuid;
use warp::Filter;

pub fn create_project_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path("project"))
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(create_project)
}

// TODO: move into handler once async closures are available?
async fn create_project<C: Context>(
    ctx: C,
    create: CreateProject,
) -> Result<impl warp::Reply, warp::Rejection> {
    let create = create.validated()?;
    let id = ctx
        .project_db_ref_mut()
        .await
        .create(ctx.session()?.user, create)
        .await?;
    Ok(warp::reply::json(&IdResponse::from_id(id)))
}

pub fn list_projects_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path("projects"))
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(list_projects)
}

// TODO: move into handler once async closures are available?
async fn list_projects<C: Context>(
    ctx: C,
    options: ProjectListOptions,
) -> Result<impl warp::Reply, warp::Rejection> {
    let options = options.validated()?;
    let listing = ctx
        .project_db_ref()
        .await
        .list(ctx.session()?.user, options)
        .await?;
    Ok(warp::reply::json(&listing))
}

pub fn load_project_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(
            (warp::path!("project" / Uuid / Uuid).map(|project_id: Uuid, version_id: Uuid| {
                (ProjectId(project_id), LoadVersion::from(Some(version_id)))
            }))
            .or(warp::path!("project" / Uuid)
                .map(|project_id| (ProjectId(project_id), LoadVersion::Latest)))
            .unify(),
        )
        .and(authenticate(ctx))
        .and_then(load_project)
}

// TODO: move into handler once async closures are available?
async fn load_project<C: Context>(
    project: (ProjectId, LoadVersion),
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    let id = ctx
        .project_db_ref()
        .await
        .load(ctx.session()?.user, project.0, project.1)
        .await?;
    Ok(warp::reply::json(&id))
}

pub fn update_project_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::patch()
        .and(warp::path!("project" / Uuid).map(ProjectId))
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(update_project)
}

// TODO: move into handler once async closures are available?
async fn update_project<C: Context>(
    project: ProjectId,
    ctx: C,
    mut update: UpdateProject,
) -> Result<impl warp::Reply, warp::Rejection> {
    update.id = project; // TODO: avoid passing project id in path AND body
    let update = update.validated()?;
    ctx.project_db_ref_mut()
        .await
        .update(ctx.session()?.user, update)
        .await?;
    Ok(warp::reply())
}

pub fn delete_project_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::delete()
        .and(warp::path!("project" / Uuid).map(ProjectId))
        .and(authenticate(ctx))
        .and_then(delete_project)
}

// TODO: move into handler once async closures are available?
async fn delete_project<C: Context>(
    project: ProjectId,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.project_db_ref_mut()
        .await
        .delete(ctx.session()?.user, project)
        .await?;
    Ok(warp::reply())
}

pub fn project_versions_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path!("project" / "versions"))
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(project_versions)
}

// TODO: move into handler once async closures are available?
async fn project_versions<C: Context>(
    ctx: C,
    project: ProjectId,
) -> Result<impl warp::Reply, warp::Rejection> {
    let versions = ctx
        .project_db_ref_mut()
        .await
        .versions(ctx.session()?.user, project)
        .await?;
    Ok(warp::reply::json(&versions))
}

pub fn add_permission_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "permission" / "add"))
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(add_permission)
}

// TODO: move into handler once async closures are available?
async fn add_permission<C: Context>(
    ctx: C,
    permission: UserProjectPermission,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.project_db_ref_mut()
        .await
        .add_permission(ctx.session()?.user, permission)
        .await?;
    Ok(warp::reply())
}

pub fn remove_permission_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::delete()
        .and(warp::path!("project" / "permission"))
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(remove_permission)
}

// TODO: move into handler once async closures are available?
async fn remove_permission<C: Context>(
    ctx: C,
    permission: UserProjectPermission,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.project_db_ref_mut()
        .await
        .remove_permission(ctx.session()?.user, permission)
        .await?;
    Ok(warp::reply())
}

pub fn list_permissions_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path!("project" / Uuid / "permissions").map(ProjectId))
        .and(authenticate(ctx))
        .and_then(list_permissions)
}

// TODO: move into handler once async closures are available?
async fn list_permissions<C: Context>(
    project: ProjectId,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    let permissions = ctx
        .project_db_ref_mut()
        .await
        .list_permissions(ctx.session()?.user, project)
        .await?;
    Ok(warp::reply::json(&permissions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::users::user::{UserCredentials, UserRegistration};
    use crate::users::userdb::UserDB;
    use crate::util::identifiers::Identifier;
    use crate::workflows::workflow::WorkflowId;
    use crate::{
        contexts::InMemoryContext,
        projects::project::{
            Layer, LayerInfo, OrderBy, Project, ProjectFilter, ProjectId, ProjectListing,
            ProjectPermission, ProjectVersion, RasterInfo, STRectangle, UpdateProject,
        },
    };
    use geoengine_datatypes::operations::image::Colorizer;
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
            bounds: STRectangle::new(SpatialReference::wgs84(), 0., 0., 1., 1., 0, 1).unwrap(),
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
            }
            .validated()
            .unwrap();
            ctx.project_db()
                .write()
                .await
                .create(session.user, create)
                .await
                .unwrap();
        }

        let options = ProjectListOptions {
            permissions: vec![
                ProjectPermission::Owner,
                ProjectPermission::Write,
                ProjectPermission::Read,
            ],
            filter: ProjectFilter::None,
            order: OrderBy::NameDesc,
            offset: 0,
            limit: 2,
        };

        let res = warp::test::request()
            .method("GET")
            .path("/projects")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .json(&options)
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
                session.user,
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
                session.user,
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
                session.user,
                UpdateProject {
                    id: project,
                    name: Some("TestUpdate".to_string()),
                    description: None,
                    layers: None,
                    bounds: None,
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
            .versions(session.user, project)
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
                session.user,
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
            layers: Some(vec![Some(Layer {
                workflow: WorkflowId::new(),
                name: "L1".to_string(),
                info: LayerInfo::Raster(RasterInfo {
                    colorizer: Colorizer::Rgba,
                }),
            })]),
            bounds: None,
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
            .load_latest(session.user, project)
            .await
            .unwrap();
        assert_eq!(loaded.name, "TestUpdate");
        assert_eq!(loaded.layers.len(), 1);
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
                session.user,
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
            .load_latest(session.user, project)
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
                session.user,
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
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let _ = ctx.project_db().write().await.update(
            session.user,
            UpdateProject {
                id: project,
                name: Some("TestUpdate".to_string()),
                description: None,
                layers: None,
                bounds: None,
            }
            .validated()
            .unwrap(),
        );

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
                session.user,
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
                session.user,
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
            .add_permission(session.user, permission.clone())
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
                session.user,
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
            .add_permission(session.user, permission.clone())
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
