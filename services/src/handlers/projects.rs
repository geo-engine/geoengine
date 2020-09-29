use crate::handlers::{authenticate, Context};
use crate::projects::project::{
    CreateProject, LoadVersion, ProjectId, ProjectListOptions, UpdateProject, UserProjectPermission,
};
use crate::projects::projectdb::ProjectDB;
use crate::util::user_input::UserInput;
use uuid::Uuid;
use warp::Filter;

pub fn create_project_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "create"))
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
        .project_db()
        .write()
        .await
        .create(ctx.session()?.user, create);
    Ok(warp::reply::json(&id))
}

pub fn list_projects_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "list"))
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
        .project_db()
        .read()
        .await
        .list(ctx.session()?.user, options);
    Ok(warp::reply::json(&listing))
}

pub fn load_project_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(
            (warp::path!("project" / "load" / Uuid).map(|id: Uuid| LoadVersion::from(Some(id))))
                .or(warp::path!("project" / "load").map(|| LoadVersion::Latest))
                .unify(),
        )
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(load_project)
}

// TODO: move into handler once async closures are available?
async fn load_project<C: Context>(
    version: LoadVersion,
    ctx: C,
    project: ProjectId,
) -> Result<impl warp::Reply, warp::Rejection> {
    let id = ctx
        .project_db()
        .read()
        .await
        .load(ctx.session()?.user, project, version)?;
    Ok(warp::reply::json(&id))
}

pub fn update_project_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "update"))
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(update_project)
}

// TODO: move into handler once async closures are available?
async fn update_project<C: Context>(
    ctx: C,
    update: UpdateProject,
) -> Result<impl warp::Reply, warp::Rejection> {
    let update = update.validated()?;
    ctx.project_db()
        .write()
        .await
        .update(ctx.session()?.user, update)?;
    Ok(warp::reply())
}

pub fn delete_project_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "delete"))
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(delete_project)
}

// TODO: move into handler once async closures are available?
async fn delete_project<C: Context>(
    ctx: C,
    project: ProjectId,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.project_db()
        .write()
        .await
        .delete(ctx.session()?.user, project)?;
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
        .project_db()
        .write()
        .await
        .versions(ctx.session()?.user, project)?;
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
    ctx.project_db()
        .write()
        .await
        .add_permission(ctx.session()?.user, permission)?;
    Ok(warp::reply())
}

pub fn remove_permission_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "permission" / "remove"))
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(remove_permission)
}

// TODO: move into handler once async closures are available?
async fn remove_permission<C: Context>(
    ctx: C,
    permission: UserProjectPermission,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.project_db()
        .write()
        .await
        .remove_permission(ctx.session()?.user, permission)?;
    Ok(warp::reply())
}

pub fn list_permissions_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "permission" / "list"))
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(list_permissions)
}

// TODO: move into handler once async closures are available?
async fn list_permissions<C: Context>(
    ctx: C,
    project: ProjectId,
) -> Result<impl warp::Reply, warp::Rejection> {
    let permissions = ctx
        .project_db()
        .write()
        .await
        .list_permissions(ctx.session()?.user, project)?;
    Ok(warp::reply::json(&permissions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::InMemoryContext;
    use crate::projects::project::{
        Layer, LayerInfo, OrderBy, Project, ProjectFilter, ProjectId, ProjectListing,
        ProjectPermission, ProjectVersion, RasterInfo, STRectangle, UpdateProject,
    };
    use crate::users::user::{UserCredentials, UserRegistration};
    use crate::users::userdb::UserDB;
    use crate::util::identifiers::Identifier;
    use crate::workflows::workflow::WorkflowId;
    use geoengine_datatypes::operations::image::Colorizer;

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
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .unwrap();

        let create = CreateProject {
            name: "Test".to_string(),
            description: "Foo".to_string(),
            view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
            bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
        };

        let res = warp::test::request()
            .method("POST")
            .path("/project/create")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&create)
            .reply(&create_project_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert!(serde_json::from_str::<ProjectId>(&body).is_ok());
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
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .unwrap();

        for i in 0..10 {
            let create = CreateProject {
                name: format!("Test{}", i),
                description: format!("Test{}", 10 - i),
                view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
                bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
            }
            .validated()
            .unwrap();
            ctx.project_db().write().await.create(session.user, create);
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
            .method("POST")
            .path("/project/list")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
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
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .unwrap();

        let project = ctx.project_db().write().await.create(
            session.user,
            CreateProject {
                name: "Test".to_string(),
                description: "Foo".to_string(),
                view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
                bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
            }
            .validated()
            .unwrap(),
        );

        let res = warp::test::request()
            .method("POST")
            .path("/project/load")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&project)
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
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .unwrap();

        let project = ctx.project_db().write().await.create(
            session.user,
            CreateProject {
                name: "Test".to_string(),
                description: "Foo".to_string(),
                view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
                bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
            }
            .validated()
            .unwrap(),
        );

        let _ = ctx.project_db().write().await.update(
            session.user,
            UpdateProject {
                id: project,
                name: Some("TestUpdate".to_string()),
                description: None,
                layers: None,
                view: None,
                bounds: None,
            }
            .validated()
            .unwrap(),
        );

        let res = warp::test::request()
            .method("POST")
            .path("/project/load")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&project)
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
            .unwrap();
        let version_id = versions.first().unwrap().id;

        let res = warp::test::request()
            .method("POST")
            .path(&format!("/project/load/{}", version_id.to_string()))
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&project)
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
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .unwrap();

        let project = ctx.project_db().write().await.create(
            session.user,
            CreateProject {
                name: "Test".to_string(),
                description: "Foo".to_string(),
                view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
                bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
            }
            .validated()
            .unwrap(),
        );

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
            view: None,
            bounds: None,
        };

        let res = warp::test::request()
            .method("POST")
            .path("/project/update")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&update)
            .reply(&update_project_handler(ctx.clone()))
            .await;

        assert_eq!(res.status(), 200);

        let loaded = ctx
            .project_db()
            .read()
            .await
            .load_latest(session.user, project)
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
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .unwrap();

        let project = ctx.project_db().write().await.create(
            session.user,
            CreateProject {
                name: "Test".to_string(),
                description: "Foo".to_string(),
                view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
                bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
            }
            .validated()
            .unwrap(),
        );

        let res = warp::test::request()
            .method("POST")
            .path("/project/delete")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&project)
            .reply(&delete_project_handler(ctx.clone()))
            .await;

        assert_eq!(res.status(), 200);

        assert!(ctx
            .project_db()
            .read()
            .await
            .load_latest(session.user, project)
            .is_err());

        let res = warp::test::request()
            .method("POST")
            .path("/project/delete")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&project)
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
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .unwrap();

        let project = ctx.project_db().write().await.create(
            session.user,
            CreateProject {
                name: "Test".to_string(),
                description: "Foo".to_string(),
                view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
                bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
            }
            .validated()
            .unwrap(),
        );

        let _ = ctx.project_db().write().await.update(
            session.user,
            UpdateProject {
                id: project,
                name: Some("TestUpdate".to_string()),
                description: None,
                layers: None,
                view: None,
                bounds: None,
            }
            .validated()
            .unwrap(),
        );

        let res = warp::test::request()
            .method("GET")
            .path("/project/versions")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
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
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .unwrap();

        let project = ctx.project_db().write().await.create(
            session.user,
            CreateProject {
                name: "Test".to_string(),
                description: "Foo".to_string(),
                view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
                bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
            }
            .validated()
            .unwrap(),
        );

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        let res = warp::test::request()
            .method("POST")
            .path("/project/permission/add")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&permission)
            .reply(&add_permission_handler(ctx.clone()))
            .await;

        assert_eq!(res.status(), 200);

        assert!(ctx
            .project_db()
            .write()
            .await
            .load_latest(target_user, project)
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
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .unwrap();

        let project = ctx.project_db().write().await.create(
            session.user,
            CreateProject {
                name: "Test".to_string(),
                description: "Foo".to_string(),
                view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
                bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
            }
            .validated()
            .unwrap(),
        );

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        ctx.project_db()
            .write()
            .await
            .add_permission(session.user, permission.clone())
            .unwrap();

        let res = warp::test::request()
            .method("POST")
            .path("/project/permission/remove")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&permission)
            .reply(&remove_permission_handler(ctx.clone()))
            .await;

        assert_eq!(res.status(), 200);

        assert!(ctx
            .project_db()
            .write()
            .await
            .load_latest(target_user, project)
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
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .unwrap();

        let project = ctx.project_db().write().await.create(
            session.user,
            CreateProject {
                name: "Test".to_string(),
                description: "Foo".to_string(),
                view: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
                bounds: STRectangle::new(0., 0., 1., 1., 0, 1).unwrap(),
            }
            .validated()
            .unwrap(),
        );

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        ctx.project_db()
            .write()
            .await
            .add_permission(session.user, permission.clone())
            .unwrap();

        let res = warp::test::request()
            .method("POST")
            .path("/project/permission/list")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&project)
            .reply(&list_permissions_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let result = serde_json::from_str::<Vec<UserProjectPermission>>(&body);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }
}
