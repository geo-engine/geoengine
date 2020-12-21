use crate::error;
use crate::error::Result;
use crate::handlers::{authenticate, Context};
use crate::projects::project::{ProjectId, STRectangle};
use crate::users::user::{UserCredentials, UserRegistration};
use crate::users::userdb::UserDB;
use crate::util::identifiers::IdResponse;
use crate::util::user_input::UserInput;
use snafu::ResultExt;
use uuid::Uuid;
use warp::reply::Reply;
use warp::Filter;

pub fn register_user_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path("user"))
        .and(warp::body::json())
        .and(warp::any().map(move || ctx.clone()))
        .and_then(register_user)
}

// TODO: move into handler once async closures are available?
async fn register_user<C: Context>(
    user: UserRegistration,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    let user = user.validated()?;
    let id = ctx.user_db_ref_mut().await.register(user).await?;
    Ok(warp::reply::json(&IdResponse::from_id(id)))
}

pub fn anonymous_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("anonymous"))
        .and(warp::any().map(move || ctx.clone()))
        .and_then(anonymous)
}

// TODO: move into handler once async closures are available?
async fn anonymous<C: Context>(ctx: C) -> Result<impl warp::Reply, warp::Rejection> {
    let session = ctx.user_db_ref_mut().await.anonymous().await?;
    Ok(warp::reply::json(&session))
}

pub fn login_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path("login"))
        .and(warp::body::json())
        .and(warp::any().map(move || ctx.clone()))
        .and_then(login)
}

// TODO: move into handler once async closures are available?
async fn login<C: Context>(
    user: UserCredentials,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    let session = ctx
        .user_db_ref_mut()
        .await
        .login(user)
        .await
        .map_err(Box::new)
        .context(error::Authorization)?;
    Ok(warp::reply::json(&session).into_response())
}

pub fn logout_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path("logout"))
        .and(authenticate(ctx))
        .and_then(logout)
}

// TODO: move into handler once async closures are available?
async fn logout<C: Context>(ctx: C) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.user_db_ref_mut()
        .await
        .logout(ctx.session()?.id)
        .await?;
    Ok(warp::reply().into_response())
}

pub fn session_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path("session"))
        .and(authenticate(ctx))
        .and_then(session)
}

// TODO: move into handler once async closures are available?
async fn session<C: Context>(ctx: C) -> Result<impl warp::Reply, warp::Rejection> {
    Ok(warp::reply::json(
        ctx.session().expect("authentication was successful"),
    ))
}

pub fn session_project_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("session" / "project" / Uuid).map(ProjectId))
        .and(authenticate(ctx))
        .and_then(session_project)
}

// TODO: move into handler once async closures are available?
async fn session_project<C: Context>(
    project: ProjectId,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.user_db_ref_mut()
        .await
        .set_session_project(ctx.session()?, project)
        .await?;

    Ok(warp::reply())
}

pub fn session_view_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("session" / "view"))
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(session_view)
}

// TODO: move into handler once async closures are available?
async fn session_view<C: Context>(
    ctx: C,
    view: STRectangle,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.user_db_ref_mut()
        .await
        .set_session_view(ctx.session()?, view)
        .await?;

    Ok(warp::reply())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projects::project::{CreateProject, STRectangle};
    use crate::projects::projectdb::ProjectDB;
    use crate::users::session::Session;
    use crate::users::user::UserId;
    use crate::users::userdb::UserDB;
    use crate::util::user_input::Validated;
    use crate::{contexts::InMemoryContext, handlers::handle_rejection};
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use serde_json::json;

    #[tokio::test]
    async fn register() {
        let ctx = InMemoryContext::default();

        let user = UserRegistration {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
            real_name: " Foo Bar".to_string(),
        };

        // register user
        let res = warp::test::request()
            .method("POST")
            .path("/user")
            .header("Content-Length", "0")
            .json(&user)
            .reply(&register_user_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert!(serde_json::from_str::<IdResponse<UserId>>(&body).is_ok());
    }

    #[tokio::test]
    async fn register_invalid_body() {
        let ctx = InMemoryContext::default();

        let user = json!({
            "password": "secret123",
            "real_name": " Foo Bar",
        });

        // register user
        let res = warp::test::request()
            .method("POST")
            .path("/user")
            .header("Content-Length", "0")
            .json(&user)
            .reply(&register_user_handler(ctx))
            .await;

        assert_eq!(res.status(), 400);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert_eq!(
            body,
            "Request body deserialize error: missing field `email` at line 1 column 47"
        );
    }

    #[tokio::test]
    async fn register_fail() {
        let ctx = InMemoryContext::default();

        let user = UserRegistration {
            email: "notanemail".to_string(),
            password: "secret123".to_string(),
            real_name: " Foo Bar".to_string(),
        };

        // register user
        let res = warp::test::request()
            .method("POST")
            .path("/user")
            .header("Content-Length", "0")
            .json(&user)
            .reply(&register_user_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 400);
    }

    #[tokio::test]
    async fn login() {
        let ctx = InMemoryContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        };

        let res = warp::test::request()
            .method("POST")
            .path("/login")
            .header("Content-Length", "0")
            .json(&credentials)
            .reply(&login_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let _id: Session = serde_json::from_str(&body).unwrap();
    }

    #[tokio::test]
    async fn login_fail() {
        let ctx = InMemoryContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: "Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "wrong".to_string(),
        };

        let res = warp::test::request()
            .method("POST")
            .path("/login")
            .header("Content-Length", "0")
            .json(&credentials)
            .reply(&login_handler(ctx).recover(handle_rejection))
            .await;
        eprintln!("{:?}", res);
        assert_eq!(res.status(), 401);
    }

    #[tokio::test]
    async fn logout() {
        let ctx = InMemoryContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        };

        let session = ctx
            .user_db()
            .write()
            .await
            .login(credentials)
            .await
            .unwrap();

        let res = warp::test::request()
            .method("POST")
            .path("/logout")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&logout_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 200);
        assert_eq!(res.body(), "");
    }

    #[tokio::test]
    async fn logout_missing_header() {
        let ctx = InMemoryContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/logout")
            .reply(&logout_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 401);
        assert_eq!(
            res.body(),
            &json!({"error":"MissingAuthorizationHeader","message":"MissingAuthorizationHeader"})
                .to_string()
        );
    }

    #[tokio::test]
    async fn logout_wrong_token() {
        let ctx = InMemoryContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/logout")
            .header(
                "Authorization",
                format!("Bearer {}", "6ecff667-258e-4108-9dc9-93cb8c64793c"),
            )
            .reply(&logout_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 401);
        assert_eq!(
            res.body(),
            &json!({"error":"SessionDoesNotExist","message":"SessionDoesNotExist"}).to_string()
        );
    }

    #[tokio::test]
    async fn logout_wrong_scheme() {
        let ctx = InMemoryContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/logout")
            .header("Authorization", "7e855f3c-b0cd-46d1-b5b3-19e6e3f9ea5")
            .reply(&logout_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 401);
        assert_eq!(
            res.body(),
            &json!({"error":"InvalidAuthorizationScheme","message":"InvalidAuthorizationScheme"})
                .to_string()
        );
    }

    #[tokio::test]
    async fn logout_invalid_token() {
        let ctx = InMemoryContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/logout")
            .header("Authorization", format!("Bearer {}", "no uuid"))
            .reply(&logout_handler(ctx).recover(handle_rejection))
            .await;
        assert_eq!(res.status(), 401);
        assert_eq!(
            res.body(),
            &json!({"error":"InvalidUuid","message":"InvalidUuid"}).to_string()
        );
    }

    #[tokio::test]
    async fn session() {
        let ctx = InMemoryContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        };

        let session = ctx
            .user_db()
            .write()
            .await
            .login(credentials)
            .await
            .unwrap();

        let res = warp::test::request()
            .method("GET")
            .path("/session")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&session_handler(ctx.clone()).recover(handle_rejection))
            .await;

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let session: Session = serde_json::from_str(&body).unwrap();

        ctx.user_db()
            .write()
            .await
            .logout(session.id)
            .await
            .unwrap();

        let res = warp::test::request()
            .method("GET")
            .path("/session")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&session_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 401);
    }

    #[tokio::test]
    async fn session_view_project() {
        let ctx = InMemoryContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        };

        let session = ctx
            .user_db()
            .write()
            .await
            .login(credentials)
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
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let res = warp::test::request()
            .method("POST")
            .path(&format!("/session/project/{}", project.to_string()))
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&session_project_handler(ctx.clone()).recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            ctx.user_db()
                .read()
                .await
                .session(session.id)
                .await
                .unwrap()
                .project,
            Some(project)
        );

        let rect =
            STRectangle::new_unchecked(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1);
        let res = warp::test::request()
            .method("POST")
            .header("Content-Length", "0")
            .path("/session/view")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .json(&rect)
            .reply(&session_view_handler(ctx.clone()).recover(handle_rejection))
            .await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            ctx.user_db()
                .read()
                .await
                .session(session.id)
                .await
                .unwrap()
                .view,
            Some(rect)
        );
    }

    #[tokio::test]
    async fn anonymous() {
        let ctx = InMemoryContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/anonymous")
            .reply(&anonymous_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let session = serde_json::from_str::<Session>(&body).unwrap();

        assert!(session.user.real_name.is_none());
        assert!(session.user.email.is_none());
    }
}
